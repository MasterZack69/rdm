//! SFTP mirrors compare sizes, repair timestamps, and replace stale files
//! atomically. Nothing is transferred to correct metadata alone.

use anyhow::{Context, Result, ensure};
use std::collections::HashSet;
use std::path::Path;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::engine::ExistingPolicy;
use crate::safe_file;
use crate::sftp::{self, SftpOptions, SftpUrl};
use crate::sftp::batch::{Batch, download_files};
use crate::sftp::local::{Destination, STATE_DIR, lock_file};
use crate::sftp::stamp::{Compare, Difference, FileStamp, Verdict, modify_window};
use crate::ui;

use super::report::confirm_bulk_delete;
use super::sftp_orphans::{self, matches_extension};

/// How many stale files are explained before the transfers begin.
const SAMPLE: usize = 5;

/// What the scan decided, with enough detail to explain a surprising run.
#[derive(Default)]
struct Plan {
    current: usize,
    retimed: usize,
    missing: usize,
    resized: usize,
    restamped: usize,
    sample: Vec<String>,
}

impl Plan {
    fn stale(&mut self, reason: Difference, relative: &str, stamp: &FileStamp) {
        match reason {
            Difference::Missing | Difference::Replaced => self.missing += 1,
            Difference::Size { .. } => self.resized += 1,
            Difference::Modified { .. } => self.restamped += 1,
        }
        if self.sample.len() < SAMPLE {
            // A listing chooses these names, so they are only ever drawn safely.
            self.sample.push(format!(
                "{} - {}", ui::terminal_safe(relative), explain(reason, stamp),
            ));
        }
    }
}

/// The reason in the terms a user can check with `ls -l` and `stat`.
fn explain(reason: Difference, stamp: &FileStamp) -> String {
    match reason {
        Difference::Missing => String::from("not present locally"),
        Difference::Replaced => String::from("changed while sync was inspecting it"),
        Difference::Size { local } => format!("{local} bytes locally, {} remotely", stamp.size),
        Difference::Modified { local } => match (local, stamp.modified) {
            (Some(local), Some(remote)) => format!("mtime {local} locally, {remote} remotely"),
            (None, Some(remote)) => format!("no usable local mtime, {remote} remotely"),
            (Some(local), None) => format!("mtime {local} locally, none reported remotely"),
            (None, None) => String::from("no usable timestamps"),
        },
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    cfg: &Config,
    url: &str,
    requested_connections: Option<usize>,
    parallel: usize,
    delete: bool,
    extensions: Option<HashSet<String>>,
    allow_private: bool,
    output_dir: Option<String>,
    cancel: CancellationToken,
) -> Result<()> {
    ensure!(!delete || output_dir.is_some(), "SFTP sync --delete requires -o naming a dedicated mirror directory");
    let compare = Compare::from_env()?;
    let window = modify_window()?;
    let target = SftpUrl::parse(url)?;
    let options = SftpOptions::from_config(cfg)?;
    let listing = sftp::list(url, &options, allow_private, cancel.clone()).await?
        .context("SFTP sync requires a directory, not a single file")?;
    ensure!(!cancel.is_cancelled(), "SFTP sync cancelled during scanning");
    ensure!(!delete || listing.skipped == 0, "SFTP listing omitted symlinks or special files; refusing --delete");
    let root = std::path::absolute(output_dir.as_deref().unwrap_or(&cfg.download_dir))?;
    std::fs::create_dir_all(&root).context("Cannot create SFTP mirror root")?;
    if delete {
        // Only this user-selected root is canonicalised, never a path from
        // the listing. Also catches aliases such as /tmp/.. and home symlinks.
        let actual_root = std::fs::canonicalize(&root)?;
        ensure!(actual_root != Path::new("/"), "Refusing to mirror-delete the filesystem root");
        if let Some(home) = dirs::home_dir() {
            ensure!(actual_root != std::fs::canonicalize(home)?, "Refusing to mirror-delete the home directory");
        }
    }
    safe_file::create_dirs_beneath(&root, Path::new(STATE_DIR))?;
    let _mirror_lock = lock_file(&root, &Path::new(STATE_DIR).join("sync.lock"))?;
    if requested_connections.is_some_and(|count| count > 1) {
        eprintln!("  SFTP uses one stream per file; -p controls concurrent files.");
    }
    let mut plan = Plan::default();
    let mut keep = HashSet::new();
    let mut to_download = Vec::new();
    for remote in &listing.files {
        if !matches_extension(&remote.relative_path, &extensions) { continue; }
        let mut file = remote.clone();
        if output_dir.is_none() {
            file.relative_path = format!("{}/{}", target.folder_name(), file.relative_path);
        }
        // The same string the orphan sweep reads off disk, so a file this run
        // keeps can never be collected as an orphan of the same run.
        keep.insert(file.relative_path.clone());
        let destination = Destination::beneath(&root, &file.relative_path)?;
        destination.prepare()?;
        let stamp = FileStamp { size: file.size, modified: file.modified };
        let verdict = match destination.metadata()? {
            None => Verdict::Stale(Difference::Missing),
            Some(local) => match stamp.compare_local(&local, compare, window) {
                // A repair writes no payload bytes, so it happens here rather
                // than being queued as a transfer.
                Verdict::Retime { seconds } => {
                    if destination.align_modified(&local, seconds)? {
                        Verdict::Retime { seconds }
                    } else {
                        Verdict::Stale(Difference::Replaced)
                    }
                }
                verdict => verdict,
            },
        };
        match verdict {
            Verdict::Current => plan.current += 1,
            Verdict::Retime { .. } => plan.retimed += 1,
            Verdict::Stale(reason) => {
                plan.stale(reason, &file.relative_path, &stamp);
                to_download.push(file);
            }
        }
    }
    to_download.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let orphans = if delete {
        sftp_orphans::collect(&root, &keep, &extensions, &cancel)?
    } else {
        Vec::new()
    };
    let download_count = to_download.len();
    eprintln!("  Up to date : {}", plan.current);
    if plan.retimed > 0 {
        eprintln!("  Retimed    : {} (same size; timestamp taken from the server)", plan.retimed);
    }
    eprintln!(
        "  To download: {download_count} ({} missing, {} resized, {} restamped)",
        plan.missing, plan.resized, plan.restamped,
    );
    eprintln!("  Skipped    : {} symlink(s) or special file(s)", listing.skipped);
    if delete { eprintln!("  To delete  : {}", orphans.len()); }
    for line in &plan.sample {
        eprintln!("    + {line}");
    }
    // This does not touch the persistent queue or run unrelated queue items.
    // Every error is propagated before the deletion phase can be reached.
    download_files(to_download, options.clone(), Batch {
        root: root.clone(),
        parallel,
        allow_private,
        policy: ExistingPolicy::Overwrite,
        quiet: false,
    }, cancel.clone()).await?;
    ensure!(!cancel.is_cancelled(), "SFTP sync cancelled; no orphan deletion performed");
    if !orphans.is_empty() {
        // A listing is not a server-side snapshot. Re-read it immediately
        // before deleting so a tree that changed during download is refused.
        let refreshed = sftp::list(url, &options, allow_private, cancel.clone()).await?
            .context("SFTP source stopped being a directory; refusing deletion")?;
        ensure!(refreshed == listing, "SFTP tree changed during sync; no orphan deletion performed");
        let local_total = plan.current + plan.retimed + download_count + orphans.len();
        if !confirm_bulk_delete(orphans.len(), local_total) {
            return Ok(());
        }
        for orphan in &orphans {
            ensure!(!cancel.is_cancelled(), "SFTP sync cancelled during deletion");
            let label = crate::sftp::local::path_text(&orphan.relative)?;
            sftp_orphans::remove(&root, orphan).with_context(|| {
                format!("Cannot remove orphan {}", ui::terminal_safe(label))
            })?;
        }
    }
    eprintln!("  SFTP sync complete.");
    Ok(())
}
