//! SFTP mirrors compare sizes, repair timestamps, and replace stale files
//! atomically. Nothing is transferred to correct metadata alone.

use anyhow::{Context, Result, ensure};
use std::collections::HashSet;
use std::path::Path;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::engine::ExistingPolicy;
use crate::safe_file;
use crate::sftp::{self, SftpOptions, SftpUrl, discard_state};
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
    busy: usize,
    missing: usize,
    replaced: usize,
    resized: usize,
    restamped: usize,
    reclaimed: u64,
    sample: Vec<String>,
}

impl Plan {
    fn stale(&mut self, reason: Difference, relative: &str, stamp: &FileStamp) {
        match reason {
            Difference::Missing => self.missing += 1,
            Difference::Replaced => self.replaced += 1,
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
        // The name is already on the line; what the reader does not know is
        // how much this run is about to pull down.
        Difference::Missing => format!("not here yet; {} to fetch", ui::format_size(stamp.size)),
        Difference::Replaced => String::from("changed locally while sync was inspecting it"),
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
                // A repair moves no payload bytes, so it happens here rather
                // than being queued as a transfer.
                Verdict::Retime { seconds } => destination.align_modified(&local, seconds)?,
                verdict => verdict,
            },
        };
        match verdict {
            // An interrupted run keeps its partial payload, and this file's
            // published copy already matches the server, so those bytes can
            // never be resumed. Reclaim them instead of leaving them to rot.
            Verdict::Current => {
                plan.current += 1;
                plan.reclaimed += discard_state(&destination)?;
            }
            Verdict::Retime { .. } => {
                plan.retimed += 1;
                plan.reclaimed += discard_state(&destination)?;
            }
            // Downloading or sweeping would only fight whoever owns it.
            Verdict::Busy => plan.busy += 1,
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
    if plan.busy > 0 {
        eprintln!("  In use     : {} (another transfer owns them; left untouched)", plan.busy);
    }
    if plan.reclaimed > 0 {
        eprintln!("  Reclaimed  : {} of abandoned partial data", ui::format_size(plan.reclaimed));
    }
    // Built from the nonzero reasons only, so it always adds up to the total
    // and never invites a reader to scan categories that do not apply.
    let breakdown: Vec<String> = [
        (plan.missing, "new"),
        (plan.resized, "different size"),
        (plan.restamped, "different timestamp"),
        (plan.replaced, "changed mid-scan"),
    ]
    .into_iter()
    .filter(|(count, _)| *count > 0)
    .map(|(count, label)| format!("{count} {label}"))
    .collect();
    if breakdown.is_empty() {
        eprintln!("  To download: 0");
    } else {
        eprintln!("  To download: {download_count} ({})", breakdown.join(", "));
    }
    eprintln!("  Skipped    : {} symlink(s) or special file(s)", listing.skipped);
    if !plan.sample.is_empty() {
        eprintln!("  Will download:");
        for line in &plan.sample {
            eprintln!("    + {line}");
        }
        // Five lines under a count of six reads as the whole list. Say so.
        let hidden = download_count.saturating_sub(plan.sample.len());
        if hidden > 0 {
            eprintln!("    … and {hidden} more");
        }
    }
    if delete { eprintln!("  To delete  : {}", orphans.len()); }
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
        let local_total =
            plan.current + plan.retimed + plan.busy + download_count + orphans.len();
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
