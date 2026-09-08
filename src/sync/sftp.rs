//! SFTP mirrors compare size and mtime and replace stale files atomically.

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
use crate::sftp::stamp::FileStamp;
use crate::ui;

use super::report::confirm_bulk_delete;
use super::sftp_orphans::{self, matches_extension};

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
    let mut up_to_date = 0;
    let mut to_download = Vec::new();
    for remote in &listing.files {
        if !matches_extension(&remote.relative_path, &extensions) { continue; }
        let mut file = remote.clone();
        if output_dir.is_none() {
            file.relative_path = format!("{}/{}", target.folder_name(), file.relative_path);
        }
        let destination = Destination::beneath(&root, &file.relative_path)?;
        destination.prepare()?;
        let stamp = FileStamp { size: file.size, modified: file.modified };
        if destination.metadata()?.is_some_and(|metadata| stamp.matches_local(&metadata)) {
            up_to_date += 1;
        } else {
            to_download.push(file);
        }
    }
    let keep: HashSet<String> = listing.files.iter().map(|file| file.relative_path.clone()).collect();
    let orphans = if delete {
        sftp_orphans::collect(&root, &keep, &extensions, &cancel)?
    } else {
        Vec::new()
    };
    let download_count = to_download.len();
    eprintln!("  Up to date : {up_to_date}");
    eprintln!("  To download: {download_count}");
    eprintln!("  Skipped    : {} symlink(s) or special file(s)", listing.skipped);
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
        if !confirm_bulk_delete(orphans.len(), up_to_date + download_count + orphans.len()) {
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
