//! The `rdm sync` entry point, and the generic HTTP mirror.
//!
//! The share paths are checked first and return early, so everything after
//! them is the HTTP case: the only one that has to scrape a listing, and ask
//! per file whether the local copy is still current.

use anyhow::{Context, Result};
use reqwest::header::CONTENT_LENGTH;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::engine;
use crate::hoster::{gdrive, onedrive, pixeldrain};
use crate::mega;
use crate::queue;
use crate::scrape;
use crate::ui;

use super::orphans::{collect_orphan_files, remove_empty_dirs};
use super::paths::{extract_filename, file_has_ext, local_path};
use super::report::{confirm_bulk_delete, print_sample};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    cfg: &Config,
    url: &str,
    requested_connections: Option<usize>,
    parallel: usize,
    delete: bool,
    ext_filter: Option<HashSet<String>>,
    allow_private: bool,
    output_dir: Option<String>,
    cancel: CancellationToken,
) -> Result<()> {
    // MEGA first, before the queue guard: the MEGA path does not use the queue
    // at all, so a queue full of unrelated pending items is no reason to
    // refuse.
    if mega::is_mega_url(url) {
        if !mega::folder::is_folder_link(url) {
            anyhow::bail!(
                "`rdm sync` mirrors a folder \u{2014} for a single MEGA file use `rdm <link>`"
            );
        }

        // `-c` means "connections to this file", which on MEGA is its worker
        // count, so it falls back to mega_workers rather than the generic
        // default.
        let workers = requested_connections.unwrap_or(cfg.mega_workers);
        return super::mega::run_mega(
            cfg,
            url,
            workers,
            delete,
            output_dir.as_deref(),
            ext_filter,
            cancel,
        )
        .await;
    }

    // Also before the queue guard, and for the same reason: this path does not
    // use the queue either. `-c` is files-at-once here, so it falls back to
    // onedrive_workers rather than the generic default.
    if onedrive::is_onedrive_url(url) {
        let workers = requested_connections.unwrap_or(cfg.onedrive_workers);
        return super::onedrive::run_onedrive(
            cfg,
            url,
            workers,
            delete,
            output_dir.as_deref(),
            ext_filter,
            cancel,
        )
        .await;
    }

    // Same again, and for the same reason: this path does not use the queue,
    // and `-c` is files-at-once.
    if gdrive::is_gdrive_url(url) {
        let workers = requested_connections.unwrap_or(cfg.gdrive_workers);
        return super::gdrive::run_gdrive(
            cfg,
            url,
            workers,
            delete,
            output_dir.as_deref(),
            ext_filter,
            cancel,
        )
        .await;
    }

    // Third and last of the hosts that skip the queue entirely. `-c` is
    // files-at-once here too, so it falls back to pixeldrain_workers.
    if pixeldrain::is_pixeldrain_url(url) {
        let workers = requested_connections.unwrap_or(cfg.pixeldrain_workers);
        return super::pixeldrain::run_pixeldrain(
            cfg,
            url,
            workers,
            delete,
            output_dir.as_deref(),
            ext_filter,
            cancel,
        )
        .await;
    }

    let connections = requested_connections.unwrap_or(cfg.connections);

    {
        let q = queue::Queue::load_readonly();
        if q.pending_count() > 0 {
            anyhow::bail!(
                "Queue has {} pending item(s).\n  \
                 Run 'rdm queue start' to finish them, or 'rdm queue clear' to reset.",
                q.pending_count()
            );
        }
    }

    let files = scrape::discover_files(url, false, allow_private)
        .await
        .context("Failed to scan remote directory")?;

    let files = match files {
        Some(f) if !f.is_empty() => f,
        _ => {
            eprintln!("  \u{274c} No files found at {}", url);
            return Ok(());
        }
    };

    if cancel.is_cancelled() {
        eprintln!("  \u{26a0} Cancelled during scan.");
        return Ok(());
    }

    let total_before_filter = files.len();

    let files: Vec<scrape::DiscoveredFile> = match &ext_filter {
        Some(exts) => files
            .into_iter()
            .filter(|f| {
                let name = extract_filename(&f.relative_path).to_lowercase();
                file_has_ext(&name, exts)
            })
            .collect(),
        None => files,
    };

    if let Some(exts) = ext_filter.as_ref() {
        let mut sorted: Vec<&str> = exts.iter().map(|s| s.as_str()).collect();
        sorted.sort();
        eprintln!(
            "  \u{1f50e} Filter: {} → {} file(s) matching .{}",
            total_before_filter,
            files.len(),
            sorted.join(", ."),
        );
    }

    if files.is_empty() {
        eprintln!("  \u{274c} No files match the extension filter");
        return Ok(());
    }

    let remote_decoded: HashSet<String> = files
        .iter()
        .map(|f| engine::percent_decode(&f.relative_path))
        .collect();

    let sync_root_result = derive_sync_root(cfg, &files);

    if delete {
        match &sync_root_result {
            SyncRoot::MixedRoots => {
                eprintln!("  \u{26a0} Cannot use --delete: files have mixed folder roots.");
                eprintln!("    Delete must be performed manually.");
            }
            SyncRoot::Empty => {
                eprintln!("  \u{26a0} Cannot use --delete: unable to determine sync root.");
            }
            SyncRoot::Ok(_) => {}
        }
    }

    let mut needs_head: Vec<(String, String, PathBuf, u64)> = Vec::new();
    let mut to_download: Vec<(String, String)> = Vec::new();

    for f in &files {
        let path = local_path(cfg, &f.relative_path);
        match std::fs::metadata(&path) {
            Ok(m) if m.is_file() && m.len() > 0 => {
                needs_head.push((f.url.clone(), f.relative_path.clone(), path, m.len()));
            }
            _ => {
                to_download.push((f.url.clone(), f.relative_path.clone()));
            }
        }
    }

    let mut up_to_date = 0u64;

    if !needs_head.is_empty() {
        let client = reqwest::Client::builder()
            .user_agent("rdm")
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .context("Failed to build HTTP client")?;

        let sem = Arc::new(Semaphore::new(8));
        let mut tasks = JoinSet::new();

        // Capture the total before needs_head is moved into the spawn loop.
        let progress = ui::CountProgress::new("Verifying local files", needs_head.len());

        for (file_url, relative, path, size) in needs_head {
            let client = client.clone();
            let sem = sem.clone();
            let cancel = cancel.clone();

            tasks.spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                if cancel.is_cancelled() {
                    return None;
                }
                let status = head_compare(&client, &file_url, size).await;
                Some((file_url, relative, path, status))
            });
        }

        while let Some(joined) = tasks.join_next().await {
            if cancel.is_cancelled() {
                tasks.abort_all();
                progress.finish("cancelled");
                eprintln!("  \u{26a0} Cancelled during verification.");
                return Ok(());
            }
            let (file_url, relative, _path, status) = match joined.context("Task panicked")? {
                Some(v) => v,
                None => continue,
            };
            progress.tick();
            match status {
                HeadStatus::UpToDate | HeadStatus::HeadFailed => {
                    up_to_date += 1;
                }
                HeadStatus::SizeMismatch | HeadStatus::NoContentLength => {
                    to_download.push((file_url, relative));
                }
            }
        }

        progress.finish(&format!("{} already up to date", up_to_date));
    }

    to_download.sort_by(|a, b| a.1.cmp(&b.1));

    let mut to_delete: Vec<String> = Vec::new();
    if delete && let SyncRoot::Ok(ref root) = sync_root_result {
        let root_path = Path::new(root);
        if root_path.is_dir() {
            collect_orphan_files(
                root_path,
                root_path,
                &remote_decoded,
                &ext_filter,
                &mut to_delete,
            );
            to_delete.sort();
        }
    }

    eprintln!();
    eprintln!("  Remote     : {} file(s)", files.len());
    eprintln!("  Up to date : {}", up_to_date);
    eprintln!("  To download: {}", to_download.len());
    if delete {
        eprintln!("  To delete  : {}", to_delete.len());
    }

    if to_download.is_empty() && to_delete.is_empty() {
        eprintln!();
        eprintln!("  \u{2705} Everything is up to date!");
        return Ok(());
    }

    if !to_download.is_empty() {
        eprintln!();
        print_sample(
            "+",
            to_download
                .iter()
                .map(|(_, relative)| engine::percent_decode(relative)),
            to_download.len(),
        );
    }

    if !to_delete.is_empty() {
        eprintln!();
        print_sample("-", to_delete.iter().cloned(), to_delete.len());
    }

    eprintln!();

    if !to_download.is_empty() {
        for (_, relative) in &to_download {
            let path = local_path(cfg, relative);
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(format!("{}.part", path.display()));
            let meta = crate::resume::ResumeMetadata::meta_path(&path.to_string_lossy());
            let _ = std::fs::remove_file(&meta);
        }

        for (_, relative) in &to_download {
            let path = local_path(cfg, relative);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
        }

        queue::Queue::locked(|q| {
            for (file_url, relative) in &to_download {
                let decoded = engine::percent_decode(relative);
                q.add(file_url.clone(), Some(decoded), Some(connections));
            }
            Ok(())
        })?;

        let result = queue::start(cfg, cancel.clone(), parallel).await;
        let _ = queue::Queue::locked(|q| Ok(q.clear_finished()));

        if let Err(e) = result {
            eprintln!("  \u{26a0} Some downloads failed — skipping delete phase.");
            return Err(e);
        }

        let q = queue::Queue::load_readonly();
        if q.failed_count() > 0 {
            eprintln!(
                "  \u{26a0} {} download(s) failed — skipping delete phase.",
                q.failed_count()
            );
            return Ok(());
        }
    }

    if !to_delete.is_empty() {
        if cancel.is_cancelled() {
            eprintln!("  \u{26a0} Cancelled before delete phase.");
            return Ok(());
        }

        let total_local = up_to_date as usize + to_delete.len();
        if !confirm_bulk_delete(to_delete.len(), total_local) {
            return Ok(());
        }

        let mut deleted = 0u64;
        let mut delete_failed = 0u64;

        if let SyncRoot::Ok(ref root) = sync_root_result {
            let progress = ui::CountProgress::new("Deleting orphans", to_delete.len());

            for relative in &to_delete {
                let full_path = Path::new(root).join(relative);
                match std::fs::remove_file(&full_path) {
                    Ok(_) => {
                        deleted += 1;
                        let _ = std::fs::remove_file(format!("{}.part", full_path.display()));
                        let meta =
                            crate::resume::ResumeMetadata::meta_path(&full_path.to_string_lossy());
                        let _ = std::fs::remove_file(&meta);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        delete_failed += 1;
                        progress.note(&format!("  \u{26a0} Failed to delete {}: {}", relative, e));
                    }
                }
                progress.tick();
            }

            progress.finish(&format!("{} file(s) deleted", deleted));
            remove_empty_dirs(Path::new(root));
        }

        if delete_failed > 0 {
            eprintln!("  \u{26a0} Failed to delete {} file(s)", delete_failed);
        }
    }

    eprintln!();
    eprintln!("  \u{2705} Sync complete!");
    Ok(())
}

enum HeadStatus {
    UpToDate,
    SizeMismatch,
    HeadFailed,
    NoContentLength,
}

enum SyncRoot {
    Ok(String),
    Empty,
    MixedRoots,
}

async fn head_compare(client: &reqwest::Client, url: &str, local_size: u64) -> HeadStatus {
    let resp = match client.head(url).send().await {
        Ok(r) if r.status().is_success() => r,
        _ => return HeadStatus::HeadFailed,
    };
    let remote_size = resp
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    match remote_size {
        Some(rs) if rs == local_size => HeadStatus::UpToDate,
        Some(_) => HeadStatus::SizeMismatch,
        None => HeadStatus::NoContentLength,
    }
}

fn derive_sync_root(cfg: &Config, files: &[scrape::DiscoveredFile]) -> SyncRoot {
    let first = match files.first() {
        Some(f) => f,
        None => return SyncRoot::Empty,
    };
    let prefix = match first.relative_path.split('/').next() {
        Some(p) if !p.is_empty() => p,
        _ => return SyncRoot::Empty,
    };
    if files
        .iter()
        .any(|f| f.relative_path.split('/').next() != Some(prefix))
    {
        return SyncRoot::MixedRoots;
    }
    let decoded_prefix = engine::percent_decode(prefix);
    SyncRoot::Ok(cfg.resolve_output_path(&decoded_prefix))
}
