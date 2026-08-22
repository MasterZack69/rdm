//! `rdm sync` — mirror a remote directory listing into a local folder.
//!
//! Scanning, verification, and deletion all report through [`crate::ui`], and
//! the actual downloading is handed to [`crate::queue`], so a sync shows the
//! same live per-file board as `rdm queue start`.
//!
//! MEGA and OneDrive folder shares take separate paths through [`run`]. Almost
//! none of the machinery here applies to them: there is no HTML listing to
//! scrape, no `HEAD` request to compare sizes with, and no per-file URL that
//! could become a queue item. What they do give is a node tree with exact sizes
//! in it, which makes the whole verification phase unnecessary.

use anyhow::{Context, Result};
use reqwest::header::CONTENT_LENGTH;
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::engine;
use crate::hoster::{gdrive, onedrive};
use crate::mega;
use crate::queue;
use crate::scrape;
use crate::ui;
use crate::ui::ProgressSink;

/// How many paths to show before collapsing the rest into a count.
const SAMPLE: usize = 20;

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
        return run_mega(
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
        return run_onedrive(
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
        return run_gdrive(
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
    if delete
        && let SyncRoot::Ok(ref root) = sync_root_result
    {
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

// -- MEGA ------------------------------------------------------------

/// Mirrors a MEGA folder share.
///
/// Three things differ from the HTTP path, all of them because a share is a
/// decrypted node tree rather than a scraped page:
///
/// * **No verification phase.** The tree states every file's exact size, so
///   "is my copy current?" is a `stat` call. The HTTP path needs a `HEAD` per
///   file to learn the same thing.
/// * **Sequential downloads.** `parallel` is not honoured. Each file already
///   spreads across `workers` slots that share one per-IP quota, so running
///   whole files concurrently earns 509s rather than throughput.
/// * **`--delete` needs `-o`, and a fully readable share.** See below.
async fn run_mega(
    cfg: &Config,
    url: &str,
    workers: usize,
    delete: bool,
    explicit_dir: Option<&str>,
    ext_filter: Option<HashSet<String>>,
    cancel: CancellationToken,
) -> Result<()> {
    let client = reqwest::Client::new();
    let link = mega::folder::parse_folder_link(url)?;

    let listing = mega::folder::list_folder(&client, &link)
        .await
        .context("Failed to list the MEGA folder")?;

    // Same node-selection rule as a plain folder download: a link pointing at
    // one node mirrors that node, not the whole share.
    let mut entries: Vec<mega::folder::Entry> = match link.node.as_deref() {
        Some(node) => listing
            .entries
            .iter()
            .filter(|e| e.handle == node || e.ancestors.iter().any(|a| a == node))
            .cloned()
            .collect(),
        None => listing.entries.clone(),
    };

    if entries.is_empty() {
        eprintln!("  \u{274c} No files found in that MEGA folder");
        return Ok(());
    }

    let total_before_filter = entries.len();

    if let Some(exts) = ext_filter.as_ref() {
        entries.retain(|e| {
            let name = e.path.last().map(|n| n.to_lowercase()).unwrap_or_default();
            file_has_ext(&name, exts)
        });

        let mut sorted: Vec<&str> = exts.iter().map(|s| s.as_str()).collect();
        sorted.sort();
        eprintln!(
            "  \u{1f50e} Filter: {} → {} file(s) matching .{}",
            total_before_filter,
            entries.len(),
            sorted.join(", ."),
        );
    }

    if entries.is_empty() {
        eprintln!("  \u{274c} No files match the extension filter");
        return Ok(());
    }

    if cancel.is_cancelled() {
        eprintln!("  \u{26a0} Cancelled during scan.");
        return Ok(());
    }

    // main.rs has already folded `-o` into download_dir; explicit_dir only
    // says whether the user chose it.
    let base = PathBuf::from(&cfg.download_dir);

    // rsync semantics: `sync <share> -o ~/Music` where the share's one folder
    // is also called Music mirrors into ~/Music, not ~/Music/Music. This has
    // to happen before anything is compared against the disk, because it
    // changes every path that follows — including the ones printed below, so
    // the plan shows where files actually go.
    let collapsed = mega::folder::collapse_shared_root(&base, &mut entries);

    // Exact sizes come free with the listing, so no HEAD phase.
    let mut up_to_date = 0u64;
    let mut to_download: Vec<mega::folder::Entry> = Vec::new();

    for entry in &entries {
        let path = join_relative(&base, &entry.path);
        match std::fs::metadata(&path) {
            Ok(m) if m.is_file() && m.len() == entry.size => up_to_date += 1,
            _ => to_download.push(entry.clone()),
        }
    }

    let mut to_delete: Vec<String> = Vec::new();
    if delete {
        match explicit_dir {
            // A node this key cannot open has no readable name, so nothing
            // here can match it against a file on disk — which makes "the key
            // stopped resolving" indistinguishable from "it was removed from
            // the share". One of those means the local file is an orphan; the
            // other means it is a complete, MAC-verified file we would be
            // destroying. Refusing is the only safe reading.
            Some(_) if !listing.undecryptable.is_empty() => {
                eprintln!(
                    "  \u{26a0} Skipping --delete: {} node(s) in this share cannot be",
                    listing.undecryptable.len()
                );
                eprintln!("    decrypted with this key, so their local copies cannot be told");
                eprintln!("    apart from orphans. Nothing will be deleted this run.");
            }
            Some(_) => {
                let keep: HashSet<String> = entries.iter().map(|e| e.display_path()).collect();
                if base.is_dir() {
                    collect_listing_orphans(&base, &base, &keep, &ext_filter, &mut to_delete);
                    to_delete.sort();
                }
            }
            // A share has no directory of its own — it unpacks straight into
            // the destination. Without an explicit -o that destination is the
            // general download directory, where every unrelated file would
            // look like an orphan. Declining is the only safe reading.
            None => {
                eprintln!("  \u{26a0} Cannot use --delete without -o.");
                eprintln!(
                    "    A MEGA share unpacks straight into {}, so every unrelated",
                    base.display()
                );
                eprintln!("    file there would count as an orphan. Point -o at a folder");
                eprintln!("    used only for this share.");
            }
        }
    }

    eprintln!();
    eprintln!("  Remote     : {} file(s)", entries.len());
    eprintln!("  Into       : {}", base.display());
    if let Some(folder) = collapsed.as_deref() {
        eprintln!("               (already the share's '{folder}' folder, so its");
        eprintln!("                contents mirror straight into it)");
    }
    eprintln!("  Up to date : {}", up_to_date);
    eprintln!("  To download: {}", to_download.len());
    if delete {
        eprintln!("  To delete  : {}", to_delete.len());
    }

    // Unreadable nodes are stated whether or not --delete was asked for: a
    // "complete" mirror that is quietly missing files is worse than a noisy
    // one.
    if !listing.undecryptable.is_empty() {
        eprintln!(
            "  Unreadable : {} node(s) this share key cannot open",
            listing.undecryptable.len()
        );
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
            to_download.iter().map(|e| e.display_path()),
            to_download.len(),
        );
    }

    if !to_delete.is_empty() {
        eprintln!();
        print_sample("-", to_delete.iter().cloned(), to_delete.len());
    }

    eprintln!();

    // Anything reaching this point is missing or the wrong size, so a stale
    // local copy is meant to be replaced rather than resumed onto.
    let options = mega::MegaOptions {
        workers,
        verify_mac: cfg.mega_verify_mac,
        resume_on_ip_change: cfg.mega_resume_on_ip_change,
        max_retries: cfg.max_retries,
        overwrite: true,
    };

    let mut downloaded = 0u64;
    let mut bytes = 0u64;
    let mut failed: Vec<(String, String)> = Vec::new();
    let mut cancelled = false;

    for entry in &to_download {
        if cancel.is_cancelled() {
            cancelled = true;
            break;
        }

        let relative = entry.display_path();
        let destination = join_relative(&base, &entry.path);
        let sink: Arc<dyn ProgressSink> = ui::SoloBar::new(&relative);

        let outcome = mega::run_download(
            client.clone(),
            &entry.handle,
            Some(link.handle.clone()),
            &entry.key,
            Some(destination.to_string_lossy().into_owned()),
            &cfg.download_dir,
            options.clone(),
            cancel.clone(),
            sink,
        )
        .await;

        match outcome {
            Ok(mega::MegaOutcome::Completed { bytes: n, .. }) => {
                downloaded += 1;
                bytes += n;
            }
            Ok(mega::MegaOutcome::AlreadyPresent { .. }) => up_to_date += 1,
            Ok(mega::MegaOutcome::Cancelled { .. }) => {
                cancelled = true;
                break;
            }
            // One unreadable node should not cost the rest of the mirror.
            Err(e) => failed.push((relative, e.to_string())),
        }
    }

    if downloaded > 0 {
        eprintln!();
        eprintln!(
            "  {} file(s) downloaded, {}",
            downloaded,
            ui::format_size(bytes)
        );
    }

    if !failed.is_empty() {
        eprintln!();
        eprintln!("  \u{26a0} {} file(s) failed:", failed.len());
        for (path, reason) in &failed {
            eprintln!("     - {path}: {reason}");
        }
    }

    if cancelled {
        eprintln!();
        eprintln!("  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off.");
        return Ok(());
    }

    // Deleting after a partial mirror could remove files the failed downloads
    // were meant to replace.
    if !failed.is_empty() {
        eprintln!();
        eprintln!("  \u{26a0} Skipping delete phase after failures.");
        return Ok(());
    }

    if !to_delete.is_empty() {
        let total_local = up_to_date as usize + downloaded as usize + to_delete.len();
        if !confirm_bulk_delete(to_delete.len(), total_local) {
            return Ok(());
        }

        let progress = ui::CountProgress::new("Deleting orphans", to_delete.len());
        let mut deleted = 0u64;
        let mut delete_failed = 0u64;

        for relative in &to_delete {
            let full_path = base.join(relative);
            match std::fs::remove_file(&full_path) {
                Ok(_) => deleted += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    delete_failed += 1;
                    progress.note(&format!("  \u{26a0} Failed to delete {}: {}", relative, e));
                }
            }
            progress.tick();
        }

        progress.finish(&format!("{} file(s) deleted", deleted));
        remove_empty_dirs(&base);

        if delete_failed > 0 {
            eprintln!("  \u{26a0} Failed to delete {} file(s)", delete_failed);
        }
    }

    eprintln!();
    eprintln!("  \u{2705} Sync complete!");
    Ok(())
}

// -- OneDrive --------------------------------------------------------

/// Mirrors a OneDrive folder share.
///
/// Closer to the MEGA path than the HTTP one, for the same reason: no listing
/// page to scrape, and no stable per-file URL to `HEAD` — the address the API
/// hands back is signed and was minted seconds ago. What it does give is a size
/// per child, so "is my copy current?" is a `stat` call and the verification
/// phase disappears.
///
/// Unlike a MEGA share, a folder share has a name of its own, so the mirror
/// gets its own directory and `--delete` does not need `-o` to be safe.
async fn run_onedrive(
    cfg: &Config,
    url: &str,
    workers: usize,
    delete: bool,
    explicit_dir: Option<&str>,
    ext_filter: Option<HashSet<String>>,
    cancel: CancellationToken,
) -> Result<()> {
    let options = onedrive::OneDriveOptions {
        workers,
        max_retries: cfg.max_retries,
        // Nothing reaching the download phase has a good local copy, and the
        // stale ones are removed below rather than resumed onto — which keeps
        // any .part beside them alive.
        overwrite: false,
    };

    let folder = match onedrive::resolve(reqwest::Client::new(), url, &options).await? {
        onedrive::Resolved::Folder(folder) => folder,
        onedrive::Resolved::File(_) => anyhow::bail!(
            "`rdm sync` mirrors a folder \u{2014} for a single OneDrive file use `rdm <link>`"
        ),
    };

    let listing = onedrive::list_folder(&folder, &options)
        .await
        .context("Failed to list the OneDrive folder")?;

    let mut files = listing.files;
    let total_before_filter = files.len();

    if let Some(exts) = ext_filter.as_ref() {
        files.retain(|file| file_has_ext(&file.name, exts));

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
        eprintln!("  \u{274c} No files found in that OneDrive folder");
        return Ok(());
    }

    if cancel.is_cancelled() {
        eprintln!("  \u{26a0} Cancelled during scan.");
        return Ok(());
    }

    let base = onedrive::destination_root(
        explicit_dir.map(|dir| dir.to_owned()),
        &cfg.download_dir,
        folder.name(),
    );

    let mut to_delete: Vec<String> = Vec::new();
    if delete {
        if listing.skipped > 0 {
            // The same hazard as an undecryptable MEGA node: a child with
            // nothing to fetch never reaches `keep`, and an upload still in
            // progress then looks exactly like a file the share dropped. One
            // of those readings deletes a local copy that is still wanted.
            eprintln!(
                "  \u{26a0} Skipping --delete: {} item(s) in this share are neither a",
                listing.skipped
            );
            eprintln!("    file nor a folder, so their local copies cannot be told apart");
            eprintln!("    from orphans. Nothing will be deleted this run.");
        } else if base.is_dir() {
            let keep: HashSet<String> = files.iter().map(|f| relative_key(&f.relative)).collect();
            collect_listing_orphans(&base, &base, &keep, &ext_filter, &mut to_delete);
            to_delete.sort();
        }
    }

    let total_remote = files.len();
    let mut up_to_date = 0u64;
    let mut unverified = 0u64;
    let mut to_download: Vec<onedrive::RemoteFile> = Vec::new();

    for file in files {
        let path = base.join(&file.relative);
        match (file.size, std::fs::metadata(&path)) {
            // Exact sizes come free with the listing, so this is the whole
            // verification phase.
            (Some(size), Ok(meta)) if meta.is_file() && meta.len() == size => up_to_date += 1,
            // A wrong-sized copy is stale. Removing it here rather than asking
            // the engine to overwrite keeps any .part and .rdm beside it, so an
            // interrupted mirror still resumes instead of starting over.
            (Some(_), Ok(meta)) if meta.is_file() => {
                let _ = std::fs::remove_file(&path);
                to_download.push(file);
            }
            // No size to compare against. The HTTP path makes the same call
            // when a HEAD fails: leave the file alone and say how many.
            (None, Ok(meta)) if meta.is_file() && meta.len() > 0 => {
                unverified += 1;
                up_to_date += 1;
            }
            _ => to_download.push(file),
        }
    }

    eprintln!();
    eprintln!("  Remote     : {} file(s)", total_remote);
    eprintln!("  Into       : {}", base.display());
    eprintln!("  Up to date : {}", up_to_date);
    if unverified > 0 {
        eprintln!("  Unverified : {} file(s) the listing gave no size for", unverified);
    }
    eprintln!("  To download: {}", to_download.len());
    if delete {
        eprintln!("  To delete  : {}", to_delete.len());
    }
    // Stated whether or not --delete was asked for: a "complete" mirror that is
    // quietly missing files is worse than a noisy one.
    if listing.skipped > 0 {
        eprintln!("  Skipped    : {} item(s) with nothing to fetch", listing.skipped);
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
            to_download.iter().map(|f| relative_key(&f.relative)),
            to_download.len(),
        );
    }

    if !to_delete.is_empty() {
        eprintln!();
        print_sample("-", to_delete.iter().cloned(), to_delete.len());
    }

    eprintln!();

    let mut downloaded = 0u64;

    if !to_download.is_empty() {
        onedrive::create_tree(&base, &listing.dirs)
            .await
            .context("Failed to create the mirror's directories")?;

        let done = onedrive::download_files(
            &to_download,
            &base,
            &options,
            cancel.clone(),
            onedrive::Progress::Board,
        )
        .await?;

        downloaded = done.completed as u64;
        up_to_date += done.skipped as u64;

        if downloaded > 0 {
            eprintln!();
            eprintln!(
                "  {} file(s) downloaded, {}",
                downloaded,
                ui::format_size(done.bytes)
            );
        }

        if done.cancelled {
            eprintln!();
            eprintln!(
                "  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off."
            );
            return Ok(());
        }

        // Deleting after a partial mirror could remove files the failed
        // downloads were meant to replace.
        if !done.failed.is_empty() {
            eprintln!();
            eprintln!("  \u{26a0} {} file(s) failed:", done.failed.len());
            for (path, reason) in &done.failed {
                eprintln!("     - {path}: {reason}");
            }
            eprintln!();
            eprintln!("  \u{26a0} Skipping delete phase after failures.");
            return Ok(());
        }
    }

    if !to_delete.is_empty() {
        if cancel.is_cancelled() {
            eprintln!("  \u{26a0} Cancelled before delete phase.");
            return Ok(());
        }

        let total_local = up_to_date as usize + downloaded as usize + to_delete.len();
        if !confirm_bulk_delete(to_delete.len(), total_local) {
            return Ok(());
        }

        let progress = ui::CountProgress::new("Deleting orphans", to_delete.len());
        let mut deleted = 0u64;
        let mut delete_failed = 0u64;

        for relative in &to_delete {
            let full_path = base.join(relative);
            match std::fs::remove_file(&full_path) {
                Ok(_) => {
                    deleted += 1;
                    let _ = std::fs::remove_file(format!("{}.part", full_path.display()));
                    let meta = crate::resume::ResumeMetadata::meta_path(&full_path.to_string_lossy());
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
        remove_empty_dirs(&base);

        if delete_failed > 0 {
            eprintln!("  \u{26a0} Failed to delete {} file(s)", delete_failed);
        }
    }

    eprintln!();
    eprintln!("  \u{2705} Sync complete!");
    Ok(())
}

// -- Google Drive ----------------------------------------------------

/// Mirrors a Google Drive folder.
///
/// The same shape as the OneDrive path, with one difference that changes what
/// a mirror can promise: only the API states a file's size, and never for a
/// Google document, so a copy with no size to compare against is left alone
/// and counted rather than re-fetched.
///
/// `--delete` needs a key. A keyless listing comes off a page that renders one
/// batch and does not say what it left out, so a file it never rendered is
/// indistinguishable from one the folder dropped — and deleting on that
/// reading destroys files that are still shared.
async fn run_gdrive(
    cfg: &Config,
    url: &str,
    workers: usize,
    delete: bool,
    explicit_dir: Option<&str>,
    ext_filter: Option<HashSet<String>>,
    cancel: CancellationToken,
) -> Result<()> {
    let key = cfg.gdrive_key();
    let options = gdrive::GdriveOptions {
        workers,
        max_retries: cfg.max_retries,
        api_key: key.clone(),
        doc_format: cfg.gdrive_doc_format.clone(),
        // Nothing reaching the download phase has a good local copy, and the
        // stale ones are removed below rather than resumed onto — which keeps
        // any .part beside them alive.
        overwrite: false,
    };

    let folder = match gdrive::resolve(reqwest::Client::new(), url, &options).await? {
        gdrive::Resolved::Folder(folder) => folder,
        gdrive::Resolved::File(_) => anyhow::bail!(
            "`rdm sync` mirrors a folder \u{2014} for a single Drive file use `rdm <link>`"
        ),
    };

    let listing = gdrive::list_folder(&folder, &options)
        .await
        .context("Failed to list the Google Drive folder")?;

    let mut files = listing.files;
    let total_before_filter = files.len();

    if let Some(exts) = ext_filter.as_ref() {
        files.retain(|file| file_has_ext(&file.name, exts));

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
        eprintln!("  \u{274c} No files found in that Google Drive folder");
        return Ok(());
    }

    if cancel.is_cancelled() {
        eprintln!("  \u{26a0} Cancelled during scan.");
        return Ok(());
    }

    let base = gdrive::destination_root(
        explicit_dir.map(|dir| dir.to_owned()),
        &cfg.download_dir,
        folder.name(),
    );

    let mut to_delete: Vec<String> = Vec::new();
    if delete {
        if key.is_none() {
            eprintln!("  \u{26a0} Skipping --delete: without an API key this listing comes off a");
            eprintln!("    page that stops at a cap, so a file it did not render cannot be");
            eprintln!("    told apart from one the folder dropped. Set gdrive_api_key, or");
            eprintln!("    RDM_GDRIVE_API_KEY, to mirror with deletes.");
        } else if listing.unsupported > 0 {
            // The same hazard as an undecryptable MEGA node: a child with
            // nothing to fetch never reaches `keep`, so its local copy looks
            // exactly like a file the folder dropped.
            eprintln!(
                "  \u{26a0} Skipping --delete: {} item(s) in this folder have nothing to",
                listing.unsupported
            );
            eprintln!("    fetch, so their local copies cannot be told apart from orphans.");
            eprintln!("    Nothing will be deleted this run.");
        } else if base.is_dir() {
            let keep: HashSet<String> = files.iter().map(|f| relative_key(&f.relative)).collect();
            collect_listing_orphans(&base, &base, &keep, &ext_filter, &mut to_delete);
            to_delete.sort();
        }
    }

    let total_remote = files.len();
    let mut up_to_date = 0u64;
    let mut unverified = 0u64;
    let mut to_download: Vec<gdrive::RemoteFile> = Vec::new();

    for file in files {
        let path = base.join(&file.relative);
        match (file.size, std::fs::metadata(&path)) {
            // A size from the API listing, which is the whole verification
            // phase when there is one.
            (Some(size), Ok(meta)) if meta.is_file() && meta.len() == size => up_to_date += 1,
            // A wrong-sized copy is stale. Removing it here rather than asking
            // the engine to overwrite keeps any .part and .rdm beside it, so an
            // interrupted mirror still resumes instead of starting over.
            (Some(_), Ok(meta)) if meta.is_file() => {
                let _ = std::fs::remove_file(&path);
                to_download.push(file);
            }
            // A Google document, or any file from a keyless listing: nothing
            // said how big it should be. The HTTP path makes the same call
            // when a HEAD fails — leave the file alone and say how many.
            (None, Ok(meta)) if meta.is_file() && meta.len() > 0 => {
                unverified += 1;
                up_to_date += 1;
            }
            _ => to_download.push(file),
        }
    }

    eprintln!();
    eprintln!("  Remote     : {} file(s)", total_remote);
    eprintln!("  Into       : {}", base.display());
    eprintln!("  Listing    : {}", if key.is_some() { "Drive API" } else { "folder page (no key)" });
    eprintln!("  Up to date : {}", up_to_date);
    if unverified > 0 {
        eprintln!("  Unverified : {} file(s) nothing gave a size for", unverified);
    }
    eprintln!("  To download: {}", to_download.len());
    if delete {
        eprintln!("  To delete  : {}", to_delete.len());
    }
    // Stated whether or not --delete was asked for: a "complete" mirror that is
    // quietly missing files is worse than a noisy one.
    if listing.unsupported > 0 {
        eprintln!("  Skipped    : {} item(s) with nothing to fetch", listing.unsupported);
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
            to_download.iter().map(|f| relative_key(&f.relative)),
            to_download.len(),
        );
    }

    if !to_delete.is_empty() {
        eprintln!();
        print_sample("-", to_delete.iter().cloned(), to_delete.len());
    }

    eprintln!();

    let mut downloaded = 0u64;

    if !to_download.is_empty() {
        gdrive::create_tree(&base, &listing.dirs)
            .await
            .context("Failed to create the mirror's directories")?;

        let done = gdrive::download_files(
            &to_download,
            &base,
            &options,
            cancel.clone(),
            gdrive::Progress::Board,
        )
        .await?;

        downloaded = done.completed as u64;
        up_to_date += done.skipped as u64;

        if downloaded > 0 {
            eprintln!();
            eprintln!(
                "  {} file(s) downloaded, {}",
                downloaded,
                ui::format_size(done.bytes)
            );
        }

        if done.cancelled {
            eprintln!();
            eprintln!(
                "  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off."
            );
            return Ok(());
        }

        // Deleting after a partial mirror could remove files the failed
        // downloads were meant to replace.
        if !done.failed.is_empty() {
            eprintln!();
            eprintln!("  \u{26a0} {} file(s) failed:", done.failed.len());
            for (path, reason) in &done.failed {
                eprintln!("     - {path}: {reason}");
            }
            eprintln!();
            eprintln!("  \u{26a0} Skipping delete phase after failures.");
            return Ok(());
        }
    }

    if !to_delete.is_empty() {
        if cancel.is_cancelled() {
            eprintln!("  \u{26a0} Cancelled before delete phase.");
            return Ok(());
        }

        let total_local = up_to_date as usize + downloaded as usize + to_delete.len();
        if !confirm_bulk_delete(to_delete.len(), total_local) {
            return Ok(());
        }

        let progress = ui::CountProgress::new("Deleting orphans", to_delete.len());
        let mut deleted = 0u64;
        let mut delete_failed = 0u64;

        for relative in &to_delete {
            let full_path = base.join(relative);
            match std::fs::remove_file(&full_path) {
                Ok(_) => {
                    deleted += 1;
                    let _ = std::fs::remove_file(format!("{}.part", full_path.display()));
                    let meta = crate::resume::ResumeMetadata::meta_path(&full_path.to_string_lossy());
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
        remove_empty_dirs(&base);

        if delete_failed > 0 {
            eprintln!("  \u{26a0} Failed to delete {} file(s)", delete_failed);
        }
    }

    eprintln!();
    eprintln!("  \u{2705} Sync complete!");
    Ok(())
}

/// Joins share-relative components onto the destination.
///
/// Components were sanitised when the node tree was decrypted, so this does
/// not re-sanitise; it exists so the MEGA path does not go through
/// [`local_path`], which percent-decodes and resolves against the config.
fn join_relative(base: &Path, components: &[String]) -> PathBuf {
    let mut path = base.to_path_buf();
    for part in components {
        path.push(part);
    }
    path
}

/// Local files under `base` that the share no longer contains.
///
/// Temp files are recognised structurally rather than by suffix: anything that
/// is a kept path plus a dot-suffix (`a.jpg.part`, `a.jpg.mctemp`, whatever
/// the downloader happens to use) belongs to a file we are keeping, so it is
/// left alone without this function needing to know the naming scheme.
///
/// Callers must not reach here with an incomplete listing: `keep` would be
/// missing those files' names and their local copies would be reported as
/// orphans. MEGA's undecryptable nodes and OneDrive's unwalkable children are
/// both that case.
fn collect_listing_orphans(
    dir: &Path,
    base: &Path,
    keep: &HashSet<String>,
    ext_filter: &Option<HashSet<String>>,
    out: &mut Vec<String>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            collect_listing_orphans(&path, base, keep, ext_filter, out);
            continue;
        }
        if !path.is_file() {
            continue;
        }

        let relative = match path.strip_prefix(base) {
            Ok(r) => r.to_string_lossy().to_string().replace('\\', "/"),
            Err(_) => continue,
        };
        if relative.is_empty() || keep.contains(&relative) {
            continue;
        }

        let is_temp_of_kept = keep.iter().any(|k| {
            relative.len() > k.len()
                && relative.starts_with(k)
                && relative.as_bytes()[k.len()] == b'.'
        });
        if is_temp_of_kept {
            continue;
        }

        if let Some(exts) = ext_filter.as_ref() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !file_has_ext(name, exts) {
                continue;
            }
        }

        out.push(relative);
    }
}

// -- Shared helpers --------------------------------------------------

/// Prints at most [`SAMPLE`] entries, then says how many were left out.
fn print_sample<I: Iterator<Item = String>>(marker: &str, items: I, total: usize) {
    for item in items.take(SAMPLE) {
        eprintln!("     {} {}", marker, item);
    }
    if total > SAMPLE {
        eprintln!("     \u{2026} and {} more", total - SAMPLE);
    }
}

/// Asks before a delete large enough to suggest the listing was incomplete.
///
/// Returns whether to go ahead.
fn confirm_bulk_delete(to_delete: usize, total_local: usize) -> bool {
    if total_local == 0 {
        return true;
    }

    let pct = (to_delete as f64 / total_local as f64) * 100.0;
    if to_delete <= 10 || pct <= 50.0 {
        return true;
    }

    eprintln!(
        "  \u{26a0} Warning: about to delete {} of {} local files ({:.0}%)",
        to_delete, total_local, pct,
    );
    eprintln!("    This usually means the remote listing is incomplete.");
    eprint!("    Continue? [y/N]: ");
    let _ = std::io::stderr().flush();

    let mut input = String::new();
    std::io::stdin().read_line(&mut input).ok();

    if matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
        true
    } else {
        eprintln!("  \u{26d4} Aborted.");
        false
    }
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

fn local_path(cfg: &Config, relative: &str) -> PathBuf {
    let decoded = engine::percent_decode(relative);
    PathBuf::from(cfg.resolve_output_path(&decoded))
}

/// A listing path in the form [`collect_listing_orphans`] derives from disk:
/// slash-separated and relative to the mirror root.
fn relative_key(relative: &Path) -> String {
    relative.to_string_lossy().replace('\\', "/")
}

fn extract_filename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_owned()
}

fn file_has_ext(filename: &str, exts: &HashSet<String>) -> bool {
    let lower = filename.to_lowercase();
    exts.iter().any(|ext| lower.ends_with(&format!(".{}", ext)))
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

fn collect_orphan_files(
    dir: &Path,
    base: &Path,
    remote_decoded: &HashSet<String>,
    ext_filter: &Option<HashSet<String>>,
    out: &mut Vec<String>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && (name.ends_with(".part") || name.ends_with(".rdm"))
        {
            continue;
        }
        if path.is_dir() {
            collect_orphan_files(&path, base, remote_decoded, ext_filter, out);
        } else if path.is_file() {
            if let Some(exts) = ext_filter.as_ref() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !file_has_ext(name, exts) {
                    continue;
                }
            }
            let relative = match path.strip_prefix(base) {
                Ok(r) => r.to_string_lossy().to_string().replace('\\', "/"),
                Err(_) => continue,
            };
            if relative.is_empty() {
                continue;
            }
            let folder = match base.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => return,
            };
            let full = format!("{}/{}", folder, relative);
            if !remote_decoded.contains(&full) {
                out.push(relative);
            }
        }
    }
}

fn remove_empty_dirs(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            remove_empty_dirs(&path);
            let _ = std::fs::remove_dir(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, b"x").unwrap();
    }

    fn keep_set(paths: &[&str]) -> HashSet<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn mega_orphans_are_paths_the_share_no_longer_has() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        touch(&base.join("keep.jpg"));
        touch(&base.join("sub/nested.jpg"));
        touch(&base.join("gone.jpg"));
        touch(&base.join("sub/also-gone.jpg"));

        let keep = keep_set(&["keep.jpg", "sub/nested.jpg"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, base, &keep, &None, &mut out);
        out.sort();

        assert_eq!(out, vec!["gone.jpg", "sub/also-gone.jpg"]);
    }

    /// Part files and resume state belong to a file we are keeping, so they
    /// must survive the sweep — deleting them silently throws away resumable
    /// progress. Matching on "kept path plus a dot-suffix" means this holds
    /// whatever the downloader names them.
    #[test]
    fn mega_orphans_leave_temp_files_of_kept_paths_alone() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        touch(&base.join("movie.mkv"));
        touch(&base.join("movie.mkv.part"));
        touch(&base.join("movie.mkv.rdm"));
        touch(&base.join("movie.mkv.mctemp"));
        touch(&base.join("stray.mkv.part"));

        let keep = keep_set(&["movie.mkv"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, base, &keep, &None, &mut out);

        // Only the leftover with no kept file behind it is an orphan.
        assert_eq!(out, vec!["stray.mkv.part"]);
    }

    #[test]
    fn mega_orphans_respect_the_extension_filter() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        touch(&base.join("gone.jpg"));
        touch(&base.join("notes.txt"));

        let exts: HashSet<String> = keep_set(&["jpg"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, base, &HashSet::new(), &Some(exts), &mut out);

        // notes.txt was never in scope for this sync, so it is not an orphan.
        assert_eq!(out, vec!["gone.jpg"]);
    }

    /// The hazard the undecryptable guard in `run_mega` exists for: a file
    /// whose node key stops resolving drops out of `keep`, and this function
    /// then cannot tell it from a file the share genuinely dropped. Proving
    /// that here is what makes the guard load-bearing rather than decorative.
    #[test]
    fn a_file_missing_from_keep_is_indistinguishable_from_an_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        touch(&base.join("readable.jpg"));
        touch(&base.join("key-no-longer-opens-this.jpg"));

        // Only the readable node made it into the listing.
        let keep = keep_set(&["readable.jpg"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, base, &keep, &None, &mut out);

        assert_eq!(
            out,
            vec!["key-no-longer-opens-this.jpg"],
            "a perfectly good file looks like an orphan, which is why run_mega \
             refuses to delete when any node is undecryptable"
        );
    }

    #[test]
    fn bulk_delete_only_prompts_when_it_is_drastic() {
        // Small deletions and small proportions never prompt, so these return
        // true without touching stdin.
        assert!(confirm_bulk_delete(0, 0));
        assert!(confirm_bulk_delete(5, 6));
        assert!(confirm_bulk_delete(20, 100));
    }

    #[test]
    fn relative_components_join_in_order() {
        assert_eq!(
            join_relative(Path::new("/tmp/dl"), &["a".to_owned(), "b.jpg".to_owned()]),
            PathBuf::from("/tmp/dl/a/b.jpg")
        );
    }
}
