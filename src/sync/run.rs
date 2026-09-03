//! The `rdm sync` entry point, and the generic HTTP mirror.
//!
//! The share paths are checked first and return early, so everything after
//! them is the HTTP case: the only one that has to scrape a listing, and ask
//! per file whether the local copy is still current.
//!
//! It is also the only path where the *shape* of a path is chosen remotely.
//! Everywhere else rdm writes a filename the user gave it; here a listing can
//! name nested directories. So every mkdir and every unlink below is
//! root-anchored: it goes through [`crate::safe_file`], which walks each
//! component against the previous directory descriptor with `O_NOFOLLOW`
//! rather than handing a full pathname to the kernel to resolve. A
//! `download_dir/album` that someone has replaced with a symlink to `~/.ssh`
//! then fails at `album` instead of being traversed.

use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::hoster::{gdrive, onedrive, pixeldrain};
use crate::mega;
use crate::net;
use crate::queue;
use crate::safe_file;
use crate::safe_path;
use crate::scrape;
use crate::secret_url;
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
    // The registered trust boundary has to be the directory this run actually
    // writes to. `-o` moves it for the folder hosts; the generic HTTP path
    // below ignores `output_dir` and always resolves against `download_dir`,
    // so the two cases are distinguished rather than guessed. Set-once, so
    // the first sync in a process fixes it.
    let effective_root = if mega::is_mega_url(url)
        || onedrive::is_onedrive_url(url)
        || gdrive::is_gdrive_url(url)
        || pixeldrain::is_pixeldrain_url(url)
    {
        output_dir
            .clone()
            .unwrap_or_else(|| cfg.download_dir.clone())
    } else {
        cfg.download_dir.clone()
    };
    safe_file::set_download_root(Some(PathBuf::from(effective_root)));

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
            eprintln!(
                "  \u{274c} No files found at {}",
                ui::terminal_safe(&secret_url::redact(url))
            );
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

    // Already decoded exactly once, by the scraper. Decoding here as well is
    // what let `%252F...` become an absolute path, and it also meant this set
    // was built from differently-decoded strings than the ones actually
    // written to disk, so `--delete` was comparing two different alphabets.
    let remote_decoded: HashSet<String> = files.iter().map(|f| f.relative_path.clone()).collect();

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
            SyncRoot::Ok { .. } => {}
        }
    }

    let mut needs_head: Vec<(String, String, PathBuf, u64)> = Vec::new();
    let mut to_download: Vec<(String, String)> = Vec::new();

    for f in &files {
        // The guarded join. An entry that cannot be placed beneath the
        // download directory is skipped rather than written anyway.
        let path = match local_path(cfg, &f.relative_path) {
            Ok(path) => path,
            Err(e) => {
                eprintln!(
                    "  \u{26a0} Skipping unsafe entry {}: {:#}",
                    ui::terminal_safe(&f.relative_path),
                    e
                );
                continue;
            }
        };

        // Read-only, and it only decides whether to send a HEAD. Left on the
        // full pathname deliberately: nothing is written or removed through
        // it, so the descriptor walk would be churn for no gain.
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
        let policy = net::Policy::new(allow_private);

        let sem = Arc::new(Semaphore::new(8));
        let mut tasks = JoinSet::new();

        // Capture the total before needs_head is moved into the spawn loop.
        let progress = ui::CountProgress::new("Verifying local files", needs_head.len());

        for (file_url, relative, path, size) in needs_head {
            let sem = sem.clone();
            let cancel = cancel.clone();

            tasks.spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                if cancel.is_cancelled() {
                    return None;
                }
                let status = verify_remote_size(&policy, &file_url, size).await;
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
        && let SyncRoot::Ok {
            ref prefix,
            ref path,
        } = sync_root_result
    {
        let download_root = Path::new(&cfg.download_dir);
        let root_path = Path::new(path);

        // The mirror's folder name comes from the listing, so the root of the
        // sweep is itself an untrusted component. Opening it by pathname
        // followed a symlinked `download_dir/<folder>` before any protection
        // began, and the sweep then enumerated whatever it pointed at.
        // Walking it from the download directory refuses that first.
        match safe_file::verify_dir_beneath(download_root, Path::new(prefix)) {
            Ok(()) => {
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
            Err(e) => {
                eprintln!("  \u{26a0} Skipping delete phase: {:#}", e);
            }
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
            // Already decoded by the scraper, so this only has to be made
            // safe to draw: a listing controls these strings, and an ESC in
            // one of them would repaint the preview.
            to_download
                .iter()
                .map(|(_, relative)| ui::terminal_safe(relative)),
            to_download.len(),
        );
    }

    if !to_delete.is_empty() {
        eprintln!();
        print_sample("-", to_delete.iter().cloned(), to_delete.len());
    }

    eprintln!();

    if !to_download.is_empty() {
        let root = Path::new(&cfg.download_dir);

        // Resolve each destination once and reuse it for the delete, the
        // mkdir and the queue hand-off. This used to be resolved separately in
        // three places, which is how the local file that got deleted and the
        // path that got written could disagree.
        //
        // The root-relative form is kept alongside the absolute one because
        // the removals and the mkdir below are performed relative to the
        // download root by descriptor walk, not by pathname.
        let mut resolved: Vec<(String, String, PathBuf)> = Vec::with_capacity(to_download.len());
        for (file_url, relative) in &to_download {
            match local_path(cfg, relative) {
                Ok(path) => resolved.push((file_url.clone(), relative.clone(), path)),
                Err(e) => {
                    eprintln!(
                        "  \u{26a0} Skipping unsafe entry {}: {:#}",
                        ui::terminal_safe(relative),
                        e
                    );
                }
            }
        }

        // These removals are why the walk matters here rather than only at the
        // open: sync deletes the file selected for redownload *before* queueing
        // its replacement, so a redirected unlink destroys someone else's file
        // and a redirected create then writes over the hole. Missing files are
        // the normal case, so failures stay ignored.
        for (_, relative, _) in &resolved {
            let relative = Path::new(relative);
            let _ = safe_file::unlink_beneath(root, relative);
            let _ = safe_file::unlink_beneath(root, &with_suffix(relative, ".part"));
            let _ = safe_file::unlink_beneath(root, &with_suffix(relative, ".rdm"));
        }

        // mkdir per component against a held descriptor. `openat2` has no
        // directory-creating mode, so this is the only way the intermediate
        // components can be created without a pathname resolution in the
        // middle of it.
        let mut resolved: Vec<(String, String, PathBuf)> = resolved
            .into_iter()
            .filter(|(_, relative, _)| {
                let Some(parent) = Path::new(relative).parent() else {
                    return true;
                };

                match safe_file::create_dirs_beneath(root, parent) {
                    Ok(()) => true,
                    Err(e) => {
                        // Previously `.ok()`, which meant a refusal here
                        // surfaced later as a confusing open failure. A
                        // refusal is the guard doing its job, so say so and
                        // drop the entry.
                        eprintln!(
                            "  \u{26a0} Skipping {}: {:#}",
                            ui::terminal_safe(relative),
                            e
                        );
                        false
                    }
                }
            })
            .collect();
        resolved.shrink_to_fit();

        if resolved.is_empty() {
            eprintln!("  \u{274c} No entries could be placed beneath the download directory");
            return Ok(());
        }

        queue::Queue::locked(|q| {
            for (file_url, _, path) in &resolved {
                // The already-resolved absolute destination, so the queue has
                // nothing left to decode or re-resolve. It is also exactly the
                // path removed above, which matters because the removal
                // happens before the replacement is queued.
                q.add_with_scope(
                    file_url.clone(),
                    Some(path.to_string_lossy().into_owned()),
                    Some(connections),
                    allow_private,
                );
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

        if let SyncRoot::Ok {
            ref prefix,
            ref path,
        } = sync_root_result
        {
            let download_root = Path::new(&cfg.download_dir);
            let root_path = Path::new(path);
            let progress = ui::CountProgress::new("Deleting orphans", to_delete.len());

            for relative in &to_delete {
                // Anchored at the download directory rather than at the sync
                // root, with the listing-chosen folder name put back on the
                // front as just another untrusted component. A symlink at that
                // component now fails the walk instead of being the root the
                // walk trusts.
                let relative = Path::new(prefix).join(relative);
                let relative = relative.as_path();

                match safe_file::unlink_beneath(download_root, relative) {
                    Ok(()) => {
                        deleted += 1;
                        let _ = safe_file::unlink_beneath(
                            download_root,
                            &with_suffix(relative, ".part"),
                        );
                        let _ = safe_file::unlink_beneath(
                            download_root,
                            &with_suffix(relative, ".rdm"),
                        );
                    }
                    Err(e) => {
                        // An orphan that has already gone is not a failure.
                        // The io::Error is still in the context chain, so the
                        // kind survives being wrapped.
                        let missing = e
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound);

                        if !missing {
                            delete_failed += 1;
                            progress.note(&format!(
                                "  \u{26a0} Failed to delete {}: {:#}",
                                ui::terminal_safe(&relative.to_string_lossy()),
                                e
                            ));
                        }
                    }
                }
                progress.tick();
            }

            progress.finish(&format!("{} file(s) deleted", deleted));
            remove_empty_dirs(root_path);
        }

        if delete_failed > 0 {
            eprintln!("  \u{26a0} Failed to delete {} file(s)", delete_failed);
        }
    }

    eprintln!();
    eprintln!("  \u{2705} Sync complete!");
    Ok(())
}

/// Appends `suffix` to the final component of a relative path.
///
/// `.part` and `.rdm` are siblings of the destination, so they have to be
/// named the same way it is: root-relative, so the descriptor walk applies to
/// them too. Formatting `"{}.part"` onto a full pathname would put them back
/// on the pathname-resolution route the rest of this module just left.
fn with_suffix(relative: &Path, suffix: &str) -> PathBuf {
    match relative.file_name() {
        Some(name) => {
            let mut name = name.to_os_string();
            name.push(suffix);
            relative.with_file_name(name)
        }
        // No final component means the walk will refuse it anyway.
        None => relative.to_path_buf(),
    }
}

enum HeadStatus {
    UpToDate,
    SizeMismatch,
    HeadFailed,
    NoContentLength,
}

enum SyncRoot {
    Ok { prefix: String, path: String },
    Empty,
    MixedRoots,
}

/// Compares the remote size with what is on disk, over a client that has
/// resolved and judged every hop to the file first.
///
/// This used to take a plain `reqwest::Client`, which is how a file URL the
/// scraper had cleared could still steer the *verification* request into
/// 169.254.169.254: nothing looked at where the second request went. The probe
/// is the same one-byte ranged GET the download opens with, so it also states a
/// size that a HEAD sometimes omits — and it is still one request per file.
async fn verify_remote_size(policy: &net::Policy, url: &str, local_size: u64) -> HeadStatus {
    let Ok((_, response)) = policy.probe(url).await else {
        return HeadStatus::HeadFailed;
    };

    match net::probed_size(&response) {
        Some(remote) if remote == local_size => HeadStatus::UpToDate,
        Some(_) => HeadStatus::SizeMismatch,
        None if response.status().is_success() => HeadStatus::NoContentLength,
        None => HeadStatus::HeadFailed,
    }
}

/// The local directory the mirror is rooted at: the download directory plus
/// the listing's common first component.
///
/// The component is *not* decoded here. The scraper already decoded it exactly
/// once, and decoding again is what let a listing name the root anywhere on the
/// filesystem — which mattered more here than anywhere else, because
/// `--delete` walks this directory and removes what it finds.
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
    match safe_path::resolve_under(Path::new(&cfg.download_dir), prefix) {
        Ok(root) => SyncRoot::Ok {
            prefix: prefix.to_owned(),
            path: root.to_string_lossy().into_owned(),
        },
        Err(_) => SyncRoot::Empty,
    }
}
