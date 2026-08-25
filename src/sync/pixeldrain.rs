//! Mirroring a pixeldrain list.

use anyhow::{Context, Result};
use std::collections::HashSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::hoster::pixeldrain;
use crate::ui;

use super::orphans::{collect_listing_orphans, remove_empty_dirs};
use super::paths::file_has_ext;
use super::report::{confirm_bulk_delete, print_sample};

/// Mirrors a pixeldrain list.
///
/// The simplest of the three. A list is flat, so there is no tree to create and
/// no relative path to preserve, and one request returns every name and size —
/// which makes the verification phase a `stat` per file, as it is for MEGA and
/// OneDrive, and `--delete` a single-directory sweep.
///
/// Names are settled by [`pixeldrain::resolve`] rather than here. Two files in
/// one list may share a name, and the de-duplicated form is what lands on disk,
/// so a plan built from the raw API names would compare against paths that
/// never exist and re-download every duplicate on every run.
///
/// Unlike a MEGA share, a list has a title of its own, so the mirror gets its
/// own directory and `--delete` does not need `-o` to be safe.
pub(super) async fn run_pixeldrain(
    cfg: &Config,
    url: &str,
    workers: usize,
    delete: bool,
    explicit_dir: Option<&str>,
    ext_filter: Option<HashSet<String>>,
    cancel: CancellationToken,
) -> Result<()> {
    let options = pixeldrain::PixeldrainOptions {
        workers,
        max_retries: cfg.max_retries,
        api_key: pixeldrain::api_key(&cfg.pixeldrain_api_key),
        // Nothing reaching the download phase has a good local copy, and the
        // stale ones are removed below rather than resumed onto — which keeps
        // any .part beside them alive.
        overwrite: false,
    };

    let list = match pixeldrain::resolve(url, &options).await? {
        pixeldrain::Resolved::List(list) => list,
        pixeldrain::Resolved::File(_) => anyhow::bail!(
            "`rdm sync` mirrors a list \u{2014} for a single pixeldrain file use `rdm <link>`"
        ),
    };

    let mut files: Vec<pixeldrain::RemoteFile> = list.files().to_vec();
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
        eprintln!("  \u{274c} No files found in that pixeldrain list");
        return Ok(());
    }

    if cancel.is_cancelled() {
        eprintln!("  \u{26a0} Cancelled during scan.");
        return Ok(());
    }

    let base = pixeldrain::destination_root(
        explicit_dir.map(|dir| dir.to_owned()),
        &cfg.download_dir,
        list.title(),
    );

    let mut to_delete: Vec<String> = Vec::new();
    if delete {
        if list.skipped() > 0 {
            // The same hazard as an undecryptable MEGA node: an entry with no
            // id behind it never reaches `keep`, so a local copy of it cannot
            // be told apart from a file the list dropped.
            eprintln!(
                "  \u{26a0} Skipping --delete: {} entry(s) in this list have no file",
                list.skipped()
            );
            eprintln!("    behind them, so their local copies cannot be told apart from");
            eprintln!("    orphans. Nothing will be deleted this run.");
        } else if base.is_dir() {
            let keep: HashSet<String> = files.iter().map(|f| f.name.clone()).collect();
            collect_listing_orphans(&base, &base, &keep, &ext_filter, &mut to_delete);
            to_delete.sort();
        }
    }

    let total_remote = files.len();
    let mut up_to_date = 0u64;
    let mut unverified = 0u64;
    let mut to_download: Vec<pixeldrain::RemoteFile> = Vec::new();

    for file in files {
        let path = base.join(&file.name);
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
        eprintln!(
            "  Unverified : {} file(s) the listing gave no size for",
            unverified
        );
    }
    eprintln!("  To download: {}", to_download.len());
    if delete {
        eprintln!("  To delete  : {}", to_delete.len());
    }
    // Stated whether or not --delete was asked for: a "complete" mirror that is
    // quietly missing files is worse than a noisy one.
    if list.skipped() > 0 {
        eprintln!(
            "  Skipped    : {} entry(s) with nothing to fetch",
            list.skipped()
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
            to_download.iter().map(|f| f.name.clone()),
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
        std::fs::create_dir_all(&base)
            .with_context(|| format!("Failed to create {}", base.display()))?;

        // The list's own client, not a new one: the API key is in its headers,
        // and a mirror is the run where being throttled costs the most.
        let done = pixeldrain::download_files(
            list.client(),
            &to_download,
            &base,
            &options,
            cancel.clone(),
            false,
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
        remove_empty_dirs(&base);

        if delete_failed > 0 {
            eprintln!("  \u{26a0} Failed to delete {} file(s)", delete_failed);
        }
    }

    eprintln!();
    eprintln!("  \u{2705} Sync complete!");
    Ok(())
}
