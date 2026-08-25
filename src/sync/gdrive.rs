//! Mirroring a Google Drive folder.

use anyhow::{Context, Result};
use std::collections::HashSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::hoster::gdrive;
use crate::ui;

use super::orphans::{collect_listing_orphans, remove_empty_dirs};
use super::paths::{file_has_ext, relative_key};
use super::report::{confirm_bulk_delete, print_sample};

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
pub(super) async fn run_gdrive(
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
    eprintln!(
        "  Listing    : {}",
        if key.is_some() {
            "Drive API"
        } else {
            "folder page (no key)"
        }
    );
    eprintln!("  Up to date : {}", up_to_date);
    if unverified > 0 {
        eprintln!(
            "  Unverified : {} file(s) nothing gave a size for",
            unverified
        );
    }
    eprintln!("  To download: {}", to_download.len());
    if delete {
        eprintln!("  To delete  : {}", to_delete.len());
    }
    // Stated whether or not --delete was asked for: a "complete" mirror that is
    // quietly missing files is worse than a noisy one.
    if listing.unsupported > 0 {
        eprintln!(
            "  Skipped    : {} item(s) with nothing to fetch",
            listing.unsupported
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
