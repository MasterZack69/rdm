//! Mirroring a MEGA folder share.

use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::mega;
use crate::ui;
use crate::ui::ProgressSink;

use super::orphans::{collect_listing_orphans, remove_empty_dirs};
use super::paths::{file_has_ext, join_relative};
use super::report::{confirm_bulk_delete, print_sample};

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
pub(super) async fn run_mega(
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
        eprintln!(
            "  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off."
        );
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
