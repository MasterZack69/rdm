//! Share links: ask the API what the link points at, then hand the answer to
//! the engine.
//!
//! What gets stored in the queue is always the link, never the resolved URL:
//! those come back signed and short-lived, so an item that sat in the queue
//! overnight has to ask again.

use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::engine::{self, DownloadRequest, ExistingPolicy};
use crate::hoster::{gdrive, onedrive, pixeldrain};
use crate::ui;

use super::dispatch::{ItemOutcome, from_engine};
use super::item::Item;

/// Runs one OneDrive item.
///
/// A share link is not a fetchable address, so the API is asked what it points
/// at first, and the answer decides the shape of the work. A file goes to the
/// engine like any other download. A folder is a whole tree behind one item,
/// which the queue cannot represent as separate rows, so it is fetched under
/// this item and reported into this item's one progress line.
pub(super) async fn run_onedrive_item(
    cfg: &Config,
    item: &Item,
    cancel: CancellationToken,
    sink: Arc<dyn ui::ProgressSink>,
) -> Result<ItemOutcome> {
    let options = onedrive::OneDriveOptions {
        // On a folder `-c` means files at once, the way it means workers for
        // MEGA. On a single file it stays chunks — see below.
        workers: item.connections.unwrap_or(cfg.onedrive_workers),
        max_retries: cfg.max_retries,
        // The queue never re-downloads what is already there.
        overwrite: false,
    };

    let (output, download_dir) = item.share_destination(cfg);

    let folder = match onedrive::resolve(reqwest::Client::new(), &item.url, &options).await? {
        onedrive::Resolved::File(link) => {
            let destination = output.unwrap_or_else(|| cfg.resolve_output_path(&link.name));
            if let Some(parent) = std::path::Path::new(&destination).parent() {
                std::fs::create_dir_all(parent).ok();
            }

            let request = DownloadRequest::new(
                link.url,
                Some(destination),
                item.connections.unwrap_or(cfg.connections),
            )
            .with_policy(ExistingPolicy::Reuse)
            // A fresh signature every run, so the URL cannot be the thing
            // resume recognises the file by.
            .with_resume_identity(format!("onedrive:{}", link.id));

            return Ok(from_engine(engine::download(request, cancel, sink).await?));
        }
        onedrive::Resolved::Folder(folder) => folder,
    };

    let summary = onedrive::download_folder(
        folder,
        output,
        &download_dir,
        options,
        cancel,
        onedrive::Progress::Lane(sink),
    )
    .await?;

    if summary.cancelled {
        return Ok(ItemOutcome::Cancelled);
    }

    // One failed file must not read as a finished folder: leaving the item
    // failed is what lets `queue retry failed` finish the job, and the files
    // already on disk are skipped when it does.
    if !summary.failed.is_empty() {
        let (path, reason) = &summary.failed[0];
        anyhow::bail!(
            "{} of {} file(s) failed, starting with {}: {}",
            summary.failed.len(),
            summary.total,
            path,
            reason
        );
    }

    if summary.completed == 0 && summary.skipped > 0 {
        return Ok(ItemOutcome::AlreadyPresent { path: None });
    }

    // A folder is many files; there is no one path to name it by.
    Ok(ItemOutcome::Completed {
        bytes: summary.bytes,
        path: None,
    })
}

/// Runs one Google Drive item.
///
/// Same shape as the OneDrive path and for the same reason: a link is not a
/// fetchable address, so what it points at decides the shape of the work. A
/// file goes to the engine; a folder is a whole tree behind one item, fetched
/// under this item and reported into this item's one progress line.
pub(super) async fn run_gdrive_item(
    cfg: &Config,
    item: &Item,
    cancel: CancellationToken,
    sink: Arc<dyn ui::ProgressSink>,
) -> Result<ItemOutcome> {
    let options = gdrive::GdriveOptions {
        // On a folder `-c` means files at once. On a single file it stays
        // chunks — see below.
        workers: item.connections.unwrap_or(cfg.gdrive_workers),
        max_retries: cfg.max_retries,
        api_key: cfg.gdrive_key(),
        doc_format: cfg.gdrive_doc_format.clone(),
        // The queue never re-downloads what is already there.
        overwrite: false,
    };

    let (output, download_dir) = item.share_destination(cfg);

    let folder = match gdrive::resolve(reqwest::Client::new(), &item.url, &options).await? {
        gdrive::Resolved::File(link) => {
            let destination = output.unwrap_or_else(|| cfg.resolve_output_path(&link.name));
            if let Some(parent) = std::path::Path::new(&destination).parent() {
                std::fs::create_dir_all(parent).ok();
            }

            let request = DownloadRequest::new(
                link.url,
                Some(destination),
                item.connections.unwrap_or(cfg.connections),
            )
            .with_policy(ExistingPolicy::Reuse)
            // A fresh token every run, so the URL cannot be the thing resume
            // recognises the file by.
            .with_resume_identity(format!("gdrive:{}", link.id));

            return Ok(from_engine(engine::download(request, cancel, sink).await?));
        }
        gdrive::Resolved::Folder(folder) => folder,
    };

    let listing = gdrive::list_folder(&folder, &options).await?;
    let root = gdrive::destination_root(output, &download_dir, folder.name());

    gdrive::create_tree(&root, &listing.dirs).await?;
    let summary = gdrive::download_files(
        &listing.files,
        &root,
        &options,
        cancel,
        gdrive::Progress::Lane(sink),
    )
    .await?;

    if summary.cancelled {
        return Ok(ItemOutcome::Cancelled);
    }

    // One failed file must not read as a finished folder: leaving the item
    // failed is what lets `queue retry failed` finish the job, and the files
    // already on disk are skipped when it does.
    if !summary.failed.is_empty() {
        let (path, reason) = &summary.failed[0];
        anyhow::bail!(
            "{} of {} file(s) failed, starting with {}: {}",
            summary.failed.len(),
            summary.total,
            path,
            reason
        );
    }

    if summary.completed == 0 && summary.skipped > 0 {
        return Ok(ItemOutcome::AlreadyPresent { path: None });
    }

    // A folder is many files; there is no one path to name it by.
    Ok(ItemOutcome::Completed {
        bytes: summary.bytes,
        path: None,
    })
}

/// Runs one pixeldrain item.
///
/// Much less work than OneDrive: there is no signature to refresh and no tree
/// to walk, so the address stored at `queue add` time is still the right one.
/// The API is asked anyway, for the two things a URL cannot carry — the real
/// filename, and whether pixeldrain has already decided not to serve this file.
/// Both are worth one small GET before a large transfer.
///
/// No resume identity either, for the same reason: the URL is stable, so it is
/// a sound thing for resume to recognise the file by. OneDrive needs one only
/// because its address is minted fresh every time.
///
/// Lists never reach here — `queue add` turns them away, because a list is many
/// files behind one row and the queue has no way to show that. The arm below is
/// for a hand-edited `queue.json`, and says where to go instead.
pub(super) async fn run_pixeldrain_item(
    cfg: &Config,
    item: &Item,
    cancel: CancellationToken,
    sink: Arc<dyn ui::ProgressSink>,
) -> Result<ItemOutcome> {
    let options = pixeldrain::PixeldrainOptions {
        // Files-at-once, which a lone file has no use for; `-c` below stays
        // chunks, the way it does for an ordinary download.
        workers: cfg.pixeldrain_workers,
        max_retries: cfg.max_retries,
        api_key: pixeldrain::api_key(&cfg.pixeldrain_api_key),
        // The queue never re-downloads what is already there.
        overwrite: false,
    };

    let link = match pixeldrain::resolve(&item.url, &options).await? {
        pixeldrain::Resolved::File(link) => link,
        pixeldrain::Resolved::List(_) => anyhow::bail!(
            "this is a pixeldrain list, which the queue cannot hold as one item \u{2014} \
             run `rdm sync {}` to mirror it",
            item.url
        ),
    };

    let (output, _) = item.share_destination(cfg);
    let destination = output.unwrap_or_else(|| cfg.resolve_output_path(&link.name));
    if let Some(parent) = std::path::Path::new(&destination).parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let request = DownloadRequest::new(
        link.url,
        Some(destination),
        item.connections.unwrap_or(cfg.connections),
    )
    .with_policy(ExistingPolicy::Reuse)
    // The key is in the client's headers, not in the URL, so handing the engine
    // a bare address here is exactly what an account holder would feel as
    // throttling.
    .with_client(link.client);

    Ok(from_engine(engine::download(request, cancel, sink).await?))
}
