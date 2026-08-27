//! Per-item dispatch: picking a downloader, and flattening what it returns.

use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::engine::{self, DownloadRequest, ExistingPolicy, Outcome};
use crate::mega;
use crate::ui;

use super::item::Item;
use super::share::{run_gdrive_item, run_onedrive_item, run_pixeldrain_item};

/// What happened to one item, with the differences between the downloaders
/// already flattened out.
///
/// `path` is the file the download ended up at, and is `Some` only where the
/// item's URL could not have named it. [`Item::display_name`] can label a share
/// link with nothing better than its id until the API answers; this is that
/// answer, so the finished line and `queue list` show a file instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ItemOutcome {
    Completed { bytes: u64, path: Option<String> },
    AlreadyPresent { path: Option<String> },
    Cancelled,
}

impl ItemOutcome {
    /// The file on disk, when the downloader ended up somewhere it can name.
    pub(super) fn path(&self) -> Option<&str> {
        match self {
            Self::Completed { path, .. } | Self::AlreadyPresent { path } => path.as_deref(),
            Self::Cancelled => None,
        }
    }
}

/// Flattens an engine outcome, keeping the path it settled on.
///
/// The engine may take the server's suggested filename over the one it was
/// asked for, so the file that exists is the only one worth naming.
pub(super) fn from_engine(outcome: Outcome) -> ItemOutcome {
    match outcome {
        Outcome::Completed { path, bytes } => ItemOutcome::Completed {
            bytes,
            path: Some(path),
        },
        Outcome::AlreadyPresent { path } => ItemOutcome::AlreadyPresent { path: Some(path) },
        Outcome::Cancelled => ItemOutcome::Cancelled,
    }
}

/// The filename part of a path, for labelling a finished item.
pub(super) fn file_name_of(path: &str) -> Option<String> {
    std::path::Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

/// Runs one queue item on whichever downloader it needs.
///
/// `mega_gate` serialises MEGA items. MEGA's bandwidth limit is enforced per
/// IP, not per file, so running three of them at once does not go three times
/// faster — all three hit the same 509 wall and each sits out its own backoff,
/// while the file already has `mega_workers` parallel connections inside it.
pub(super) async fn run_item(
    cfg: &Config,
    item: &Item,
    cancel: CancellationToken,
    sink: Arc<dyn ui::ProgressSink>,
    mega_client: reqwest::Client,
    mega_gate: Arc<tokio::sync::Semaphore>,
) -> Result<ItemOutcome> {
    if item.is_mega() {
        let _permit = mega_gate.acquire_owned().await;

        if cancel.is_cancelled() {
            return Ok(ItemOutcome::Cancelled);
        }

        let (output, download_dir) = item.share_destination(cfg);
        let options = mega::MegaOptions {
            workers: item.connections.unwrap_or(cfg.mega_workers),
            verify_mac: cfg.mega_verify_mac,
            resume_on_ip_change: cfg.mega_resume_on_ip_change,
            max_retries: cfg.max_retries,
            // The queue never stops to ask, and an existing file of the right
            // size is treated as already downloaded.
            overwrite: false,
        };

        let outcome = mega::download(
            mega_client,
            &item.url,
            output,
            &download_dir,
            options,
            cancel,
            sink,
        )
        .await?;

        return Ok(match outcome {
            mega::MegaOutcome::Completed { bytes, .. } => {
                ItemOutcome::Completed { bytes, path: None }
            }
            mega::MegaOutcome::AlreadyPresent { .. } => ItemOutcome::AlreadyPresent { path: None },
            mega::MegaOutcome::Cancelled { .. } => ItemOutcome::Cancelled,
        });
    }

    if item.is_onedrive() {
        return run_onedrive_item(cfg, item, cancel, sink).await;
    }

    if item.is_gdrive() {
        return run_gdrive_item(cfg, item, cancel, sink).await;
    }

    if item.is_pixeldrain() {
        return run_pixeldrain_item(cfg, item, cancel, sink).await;
    }

    let request = DownloadRequest::new(
        item.url.clone(),
        Some(item.resolve_output(cfg)),
        item.connections.unwrap_or(cfg.connections),
    )
    // Never stop a batch run to ask about an existing file.
    .with_policy(ExistingPolicy::Reuse)
    .with_allow_private(item.allow_private);

    Ok(from_engine(engine::download(request, cancel, sink).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The engine may take the server's suggested filename over the one it was
    /// asked for, so the finished line has to follow the file, not the request.
    #[test]
    fn an_engine_outcome_carries_the_file_it_settled_on() {
        let completed = from_engine(Outcome::Completed {
            path: "/home/z/Downloads/holiday.mkv".into(),
            bytes: 4096,
        });
        assert_eq!(
            completed.path().and_then(file_name_of),
            Some("holiday.mkv".to_owned())
        );

        let present = from_engine(Outcome::AlreadyPresent {
            path: "/home/z/Downloads/holiday.mkv".into(),
        });
        assert_eq!(
            present.path().and_then(file_name_of),
            Some("holiday.mkv".to_owned())
        );

        assert_eq!(from_engine(Outcome::Cancelled).path(), None);
    }
}
