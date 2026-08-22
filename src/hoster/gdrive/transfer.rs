//! Turning a walked folder into files on disk.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use futures_util::{StreamExt, stream};
use tokio::fs;
use tokio_util::sync::CancellationToken;

use crate::engine::{self, DownloadRequest, ExistingPolicy, Outcome};
use crate::ui::{self, Board, ProgressSink};

use super::{GdriveOptions, GdriveSummary, Progress, RemoteFile, WORKERS_MAX};

/// Creates the download root and every directory the walk saw.
///
/// Empty ones included: an empty folder is still part of the structure that was
/// shared. Doing it up front also means every file's parent exists before any
/// transfer starts, so nothing below has to create anything.
pub async fn create_tree(root: &Path, dirs: &[PathBuf]) -> Result<()> {
    fs::create_dir_all(root)
        .await
        .with_context(|| format!("could not create {}", root.display()))?;

    for dir in dirs {
        let path = root.join(dir);
        fs::create_dir_all(&path)
            .await
            .with_context(|| format!("could not create {}", path.display()))?;
    }

    Ok(())
}

/// Downloads files under `root`, several at a time.
///
/// One failure is not the end of the folder: it is recorded and the rest carry
/// on, because thirty-nine files out of forty is a better outcome than none.
///
/// Takes a slice rather than a [`Listing`](super::Listing) so a mirror can
/// download the part of a folder that is actually out of date.
pub async fn download_files(
    files: &[RemoteFile],
    root: &Path,
    options: &GdriveOptions,
    cancel: CancellationToken,
    progress: Progress,
) -> Result<GdriveSummary> {
    let total = files.len();

    if total == 0 {
        // A folder with nothing fetchable in it is a result, not a failure: the
        // directories are on disk and there is nothing else to do.
        return Ok(GdriveSummary {
            root: root.to_path_buf(),
            total: 0,
            completed: 0,
            skipped: 0,
            bytes: 0,
            // Filled in by the caller, which is the only thing holding the
            // listing these files came from.
            unsupported: 0,
            failed: Vec::new(),
            cancelled: false,
        });
    }

    let workers = options.workers.clamp(1, WORKERS_MAX);
    // A lane is one line on a board somebody else is rendering, so this must
    // not build one of its own: two renderers writing the same terminal is a
    // scrambled display.
    let board = match &progress {
        Progress::Board => Some(Board::new("Google Drive", total, workers)),
        Progress::Quiet | Progress::Lane(_) => None,
    };
    let lane_sink = match &progress {
        Progress::Lane(sink) => Some(Arc::clone(sink)),
        Progress::Board | Progress::Quiet => None,
    };
    let renderer = board.as_ref().map(|board| board.spawn_renderer());

    let completed = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let bytes = AtomicU64::new(0);
    let cancelled = AtomicBool::new(false);
    let failed: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

    {
        let board = board.as_ref();
        let lane_sink = lane_sink.as_ref();
        let completed = &completed;
        let skipped = &skipped;
        let bytes = &bytes;
        let cancelled = &cancelled;
        let failed = &failed;
        let overwrite = options.overwrite;

        stream::iter(files.iter().enumerate())
            .for_each_concurrent(workers, move |(index, file)| {
                let cancel = cancel.clone();
                async move {
                    if cancel.is_cancelled() {
                        cancelled.store(true, Ordering::Relaxed);
                        return;
                    }

                    // The lane is held for the whole file: dropping it early
                    // would hand the display slot to another worker while this
                    // one is still reporting into it.
                    let lane = board.and_then(|board| board.claim(index as u64 + 1, &file.name));
                    let sink: Arc<dyn ProgressSink> = match (lane.as_ref(), lane_sink) {
                        (Some(lane), _) => lane.sink(),
                        // Every file reports into the one shared line, so the
                        // queue shows the folder's throughput rather than any
                        // single file's.
                        (None, Some(sink)) => Arc::clone(sink),
                        (None, None) => ui::silent(),
                    };

                    let destination = root.join(&file.relative);
                    let request = DownloadRequest::new(
                        file.url.clone(),
                        Some(destination.to_string_lossy().into_owned()),
                        // One connection per file: the parallelism here is
                        // files at once, and multiplying the two would point
                        // workers \u{00d7} chunks sockets at a single API key.
                        1,
                    )
                    .with_policy(if overwrite {
                        ExistingPolicy::Overwrite
                    } else {
                        // Never `Ask`: a folder of four hundred files must not
                        // stop on a prompt hidden behind the progress board.
                        ExistingPolicy::Reuse
                    })
                    // Keyed on the Drive id, because a confirmed download URL
                    // carries a token that expires long before the download
                    // being resumed does.
                    .with_resume_identity(format!("gdrive:{}", file.id));

                    match engine::download(request, cancel, Arc::clone(&sink)).await {
                        Ok(Outcome::Completed { bytes: written, .. }) => {
                            completed.fetch_add(1, Ordering::Relaxed);
                            bytes.fetch_add(written, Ordering::Relaxed);
                            if let Some(board) = board {
                                board.file_completed(written);
                            }
                        }
                        Ok(Outcome::AlreadyPresent { .. }) => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            if let Some(board) = board {
                                board.file_skipped();
                            }
                        }
                        Ok(Outcome::Cancelled) => {
                            cancelled.store(true, Ordering::Relaxed);
                        }
                        Err(error) => {
                            let reason = format!("{error:#}");
                            let name = file.relative.to_string_lossy().into_owned();
                            if let Some(board) = board {
                                board.file_failed();
                                board.log(&format!("  \u{26a0} {name}: {reason}"));
                            }
                            failed
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .push((name, reason));
                            // The engine closes the sink on every outcome it
                            // returns; an error is the one way out that leaves
                            // the lane still open.
                            sink.finish();
                        }
                    }
                }
            })
            .await;
    }

    if let Some(renderer) = renderer {
        renderer.abort();
    }
    if let Some(board) = &board {
        board.finish();
    }

    Ok(GdriveSummary {
        root: root.to_path_buf(),
        total,
        completed: completed.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed),
        bytes: bytes.load(Ordering::Relaxed),
        unsupported: 0,
        failed: failed.into_inner().unwrap_or_else(|poisoned| poisoned.into_inner()),
        cancelled: cancelled.load(Ordering::Relaxed),
    })
}
