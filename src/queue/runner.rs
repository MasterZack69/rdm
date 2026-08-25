//! The queue processor: works through every pending item, `parallel` files at
//! a time.

use anyhow::{Context, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::ui;

use super::dispatch::{ItemOutcome, file_name_of, run_item};
use super::item::Status;
use super::lock::FileLock;
use super::signals::{clear_signal, read_signal};
use super::state::Queue;

/// Works through every pending item, `parallel` files at a time.
///
/// This is the single download path for `rdm queue start`, `rdm sync`, and
/// `rdm <directory-url>`, so all three get the same progress display.
pub async fn start(cfg: &Config, cancel: CancellationToken, parallel: usize) -> Result<()> {
    let _processor_lock =
        FileLock::processor().context("Another `rdm queue start` is already running")?;

    clear_signal();

    // Reset stale Downloading items from a previous crash.
    Queue::locked(|q| {
        q.requeue_in_flight();
        Ok(())
    })?;

    let snapshot = Queue::load_readonly();
    let pending = snapshot.pending_count();
    let pending_mega = snapshot.pending_mega_count();
    if pending == 0 {
        eprintln!("  Queue is empty — nothing to do.");
        return Ok(());
    }

    let parallel = parallel.max(1).min(pending);

    let board = ui::Board::new("Queue", pending, parallel);
    board.log(&format!(
        "  \u{1f680} {} file(s) queued \u{b7} {} at a time",
        pending, parallel
    ));
    if pending_mega > 1 {
        board.log(&format!(
            "  \u{2139} {} MEGA link(s) \u{b7} run one at a time — the quota is per-IP, not per-file",
            pending_mega
        ));
    }
    let renderer = board.spawn_renderer();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallel));
    // Separate from `semaphore`: this one is about MEGA's quota, not about how
    // many files the user wants in flight.
    let mega_gate = Arc::new(tokio::sync::Semaphore::new(1));
    let mega_client = reqwest::Client::new();
    let completed = Arc::new(AtomicU32::new(0));
    let failed = Arc::new(AtomicU32::new(0));
    let skipped = Arc::new(AtomicU32::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let active_children: Arc<Mutex<Vec<(u64, CancellationToken)>>> =
        Arc::new(Mutex::new(Vec::new()));

    // Signal watcher — polls for skip/stop sent from another terminal.
    let watcher_cancel = cancel.clone();
    let watcher_children = Arc::clone(&active_children);
    let watcher_stop = Arc::clone(&stop_flag);
    let watcher_board = board.clone();
    let watcher = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if watcher_cancel.is_cancelled() {
                break;
            }

            match read_signal() {
                Some(ref s) if s == "skip" => {
                    clear_signal();
                    let children = watcher_children.lock().await;
                    for (id, token) in children.iter() {
                        watcher_board.log(&format!("  \u{23ed} Skipping #{}", id));
                        token.cancel();
                    }
                }
                Some(ref s) if s == "stop" => {
                    clear_signal();
                    watcher_stop.store(true, Ordering::SeqCst);
                    watcher_board.log("  \u{23f9} Stop signal — finishing active downloads\u{2026}");
                }
                _ => {}
            }
        }
    });

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        if cancel.is_cancelled() || stop_flag.load(Ordering::SeqCst) {
            break;
        }

        handles.retain(|h| !h.is_finished());

        let permit = tokio::select! {
            p = semaphore.clone().acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => break,
            },
            _ = cancel.cancelled() => break,
        };

        if cancel.is_cancelled() || stop_flag.load(Ordering::SeqCst) {
            drop(permit);
            break;
        }

        let next = Queue::locked(|q| match q.next_pending().cloned() {
            Some(item) => {
                q.set_status(item.id, Status::Downloading);
                Ok(Some(item))
            }
            None => Ok(None),
        })?;

        let next = match next {
            Some(item) => item,
            None => {
                drop(permit);
                if handles.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        };

        let child = cancel.child_token();
        {
            let mut children = active_children.lock().await;
            children.push((next.id, child.clone()));
        }

        let cfg = cfg.clone();
        let board = board.clone();
        let completed = Arc::clone(&completed);
        let failed = Arc::clone(&failed);
        let skipped = Arc::clone(&skipped);
        let bytes_total = Arc::clone(&bytes_total);
        let active_children = Arc::clone(&active_children);
        let cancel_main = cancel.clone();
        let mega_gate = Arc::clone(&mega_gate);
        let mega_client = mega_client.clone();

        let handle = tokio::spawn(async move {
            let _permit = permit; // held until the task completes
            let item_id = next.id;
            let name = next.display_name();

            // MEGA creates its own parent directories once it knows the real
            // filename; for engine downloads the path is known up front.
            if !next.is_mega() {
                let output = next.resolve_output(&cfg);
                if let Some(parent) = std::path::Path::new(&output).parent()
                    && !parent.exists()
                {
                    std::fs::create_dir_all(parent).ok();
                }
            }

            // A lane is a live progress line on the board. If the board is
            // somehow full we still download, just without a line.
            let lane = board.claim(item_id, &name);
            let sink = match &lane {
                Some(l) => l.sink(),
                None => ui::silent(),
            };

            let elapsed_before = lane.as_ref().map(|l| l.elapsed());
            let result = run_item(&cfg, &next, child.clone(), sink, mega_client, mega_gate).await;
            let elapsed = elapsed_before
                .map(|_| lane.as_ref().map(|l| l.elapsed()).unwrap_or_default())
                .unwrap_or_default();

            // Free the progress line before we print the outcome.
            drop(lane);

            {
                let mut children = active_children.lock().await;
                children.retain(|(id, _)| *id != item_id);
            }

            // A cancelled child token with the main token still alive means
            // the user asked to skip this one file.
            let was_skipped = child.is_cancelled() && !cancel_main.is_cancelled();

            let downloaded = match &result {
                Ok(ItemOutcome::Completed { bytes, .. }) => Some(*bytes),
                _ => None,
            };

            // The lane was claimed with a placeholder for anything that only
            // learns its filename by asking. It has asked by now.
            let found = result
                .as_ref()
                .ok()
                .and_then(ItemOutcome::path)
                .and_then(file_name_of);

            // Always write the final status — even during Ctrl+C.
            let _ = Queue::locked(|q| {
                if cancel_main.is_cancelled() {
                    match &result {
                        Ok(ItemOutcome::Completed { .. })
                        | Ok(ItemOutcome::AlreadyPresent { .. }) => {
                            q.finish_item(item_id, Status::Complete, downloaded, found.as_deref())
                        }
                        _ => q.set_status(item_id, Status::Pending),
                    }
                } else if was_skipped {
                    q.set_status(item_id, Status::Skipped);
                } else {
                    match &result {
                        Ok(ItemOutcome::Completed { .. })
                        | Ok(ItemOutcome::AlreadyPresent { .. }) => {
                            q.finish_item(item_id, Status::Complete, downloaded, found.as_deref());
                        }
                        Ok(ItemOutcome::Cancelled) => q.set_status(item_id, Status::Skipped),
                        Err(e) => {
                            let attempts = q.attempts_so_far(item_id) + 1;
                            q.set_status(
                                item_id,
                                Status::Failed {
                                    reason: format!("{:#}", e),
                                    attempts,
                                },
                            );
                        }
                    }
                }
                Ok(())
            });

            if cancel_main.is_cancelled() {
                return;
            }

            let name = found.unwrap_or(name);

            if was_skipped {
                skipped.fetch_add(1, Ordering::Relaxed);
                board.file_skipped();
                board.log(&format!("  \u{23ed} #{}  {} — skipped", item_id, name));
                return;
            }

            match result {
                Ok(ItemOutcome::Completed { bytes, .. }) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                    bytes_total.fetch_add(bytes, Ordering::Relaxed);
                    board.file_completed(bytes);

                    let secs = elapsed.as_secs_f64();
                    let avg = if secs > 0.1 {
                        Some((bytes as f64 / secs) as u64)
                    } else {
                        None
                    };
                    board.log(&format!(
                        "  \u{2705} #{}  {}  {} in {} ({})",
                        item_id,
                        name,
                        ui::format_size(bytes),
                        ui::format_duration(elapsed.as_secs()),
                        ui::format_speed(avg),
                    ));
                }
                Ok(ItemOutcome::AlreadyPresent { path }) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                    board.file_completed(0);

                    // The claim is about a file on disk, so where there is one,
                    // say how big it is rather than asking to be believed.
                    let size = path
                        .as_deref()
                        .and_then(|p| std::fs::metadata(p).ok())
                        .map(|m| format!(" ({})", ui::format_size(m.len())))
                        .unwrap_or_default();
                    board.log(&format!(
                        "  \u{2713} #{}  {} — already downloaded{}",
                        item_id, name, size
                    ));
                }
                Ok(ItemOutcome::Cancelled) => {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    board.file_skipped();
                    board.log(&format!("  \u{23ed} #{}  {} — skipped", item_id, name));
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    board.file_failed();
                    board.log(&format!("  \u{274c} #{}  {} — {:#}", item_id, name, e));
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    watcher.abort();
    board.finish();
    let _ = renderer.await;

    // Ctrl+C — catch any truly orphaned tasks.
    if cancel.is_cancelled() {
        let _ = Queue::locked(|q| {
            q.requeue_in_flight();
            Ok(())
        });
        eprintln!();
        eprintln!(
            "  \u{26a0} Queue interrupted — progress saved. Run `rdm queue start` to resume."
        );
        return Ok(());
    }

    print_summary(
        completed.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed),
        bytes_total.load(Ordering::Relaxed),
        board.elapsed(),
    );

    Ok(())
}

fn print_summary(completed: u32, failed: u32, skipped: u32, bytes: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let avg = if secs > 0.1 && bytes > 0 {
        Some((bytes as f64 / secs) as u64)
    } else {
        None
    };

    let mut parts = vec![format!("{} completed", completed)];
    if failed > 0 {
        parts.push(format!("{} failed", failed));
    }
    if skipped > 0 {
        parts.push(format!("{} skipped", skipped));
    }

    eprintln!();
    eprintln!(
        "  Done in {} \u{b7} {}",
        ui::format_duration(elapsed.as_secs()),
        parts.join(" \u{b7} "),
    );
    if bytes > 0 {
        eprintln!(
            "  {} downloaded at {}",
            ui::format_size(bytes),
            ui::format_speed(avg)
        );
    }
    if failed > 0 {
        eprintln!("  Run `rdm queue retry failed` to requeue the failures.");
    }
}
