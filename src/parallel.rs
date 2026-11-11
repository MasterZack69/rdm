use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode, header};
use std::collections::HashMap;
use std::fmt;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, Notify};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::chunk::Chunk;
use crate::pressure::PressureController;
use crate::range_download::{self, DownloadStatus};
use crate::resume::{self, IdentityCheck, ResumeMetadata};
use crate::retry::{self, RetryConfig};
use crate::safe_file::{self, Access, Existing};

type ProgressCallback = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// Groups all parameters needed for a parallel download session.
pub struct ParallelDownloadCtx<'a> {
    pub client: &'a Client,
    pub url: &'a str,
    pub fetch_url: &'a str,
    pub output_path: &'a str,
    pub file_size: u64,
    /// Maximum number of simultaneous range requests. This is independent of
    /// the number of queued ranges in the experimental schedule.
    pub workers: usize,
    pub chunks: &'a [Chunk],
    pub retry_config: &'a RetryConfig,
    pub cancel: CancellationToken,
    pub identity: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub async fn download_parallel<F>(
    ctx: &ParallelDownloadCtx<'_>,
    progress_callback: Option<F>,
) -> Result<u64>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
    download_parallel_with_strategy(ctx, progress_callback, false).await
}

/// Downloads known-size range resources with an experimental, continuous
/// response scheduler. Logical pieces are used for ownership and resume state;
/// a worker's open-ended response may cross up to its piece budget of adjacent
/// pieces when it can safely claim them.
pub async fn download_parallel_adaptive_stream<F>(
    ctx: &ParallelDownloadCtx<'_>,
    progress_callback: Option<F>,
) -> Result<u64>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
    download_parallel_with_strategy(ctx, progress_callback, true).await
}

async fn download_parallel_with_strategy<F>(
    ctx: &ParallelDownloadCtx<'_>,
    progress_callback: Option<F>,
    adaptive_stream: bool,
) -> Result<u64>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
    if ctx.chunks.is_empty() {
        anyhow::bail!("No chunks provided for parallel download");
    }

    let temp_path = format!("{}.part", ctx.output_path);
    let meta_path = ResumeMetadata::meta_path(ctx.output_path);

    // Whether anything was at the destination before the transfer began.
    // Whether to overwrite was settled further up, at the moment that question
    // was asked; a file that appears while the download runs was never part of
    // that decision, and the rename below will not take it.
    let destination_existed = fs::symlink_metadata(ctx.output_path).await.is_ok();

    // The stream path can discover that the server ignored Range after some
    // workers have already started. Keep the callback shareable so a safe,
    // clean single-request fallback can retain the same progress sink.
    let progress_callback: Option<ProgressCallback> =
        progress_callback.map(|callback| Arc::new(callback) as ProgressCallback);
    let transfer_result = if adaptive_stream {
        parallel_inner_adaptive_stream(ctx, &temp_path, &meta_path, progress_callback.clone()).await
    } else {
        parallel_inner(ctx, &temp_path, &meta_path, progress_callback.clone()).await
    };

    let transfer_result = match transfer_result {
        Err(error) if adaptive_stream && error.downcast_ref::<RangeUnsupported>().is_some() => {
            eprintln!("  [Range] Server ignored byte ranges; restarting with one full response");
            // The adaptive workers may have safely written different owned
            // pieces before another response exposed the lack of range
            // support. Discard that mixed partial state and start the proven
            // one-request path from byte zero.
            resume::delete(&meta_path).await?;
            match fs::remove_file(&temp_path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(e).context("Failed to discard ranged partial file for fallback");
                }
            }

            let fallback_chunks = [Chunk {
                id: 1,
                start: 0,
                end: ctx.file_size - 1,
            }];
            let fallback_ctx = ParallelDownloadCtx {
                client: ctx.client,
                url: ctx.url,
                fetch_url: ctx.fetch_url,
                output_path: ctx.output_path,
                file_size: ctx.file_size,
                workers: 1,
                chunks: &fallback_chunks,
                retry_config: ctx.retry_config,
                cancel: ctx.cancel.clone(),
                identity: ctx.identity.clone(),
                etag: ctx.etag.clone(),
                last_modified: ctx.last_modified.clone(),
            };
            parallel_inner(&fallback_ctx, &temp_path, &meta_path, progress_callback).await
        }
        other => other,
    };

    match transfer_result {
        Ok(total) => {
            resume::delete(&meta_path).await?;
            let temp = Path::new(&temp_path);
            let final_path = Path::new(ctx.output_path);

            // The chunk workers have each closed their own handle, so
            // publication reopens the staged file — and validates it again
            // before linking it into place, because what gets published is
            // that descriptor and not the `.part` name it was found under.
            if destination_existed {
                safe_file::publish_replacing(temp, final_path)
            } else {
                safe_file::publish_no_replace(temp, final_path)
            }
            .with_context(|| {
                format!("Failed to publish '{}' as '{}'", temp_path, ctx.output_path)
            })?;
            Ok(total)
        }
        Err(e) => Err(e),
    }
}

fn chunks_from_metadata(meta: &ResumeMetadata) -> Vec<Chunk> {
    meta.chunks
        .iter()
        .map(|c| Chunk {
            id: c.id,
            start: c.start,
            end: c.end,
        })
        .collect()
}

async fn parallel_inner(
    ctx: &ParallelDownloadCtx<'_>,
    temp_path: &str,
    meta_path: &str,
    progress_callback: Option<ProgressCallback>,
) -> Result<u64> {
    use std::collections::VecDeque;

    let meta = load_or_create_metadata(
        meta_path,
        temp_path,
        ctx.url,
        ctx.identity.as_deref(),
        ctx.file_size,
        ctx.chunks,
        ctx.etag.as_deref(),
        ctx.last_modified.as_deref(),
    )
    .await?;

    let chunks = chunks_from_metadata(&meta);

    ensure_file_allocated(temp_path, ctx.file_size, &chunks, &meta).await?;

    let shared_meta = Arc::new(Mutex::new(meta));

    let mut queue: VecDeque<Chunk> = chunks.iter().cloned().collect();

    let chunk_counters: Vec<(u32, Arc<AtomicU64>)> = {
        let meta_guard = shared_meta.lock().await;
        chunks
            .iter()
            .map(|c| {
                let completed = meta_guard
                    .chunks
                    .iter()
                    .find(|s| s.id == c.id)
                    .map(|s| s.completed)
                    .unwrap_or(0);
                (c.id, Arc::new(AtomicU64::new(completed)))
            })
            .collect()
    };

    let initial_completed: u64 = chunk_counters
        .iter()
        .map(|(_, c)| c.load(Ordering::Relaxed))
        .sum();

    // One view of how the server is coping, shared by every worker and by
    // the loop that hands out ranges.
    let pressure = Arc::new(PressureController::new(ctx.workers.max(1)));
    let background_stop = CancellationToken::new();
    // If the caller drops this future, do not leave the background loops alive.
    let _stop_on_drop = background_stop.clone().drop_guard();

    let autosave_handle = spawn_autosave(
        meta_path.to_owned(),
        Arc::clone(&shared_meta),
        chunk_counters
            .iter()
            .map(|(id, c)| (*id, Arc::clone(c)))
            .collect(),
        background_stop.clone(),
    );

    let monitor_handle = spawn_progress_monitor(
        progress_callback,
        chunk_counters.clone(),
        background_stop.clone(),
        ctx.file_size,
    );

    let mut join_set: JoinSet<Result<u64>> = JoinSet::new();
    let mut total_bytes: u64 = initial_completed;

    let worker_result: Result<()> = async {
        while !queue.is_empty() || !join_set.is_empty() {
            // A server that asked for a pause gets one before any further range
            // is assigned. With nothing in flight there is nothing else to wait
            // on, so the loop waits out the pause itself rather than spinning.
            if join_set.is_empty()
                && let Some(wait) = pressure.pause_remaining()
            {
                tokio::select! {
                    biased;
                    _ = ctx.cancel.cancelled() => {
                        anyhow::bail!("Cancelled while waiting out the server's back-off");
                    }
                    _ = tokio::time::sleep(wait) => {}
                }
            }

            // Read fresh each time round: the controller may have lowered it
            // while these workers were running.
            let active_workers = pressure.limit();

            while join_set.len() < active_workers && !queue.is_empty() && !pressure.is_paused() {
                let chunk = queue.pop_front().unwrap();

                let client = ctx.client.clone();
                let url = ctx.fetch_url.to_owned();
                let path = temp_path.to_owned();
                let cancel = ctx.cancel.clone();
                let config = ctx.retry_config.clone();
                let chunk_progress = chunk_counters
                    .iter()
                    .find(|(id, _)| *id == chunk.id)
                    .unwrap()
                    .1
                    .clone();

                let pressure = Arc::clone(&pressure);

                join_set.spawn(async move {
                    download_chunk_with_retry(
                        &client,
                        &url,
                        &path,
                        &chunk,
                        &config,
                        chunk_progress,
                        cancel,
                        &pressure,
                    )
                    .await
                });
            }

            // Paused with nothing spawned yet: go back and wait it out.
            if join_set.is_empty() {
                continue;
            }

            match join_set.join_next().await {
                Some(Ok(Ok(bytes))) => total_bytes += bytes,
                Some(Ok(Err(e))) => return Err(e),
                Some(Err(e)) => return Err(anyhow::anyhow!("Worker panic: {}", e)),
                None => break,
            }
        }

        Ok(())
    }
    .await;

    // Signal both loops together. On failure, let in-flight workers observe
    // cancellation and flush before taking the final resume snapshot.
    if worker_result.is_err() {
        ctx.cancel.cancel();
        while join_set.join_next().await.is_some() {}
    }
    background_stop.cancel();
    if let Some(h) = monitor_handle {
        let _ = h.await;
    }
    // A save already in progress must finish before the manifest is deleted
    // or the final failure snapshot is written.
    let _ = autosave_handle.await;

    let result = worker_result.and_then(|()| {
        if total_bytes != ctx.file_size {
            anyhow::bail!(
                "Total bytes mismatch: expected {} but downloaded {}",
                ctx.file_size,
                total_bytes
            );
        }
        Ok(total_bytes)
    });
    if result.is_err() {
        save_progress(meta_path, &shared_meta, &chunk_counters).await;
    }
    result
}

#[derive(Debug)]
struct RangeUnsupported;

impl fmt::Display for RangeUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("server ignored the byte range request")
    }
}

impl std::error::Error for RangeUnsupported {}

/// Logical pieces have independent resume counters and a single owner at a
/// time. The owner map is only locked when a response crosses a piece boundary
/// or a worker claims/releases work, never for every network buffer.
struct AdaptiveScheduler {
    pieces: Vec<Chunk>,
    progress: Vec<Arc<AtomicU64>>,
    owners: Mutex<Vec<Option<usize>>>,
    changed: Notify,
}

/// Maximum number of logical pieces one open response may acquire before it
/// yields. This leaves large unclaimed spans available to faster idle workers
/// while still amortizing requests across adjacent 16 MiB pieces.
const ADAPTIVE_RESPONSE_PIECE_LIMIT: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PieceClaim {
    Owned,
    Acquired,
    Blocked,
    Yield,
}

impl AdaptiveScheduler {
    fn new(pieces: Vec<Chunk>, progress: Vec<Arc<AtomicU64>>) -> Self {
        let owners = vec![None; pieces.len()];
        Self {
            pieces,
            progress,
            owners: Mutex::new(owners),
            changed: Notify::new(),
        }
    }

    async fn seed_workers(&self, worker_limit: usize) -> Vec<(usize, usize)> {
        let mut owners = self.owners.lock().await;
        let available: Vec<usize> = self
            .pieces
            .iter()
            .enumerate()
            .filter_map(|(index, piece)| {
                (self.progress[index].load(Ordering::SeqCst) < piece_len(piece)).then_some(index)
            })
            .collect();
        let count = available.len().min(worker_limit.max(1));
        if count == 0 {
            return Vec::new();
        }

        // Seed at evenly spaced points through the remaining file. As each
        // response proceeds, the worker claims adjacent pieces until it meets
        // another owner. This prevents all connections from starting at byte
        // zero while keeping the policy deterministic across runs.
        (0..count)
            .map(|worker| {
                let position = if count == 1 {
                    0
                } else {
                    worker * (available.len() - 1) / (count - 1)
                };
                let piece_index = available[position];
                owners[piece_index] = Some(worker);
                (worker, piece_index)
            })
            .collect()
    }

    async fn claim_for_response(
        &self,
        worker: usize,
        index: usize,
        response_owned_pieces: usize,
        response_piece_limit: usize,
    ) -> PieceClaim {
        let Some(piece) = self.pieces.get(index) else {
            return PieceClaim::Blocked;
        };
        let mut owners = self.owners.lock().await;
        if self.progress[index].load(Ordering::SeqCst) >= piece_len(piece) {
            return PieceClaim::Blocked;
        }
        match owners[index] {
            Some(owner) if owner == worker => PieceClaim::Owned,
            Some(_) => PieceClaim::Blocked,
            None => {
                if response_owned_pieces >= response_piece_limit {
                    return PieceClaim::Yield;
                }
                owners[index] = Some(worker);
                PieceClaim::Acquired
            }
        }
    }

    async fn claim_largest_region(
        &self,
        worker: usize,
        cancel: &CancellationToken,
    ) -> Result<Option<usize>> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            let mut owners = self.owners.lock().await;
            let mut best: Option<(usize, usize, u64)> = None;
            let mut index = 0;

            while index < self.pieces.len() {
                let is_available = |index: usize| {
                    owners[index].is_none()
                        && self.progress[index].load(Ordering::SeqCst)
                            < piece_len(&self.pieces[index])
                };
                if !is_available(index) {
                    index += 1;
                    continue;
                }

                let start = index;
                while index < self.pieces.len() && is_available(index) {
                    index += 1;
                }
                let end = index;
                let span_bytes = self.pieces[end - 1].end - self.pieces[start].start + 1;
                if best.is_none_or(|(_, _, best_bytes)| span_bytes > best_bytes) {
                    best = Some((start, end, span_bytes));
                }
            }

            if let Some((start, end, span_bytes)) = best {
                // Starting near the middle leaves work on both sides for
                // other idle workers. The response claims forward through
                // this gap, up to its per-response piece budget.
                let midpoint = self.pieces[start].start + span_bytes / 2;
                let selected = (start..end)
                    .find(|index| midpoint <= self.pieces[*index].end)
                    .unwrap_or(end - 1);
                owners[selected] = Some(worker);
                drop(owners);
                self.changed.notify_waiters();
                return Ok(Some(selected));
            }

            let work_is_owned = owners.iter().any(Option::is_some);
            drop(owners);
            if !work_is_owned {
                return Ok(None);
            }

            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    anyhow::bail!("Cancelled while waiting for an adaptive work region");
                }
                _ = &mut changed => {}
            }
        }
    }

    async fn checkpoint_piece(&self, worker: usize, index: usize, completed: u64) -> Result<()> {
        let piece = &self.pieces[index];
        let mut owners = self.owners.lock().await;
        if owners[index] != Some(worker) {
            anyhow::bail!(
                "Adaptive piece #{} lost its owner before checkpoint",
                piece.id
            );
        }
        let completed = completed.min(piece_len(piece));
        self.progress[index].store(completed, Ordering::SeqCst);
        if completed == piece_len(piece) {
            owners[index] = None;
            drop(owners);
            self.changed.notify_waiters();
        }
        Ok(())
    }

    async fn retain_first_incomplete(&self, worker: usize) -> Option<usize> {
        let mut owners = self.owners.lock().await;
        let keep = self
            .pieces
            .iter()
            .enumerate()
            .find(|(index, piece)| {
                owners[*index] == Some(worker)
                    && self.progress[*index].load(Ordering::SeqCst) < piece_len(piece)
            })
            .map(|(index, _)| index);
        for (index, owner) in owners.iter_mut().enumerate() {
            if *owner == Some(worker) && Some(index) != keep {
                *owner = None;
            }
        }
        drop(owners);
        self.changed.notify_waiters();
        keep
    }

    async fn release_worker(&self, worker: usize) {
        let mut owners = self.owners.lock().await;
        for owner in owners.iter_mut() {
            if *owner == Some(worker) {
                *owner = None;
            }
        }
        drop(owners);
        self.changed.notify_waiters();
    }

    fn counter_list(&self) -> Vec<(u32, Arc<AtomicU64>)> {
        self.pieces
            .iter()
            .zip(&self.progress)
            .map(|(piece, progress)| (piece.id, Arc::clone(progress)))
            .collect()
    }
}

fn piece_len(piece: &Chunk) -> u64 {
    piece.end - piece.start + 1
}

struct RequestGate {
    active: AtomicUsize,
    changed: Notify,
    pressure: Arc<PressureController>,
}

impl RequestGate {
    fn new(pressure: Arc<PressureController>) -> Arc<Self> {
        Arc::new(Self {
            active: AtomicUsize::new(0),
            changed: Notify::new(),
            pressure,
        })
    }

    async fn enter(self: &Arc<Self>, cancel: &CancellationToken) -> Result<RequestPermit> {
        loop {
            self.pressure.wait_while_paused(cancel).await;
            let changed = self.changed.notified();
            if cancel.is_cancelled() {
                anyhow::bail!("Cancelled while waiting for an adaptive request slot");
            }

            let active = self.active.load(Ordering::Acquire);
            if active < self.pressure.limit()
                && self
                    .active
                    .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                if self.active.load(Ordering::Acquire) <= self.pressure.limit()
                    && !self.pressure.is_paused()
                {
                    return Ok(RequestPermit(Arc::clone(self)));
                }
                self.active.fetch_sub(1, Ordering::AcqRel);
                self.changed.notify_one();
                continue;
            }

            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    anyhow::bail!("Cancelled while waiting for an adaptive request slot");
                }
                _ = changed => {}
            }
        }
    }
}

struct RequestPermit(Arc<RequestGate>);

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_one();
    }
}

#[derive(Default)]
struct AdaptiveStats {
    received_but_discarded: AtomicU64,
}

async fn parallel_inner_adaptive_stream(
    ctx: &ParallelDownloadCtx<'_>,
    temp_path: &str,
    meta_path: &str,
    progress_callback: Option<ProgressCallback>,
) -> Result<u64> {
    let meta = load_or_create_metadata(
        meta_path,
        temp_path,
        ctx.url,
        ctx.identity.as_deref(),
        ctx.file_size,
        ctx.chunks,
        ctx.etag.as_deref(),
        ctx.last_modified.as_deref(),
    )
    .await?;
    let pieces = chunks_from_metadata(&meta);
    ensure_file_allocated(temp_path, ctx.file_size, &pieces, &meta).await?;

    let progress: Vec<Arc<AtomicU64>> = meta
        .chunks
        .iter()
        .map(|piece| Arc::new(AtomicU64::new(piece.completed)))
        .collect();
    let scheduler = Arc::new(AdaptiveScheduler::new(pieces, progress));
    let counters = scheduler.counter_list();
    let initial_completed: u64 = counters
        .iter()
        .map(|(_, counter)| counter.load(Ordering::SeqCst))
        .sum();
    let seeds = scheduler.seed_workers(ctx.workers).await;

    let pressure = Arc::new(PressureController::new(seeds.len().max(1)));
    let gate = RequestGate::new(Arc::clone(&pressure));
    let stats = Arc::new(AdaptiveStats::default());
    // A worker failure must stop its siblings without cancelling the caller's
    // token. Caller cancellation still propagates into this child token.
    let worker_cancel = ctx.cancel.child_token();
    let background_stop = CancellationToken::new();
    let _stop_on_drop = background_stop.clone().drop_guard();
    let shared_meta = Arc::new(Mutex::new(meta));

    let autosave_handle = spawn_autosave(
        meta_path.to_owned(),
        Arc::clone(&shared_meta),
        counters.clone(),
        background_stop.clone(),
    );
    let monitor_handle = spawn_progress_monitor(
        progress_callback,
        counters.clone(),
        background_stop.clone(),
        ctx.file_size,
    );

    let mut join_set: JoinSet<Result<()>> = JoinSet::new();
    for (worker, seed) in seeds {
        let client = ctx.client.clone();
        let url = ctx.fetch_url.to_owned();
        let path = temp_path.to_owned();
        let identity = ctx.etag.clone();
        let last_modified = ctx.last_modified.clone();
        let config = ctx.retry_config.clone();
        let scheduler = Arc::clone(&scheduler);
        let pressure = Arc::clone(&pressure);
        let gate = Arc::clone(&gate);
        let cancel = worker_cancel.clone();
        let stats = Arc::clone(&stats);
        let file_size = ctx.file_size;

        join_set.spawn(async move {
            adaptive_worker(
                &client,
                &url,
                &path,
                file_size,
                identity.as_deref(),
                last_modified.as_deref(),
                &config,
                scheduler,
                worker,
                seed,
                cancel,
                pressure,
                gate,
                stats,
            )
            .await
        });
    }

    let mut first_error = None;
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                    worker_cancel.cancel();
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!("Adaptive worker panic: {error}"));
                    worker_cancel.cancel();
                }
            }
        }
    }

    background_stop.cancel();
    if let Some(handle) = monitor_handle {
        let _ = handle.await;
    }
    let _ = autosave_handle.await;

    let total_completed: u64 = counters
        .iter()
        .map(|(_, counter)| counter.load(Ordering::SeqCst))
        .sum();

    if let Some(error) = first_error {
        save_progress(meta_path, &shared_meta, &counters).await;
        return Err(error);
    }
    if total_completed != ctx.file_size {
        save_progress(meta_path, &shared_meta, &counters).await;
        anyhow::bail!(
            "Adaptive schedule stopped with {} of {} bytes complete",
            total_completed,
            ctx.file_size
        );
    }

    let discarded = stats.received_but_discarded.load(Ordering::Relaxed);
    if discarded > 0 {
        eprintln!(
            "  [Adaptive] Discarded {} already-received bytes at ownership boundaries",
            crate::ui::format_size(discarded)
        );
    }
    debug_assert!(total_completed >= initial_completed);
    Ok(total_completed)
}

#[allow(clippy::too_many_arguments)]
async fn adaptive_worker(
    client: &Client,
    url: &str,
    file_path: &str,
    file_size: u64,
    etag: Option<&str>,
    last_modified: Option<&str>,
    config: &RetryConfig,
    scheduler: Arc<AdaptiveScheduler>,
    worker: usize,
    initial_piece: usize,
    cancel: CancellationToken,
    pressure: Arc<PressureController>,
    gate: Arc<RequestGate>,
    stats: Arc<AdaptiveStats>,
) -> Result<()> {
    let file = safe_file::open_guarded(
        Path::new(file_path),
        Existing::OpenExisting,
        Access::ReadWrite,
        safe_file::DEFAULT_FILE_MODE,
    )
    .with_context(|| format!("Failed to open adaptive partial file: {file_path}"))?;
    let mut writer = BufWriter::with_capacity(512 * 1024, tokio::fs::File::from_std(file));

    let mut next_piece = Some(initial_piece);
    while let Some(piece) = next_piece {
        if cancel.is_cancelled() {
            scheduler.release_worker(worker).await;
            anyhow::bail!("Adaptive worker cancelled before claiming more work");
        }
        let request_result = adaptive_request_with_retry(
            client,
            url,
            file_size,
            etag,
            last_modified,
            config,
            &scheduler,
            &mut writer,
            worker,
            piece,
            &cancel,
            &pressure,
            &gate,
            &stats,
        )
        .await;
        if let Err(error) = request_result {
            scheduler.release_worker(worker).await;
            return Err(error);
        }

        if cancel.is_cancelled() {
            scheduler.release_worker(worker).await;
            anyhow::bail!("Adaptive worker cancelled after its response");
        }
        next_piece = scheduler.claim_largest_region(worker, &cancel).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn adaptive_request_with_retry(
    client: &Client,
    url: &str,
    file_size: u64,
    etag: Option<&str>,
    last_modified: Option<&str>,
    config: &RetryConfig,
    scheduler: &AdaptiveScheduler,
    writer: &mut BufWriter<tokio::fs::File>,
    worker: usize,
    initial_piece: usize,
    cancel: &CancellationToken,
    pressure: &PressureController,
    gate: &Arc<RequestGate>,
    stats: &AdaptiveStats,
) -> Result<()> {
    let mut anchor = initial_piece;
    for attempt in 0..=config.max_retries {
        pressure.wait_while_paused(cancel).await;
        if cancel.is_cancelled() {
            scheduler.release_worker(worker).await;
            anyhow::bail!(
                "Adaptive range request cancelled before attempt {}",
                attempt + 1
            );
        }

        let piece = &scheduler.pieces[anchor];
        let completed = scheduler.progress[anchor].load(Ordering::SeqCst);
        if completed >= piece_len(piece) {
            scheduler.release_worker(worker).await;
            return Ok(());
        }
        let request_start = piece.start + completed;
        let permit = match gate.enter(cancel).await {
            Ok(permit) => permit,
            Err(error) => {
                scheduler.release_worker(worker).await;
                return Err(error);
            }
        };

        let range_value = format!("bytes={request_start}-");
        let if_range = resume::strong_validator(etag, last_modified);
        let mut request = client.get(url).header(header::RANGE, &range_value);
        if let Some(validator) = if_range {
            request = request.header(header::IF_RANGE, validator);
        }

        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                drop(permit);
                scheduler.release_worker(worker).await;
                anyhow::bail!("Adaptive range request cancelled before response");
            }
            result = request.send() => match result {
                Ok(response) => response,
                Err(error) => {
                    drop(permit);
                    let error = anyhow::Error::new(error.without_url())
                        .context(format!("Range GET failed for {range_value}"));
                    if retry_adaptive_request(
                        &error,
                        attempt,
                        config,
                        cancel,
                        pressure,
                        scheduler.pieces[anchor].id,
                    )
                    .await?
                    {
                        anchor = scheduler.retain_first_incomplete(worker).await.unwrap_or(anchor);
                        continue;
                    }
                    scheduler.release_worker(worker).await;
                    return Err(error);
                }
            }
        };

        let status = response.status();
        let response_headers = response.headers();
        if status == StatusCode::OK {
            let validator_matches = response_validator_matches(response_headers, if_range);
            drop(response);
            drop(permit);
            scheduler.release_worker(worker).await;
            if if_range.is_some() && !validator_matches {
                anyhow::bail!(
                    "Server returned a full response after If-Range; the representation may have changed"
                );
            }
            return Err(anyhow::Error::new(RangeUnsupported));
        }
        if status != StatusCode::PARTIAL_CONTENT {
            if retry::is_transient_status(status) {
                let retry_after = response
                    .headers()
                    .get(header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| retry::parse_retry_after(value, unix_now()));
                drop(response);
                drop(permit);
                let error = anyhow::Error::new(retry::TransientError::after(
                    format!(
                        "Transient HTTP {} for range {}",
                        status.as_u16(),
                        range_value
                    ),
                    retry_after,
                ));
                if retry_adaptive_request(
                    &error,
                    attempt,
                    config,
                    cancel,
                    pressure,
                    scheduler.pieces[anchor].id,
                )
                .await?
                {
                    anchor = scheduler
                        .retain_first_incomplete(worker)
                        .await
                        .unwrap_or(anchor);
                    continue;
                }
                scheduler.release_worker(worker).await;
                return Err(error);
            }
            drop(response);
            drop(permit);
            scheduler.release_worker(worker).await;
            anyhow::bail!(
                "Permanent HTTP error for range {}: {} {}",
                range_value,
                status.as_u16(),
                status.canonical_reason().unwrap_or("Unknown")
            );
        }

        if if_range
            .is_some_and(|expected| !response_validator_matches(response.headers(), Some(expected)))
        {
            drop(response);
            drop(permit);
            scheduler.release_worker(worker).await;
            anyhow::bail!(
                "Server returned a range without the expected If-Range validator; representation continuity is unproven"
            );
        }

        let (response_end, response_total) =
            match parse_open_content_range(response.headers(), request_start, file_size) {
                Ok(range) => range,
                Err(error) => {
                    drop(response);
                    drop(permit);
                    scheduler.release_worker(worker).await;
                    return Err(error);
                }
            };
        debug_assert_eq!(response_total, file_size);
        if let Some(value) = response.headers().get(header::CONTENT_LENGTH) {
            let length = value
                .to_str()
                .context("Content-Length is not valid UTF-8")?
                .parse::<u64>()
                .context("Content-Length is not a valid byte count")?;
            if length != response_end - request_start + 1 {
                drop(response);
                drop(permit);
                scheduler.release_worker(worker).await;
                anyhow::bail!("Range Content-Length disagrees with Content-Range");
            }
        }

        let body_result = process_adaptive_body(
            response,
            scheduler,
            writer,
            worker,
            AdaptiveRequest {
                anchor_piece: anchor,
                start: request_start,
                end: response_end,
            },
            cancel,
            stats,
        )
        .await;
        drop(permit);

        match body_result {
            Ok(AdaptiveBodyEnd::Cancelled) => {
                scheduler.release_worker(worker).await;
                anyhow::bail!("Adaptive range response cancelled");
            }
            Ok(AdaptiveBodyEnd::Boundary | AdaptiveBodyEnd::ResponseComplete) => {
                scheduler.release_worker(worker).await;
                if let Some(restored) = pressure.record_success() {
                    eprintln!(
                        "  \u{2713} Server steady again \u{2014} parallel connections back to {}",
                        restored
                    );
                }
                return Ok(());
            }
            Err(error) => {
                let keep = scheduler.retain_first_incomplete(worker).await;
                if keep.is_none() {
                    scheduler.release_worker(worker).await;
                    if retry::is_retryable(&error) {
                        return Ok(());
                    }
                    return Err(error);
                }
                anchor = keep.unwrap();
                if retry_adaptive_request(
                    &error,
                    attempt,
                    config,
                    cancel,
                    pressure,
                    scheduler.pieces[anchor].id,
                )
                .await?
                {
                    continue;
                }
                scheduler.release_worker(worker).await;
                return Err(error.context(format!(
                    "Adaptive piece #{} failed permanently after {} attempt(s)",
                    scheduler.pieces[anchor].id,
                    attempt + 1
                )));
            }
        }
    }

    scheduler.release_worker(worker).await;
    anyhow::bail!("Adaptive range exhausted its retry attempts")
}

#[derive(Debug)]
enum AdaptiveBodyEnd {
    Boundary,
    ResponseComplete,
    Cancelled,
}

struct AdaptiveRequest {
    anchor_piece: usize,
    start: u64,
    end: u64,
}

async fn process_adaptive_body(
    response: reqwest::Response,
    scheduler: &AdaptiveScheduler,
    writer: &mut BufWriter<tokio::fs::File>,
    worker: usize,
    request: AdaptiveRequest,
    cancel: &CancellationToken,
    stats: &AdaptiveStats,
) -> Result<AdaptiveBodyEnd> {
    let AdaptiveRequest {
        anchor_piece,
        start: request_start,
        end: response_end,
    } = request;
    let expected_len = response_end - request_start + 1;
    writer
        .seek(SeekFrom::Start(request_start))
        .await
        .with_context(|| format!("Failed to seek to adaptive offset {request_start}"))?;

    let mut stream = response.bytes_stream();
    let mut cursor = request_start;
    let mut received = 0u64;
    let mut touched = HashMap::<usize, u64>::new();
    let mut response_owned = HashMap::from([(anchor_piece, ())]);
    let mut stopped_at_boundary = false;
    let mut was_cancelled = false;
    let mut body_error = None;

    'body: loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                was_cancelled = true;
                break;
            }
            next = stream.next() => next,
        };

        match next {
            Some(Ok(data)) => {
                let remaining = expected_len.saturating_sub(received);
                let allowed = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(data.len());
                let mut buffer_offset = 0usize;
                while buffer_offset < allowed {
                    let piece_index = scheduler.pieces.partition_point(|piece| piece.end < cursor);
                    if piece_index >= scheduler.pieces.len() {
                        stats
                            .received_but_discarded
                            .fetch_add((data.len() - buffer_offset) as u64, Ordering::Relaxed);
                        stopped_at_boundary = true;
                        break 'body;
                    }

                    let claim = scheduler
                        .claim_for_response(
                            worker,
                            piece_index,
                            response_owned.len(),
                            ADAPTIVE_RESPONSE_PIECE_LIMIT,
                        )
                        .await;
                    match claim {
                        PieceClaim::Owned => {}
                        PieceClaim::Acquired => {
                            response_owned.insert(piece_index, ());
                        }
                        PieceClaim::Blocked | PieceClaim::Yield => {
                            stats
                                .received_but_discarded
                                .fetch_add((data.len() - buffer_offset) as u64, Ordering::Relaxed);
                            stopped_at_boundary = true;
                            break 'body;
                        }
                    }

                    let piece = &scheduler.pieces[piece_index];
                    let available_in_piece = piece.end + 1 - cursor;
                    let segment_len = (allowed - buffer_offset)
                        .min(usize::try_from(available_in_piece).unwrap_or(usize::MAX));
                    let segment_end = cursor + segment_len as u64;
                    if let Err(error) = writer
                        .write_all(&data[buffer_offset..buffer_offset + segment_len])
                        .await
                    {
                        body_error = Some(
                            anyhow::Error::new(error)
                                .context(format!("Write failed at adaptive offset {cursor}")),
                        );
                        break 'body;
                    }

                    touched
                        .entry(piece_index)
                        .and_modify(|completed| {
                            *completed = (*completed).max(segment_end - piece.start)
                        })
                        .or_insert(segment_end - piece.start);
                    cursor = segment_end;
                    received += segment_len as u64;
                    buffer_offset += segment_len;

                    if cursor == piece.end + 1 {
                        touched.insert(piece_index, piece_len(piece));
                        checkpoint_touched(writer, scheduler, worker, &mut touched).await?;
                        if response_owned.len() >= ADAPTIVE_RESPONSE_PIECE_LIMIT
                            && cursor <= response_end
                        {
                            stats
                                .received_but_discarded
                                .fetch_add((data.len() - buffer_offset) as u64, Ordering::Relaxed);
                            stopped_at_boundary = true;
                            break 'body;
                        }
                    }
                }

                if data.len() > allowed {
                    body_error = Some(anyhow::Error::msg(format!(
                        "Server sent excess data for adaptive range beginning at {request_start}"
                    )));
                    break;
                }
            }
            Some(Err(error)) => {
                body_error = Some(anyhow::Error::new(error.without_url()).context(format!(
                    "Adaptive response stream failed at byte {}",
                    cursor - request_start
                )));
                break;
            }
            None if received == expected_len => break,
            None => {
                body_error = Some(anyhow::Error::new(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "Truncated adaptive response: expected {expected_len} bytes, received {received}"
                    ),
                )));
                break;
            }
        }
    }

    // Dropping the stream stops the response before releasing ownership. Any
    // bytes already accepted by the buffered writer are flushed and synced
    // before their per-piece progress reaches the resume manifest.
    drop(stream);
    checkpoint_touched(writer, scheduler, worker, &mut touched).await?;

    if was_cancelled {
        return Ok(AdaptiveBodyEnd::Cancelled);
    }
    if let Some(error) = body_error {
        return Err(error);
    }
    if stopped_at_boundary {
        return Ok(AdaptiveBodyEnd::Boundary);
    }
    Ok(AdaptiveBodyEnd::ResponseComplete)
}

async fn checkpoint_touched(
    writer: &mut BufWriter<tokio::fs::File>,
    scheduler: &AdaptiveScheduler,
    worker: usize,
    touched: &mut HashMap<usize, u64>,
) -> Result<()> {
    if touched.is_empty() {
        return Ok(());
    }
    writer
        .flush()
        .await
        .context("Failed to flush adaptive piece data")?;
    writer
        .get_ref()
        .sync_data()
        .await
        .context("Failed to sync adaptive piece data before checkpoint")?;

    for (index, completed) in touched.drain() {
        scheduler.checkpoint_piece(worker, index, completed).await?;
    }
    Ok(())
}

async fn retry_adaptive_request(
    error: &anyhow::Error,
    attempt: u32,
    config: &RetryConfig,
    cancel: &CancellationToken,
    pressure: &PressureController,
    piece_id: u32,
) -> Result<bool> {
    if !retry::is_retryable(error) || attempt >= config.max_retries {
        return Ok(false);
    }

    let asked_for = retry::retry_after(error);
    let reaction = pressure.record_retryable(asked_for);
    if let Some(from) = reaction.reduced_from {
        eprintln!(
            "  \u{26a0} Server overloaded \u{2014} reducing parallel connections: {} \u{2192} {}",
            from, reaction.limit
        );
    }
    if let Some(pause) = reaction.pause {
        eprintln!(
            "  \u{23f8} Server asked for {:.0}s \u{2014} holding every connection",
            pause.as_secs_f64()
        );
    }
    let backoff = config.delay_for_attempt(attempt);
    let delay = asked_for.map_or(backoff, |wait| wait.max(backoff));
    eprintln!(
        "   \u{26a0} Adaptive piece #{}: {}/{} failed, retry in {:.1}s \u{2014} {}",
        piece_id,
        attempt + 1,
        config.max_retries + 1,
        delay.as_secs_f64(),
        short_error(error),
    );

    tokio::select! {
        biased;
        _ = cancel.cancelled() => anyhow::bail!("Adaptive range cancelled during retry backoff"),
        _ = tokio::time::sleep(delay) => Ok(true),
    }
}

fn parse_open_content_range(
    headers: &header::HeaderMap,
    expected_start: u64,
    expected_total: u64,
) -> Result<(u64, u64)> {
    let value = headers
        .get(header::CONTENT_RANGE)
        .context("Server returned 206 without Content-Range")?
        .to_str()
        .context("Content-Range is not valid UTF-8")?;
    let range = value
        .strip_prefix("bytes ")
        .with_context(|| format!("Unexpected Content-Range format: '{value}'"))?;
    let (bounds, total) = range
        .split_once('/')
        .with_context(|| format!("Content-Range missing '/': '{value}'"))?;
    let (start, end) = bounds
        .split_once('-')
        .with_context(|| format!("Content-Range missing '-': '{value}'"))?;
    let start = start
        .parse::<u64>()
        .with_context(|| format!("Invalid start in Content-Range: '{value}'"))?;
    let end = end
        .parse::<u64>()
        .with_context(|| format!("Invalid end in Content-Range: '{value}'"))?;
    let total = total
        .parse::<u64>()
        .with_context(|| format!("Invalid total in Content-Range: '{value}'"))?;

    if start != expected_start || end < start || total != expected_total || end >= total {
        anyhow::bail!(
            "Content-Range mismatch: requested {}- through {}, got '{}'",
            expected_start,
            expected_total - 1,
            value
        );
    }
    Ok((end, total))
}

fn response_validator_matches(headers: &header::HeaderMap, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    response_validator(headers).is_some_and(|actual| actual == expected)
}

fn response_validator(headers: &header::HeaderMap) -> Option<&str> {
    let etag = headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok());
    let last_modified = headers
        .get(header::LAST_MODIFIED)
        .and_then(|value| value.to_str().ok());
    resume::strong_validator(etag, last_modified)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
async fn load_or_create_metadata(
    meta_path: &str,
    temp_path: &str,
    url: &str,
    identity: Option<&str>,
    file_size: u64,
    chunks: &[Chunk],
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> Result<ResumeMetadata> {
    if let Ok(existing) = resume::load(meta_path).await {
        let identity_check = existing.compare_server_identity(etag, last_modified);
        let saved_chunks = chunks_from_metadata(&existing);

        // The saved ranges are the resume contract. In particular, do not
        // discard a valid fixed-layout manifest just because the experimental
        // scheduler would choose a different layout on this invocation.
        if valid_saved_layout(&existing, url, identity, file_size, &saved_chunks)
            && identity_check == IdentityCheck::Match
        {
            eprintln!(
                "  [Resume] Using existing chunk layout ({} chunks)",
                existing.chunks.len()
            );
            return Ok(existing);
        }

        match identity_check {
            IdentityCheck::Changed => {
                eprintln!(
                    "  [Resume] Server file changed (ETag/Last-Modified mismatch), restarting"
                );
            }
            // The gap this closes: a missing validator on either side used to
            // pass the check, so bytes from a changed resource could be
            // written into the old file's chunks.
            IdentityCheck::Unknown => {
                eprintln!(
                    "  [Resume] Cannot prove the server still has the same file \
                     (no usable ETag/Last-Modified), restarting"
                );
            }
            IdentityCheck::Match => {
                eprintln!("  [Resume] Saved state is for a different source, restarting");
            }
        }
        let _ = resume::delete(meta_path).await;
        let _ = fs::remove_file(temp_path).await;
    }

    let mut meta = resume::create_new(url.to_owned(), file_size, chunks);
    meta.identity = identity.map(|s| s.to_owned());
    meta.etag = etag.map(|s| s.to_owned());
    meta.last_modified = last_modified.map(|s| s.to_owned());

    resume::save_atomic(meta_path, &meta).await?;

    Ok(meta)
}

fn valid_saved_layout(
    meta: &ResumeMetadata,
    url: &str,
    identity: Option<&str>,
    file_size: u64,
    chunks: &[Chunk],
) -> bool {
    if !resume::validate_against(meta, url, identity, file_size, chunks) || chunks.is_empty() {
        return false;
    }

    let mut next_start = 0u64;
    for (index, (state, chunk)) in meta.chunks.iter().zip(chunks).enumerate() {
        if state.id != index as u32 + 1
            || chunk.start != next_start
            || chunk.end < chunk.start
            || chunk.end >= file_size
        {
            return false;
        }
        next_start = chunk.end + 1;
    }
    next_start == file_size
}

/// Brings the `.part` file to the right length, in a single open.
///
/// This used to stat the path, decide from the answer, and then open the path
/// again to act on it: two resolutions of a name anyone sharing the directory
/// can predict, with a gap in between, and both of them followed symlinks.
/// Pointing `<output>.part` at another file meant the `set_len` truncated that
/// file instead. `open_guarded` resolves once, relative to the directory,
/// refuses to traverse a symlink at the final component, and fstats the
/// descriptor to confirm a regular file this process owns. The size is then
/// read and set through that same descriptor, so there is no second lookup for
/// anything to get in front of.
async fn ensure_file_allocated(
    path: &str,
    size: u64,
    chunks: &[Chunk],
    meta: &ResumeMetadata,
) -> Result<()> {
    let has_recorded_progress = meta.total_completed() > 0;
    let file = safe_file::open_guarded(
        Path::new(path),
        if has_recorded_progress {
            Existing::OpenExisting
        } else {
            Existing::Open
        },
        Access::ReadWrite,
        safe_file::DEFAULT_FILE_MODE,
    )
    .with_context(|| {
        if has_recorded_progress {
            format!(
                "Cannot resume: saved progress exists but the partial file '{}' is missing or unavailable",
                path
            )
        } else {
            format!("Failed to open file: {}", path)
        }
    })?;

    let current = file
        .metadata()
        .with_context(|| format!("Failed to stat file: {}", path))?
        .len();

    if has_recorded_progress {
        let required_len = meta
            .chunks
            .iter()
            .filter(|chunk| chunk.completed > 0)
            .filter_map(|chunk| chunk.start.checked_add(chunk.completed))
            .max()
            .unwrap_or(0);
        if current < required_len {
            anyhow::bail!(
                "Cannot resume: partial file '{}' is {} bytes, but saved progress requires at least {} bytes; remove the partial file and its resume metadata to restart",
                path,
                current,
                required_len
            );
        }
    }

    // Already the right length.
    if current == size {
        return Ok(());
    }

    // Single chunk starting from nothing: the file starts at 0 bytes and the
    // sequential write extends it naturally, exactly as before.
    if current == 0 && chunks.len() <= 1 {
        return Ok(());
    }

    // Multi-chunk needs pre-allocation for random-access writes, and a file of
    // the wrong length is resized either way.
    file.set_len(size)
        .with_context(|| format!("Failed to resize '{}' to {} bytes", path, size))?;

    Ok(())
}

async fn save_progress(
    meta_path: &str,
    shared_meta: &Mutex<ResumeMetadata>,
    counters: &[(u32, Arc<AtomicU64>)],
) {
    let snapshot = {
        let mut meta = shared_meta.lock().await;
        for (id, counter) in counters {
            resume::update_progress(&mut meta, *id, counter.load(Ordering::Relaxed));
        }
        meta.clone()
    };
    let _ = resume::save_best_effort(meta_path, &snapshot).await;
}

fn spawn_autosave(
    meta_path: String,
    shared_meta: Arc<Mutex<ResumeMetadata>>,
    counters: Vec<(u32, Arc<AtomicU64>)>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            save_progress(&meta_path, &shared_meta, &counters).await;
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn download_chunk_with_retry(
    client: &Client,
    url: &str,
    file_path: &str,
    chunk: &Chunk,
    config: &RetryConfig,
    chunk_progress: Arc<AtomicU64>,
    cancel: CancellationToken,
    pressure: &PressureController,
) -> Result<u64> {
    let full_chunk_size = chunk.end - chunk.start + 1;
    let initial_completed = chunk_progress.load(Ordering::SeqCst);

    for attempt in 0..=config.max_retries {
        // A pause another worker was told about applies here too.
        pressure.wait_while_paused(&cancel).await;

        if cancel.is_cancelled() {
            let written = chunk_progress.load(Ordering::SeqCst);
            anyhow::bail!(
                "Chunk #{} cancelled before attempt {} ({} of {} bytes on disk)",
                chunk.id,
                attempt + 1,
                written,
                full_chunk_size
            );
        }

        let resume_from = chunk_progress.load(Ordering::SeqCst);
        if resume_from >= full_chunk_size {
            return Ok(full_chunk_size - initial_completed);
        }

        match range_download::download_range(
            client,
            url,
            file_path,
            chunk.start,
            chunk.end,
            resume_from,
            Arc::clone(&chunk_progress),
            cancel.clone(),
        )
        .await
        {
            Ok(DownloadStatus::Complete { bytes_written: _ }) => {
                if let Some(restored) = pressure.record_success() {
                    eprintln!(
                        "  \u{2713} Server steady again \u{2014} parallel connections back to {}",
                        restored
                    );
                }
                return Ok(chunk_progress.load(Ordering::SeqCst) - initial_completed);
            }

            Ok(DownloadStatus::Cancelled { .. }) => {
                let written = chunk_progress.load(Ordering::SeqCst);
                anyhow::bail!(
                    "Chunk #{} cancelled after {} of {} bytes",
                    chunk.id,
                    written,
                    full_chunk_size
                );
            }

            Err(e)
                if e.root_cause()
                    .to_string()
                    .contains("does not support range requests")
                    && attempt < config.max_retries =>
            {
                chunk_progress.store(0, Ordering::SeqCst);
                pressure.record_retryable(None);
                eprintln!(
                    "   \u{26a0} Chunk #{}: range not supported, restarting from byte 0",
                    chunk.id,
                );
                continue;
            }

            Err(e) if retry::is_retryable(&e) && attempt < config.max_retries => {
                // Reported now, while this worker is still alive and about to
                // try again — not on the way out with a final error, which is
                // why the old counter never reached its threshold.
                let asked_for = retry::retry_after(&e);
                let reaction = pressure.record_retryable(asked_for);

                if let Some(from) = reaction.reduced_from {
                    eprintln!(
                        "  \u{26a0} Server overloaded \u{2014} reducing parallel connections: {} \u{2192} {}",
                        from, reaction.limit
                    );
                }
                if let Some(pause) = reaction.pause {
                    eprintln!(
                        "  \u{23f8} Server asked for {:.0}s \u{2014} holding every connection",
                        pause.as_secs_f64()
                    );
                }

                // The server's own figure wins over our guess when it is the
                // longer of the two; backing off less than we were asked to
                // is how a 429 becomes a ban.
                let backoff = config.delay_for_attempt(attempt);
                let delay = asked_for.map_or(backoff, |wait| wait.max(backoff));

                eprintln!(
                    "   \u{26a0} Chunk #{}: {}/{} failed, retry in {:.1}s \u{2014} {}",
                    chunk.id,
                    attempt + 1,
                    config.max_retries + 1,
                    delay.as_secs_f64(),
                    short_error(&e),
                );

                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => { anyhow::bail!("Chunk #{} cancelled during retry backoff", chunk.id); }
                    _ = tokio::time::sleep(delay) => {}
                }
            }

            Err(e) => {
                cancel.cancel();
                let written = chunk_progress.load(Ordering::SeqCst);
                return Err(e.context(format!(
                    "Chunk #{} failed permanently after {} attempt(s) ({}/{} bytes on disk)",
                    chunk.id,
                    attempt + 1,
                    written,
                    full_chunk_size,
                )));
            }
        }
    }

    cancel.cancel();
    let written = chunk_progress.load(Ordering::SeqCst);
    anyhow::bail!(
        "Chunk #{}: exhausted {} retries ({}/{} bytes on disk)",
        chunk.id,
        config.max_retries,
        written,
        full_chunk_size
    )
}

fn short_error(err: &anyhow::Error) -> String {
    let msg = err
        .chain()
        .last()
        .map(|e| e.to_string())
        .unwrap_or_else(|| err.to_string());
    if msg.len() > 80 {
        let mut end = 77;
        while end > 0 && !msg.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}\u{2026}", &msg[..end])
    } else {
        msg
    }
}

fn spawn_progress_monitor(
    callback: Option<ProgressCallback>,
    counters: Vec<(u32, Arc<AtomicU64>)>,
    stop: CancellationToken,
    total: u64,
) -> Option<JoinHandle<()>> {
    let cb = callback?;
    Some(tokio::spawn(async move {
        loop {
            if stop.is_cancelled() {
                break;
            }
            let current: u64 = counters
                .iter()
                .map(|(_, c)| c.load(Ordering::Relaxed))
                .sum();
            cb(current, total);
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
        let current: u64 = counters
            .iter()
            .map(|(_, c)| c.load(Ordering::Relaxed))
            .sum();
        cb(current, total);
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn single_chunk() -> Vec<Chunk> {
        vec![Chunk {
            id: 0,
            start: 0,
            end: 9,
        }]
    }

    #[tokio::test(start_paused = true)]
    async fn autosave_sleep_is_interrupted_by_shutdown() {
        let stop = CancellationToken::new();
        let meta = resume::create_new("https://example.com/file".into(), 10, &single_chunk());
        let task = spawn_autosave(
            "unused.rdm".into(),
            Arc::new(Mutex::new(meta)),
            Vec::new(),
            stop.clone(),
        );
        tokio::task::yield_now().await;
        let started = tokio::time::Instant::now();
        stop.cancel();
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .expect("autosave waited for its 500 ms sleep")
            .unwrap();
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn autosave_still_persists_periodic_progress() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("download.rdm");
        let path = path.to_str().unwrap();
        let meta = resume::create_new("https://example.com/file".into(), 10, &single_chunk());
        let stop = CancellationToken::new();
        let task = spawn_autosave(
            path.to_owned(),
            Arc::new(Mutex::new(meta)),
            vec![(0, Arc::new(AtomicU64::new(5)))],
            stop.clone(),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while !Path::new(path).exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("autosave did not persist progress");
        stop.cancel();
        task.await.unwrap();
        assert_eq!(resume::load(path).await.unwrap().chunks[0].completed, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn progress_monitor_wakes_and_reports_final_progress() {
        let stop = CancellationToken::new();
        let counter = Arc::new(AtomicU64::new(2));
        let reports = Arc::new(StdMutex::new(Vec::new()));
        let captured = Arc::clone(&reports);
        let task = spawn_progress_monitor(
            Some(Arc::new(move |current: u64, total: u64| {
                captured.lock().unwrap().push((current, total))
            }) as ProgressCallback),
            vec![(0, Arc::clone(&counter))],
            stop.clone(),
            10,
        )
        .unwrap();
        tokio::task::yield_now().await;
        counter.store(10, Ordering::Relaxed);
        let started = tokio::time::Instant::now();
        stop.cancel();
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .expect("monitor waited for its 200 ms sleep")
            .unwrap();
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(*reports.lock().unwrap(), vec![(2, 10), (10, 10)]);
    }

    #[tokio::test]
    async fn already_stopped_monitor_reports_only_the_final_value() {
        let stop = CancellationToken::new();
        stop.cancel();
        let reports = Arc::new(StdMutex::new(Vec::new()));
        let captured = Arc::clone(&reports);
        let task = spawn_progress_monitor(
            Some(Arc::new(move |current: u64, total: u64| {
                captured.lock().unwrap().push((current, total))
            }) as ProgressCallback),
            vec![(0, Arc::new(AtomicU64::new(10)))],
            stop,
            10,
        )
        .unwrap();
        task.await.unwrap();
        assert_eq!(*reports.lock().unwrap(), vec![(10, 10)]);
    }

    #[tokio::test]
    async fn cancelled_before_workers_start_keeps_a_resume_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("download");
        let output = output.to_str().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        let chunks = single_chunk();
        let retry_config = RetryConfig::default();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let ctx = ParallelDownloadCtx {
            client: &client,
            url: "https://example.com/file",
            fetch_url: "https://example.com/file",
            output_path: output,
            file_size: 10,
            workers: 2,
            chunks: &chunks,
            retry_config: &retry_config,
            cancel,
            identity: None,
            etag: None,
            last_modified: None,
        };
        let result = download_parallel(&ctx, None::<fn(u64, u64)>).await;
        assert!(result.is_err());
        let manifest = ResumeMetadata::meta_path(output);
        assert_eq!(
            resume::load(&manifest).await.unwrap().chunks[0].completed,
            0
        );
        fs::remove_file(&manifest).await.unwrap();
        tokio::time::sleep(Duration::from_millis(650)).await;
        assert!(!Path::new(&manifest).exists());
    }

    async fn serve(body: &'static str) -> String {
        use axum::http::{StatusCode, header};
        use axum::routing::get;

        let app = axum::Router::new().route(
            "/file",
            get(move || async move {
                (
                    StatusCode::PARTIAL_CONTENT,
                    [(header::CONTENT_RANGE, "bytes 0-9/10")],
                    body,
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/file")
    }

    async fn run_download(body: &'static str, output: &str) -> Result<u64> {
        let url = serve(body).await;
        let client = Client::builder().no_proxy().build().unwrap();
        let chunks = single_chunk();
        let retry_config = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let ctx = ParallelDownloadCtx {
            client: &client,
            url: &url,
            fetch_url: &url,
            output_path: output,
            file_size: 10,
            workers: 2,
            chunks: &chunks,
            retry_config: &retry_config,
            cancel: CancellationToken::new(),
            identity: None,
            etag: None,
            last_modified: None,
        };
        download_parallel(&ctx, Some(|_, _| {})).await
    }

    async fn serve_saved_range() -> String {
        use axum::http::{StatusCode, header};
        use axum::routing::get;

        let app = axum::Router::new().route(
            "/file",
            get(|| async {
                (
                    StatusCode::PARTIAL_CONTENT,
                    [
                        (header::CONTENT_RANGE, "bytes 5-9/10"),
                        (header::ETAG, "\"v1\""),
                    ],
                    "56789",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/file")
    }

    async fn serve_concurrent_ranges() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use axum::body::Body;
        use axum::http::{HeaderMap, Response, StatusCode, header};
        use axum::routing::get;
        use std::sync::atomic::AtomicUsize;

        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let active_for_route = Arc::clone(&active);
        let maximum_for_route = Arc::clone(&maximum);
        let app = axum::Router::new().route(
            "/file",
            get(move |headers: HeaderMap| {
                let active = Arc::clone(&active_for_route);
                let maximum = Arc::clone(&maximum_for_route);
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    active.fetch_sub(1, Ordering::SeqCst);

                    let range = headers
                        .get(header::RANGE)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.strip_prefix("bytes="))
                        .and_then(|v| v.split_once('-'))
                        .map(|(start, end)| {
                            (
                                start.parse::<usize>().unwrap(),
                                end.parse::<usize>().unwrap(),
                            )
                        })
                        .unwrap();
                    let payload = b"abcdefgh";
                    let body = payload[range.0..=range.1].to_vec();
                    Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(
                            header::CONTENT_RANGE,
                            format!("bytes {}-{}/8", range.0, range.1),
                        )
                        .body(Body::from(body))
                        .unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{address}/file"), maximum)
    }

    async fn serve_adaptive_payload(
        payload: Arc<Vec<u8>>,
        ignore_ranges: bool,
    ) -> (String, Arc<StdMutex<Vec<u64>>>) {
        use axum::body::Body;
        use axum::http::{HeaderMap, Response, StatusCode, header};
        use axum::routing::get;

        let starts = Arc::new(StdMutex::new(Vec::new()));
        let starts_for_route = Arc::clone(&starts);
        let payload_for_route = Arc::clone(&payload);
        let app = axum::Router::new().route(
            "/file",
            get(move |headers: HeaderMap| {
                let starts = Arc::clone(&starts_for_route);
                let payload = Arc::clone(&payload_for_route);
                async move {
                    if ignore_ranges {
                        return Response::builder()
                            .status(StatusCode::OK)
                            .header(header::ETAG, "\"v1\"")
                            .body(Body::from(payload.as_ref().clone()))
                            .unwrap();
                    }

                    let start = headers
                        .get(header::RANGE)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.strip_prefix("bytes="))
                        .and_then(|value| value.strip_suffix('-'))
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap();
                    starts.lock().unwrap().push(start);
                    let end = payload.len() as u64 - 1;
                    let body = payload[start as usize..].to_vec();
                    Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(
                            header::CONTENT_RANGE,
                            format!("bytes {start}-{end}/{}", payload.len()),
                        )
                        .header(header::ETAG, "\"v1\"")
                        .body(Body::from(body))
                        .unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{address}/file"), starts)
    }

    fn adaptive_test_pieces() -> Vec<Chunk> {
        (0..4)
            .map(|index| Chunk {
                id: index + 1,
                start: index as u64 * 20,
                end: index as u64 * 20 + 19,
            })
            .collect()
    }

    #[test]
    fn adaptive_content_range_requires_the_requested_start_and_inspected_size() {
        use reqwest::header::HeaderValue;

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 17-99/100"),
        );
        assert_eq!(
            parse_open_content_range(&headers, 17, 100).unwrap(),
            (99, 100)
        );
        assert!(parse_open_content_range(&headers, 18, 100).is_err());
        assert!(parse_open_content_range(&headers, 17, 101).is_err());
    }

    #[test]
    fn adaptive_if_range_requires_a_matching_response_validator() {
        use reqwest::header::HeaderValue;

        let mut headers = header::HeaderMap::new();
        headers.insert(header::ETAG, HeaderValue::from_static("\"v1\""));
        assert!(response_validator_matches(&headers, Some("\"v1\"")));
        assert!(!response_validator_matches(&headers, Some("\"v2\"")));
        assert!(response_validator_matches(&header::HeaderMap::new(), None));
        assert!(!response_validator_matches(
            &header::HeaderMap::new(),
            Some("\"v1\"")
        ));
    }

    async fn run_adaptive_test_download(
        url: &str,
        output: &str,
        chunks: &[Chunk],
        file_size: u64,
    ) -> Result<u64> {
        let client = Client::builder().no_proxy().build().unwrap();
        let retry_config = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let ctx = ParallelDownloadCtx {
            client: &client,
            url,
            fetch_url: url,
            output_path: output,
            file_size,
            workers: 2,
            chunks,
            retry_config: &retry_config,
            cancel: CancellationToken::new(),
            identity: None,
            etag: Some("\"v1\"".to_owned()),
            last_modified: None,
        };
        download_parallel_adaptive_stream(&ctx, None::<fn(u64, u64)>).await
    }

    #[tokio::test]
    async fn adaptive_response_crosses_owned_pieces_and_stops_at_another_worker() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("adaptive.bin");
        let output = output.to_str().unwrap();
        let payload = Arc::new((0..80).map(|byte| byte as u8).collect::<Vec<_>>());
        let (url, starts) = serve_adaptive_payload(Arc::clone(&payload), false).await;
        let pieces = adaptive_test_pieces();

        assert_eq!(
            run_adaptive_test_download(&url, output, &pieces, payload.len() as u64)
                .await
                .unwrap(),
            payload.len() as u64
        );
        assert_eq!(
            fs::read(output).await.unwrap().as_slice(),
            payload.as_slice()
        );
        let request_starts = starts.lock().unwrap().clone();
        assert!(request_starts.contains(&0));
        assert!(request_starts.contains(&60));
        assert!(request_starts.len() >= 2);
    }

    #[tokio::test]
    async fn adaptive_mode_restarts_safely_when_server_ignores_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("fallback.bin");
        let output = output.to_str().unwrap();
        let payload = Arc::new((0..80).map(|byte| (byte * 3) as u8).collect::<Vec<_>>());
        let (url, _) = serve_adaptive_payload(Arc::clone(&payload), true).await;
        let pieces = adaptive_test_pieces();

        assert_eq!(
            run_adaptive_test_download(&url, output, &pieces, payload.len() as u64)
                .await
                .unwrap(),
            payload.len() as u64
        );
        assert_eq!(
            fs::read(output).await.unwrap().as_slice(),
            payload.as_slice()
        );
    }

    #[tokio::test]
    async fn adaptive_resume_requests_from_the_saved_partial_piece_offset() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("resumed.bin");
        let output = output.to_str().unwrap();
        let payload = Arc::new((0..80).map(|byte| (byte + 7) as u8).collect::<Vec<_>>());
        let (url, starts) = serve_adaptive_payload(Arc::clone(&payload), false).await;
        let pieces = adaptive_test_pieces();
        let mut meta = resume::create_new(url.clone(), payload.len() as u64, &pieces);
        meta.etag = Some("\"v1\"".to_owned());
        resume::update_progress(&mut meta, 1, 5);
        resume::save_atomic(&ResumeMetadata::meta_path(output), &meta)
            .await
            .unwrap();

        let partial = format!("{output}.part");
        fs::write(&partial, &payload[..5]).await.unwrap();
        let file = safe_file::open_guarded(
            Path::new(&partial),
            Existing::OpenExisting,
            Access::ReadWrite,
            safe_file::DEFAULT_FILE_MODE,
        )
        .unwrap();
        file.set_len(payload.len() as u64).unwrap();
        drop(file);

        assert_eq!(
            run_adaptive_test_download(&url, output, &pieces, payload.len() as u64)
                .await
                .unwrap(),
            payload.len() as u64
        );
        assert_eq!(
            fs::read(output).await.unwrap().as_slice(),
            payload.as_slice()
        );
        assert!(starts.lock().unwrap().contains(&5));
    }

    #[tokio::test]
    async fn adaptive_scheduler_spreads_seeds_and_never_steals_owned_pieces() {
        let pieces = (0..12)
            .map(|index| Chunk {
                id: index + 1,
                start: index as u64 * 10,
                end: index as u64 * 10 + 9,
            })
            .collect::<Vec<_>>();
        let progress = pieces.iter().map(|_| Arc::new(AtomicU64::new(0))).collect();
        let scheduler = AdaptiveScheduler::new(pieces, progress);
        let seeds = scheduler.seed_workers(3).await;
        assert_eq!(seeds, vec![(0, 0), (1, 5), (2, 11)]);

        assert_eq!(
            scheduler.claim_for_response(0, 1, 1, 2).await,
            PieceClaim::Acquired
        );
        assert_eq!(
            scheduler.claim_for_response(1, 1, 1, 2).await,
            PieceClaim::Blocked
        );
        assert_eq!(
            scheduler.claim_for_response(0, 2, 2, 2).await,
            PieceClaim::Yield
        );
    }

    #[tokio::test]
    async fn idle_adaptive_worker_waits_for_and_claims_released_work() {
        let pieces = (0..4)
            .map(|index| Chunk {
                id: index + 1,
                start: index as u64 * 10,
                end: index as u64 * 10 + 9,
            })
            .collect::<Vec<_>>();
        let progress = pieces.iter().map(|_| Arc::new(AtomicU64::new(0))).collect();
        let scheduler = Arc::new(AdaptiveScheduler::new(pieces, progress));
        assert_eq!(scheduler.seed_workers(4).await.len(), 4);

        let worker_scheduler = Arc::clone(&scheduler);
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let waiting_worker = tokio::spawn(async move {
            worker_scheduler
                .claim_largest_region(4, &worker_cancel)
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        scheduler.release_worker(1).await;

        assert_eq!(waiting_worker.await.unwrap(), Some(1));
    }

    async fn try_resume_with_partial(output: &str, partial_len: Option<u64>) -> Result<u64> {
        let url = serve_saved_range().await;
        let chunks = vec![Chunk {
            id: 1,
            start: 0,
            end: 9,
        }];
        let mut meta = resume::create_new(url.clone(), 10, &chunks);
        meta.etag = Some("\"v1\"".to_owned());
        resume::update_progress(&mut meta, 1, 5);
        resume::save_atomic(&ResumeMetadata::meta_path(output), &meta)
            .await
            .unwrap();

        if let Some(len) = partial_len {
            let part = format!("{output}.part");
            let file = safe_file::open_guarded(
                Path::new(&part),
                Existing::Reject,
                Access::ReadWrite,
                safe_file::DEFAULT_FILE_MODE,
            )
            .unwrap();
            file.set_len(len).unwrap();
        }

        let client = Client::builder().no_proxy().build().unwrap();
        let retry_config = RetryConfig::default();
        let ctx = ParallelDownloadCtx {
            client: &client,
            url: &url,
            fetch_url: &url,
            output_path: output,
            file_size: 10,
            workers: 1,
            chunks: &chunks,
            retry_config: &retry_config,
            cancel: CancellationToken::new(),
            identity: None,
            etag: Some("\"v1\"".to_owned()),
            last_modified: None,
        };
        download_parallel(&ctx, None::<fn(u64, u64)>).await
    }

    async fn resume_with_saved_layout(
        output: &str,
        saved_chunks: Vec<Chunk>,
        requested_chunks: &[Chunk],
    ) -> Result<u64> {
        let url = serve_saved_range().await;
        let mut meta = resume::create_new(url.clone(), 10, &saved_chunks);
        meta.etag = Some("\"v1\"".to_owned());
        resume::update_progress(&mut meta, saved_chunks[0].id, 5);
        resume::save_atomic(&ResumeMetadata::meta_path(output), &meta)
            .await
            .unwrap();

        let part = format!("{output}.part");
        fs::write(&part, b"01234").await.unwrap();
        let file = safe_file::open_guarded(
            Path::new(&part),
            Existing::OpenExisting,
            Access::ReadWrite,
            safe_file::DEFAULT_FILE_MODE,
        )
        .unwrap();
        file.set_len(10).unwrap();
        drop(file);

        let client = Client::builder().no_proxy().build().unwrap();
        let retry_config = RetryConfig::default();
        let ctx = ParallelDownloadCtx {
            client: &client,
            url: &url,
            fetch_url: &url,
            output_path: output,
            file_size: 10,
            workers: 2,
            chunks: requested_chunks,
            retry_config: &retry_config,
            cancel: CancellationToken::new(),
            identity: None,
            etag: Some("\"v1\"".to_owned()),
            last_modified: None,
        };
        download_parallel(&ctx, None::<fn(u64, u64)>).await
    }

    #[tokio::test]
    async fn saved_progress_with_a_missing_part_is_never_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("download");
        let output = output.to_str().unwrap();

        let err = try_resume_with_partial(output, None)
            .await
            .expect_err("missing partial file must not be recreated and resumed");

        assert!(format!("{err:#}").contains("saved progress exists"));
        assert!(!Path::new(&format!("{output}.part")).exists());
        assert!(!Path::new(output).exists());
    }

    #[tokio::test]
    async fn saved_progress_beyond_a_truncated_part_is_never_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("download");
        let output = output.to_str().unwrap();

        let err = try_resume_with_partial(output, Some(2))
            .await
            .expect_err("truncated partial file must not be padded and resumed");

        assert!(format!("{err:#}").contains("requires at least 5 bytes"));
        assert_eq!(
            fs::metadata(format!("{output}.part")).await.unwrap().len(),
            2
        );
        assert!(!Path::new(output).exists());
    }

    #[tokio::test]
    async fn resume_uses_the_saved_adaptive_layout_when_default_layout_is_requested() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("download");
        let output = output.to_str().unwrap();
        let adaptive_layout = vec![
            Chunk {
                id: 1,
                start: 0,
                end: 4,
            },
            Chunk {
                id: 2,
                start: 5,
                end: 9,
            },
        ];
        let default_layout = vec![Chunk {
            id: 1,
            start: 0,
            end: 9,
        }];

        assert_eq!(
            resume_with_saved_layout(output, adaptive_layout, &default_layout)
                .await
                .unwrap(),
            10
        );
        assert_eq!(fs::read(output).await.unwrap(), b"0123456789");
    }

    #[tokio::test]
    async fn resume_uses_the_saved_default_layout_when_adaptive_layout_is_requested() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("download");
        let output = output.to_str().unwrap();
        let default_layout = vec![Chunk {
            id: 1,
            start: 0,
            end: 9,
        }];
        let adaptive_layout = vec![
            Chunk {
                id: 1,
                start: 0,
                end: 4,
            },
            Chunk {
                id: 2,
                start: 5,
                end: 9,
            },
        ];

        assert_eq!(
            resume_with_saved_layout(output, default_layout, &adaptive_layout)
                .await
                .unwrap(),
            10
        );
        assert_eq!(fs::read(output).await.unwrap(), b"0123456789");
    }

    #[tokio::test]
    async fn queued_ranges_never_exceed_the_requested_worker_limit() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("download");
        let output = output.to_str().unwrap();
        let (url, maximum) = serve_concurrent_ranges().await;
        let chunks = (0..4)
            .map(|index| Chunk {
                id: index + 1,
                start: index as u64 * 2,
                end: index as u64 * 2 + 1,
            })
            .collect::<Vec<_>>();
        let client = Client::builder().no_proxy().build().unwrap();
        let retry_config = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let ctx = ParallelDownloadCtx {
            client: &client,
            url: &url,
            fetch_url: &url,
            output_path: output,
            file_size: 8,
            workers: 2,
            chunks: &chunks,
            retry_config: &retry_config,
            cancel: CancellationToken::new(),
            identity: None,
            etag: None,
            last_modified: None,
        };

        assert_eq!(
            download_parallel(&ctx, None::<fn(u64, u64)>).await.unwrap(),
            8
        );
        assert_eq!(fs::read(output).await.unwrap(), b"abcdefgh");
        assert_eq!(maximum.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn saved_layout_validation_rejects_gaps_overlaps_and_bad_progress() {
        let chunks = vec![
            Chunk {
                id: 1,
                start: 0,
                end: 49,
            },
            Chunk {
                id: 2,
                start: 50,
                end: 99,
            },
        ];
        let url = "https://example.com/file";
        let mut meta = resume::create_new(url.to_owned(), 100, &chunks);
        meta.etag = Some("\"v1\"".to_owned());
        assert!(valid_saved_layout(
            &meta,
            url,
            None,
            100,
            &chunks_from_metadata(&meta)
        ));

        meta.chunks[1].start = 49;
        assert!(!valid_saved_layout(
            &meta,
            url,
            None,
            100,
            &chunks_from_metadata(&meta)
        ));

        meta.chunks[1].start = 51;
        assert!(!valid_saved_layout(
            &meta,
            url,
            None,
            100,
            &chunks_from_metadata(&meta)
        ));

        meta.chunks[1].start = 50;
        meta.chunks[0].completed = 51;
        assert!(!valid_saved_layout(
            &meta,
            url,
            None,
            100,
            &chunks_from_metadata(&meta)
        ));
    }

    #[tokio::test]
    async fn successful_download_does_not_recreate_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("download");
        let output = output.to_str().unwrap();
        assert_eq!(run_download("0123456789", output).await.unwrap(), 10);
        assert_eq!(fs::read(output).await.unwrap(), b"0123456789");
        let manifest = ResumeMetadata::meta_path(output);
        assert!(!Path::new(&manifest).exists());
        tokio::time::sleep(Duration::from_millis(650)).await;
        assert!(!Path::new(&manifest).exists());
    }

    #[tokio::test]
    async fn failed_download_saves_progress_and_stops_background_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("download");
        let output = output.to_str().unwrap();
        assert!(run_download("01234", output).await.is_err());
        let manifest = ResumeMetadata::meta_path(output);
        let saved = resume::load(&manifest).await.unwrap();
        assert_eq!(saved.chunks[0].completed, 5);
        assert_eq!(fs::read(format!("{output}.part")).await.unwrap(), b"01234");
        fs::remove_file(&manifest).await.unwrap();
        tokio::time::sleep(Duration::from_millis(650)).await;
        assert!(!Path::new(&manifest).exists());
    }
}
