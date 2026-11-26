use anyhow::{Context, Result};
use reqwest::Client;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::chunk::Chunk;
use crate::dynamic_range::RangeLimit;
use crate::pressure::PressureController;
use crate::range_download::{self, DownloadStatus};
use crate::resume::{self, IdentityCheck, ResumeMetadata};
use crate::retry::{self, RetryConfig};
use crate::safe_file::{self, Access, Existing};

/// Groups all parameters needed for a parallel download session.
pub struct ParallelDownloadCtx<'a> {
    pub client: &'a Client,
    pub url: &'a str,
    pub fetch_url: &'a str,
    pub output_path: &'a str,
    pub file_size: u64,
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
    download_parallel_with_initial(
        ctx,
        ctx.chunks.len().max(1),
        None,
        ctx.chunks.len() > 1,
        progress_callback,
    )
    .await
}

pub(crate) async fn download_parallel_with_initial<F>(
    ctx: &ParallelDownloadCtx<'_>,
    max_workers: usize,
    initial_response: Option<reqwest::Response>,
    allow_split: bool,
    progress_callback: Option<F>,
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

    match parallel_inner(
        ctx,
        &temp_path,
        &meta_path,
        max_workers,
        initial_response,
        allow_split,
        progress_callback,
    )
    .await
    {
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

async fn parallel_inner<F>(
    ctx: &ParallelDownloadCtx<'_>,
    temp_path: &str,
    meta_path: &str,
    max_workers: usize,
    initial_response: Option<reqwest::Response>,
    allow_split: bool,
    progress_callback: Option<F>,
) -> Result<u64>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
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

    let mut queue: VecDeque<Chunk> = chunks
        .iter()
        .filter(|chunk| {
            let state = meta.chunks.iter().find(|s| s.id == chunk.id).unwrap();
            state.completed < state.total_bytes()
        })
        .cloned()
        .collect();
    let shared = Arc::new(Mutex::new(DynamicState::new(meta)));
    // One view of how the server is coping, shared by every worker and by
    // the loop that hands out ranges.
    let pressure = Arc::new(PressureController::new(max_workers.max(1)));
    let background_stop = CancellationToken::new();
    // If the caller drops this future, do not leave the background loops alive.
    let _stop_on_drop = background_stop.clone().drop_guard();

    let autosave_handle = spawn_dynamic_autosave(
        meta_path.to_owned(),
        Arc::clone(&shared),
        background_stop.clone(),
    );
    let monitor_handle = spawn_dynamic_monitor(
        progress_callback,
        Arc::clone(&shared),
        background_stop.clone(),
        ctx.file_size,
    );
    let mut join_set: JoinSet<Result<(u32, u64)>> = JoinSet::new();
    let mut active = std::collections::HashSet::new();
    let mut initial_response = initial_response;

    let worker_result: Result<()> = async {
        if let Some(response) = initial_response.take() {
            let first = chunks
                .first()
                .context("Initial response has no assigned range")?;
            let progress = shared
                .lock()
                .await
                .progress(first.id)
                .context("Initial response range has no progress counter")?;
            let matches_saved = if response.status() == reqwest::StatusCode::OK {
                first.start == 0 && first.end == ctx.file_size - 1
            } else {
                crate::inspect::initial_range(&response) == Some((first.start, first.end))
            };
            let previous = progress.load(Ordering::Relaxed);
            let first_len = first.end - first.start + 1;

            // A fully completed first range is already on disk and belongs to
            // the validated resume state. The probe response is then discarded
            // without touching those bytes.
            if matches_saved && previous < first_len {
                // The retained response always starts at byte zero and covers
                // this whole first work unit. Restart its counter while that
                // body is written so a short response cannot leave stale
                // progress claiming bytes beyond what was just verified.
                progress.store(0, Ordering::Release);
                match range_download::download_response(
                    response,
                    temp_path,
                    first.start,
                    first.end,
                    0,
                    Arc::clone(&progress),
                    ctx.cancel.clone(),
                )
                .await?
                {
                    DownloadStatus::Complete { .. } => {
                        queue.retain(|chunk| chunk.id != first.id);
                    }
                    DownloadStatus::Cancelled { bytes_on_disk } => {
                        anyhow::bail!(
                            "Initial response cancelled after {} of {} bytes",
                            bytes_on_disk,
                            first_len
                        );
                    }
                }
            }
        }

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

            while join_set.len() < active_workers && !pressure.is_paused() {
                // Assign saved/unstarted ranges first. Once they are gone,
                // every idle slot may split the largest in-flight tail.
                if queue.is_empty()
                    && allow_split
                    && let Some(tail) = shared.lock().await.split_largest(&active, 1024 * 1024)
                {
                    queue.push_back(tail);
                }
                let Some(chunk) = queue.pop_front() else {
                    break;
                };

                let client = ctx.client.clone();
                let url = ctx.fetch_url.to_owned();
                let path = temp_path.to_owned();
                let cancel = ctx.cancel.clone();
                let config = ctx.retry_config.clone();
                let (chunk_progress, limit) = shared
                    .lock()
                    .await
                    .entry(chunk.id)
                    .context("Missing chunk state")?;
                active.insert(chunk.id);

                let pressure = Arc::clone(&pressure);

                join_set.spawn(async move {
                    let id = chunk.id;
                    download_chunk_with_retry(
                        &client,
                        &url,
                        &path,
                        &chunk,
                        &config,
                        chunk_progress,
                        cancel,
                        &pressure,
                        limit,
                    )
                    .await
                    .map(|bytes| (id, bytes))
                });
            }

            // Paused with nothing spawned yet: go back and wait it out.
            if join_set.is_empty() {
                continue;
            }

            match join_set.join_next().await {
                Some(Ok(Ok((id, _bytes)))) => {
                    active.remove(&id);
                }
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

    let total_bytes = shared.lock().await.snapshot().total_completed();
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
        save_dynamic_progress(meta_path, &shared).await;
    }
    result
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

        // The saved ranges are the resume contract. Do not discard a valid
        // manifest just because this invocation would plan a different layout
        // (for example, a different connection count).
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

/// Validates the saved manifest against its own recorded layout: contiguous,
/// in-order ranges that tile the whole file, with per-chunk progress in
/// bounds. The planned layout is irrelevant here; the saved ranges are the
/// resume contract.
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

// All layout changes and snapshots share this lock. Workers reserve bytes on
// their own limit before writing, and publish only flushed progress.
struct TrackedRange {
    id: u32,
    progress: Arc<AtomicU64>,
    limit: Arc<RangeLimit>,
}
struct DynamicState {
    meta: ResumeMetadata,
    entries: Vec<TrackedRange>,
}
impl DynamicState {
    fn new(meta: ResumeMetadata) -> Self {
        let entries = meta
            .chunks
            .iter()
            .map(|c| TrackedRange {
                id: c.id,
                progress: Arc::new(AtomicU64::new(c.completed)),
                limit: Arc::new(RangeLimit::new(c.end, c.start + c.completed)),
            })
            .collect();
        Self { meta, entries }
    }
    fn entry(&self, id: u32) -> Option<(Arc<AtomicU64>, Arc<RangeLimit>)> {
        self.entries
            .iter()
            .find(|e| e.id == id)
            .map(|e| (Arc::clone(&e.progress), Arc::clone(&e.limit)))
    }
    fn progress(&self, id: u32) -> Option<Arc<AtomicU64>> {
        self.entry(id).map(|e| e.0)
    }
    fn snapshot(&self) -> ResumeMetadata {
        let mut meta = self.meta.clone();
        for (index, chunk) in meta.chunks.iter_mut().enumerate() {
            chunk.completed = self
                .progress(chunk.id)
                .expect("chunk state")
                .load(Ordering::SeqCst);
            // Persistent layouts use consecutive IDs; runtime IDs stay stable.
            chunk.id = index as u32 + 1;
        }
        meta
    }
    fn split_largest(
        &mut self,
        active: &std::collections::HashSet<u32>,
        min_piece: u64,
    ) -> Option<Chunk> {
        // Bound manifest growth even for transfers which last a very long time.
        if self.meta.chunks.len() >= 4096 {
            return None;
        }
        let picked = self
            .entries
            .iter()
            .filter(|e| active.contains(&e.id))
            .max_by_key(|e| e.limit.remaining())?;
        let parent_id = picked.id;
        let id = self.entries.iter().map(|e| e.id).max()?.checked_add(1)?;
        let index = self.meta.chunks.iter().position(|c| c.id == parent_id)?;
        let (start, end) = picked.limit.split(min_piece)?;
        self.meta.chunks[index].end = start - 1;
        self.meta.chunks.insert(
            index + 1,
            resume::ChunkState {
                id,
                start,
                end,
                completed: 0,
            },
        );
        self.entries.push(TrackedRange {
            id,
            progress: Arc::new(AtomicU64::new(0)),
            limit: Arc::new(RangeLimit::new(end, start)),
        });
        Some(Chunk { id, start, end })
    }
}

async fn save_dynamic_progress(path: &str, shared: &Mutex<DynamicState>) {
    let snapshot = shared.lock().await.snapshot();
    let _ = resume::save_best_effort(path, &snapshot).await;
}
fn spawn_dynamic_autosave(
    path: String,
    shared: Arc<Mutex<DynamicState>>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            save_dynamic_progress(&path, &shared).await;
        }
    })
}
fn spawn_dynamic_monitor<F>(
    callback: Option<F>,
    shared: Arc<Mutex<DynamicState>>,
    stop: CancellationToken,
    total: u64,
) -> Option<JoinHandle<()>>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
    let cb = callback?;
    Some(tokio::spawn(async move {
        loop {
            if stop.is_cancelled() {
                break;
            }
            let current = shared.lock().await.snapshot().total_completed();
            cb(current, total);
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
        let current = shared.lock().await.snapshot().total_completed();
        cb(current, total);
    }))
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
    limit: Arc<RangeLimit>,
) -> Result<u64> {
    let initial_completed = chunk_progress.load(Ordering::SeqCst);

    for attempt in 0..=config.max_retries {
        let full_chunk_size = limit.end() - chunk.start + 1;
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
            return Ok(full_chunk_size.saturating_sub(initial_completed));
        }

        limit.reset(chunk.start + resume_from);
        let request_end = limit.end();
        match range_download::download_range_dynamic(
            client,
            url,
            file_path,
            chunk.start,
            request_end,
            resume_from,
            Arc::clone(&chunk_progress),
            cancel.clone(),
            Arc::clone(&limit),
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
                return Ok(chunk_progress
                    .load(Ordering::SeqCst)
                    .saturating_sub(initial_completed));
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
    let full_chunk_size = limit.end() - chunk.start + 1;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    const DYNAMIC_SIZE: usize = 8 * 1024 * 1024;
    async fn skewed_server(
        mode: Arc<std::sync::atomic::AtomicUsize>,
        split_seen: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        payload: Arc<Vec<u8>>,
    ) -> String {
        use axum::{
            Router,
            body::{Body, Bytes},
            http::{HeaderMap, Response, StatusCode, header},
            routing::get,
        };
        use futures_util::stream::{self, StreamExt};
        let app = Router::new().route(
            "/file",
            get(move |headers: HeaderMap| {
                let mode = Arc::clone(&mode);
                let split_seen = Arc::clone(&split_seen);
                let release = Arc::clone(&release);
                let payload = Arc::clone(&payload);
                async move {
                    let text = headers.get(header::RANGE).unwrap().to_str().unwrap();
                    let (from, to) = text
                        .strip_prefix("bytes=")
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    let start: usize = from.parse().unwrap();
                    let end: usize = to.parse().unwrap();
                    let phase = mode.load(Ordering::SeqCst);
                    let is_tail = start > 0 && (start != DYNAMIC_SIZE / 2 || phase == 3);
                    if is_tail && phase != 2 {
                        split_seen.notify_one();
                        release.notify_one();
                    }
                    let bytes = Bytes::copy_from_slice(&payload[start..=end]);
                    let body = if start < DYNAMIC_SIZE / 2 && phase == 1 {
                        let n = bytes.len().min(65536);
                        let first = bytes.slice(..n);
                        Body::from_stream(
                            stream::once(async move { Ok::<_, std::io::Error>(first) })
                                .chain(stream::pending()),
                        )
                    } else if start == 0 && (phase == 0 || phase == 3) {
                        let n = bytes.len().min(65536);
                        let first = bytes.slice(..n);
                        let rest = bytes.slice(n..);
                        let first = stream::once(async move { Ok::<_, std::io::Error>(first) });
                        let rest = stream::once(async move {
                            release.notified().await;
                            Ok::<_, std::io::Error>(rest)
                        });
                        Body::from_stream(first.chain(rest))
                    } else {
                        Body::from(bytes)
                    };
                    Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(
                            header::CONTENT_RANGE,
                            format!("bytes {start}-{end}/{DYNAMIC_SIZE}"),
                        )
                        .header(header::ETAG, "\"stable\"")
                        .body(body)
                        .unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/file")
    }

    fn dynamic_chunks() -> Vec<Chunk> {
        vec![
            Chunk {
                id: 1,
                start: 0,
                end: DYNAMIC_SIZE as u64 / 2 - 1,
            },
            Chunk {
                id: 2,
                start: DYNAMIC_SIZE as u64 / 2,
                end: DYNAMIC_SIZE as u64 - 1,
            },
        ]
    }

    #[tokio::test]
    async fn idle_worker_splits_a_live_slow_range_without_corruption() {
        use std::sync::atomic::AtomicUsize;
        let mode = Arc::new(AtomicUsize::new(0));
        let split = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let payload = Arc::new(
            (0..DYNAMIC_SIZE)
                .map(|i| (i.wrapping_mul(31) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let url = skewed_server(mode, Arc::clone(&split), release, Arc::clone(&payload)).await;
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file");
        let client = Client::builder().no_proxy().build().unwrap();
        let chunks = dynamic_chunks();
        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let ctx = ParallelDownloadCtx {
            client: &client,
            url: &url,
            fetch_url: &url,
            output_path: output.to_str().unwrap(),
            file_size: DYNAMIC_SIZE as u64,
            chunks: &chunks,
            retry_config: &retry,
            cancel: CancellationToken::new(),
            identity: None,
            etag: Some("\"stable\"".into()),
            last_modified: None,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            download_parallel_with_initial(&ctx, 2, None, true, None::<fn(u64, u64)>),
        )
        .await
        .expect("worker did not steal slow range")
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), split.notified())
            .await
            .expect("no split request was made");
        assert_eq!(result, DYNAMIC_SIZE as u64);
        assert_eq!(fs::read(output).await.unwrap(), *payload);
    }

    #[tokio::test]
    async fn idle_workers_can_split_a_single_saved_layout_at_start() {
        use std::sync::atomic::AtomicUsize;
        let split = Arc::new(tokio::sync::Notify::new());
        let payload = Arc::new(
            (0..DYNAMIC_SIZE)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let url = skewed_server(
            Arc::new(AtomicUsize::new(3)),
            Arc::clone(&split),
            Arc::new(tokio::sync::Notify::new()),
            Arc::clone(&payload),
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file");
        let client = Client::builder().no_proxy().build().unwrap();
        let chunks = vec![Chunk {
            id: 1,
            start: 0,
            end: DYNAMIC_SIZE as u64 - 1,
        }];
        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let ctx = ParallelDownloadCtx {
            client: &client,
            url: &url,
            fetch_url: &url,
            output_path: output.to_str().unwrap(),
            file_size: DYNAMIC_SIZE as u64,
            chunks: &chunks,
            retry_config: &retry,
            cancel: CancellationToken::new(),
            identity: None,
            etag: Some("\"stable\"".into()),
            last_modified: None,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            download_parallel_with_initial(&ctx, 2, None, true, None::<fn(u64, u64)>),
        )
        .await
        .expect("idle worker did not split the sole range")
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), split.notified())
            .await
            .expect("no split request was made");
        assert_eq!(result, DYNAMIC_SIZE as u64);
        assert_eq!(fs::read(output).await.unwrap(), *payload);
    }

    #[tokio::test]
    async fn cancellation_after_live_split_saves_and_resumes_each_prefix() {
        use std::sync::atomic::AtomicUsize;
        let mode = Arc::new(AtomicUsize::new(1));
        let split = Arc::new(tokio::sync::Notify::new());
        let payload = Arc::new(
            (0..DYNAMIC_SIZE)
                .map(|i| (i.wrapping_mul(17) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let url = skewed_server(
            Arc::clone(&mode),
            Arc::clone(&split),
            Arc::new(tokio::sync::Notify::new()),
            Arc::clone(&payload),
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file");
        let output = output.to_str().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        let chunks = dynamic_chunks();
        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let checkpoint_path = ResumeMetadata::meta_path(output);
        let waiter = tokio::spawn(async move {
            split.notified().await;
            // Cross an autosave tick while split workers are still active.
            tokio::time::sleep(Duration::from_millis(850)).await;
            let checkpoint = resume::load(&checkpoint_path).await;
            trigger.cancel();
            checkpoint
        });
        let ctx = ParallelDownloadCtx {
            client: &client,
            url: &url,
            fetch_url: &url,
            output_path: output,
            file_size: DYNAMIC_SIZE as u64,
            chunks: &chunks,
            retry_config: &retry,
            cancel,
            identity: None,
            etag: Some("\"stable\"".into()),
            last_modified: None,
        };
        let stopped = tokio::time::timeout(
            Duration::from_secs(10),
            download_parallel_with_initial(&ctx, 2, None, true, None::<fn(u64, u64)>),
        )
        .await
        .expect("split or cancellation stalled");
        assert!(stopped.is_err());
        let live = waiter.await.unwrap().unwrap();
        assert!(live.chunks.len() >= 3, "autosave did not capture the split");
        assert!(valid_saved_layout(
            &live,
            &url,
            None,
            DYNAMIC_SIZE as u64,
            &chunks_from_metadata(&live)
        ));
        let meta = resume::load(&ResumeMetadata::meta_path(output))
            .await
            .unwrap();
        assert!(meta.chunks.len() >= 3, "the split must be persisted");
        assert!(valid_saved_layout(
            &meta,
            &url,
            None,
            DYNAMIC_SIZE as u64,
            &chunks_from_metadata(&meta)
        ));
        let part = fs::read(format!("{output}.part")).await.unwrap();
        for c in &meta.chunks {
            let lo = c.start as usize;
            let hi = lo + c.completed as usize;
            assert_eq!(&part[lo..hi], &payload[lo..hi]);
        }
        mode.store(2, Ordering::SeqCst);
        let ctx = ParallelDownloadCtx {
            cancel: CancellationToken::new(),
            ..ctx
        };
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            download_parallel_with_initial(&ctx, 2, None, true, None::<fn(u64, u64)>),
        )
        .await
        .expect("resume stalled")
        .unwrap();
        assert_eq!(result, DYNAMIC_SIZE as u64);
        assert_eq!(fs::read(output).await.unwrap(), *payload);
    }

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
        let task = spawn_dynamic_autosave(
            "unused.rdm".into(),
            Arc::new(Mutex::new(DynamicState::new(meta))),
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
        let shared = Arc::new(Mutex::new(DynamicState::new(meta)));
        shared
            .lock()
            .await
            .progress(0)
            .unwrap()
            .store(5, Ordering::SeqCst);
        let task = spawn_dynamic_autosave(path.to_owned(), shared, stop.clone());
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
        let meta = resume::create_new("https://example.com/file".into(), 10, &single_chunk());
        let shared = Arc::new(Mutex::new(DynamicState::new(meta)));
        let counter = shared.lock().await.progress(0).unwrap();
        counter.store(2, Ordering::SeqCst);
        let reports = Arc::new(StdMutex::new(Vec::new()));
        let captured = Arc::clone(&reports);
        let task = spawn_dynamic_monitor(
            Some(move |current, total| captured.lock().unwrap().push((current, total))),
            shared,
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
        let meta = resume::create_new("https://example.com/file".into(), 10, &single_chunk());
        let shared = Arc::new(Mutex::new(DynamicState::new(meta)));
        shared
            .lock()
            .await
            .progress(0)
            .unwrap()
            .store(10, Ordering::SeqCst);
        let task = spawn_dynamic_monitor(
            Some(move |current, total| captured.lock().unwrap().push((current, total))),
            shared,
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
