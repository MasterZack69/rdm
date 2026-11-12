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
    download_parallel_with_initial(ctx, ctx.chunks.len().max(1), None, progress_callback).await
}

pub(crate) async fn download_parallel_with_initial<F>(
    ctx: &ParallelDownloadCtx<'_>,
    max_workers: usize,
    initial_response: Option<reqwest::Response>,
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

    ensure_file_allocated(temp_path, ctx.file_size, chunks.len()).await?;

    let shared_meta = Arc::new(Mutex::new(meta));

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

    let mut queue: VecDeque<Chunk> = chunks
        .iter()
        .filter(|chunk| {
            let completed = chunk_counters
                .iter()
                .find(|(id, _)| *id == chunk.id)
                .map(|(_, progress)| progress.load(Ordering::Relaxed))
                .unwrap_or(0);
            completed < chunk.end - chunk.start + 1
        })
        .cloned()
        .collect();

    // One view of how the server is coping, shared by every worker and by
    // the loop that hands out ranges.
    let pressure = Arc::new(PressureController::new(max_workers.max(1)));
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
    let mut initial_response = initial_response;

    let worker_result: Result<()> = async {
        if let Some(response) = initial_response.take() {
            let first = chunks
                .first()
                .context("Initial response has no assigned range")?;
            let progress = chunk_counters
                .iter()
                .find(|(id, _)| *id == first.id)
                .map(|(_, progress)| Arc::clone(progress))
                .context("Initial response range has no progress counter")?;
            let previous = progress.load(Ordering::Relaxed);
            let first_len = first.end - first.start + 1;

            // A fully completed first range is already on disk and belongs to
            // the validated resume state. The probe response is then discarded
            // without touching those bytes.
            if previous < first_len {
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
                        total_bytes += first_len - previous;
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

        if resume::validate_against(&existing, url, identity, file_size, chunks)
            && identity_check == IdentityCheck::Match
        {
            eprintln!(
                "  [Resume] Using existing chunk layout ({} chunks)",
                existing.chunks.len()
            );
            return Ok(existing);
        }

        // A newer range plan may have more work units than the saved one.
        // When the source and its validator still match, translate each
        // completed prefix into the new layout instead of throwing away a
        // resumable partial file because its chunk boundaries changed.
        if identity_check == IdentityCheck::Match
            && existing.describes_same_source(url, identity)
            && existing.file_size == file_size
            && let Some(repartitioned) = resume::repartition(&existing, chunks)
        {
            resume::save_atomic(meta_path, &repartitioned).await?;
            eprintln!(
                "  [Resume] Preserved progress while updating the range layout ({} chunks)",
                repartitioned.chunks.len()
            );
            return Ok(repartitioned);
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
async fn ensure_file_allocated(path: &str, size: u64, chunk_count: usize) -> Result<()> {
    let file = safe_file::open_guarded(
        Path::new(path),
        Existing::Open,
        Access::ReadWrite,
        safe_file::DEFAULT_FILE_MODE,
    )
    .with_context(|| format!("Failed to open file: {}", path))?;

    let current = file
        .metadata()
        .with_context(|| format!("Failed to stat file: {}", path))?
        .len();

    // Already the right length.
    if current == size {
        return Ok(());
    }

    // Single chunk starting from nothing: the file starts at 0 bytes and the
    // sequential write extends it naturally, exactly as before.
    if current == 0 && chunk_count <= 1 {
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

fn spawn_progress_monitor<F>(
    callback: Option<F>,
    counters: Vec<(u32, Arc<AtomicU64>)>,
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
            Some(move |current, total| captured.lock().unwrap().push((current, total))),
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
            Some(move |current, total| captured.lock().unwrap().push((current, total))),
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
