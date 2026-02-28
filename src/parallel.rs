use anyhow::{Context, Result};
use reqwest::Client;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::chunk::Chunk;
use crate::range_download::{self, DownloadStatus};
use crate::resume::{self, ResumeMetadata};
use crate::retry::{self, RetryConfig};

pub async fn download_parallel<F>(
    client: &Client,
    url: &str,
    output_path: &str,
    file_size: u64,
    chunks: &[Chunk],
    retry_config: &RetryConfig,
    progress_callback: Option<F>,
    cancel: CancellationToken,
) -> Result<u64>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
    if chunks.is_empty() {
        anyhow::bail!("No chunks provided for parallel download");
    }

    let temp_path = format!("{}.part", output_path);
    let meta_path = ResumeMetadata::meta_path(output_path);

    match parallel_inner(client, url, &temp_path, &meta_path, file_size, chunks, retry_config, progress_callback, cancel).await {
        Ok(total) => {
            resume::delete(&meta_path).await?;
            fs::rename(&temp_path, output_path).await
                .with_context(|| format!("Failed to rename '{}' to '{}'", temp_path, output_path))?;
            Ok(total)
        }
        Err(e) => Err(e),
    }
}

fn chunks_from_metadata(meta: &ResumeMetadata) -> Vec<Chunk> {
    meta.chunks.iter().map(|c| {
        Chunk {
            id: c.id,
            start: c.start,
            end: c.end,
        }
    }).collect()
}

async fn parallel_inner<F>(
    client: &Client,
    url: &str,
    temp_path: &str,
    meta_path: &str,
    file_size: u64,
    chunks: &[Chunk],
    retry_config: &RetryConfig,
    progress_callback: Option<F>,
    cancel: CancellationToken,
) -> Result<u64>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;

    let meta = load_or_create_metadata(
        meta_path,
        url,
        file_size,
        chunks,
    ).await?;

    let chunks = chunks_from_metadata(&meta);

    ensure_file_allocated(temp_path, file_size).await?;

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

    let initial_completed: u64 =
        chunk_counters.iter().map(|(_, c)| c.load(Ordering::Relaxed)).sum();

    let global_progress = Arc::new(AtomicU64::new(initial_completed));
    let retry_pressure = Arc::new(AtomicUsize::new(0));
    let done_flag = Arc::new(AtomicBool::new(false));

    let autosave_handle = spawn_autosave(
        meta_path.to_string(),
        Arc::clone(&shared_meta),
        chunk_counters
            .iter()
            .map(|(id, c)| (*id, Arc::clone(c)))
            .collect(),
        Arc::clone(&done_flag),
    );

    let monitor_handle = spawn_progress_monitor(
        progress_callback,
        chunk_counters.clone(),
        Arc::clone(&done_flag),
        file_size,
    );

    let mut active_workers = chunks.len().max(1);
    let mut join_set: JoinSet<Result<u64>> = JoinSet::new();
    // FIX: Initialize from initial_completed so resumed bytes are included in total
    let mut total_bytes: u64 = initial_completed;

    while !queue.is_empty() || !join_set.is_empty() {
        while join_set.len() < active_workers && !queue.is_empty() {
            let chunk = queue.pop_front().unwrap();

            let client = client.clone();
            let url = url.to_string();
            let path = temp_path.to_string();
            let gp = Arc::clone(&global_progress);
            let cancel = cancel.clone();
            let config = retry_config.clone();
            let chunk_progress = chunk_counters
                .iter()
                .find(|(id, _)| *id == chunk.id)
                .unwrap()
                .1
                .clone();

            let pressure = retry_pressure.clone();

            join_set.spawn(async move {
                match download_chunk_with_retry(
                    &client,
                    &url,
                    &path,
                    &chunk,
                    &config,
                    chunk_progress,
                    gp,
                    cancel,
                )
                .await
                {
                    Ok(b) => Ok(b),
                    Err(e) => {
                        if retry::is_retryable(&e) {
                            pressure.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e)
                    }
                }
            });
        }

        // FIX: Use active_workers instead of chunks.len() for pressure threshold
        if retry_pressure.load(Ordering::Relaxed) >= active_workers * 2 && active_workers > 1 {
            let new = (active_workers / 2).max(1);
            eprintln!(
                "  ⚠ Server overloaded — reducing parallel connections: {} → {}",
                active_workers, new
            );
            active_workers = new;
            retry_pressure.store(0, Ordering::Relaxed);
        }

        match join_set.join_next().await {
            Some(Ok(Ok(bytes))) => total_bytes += bytes,
            Some(Ok(Err(e))) => return Err(e),
            Some(Err(e)) => return Err(anyhow::anyhow!("Worker panic: {}", e)),
            None => break,
        }
    }

    done_flag.store(true, Ordering::Relaxed);
    if let Some(h) = monitor_handle {
        let _ = h.await;
    }
    let _ = autosave_handle.await;

    if total_bytes != file_size {
        anyhow::bail!(
            "Total bytes mismatch: expected {} but downloaded {}",
            file_size,
            total_bytes
        );
    }

    Ok(total_bytes)
}

async fn load_or_create_metadata(
    meta_path: &str,
    url: &str,
    file_size: u64,
    chunks: &[Chunk],
) -> Result<ResumeMetadata> {

    if let Ok(existing) = resume::load(meta_path).await {

        if resume::validate_against(&existing, url, file_size, chunks) {
            eprintln!("  [Resume] Using existing chunk layout ({} chunks)", existing.chunks.len());
            return Ok(existing);
        }

        // Incompatible → delete and start fresh
        let _ = resume::delete(meta_path).await;
    }

    let meta = resume::create_new(url.to_string(), file_size, chunks);

    resume::save_atomic(meta_path, &meta).await?;

    Ok(meta)
}

async fn ensure_file_allocated(path: &str, size: u64) -> Result<()> {
    match fs::metadata(path).await {
        Ok(m) if m.len() == size => Ok(()),
        Ok(_) => {
            let file = fs::OpenOptions::new().write(true).open(path).await
                .with_context(|| format!("Failed to open existing file: {}", path))?;
            file.set_len(size).await
                .with_context(|| format!("Failed to resize '{}' to {} bytes", path, size))?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let file = fs::File::create(path).await
                .with_context(|| format!("Failed to create file: {}", path))?;
            file.set_len(size).await
                .with_context(|| format!("Failed to pre-allocate {} bytes for '{}'", size, path))?;
            Ok(())
        }
        Err(e) => Err(e).with_context(|| format!("Failed to stat file: {}", path)),
    }
}

fn spawn_autosave(
    meta_path: String, shared_meta: Arc<Mutex<ResumeMetadata>>,
    counters: Vec<(u32, Arc<AtomicU64>)>, done: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if done.load(Ordering::Relaxed) { break; }
            let snapshot = {
                let mut meta = shared_meta.lock().await;
                for (id, counter) in &counters {
                    resume::update_progress(&mut meta, *id, counter.load(Ordering::Relaxed));
                }
                meta.clone()
            };
            let _ = resume::save_atomic(&meta_path, &snapshot).await;
        }
    })
}

async fn download_chunk_with_retry(
    client: &Client, url: &str, file_path: &str, chunk: &Chunk, config: &RetryConfig,
    chunk_progress: Arc<AtomicU64>, global_progress: Arc<AtomicU64>, cancel: CancellationToken,
) -> Result<u64> {
    let full_chunk_size = chunk.end - chunk.start + 1;
    let initial_completed = chunk_progress.load(Ordering::SeqCst);

    for attempt in 0..=config.max_retries {
        if cancel.is_cancelled() {
            let written = chunk_progress.load(Ordering::SeqCst);
            anyhow::bail!("Chunk #{} cancelled before attempt {} ({} of {} bytes on disk)", chunk.id, attempt + 1, written, full_chunk_size);
        }

        let resume_from = chunk_progress.load(Ordering::SeqCst);
        if resume_from >= full_chunk_size { return Ok(full_chunk_size - initial_completed); }

        match range_download::download_range(
            client, url, file_path, chunk.start, chunk.end, resume_from,
            Arc::clone(&chunk_progress), Some(Arc::clone(&global_progress)), cancel.clone(),
        ).await {
            // FIX 3: Report only bytes written during this invocation
            Ok(DownloadStatus::Complete { bytes_written: _ }) => {
                return Ok(chunk_progress.load(Ordering::SeqCst) - initial_completed);
            }

            Ok(DownloadStatus::Cancelled { .. }) => {
                let written = chunk_progress.load(Ordering::SeqCst);
                anyhow::bail!("Chunk #{} cancelled after {} of {} bytes", chunk.id, written, full_chunk_size);
            }

            Err(e) if e.root_cause().to_string().contains("does not support range requests")
                    && attempt < config.max_retries =>
                {
                    let old = chunk_progress.swap(0, Ordering::SeqCst);
                    global_progress.fetch_sub(old, Ordering::Relaxed);
                    eprintln!(
                        "   ⚠ Chunk #{}: range not supported, restarting from byte 0",
                        chunk.id,
                    );
                    continue;
                }

            Err(e) if retry::is_retryable(&e) && attempt < config.max_retries => {
                let written = chunk_progress.load(Ordering::SeqCst);
                let delay = config.delay_for_attempt(attempt);
                eprintln!(
    "   ⚠ Chunk #{}: {}/{} failed, retry in {:.1}s — {}",
                    chunk.id, attempt + 1, config.max_retries + 1,
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
                    chunk.id, attempt + 1, written, full_chunk_size,
                )));
            }
        }
    }

    cancel.cancel();
    let written = chunk_progress.load(Ordering::SeqCst);
    anyhow::bail!("Chunk #{}: exhausted {} retries ({}/{} bytes on disk)", chunk.id, config.max_retries, written, full_chunk_size)
}

fn short_error(err: &anyhow::Error) -> String {
    let msg = err.chain().last().map(|e| e.to_string()).unwrap_or_else(|| err.to_string());
    if msg.len() > 80 {
        let mut end = 77;
        while end > 0 && !msg.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &msg[..end])
    } else {
        msg
    }
}

fn spawn_progress_monitor<F>(
    callback: Option<F>,
    counters: Vec<(u32, Arc<AtomicU64>)>,
    done: Arc<AtomicBool>,
    total: u64,
) -> Option<JoinHandle<()>>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
    let cb = callback?;
    Some(tokio::spawn(async move {
        loop {
            let current: u64 = counters
                .iter()
                .map(|(_, c)| c.load(Ordering::Relaxed))
                .sum();
            cb(current, total);
            if done.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let current: u64 = counters
            .iter()
            .map(|(_, c)| c.load(Ordering::Relaxed))
            .sum();
        cb(current, total);
    }))
}
