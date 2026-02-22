use anyhow::{Context, Result};
use reqwest::Client;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
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
    let meta = load_or_create_metadata(meta_path, url, file_size, chunks).await?;
    ensure_file_allocated(temp_path, file_size).await?;

    let shared_meta = Arc::new(Mutex::new(meta));

    let chunk_counters: Vec<(u32, Arc<AtomicU64>)> = {
        let meta_guard = shared_meta.lock().await;
        chunks.iter().map(|c| {
            let completed = meta_guard.chunks.iter().find(|s| s.id == c.id).map(|s| s.completed).unwrap_or(0);
            (c.id, Arc::new(AtomicU64::new(completed)))
        }).collect()
    };

    let initial_completed: u64 = chunk_counters.iter().map(|(_, c)| c.load(Ordering::Relaxed)).sum();
    let global_progress = Arc::new(AtomicU64::new(initial_completed));
    let done_flag = Arc::new(AtomicBool::new(false));

    let autosave_handle = spawn_autosave(
        meta_path.to_string(), Arc::clone(&shared_meta),
        chunk_counters.iter().map(|(id, c)| (*id, Arc::clone(c))).collect(),
        Arc::clone(&done_flag),
    );

    let monitor_handle = spawn_progress_monitor(
        progress_callback, Arc::clone(&global_progress), Arc::clone(&done_flag), file_size,
    );

    let handles: Vec<(u32, JoinHandle<Result<u64>>)> = chunks.iter().enumerate().map(|(i, chunk)| {
        let client = client.clone();
        let url = url.to_string();
        let path = temp_path.to_string();
        let gp = Arc::clone(&global_progress);
        let cancel = cancel.clone();
        let config = retry_config.clone();
        let chunk = chunk.clone();
        let chunk_progress = Arc::clone(&chunk_counters[i].1);

        let id = chunk.id;
        let handle = tokio::spawn(async move {
    download_chunk_with_retry(
        &client,
        &url,
        &path,
        &chunk,
        &config,
        chunk_progress,
        gp,
        cancel,
    ).await
});

        (id, handle)
    }).collect();

    let mut total_bytes: u64 = 0;
    let mut errors: Vec<String> = Vec::new();

    for (id, handle) in handles {
        match handle.await {
            Ok(Ok(bytes)) => total_bytes += bytes,
            Ok(Err(e)) => errors.push(format!("Chunk #{}: {:#}", id, e)),
            Err(join_err) => {
                if join_err.is_panic() { errors.push(format!("Chunk #{}: task panicked", id)); }
                else { errors.push(format!("Chunk #{}: task aborted", id)); }
            }
        }
    }

    done_flag.store(true, Ordering::Relaxed);
    if let Some(h) = monitor_handle { let _ = h.await; }
    let _ = autosave_handle.await;

    let snapshot = {
        let mut meta_guard = shared_meta.lock().await;
        for (id, counter) in &chunk_counters {
            resume::update_progress(&mut meta_guard, *id, counter.load(Ordering::SeqCst));
        }
        meta_guard.clone()
    };
    let _ = resume::save_atomic(meta_path, &snapshot).await;

    if !errors.is_empty() {
        let report = errors.join("\n  • ");
        anyhow::bail!("{} of {} chunk(s) failed:\n  • {}", errors.len(), chunks.len(), report);
    }

    if total_bytes != file_size {
        anyhow::bail!("Total bytes mismatch: expected {} but downloaded {}", file_size, total_bytes);
    }

    Ok(total_bytes)
}

async fn load_or_create_metadata(meta_path: &str, url: &str, file_size: u64, chunks: &[Chunk]) -> Result<ResumeMetadata> {
    if let Ok(existing) = resume::load(meta_path).await {
        if resume::validate_against(&existing, url, file_size, chunks) { return Ok(existing); }
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

    for attempt in 0..=config.max_retries {
        if cancel.is_cancelled() {
            let written = chunk_progress.load(Ordering::SeqCst);
            anyhow::bail!("Chunk #{} cancelled before attempt {} ({} of {} bytes on disk)", chunk.id, attempt + 1, written, full_chunk_size);
        }

        let resume_from = chunk_progress.load(Ordering::SeqCst);
        if resume_from >= full_chunk_size { return Ok(full_chunk_size); }

        match range_download::download_range(
            client, url, file_path, chunk.start, chunk.end, resume_from,
            Arc::clone(&chunk_progress), Some(Arc::clone(&global_progress)), cancel.clone(),
        ).await {
            Ok(DownloadStatus::Complete { bytes_written }) => return Ok(resume_from + bytes_written),

            Ok(DownloadStatus::Cancelled { .. }) => {
                let written = chunk_progress.load(Ordering::SeqCst);
                anyhow::bail!("Chunk #{} cancelled after {} of {} bytes", chunk.id, written, full_chunk_size);
            }

            Err(e) if retry::is_retryable(&e) && attempt < config.max_retries => {
                let written = chunk_progress.load(Ordering::SeqCst);
                let delay = config.delay_for_attempt(attempt);
                eprintln!(
                    "   ⚠ Chunk #{}: attempt {}/{} failed ({}/{} bytes), retrying in {:.1}s — {}",
                    chunk.id, attempt + 1, config.max_retries + 1, written, full_chunk_size,
                    delay.as_secs_f64(), single_line_error(&e),
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

fn single_line_error(err: &anyhow::Error) -> String {
    format!("{:#}", err).replace('\n', " | ")
}

fn spawn_progress_monitor<F>(callback: Option<F>, counter: Arc<AtomicU64>, done: Arc<AtomicBool>, total: u64) -> Option<JoinHandle<()>>
where F: Fn(u64, u64) + Send + Sync + 'static {
    let cb = callback?;
    Some(tokio::spawn(async move {
        loop {
            cb(counter.load(Ordering::Relaxed), total);
            if done.load(Ordering::Relaxed) { break; }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        cb(counter.load(Ordering::Relaxed), total);
    }))
}
