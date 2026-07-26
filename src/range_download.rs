use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio_util::sync::CancellationToken;

use crate::retry::{is_transient_status, TransientError};

#[derive(Debug)]
pub enum DownloadStatus {
    Complete { bytes_written: u64 },
    Cancelled { bytes_on_disk: u64 },
}

#[allow(clippy::too_many_arguments)]
pub async fn download_range(
    client: &Client,
    url: &str,
    file_path: &str,
    start: u64,
    end: u64,
    resume_from: u64,
    chunk_progress: Arc<AtomicU64>,
    cancel: CancellationToken,
) -> Result<DownloadStatus> {
    if end < start {
        anyhow::bail!("Invalid byte range: start ({}) > end ({})", start, end);
    }

    let full_chunk_len = end - start + 1;

    if resume_from >= full_chunk_len {
        return Ok(DownloadStatus::Complete { bytes_written: 0 });
    }

    let effective_start = start + resume_from;
    let expected_len = end - effective_start + 1;
    let range_value = format!("bytes={}-{}", effective_start, end);

    let response = tokio::select! {
        biased;

        _ = cancel.cancelled() => {
            return Ok(DownloadStatus::Cancelled {
                bytes_on_disk: resume_from
            });
        }

        result = client
            .get(url)
            .header(header::RANGE, &range_value)
            .send() =>
        {
            result.with_context(|| format!("Range GET failed for {}", range_value))?
        }
    };

    let status = response.status();

    if status == StatusCode::OK && effective_start == 0 {
        // Server doesn't support ranges but we're downloading from the start.
    } else if status == StatusCode::OK && effective_start > 0 {
        anyhow::bail!(
            "Server does not support range requests — cannot resume from byte {}",
            effective_start,
        );
    } else if status != StatusCode::PARTIAL_CONTENT {
        if is_transient_status(status) {
            return Err(anyhow::Error::new(TransientError {
                message: format!("Transient HTTP {} for range {}", status.as_u16(), range_value),
            }));
        }
        anyhow::bail!(
            "Permanent HTTP error for range {}: {} {}",
            range_value, status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown"),
        );
    } else {
        validate_content_range(response.headers(), effective_start, end)?;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)
        .await
        .with_context(|| format!("Failed to open file: {}", file_path))?;

    let mut file = BufWriter::with_capacity(2 * 512 * 1024, file);

    file.seek(SeekFrom::Start(effective_start))
        .await
        .with_context(|| format!("Failed to seek to offset {}", effective_start))?;

    let mut stream = response.bytes_stream();
    let mut bytes_written: u64 = 0;
    let mut bytes_since_flush: u64 = 0;

        loop {
        let chunk = tokio::select! {
            c = stream.next() => c,
            _ = cancel.cancelled() => {
                file.flush().await.ok();
                chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
                return Ok(DownloadStatus::Cancelled {
                    bytes_on_disk: resume_from + bytes_written,
                });
            }
        };

        match chunk {
            Some(Ok(data)) => {
                let data_len: u64 = data.len() as u64