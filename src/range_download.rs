use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode, header};
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio_util::sync::CancellationToken;

use crate::retry::{TransientError, is_transient_status};
use crate::safe_file::{self, Access, Existing};

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
            // `without_url` before `context`: reqwest puts the URL into its
            // own error Display, `context` keeps that error in the chain, and
            // `{:#}` prints the chain. This is the retrying path -- the caller
            // formats the error once per attempt -- so a gdrive fetch URL
            // would have repeated its `key=` on every retry line. The byte
            // range stays; it is not a secret.
            result
                .map_err(reqwest::Error::without_url)
                .with_context(|| format!("Range GET failed for {}", range_value))?
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
                message: format!(
                    "Transient HTTP {} for range {}",
                    status.as_u16(),
                    range_value
                ),
            }));
        }
        anyhow::bail!(
            "Permanent HTTP error for range {}: {} {}",
            range_value,
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown"),
        );
    } else {
        validate_content_range(response.headers(), effective_start, end)?;
    }

    // `<output>.part` is a predictable name in a directory the user may share,
    // and every chunk worker opens it. An ordinary open follows a symlink at
    // the final component, so a planted link turned these offset writes into
    // writes through to another file. `open_guarded` resolves once relative to
    // the directory, refuses to traverse a symlink, and fstats the descriptor
    // to confirm a regular file this process owns.
    let file = safe_file::open_guarded(
        Path::new(file_path),
        Existing::Open,
        Access::ReadWrite,
        safe_file::DEFAULT_FILE_MODE,
    )
    .with_context(|| format!("Failed to open file: {}", file_path))?;

    let mut file = BufWriter::with_capacity(2 * 512 * 1024, tokio::fs::File::from_std(file));

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
                let data_len: u64 = data.len() as u64;

                if bytes_written + data_len > expected_len {
                    file.flush().await.ok();
                    chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
                    drop(stream);
                    anyhow::bail!(
                        "Server sent excess data for range {}: expected {} bytes, got at least {}",
                        range_value,
                        expected_len,
                        bytes_written + data_len,
                    );
                }

                file.write_all(&data).await.with_context(|| {
                    format!(
                        "Write failed at offset {} in {}",
                        effective_start + bytes_written,
                        file_path
                    )
                })?;

                bytes_written += data_len;
                bytes_since_flush += data_len;

                if bytes_since_flush >= 16 * 1024 * 1024 {
                    file.flush().await.with_context(|| {
                        format!(
                            "Periodic flush failed at offset {}",
                            effective_start + bytes_written
                        )
                    })?;
                    bytes_since_flush = 0;
                }

                chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
            }
            Some(Err(e)) => {
                file.flush().await.ok();
                chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
                // The stream error names the URL as well.
                return Err(e.without_url()).context(format!(
                    "Stream error at byte {} of range {}",
                    bytes_written, range_value,
                ));
            }
            None => break,
        }
    }

    file.flush()
        .await
        .context("Failed to flush file after range write")?;

    if bytes_written != expected_len {
        chunk_progress.store(resume_from + bytes_written, Ordering::SeqCst);
        anyhow::bail!(
            "Truncated range {}: expected {} bytes, wrote {}",
            range_value,
            expected_len,
            bytes_written,
        );
    }

    chunk_progress.store(full_chunk_len, Ordering::SeqCst);
    Ok(DownloadStatus::Complete { bytes_written })
}

fn validate_content_range(
    headers: &header::HeaderMap,
    expected_start: u64,
    expected_end: u64,
) -> Result<()> {
    let value = headers
        .get(header::CONTENT_RANGE)
        .context("Server returned 206 without Content-Range")?
        .to_str()
        .context("Content-Range is not valid UTF-8")?;

    let rest = value
        .strip_prefix("bytes ")
        .with_context(|| format!("Unexpected Content-Range format: '{}'", value))?;

    let (range_part, _) = rest
        .split_once('/')
        .with_context(|| format!("Content-Range missing '/': '{}'", value))?;

    let dash = range_part
        .find('-')
        .with_context(|| format!("Content-Range missing '-': '{}'", value))?;

    let actual_start: u64 = range_part[..dash]
        .parse()
        .with_context(|| format!("Invalid start in Content-Range: '{}'", value))?;
    let actual_end: u64 = range_part[dash + 1..]
        .parse()
        .with_context(|| format!("Invalid end in Content-Range: '{}'", value))?;

    if actual_start != expected_start || actual_end != expected_end {
        anyhow::bail!(
            "Content-Range mismatch: requested {}-{}, got '{}'",
            expected_start,
            expected_end,
            value
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn test_valid_content_range() {
        let mut h = HeaderMap::new();
        h.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-999/8000"),
        );
        assert!(validate_content_range(&h, 0, 999).is_ok());
    }

    #[test]
    fn test_resumed_content_range() {
        let mut h = HeaderMap::new();
        h.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 500-999/8000"),
        );
        assert!(validate_content_range(&h, 500, 999).is_ok());
    }

    #[test]
    fn test_content_range_mismatch() {
        let mut h = HeaderMap::new();
        h.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-499/8000"),
        );
        assert!(validate_content_range(&h, 0, 999).is_err());
    }

    #[test]
    fn test_content_range_wildcard() {
        let mut h = HeaderMap::new();
        h.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 100-199/*"),
        );
        assert!(validate_content_range(&h, 100, 199).is_ok());
    }

    #[test]
    fn test_content_range_missing() {
        assert!(validate_content_range(&HeaderMap::new(), 0, 99).is_err());
    }

    #[test]
    fn test_content_range_bad_prefix() {
        let mut h = HeaderMap::new();
        h.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_static("octets 0-99/100"),
        );
        assert!(validate_content_range(&h, 0, 99).is_err());
    }

    /// See the note in `inspect.rs`: a free-then-closed port rather than a
/// hardcoded low one, and `.no_proxy()`, so that a system proxy cannot
/// answer for the unreachable address and let the send succeed.
fn refused_url(query: &str) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("http://127.0.0.1:{port}/f?{query}")
}

/// The chunk path retries, so this error text is printed once per attempt.
#[tokio::test]
async fn a_failed_range_request_does_not_name_the_url_it_failed_on() {
    let progress = Arc::new(AtomicU64::new(0));
    let error = download_range(
        &Client::builder().no_proxy().build().unwrap(),
        &refused_url("key=SUPERSECRETKEY"),
        "/nonexistent-directory-for-rdm-test/x.part",
        0,
        1023,
        0,
        progress,
        CancellationToken::new(),
    )
    .await
    .expect_err("a closed loopback port must not answer");

    let chain = format!("{error:#}");
    assert!(!chain.contains("SUPERSECRETKEY"), "leaked: {chain}");
    assert!(!chain.contains("127.0.0.1"), "leaked: {chain}");
}

}
