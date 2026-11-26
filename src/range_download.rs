use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode, header};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;

use crate::range_writer::RangeWriter;
use crate::retry::{TransientError, is_transient_status};
use crate::safe_file::{self, Access, Existing};

/// Seconds since the epoch, for `Retry-After`'s date form.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

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

    download_response(
        response,
        file_path,
        start,
        end,
        resume_from,
        chunk_progress,
        cancel,
    )
    .await
}

/// Writes an already-open response into its assigned byte interval. The
/// response may be the useful body returned by the initial download probe.
#[allow(clippy::too_many_arguments)]
pub async fn download_response(
    response: reqwest::Response,
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
            // A 429 or 503 often says how long to wait. Reading it is what
            // turns a retry storm into a queue.
            let retry_after = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| crate::retry::parse_retry_after(v, unix_now()));

            return Err(anyhow::Error::new(TransientError::after(
                format!(
                    "Transient HTTP {} for range {}",
                    status.as_u16(),
                    range_value
                ),
                retry_after,
            )));
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
    // to confirm a regular file this process owns — so a worker only ever
    // writes to a file this process could have created, and publication at the
    // end re-validates before linking anything into place.
    let file = safe_file::open_guarded(
        Path::new(file_path),
        Existing::Open,
        Access::ReadWrite,
        safe_file::DEFAULT_FILE_MODE,
    )
    .with_context(|| format!("Failed to open file: {}", file_path))?;

    let mut file = RangeWriter::new(
        file,
        effective_start,
        resume_from,
        Arc::clone(&chunk_progress),
    );
    // Flush small, slow responses too, so their saved progress does not wait
    // indefinitely for a full batch.
    let mut flush_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    flush_tick.tick().await;
    let mut stream = response.bytes_stream();
    let mut bytes_written: u64 = 0;

    loop {
        let chunk = tokio::select! {
            c = stream.next() => c,
            _ = cancel.cancelled() => {
                file.flush().await.context("Failed to flush cancelled range")?;
                return Ok(DownloadStatus::Cancelled {
                    bytes_on_disk: chunk_progress.load(Ordering::SeqCst),
                });
            }
            _ = flush_tick.tick(), if file.has_buffered() => {
                file.flush().await.context("Periodic range flush failed")?;
                continue;
            }
        };

        match chunk {
            Some(Ok(data)) => {
                let data_len: u64 = data.len() as u64;

                if bytes_written + data_len > expected_len {
                    file.flush()
                        .await
                        .context("Failed to flush valid prefix of excess response")?;
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
            }
            Some(Err(e)) => {
                file.flush()
                    .await
                    .context("Failed to flush interrupted range")?;
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

    async fn range_server(body: axum::body::Body, content_range: &'static str) -> String {
        use axum::{Router, http::StatusCode, routing::get};
        use std::sync::Mutex;
        // The test makes one request; retaining the body behind a mutex lets
        // axum's reusable handler take it without requiring Body: Clone.
        let body = Arc::new(Mutex::new(Some(body)));
        let app = Router::new().route(
            "/file",
            get(move || {
                let body = body.lock().unwrap().take().unwrap();
                async move {
                    (
                        StatusCode::PARTIAL_CONTENT,
                        [(header::CONTENT_RANGE, content_range)],
                        body,
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/file")
    }

    #[tokio::test]
    async fn short_response_flushes_only_the_real_prefix() {
        let url = range_server(axum::body::Body::from("abc"), "bytes 0-9/10").await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, [0; 10]).unwrap();
        let progress = Arc::new(AtomicU64::new(0));
        let err = download_range(
            &Client::builder().no_proxy().build().unwrap(),
            &url,
            path.to_str().unwrap(),
            0,
            9,
            0,
            Arc::clone(&progress),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("Truncated range"));
        assert_eq!(progress.load(Ordering::SeqCst), 3);
        assert_eq!(&std::fs::read(path).unwrap(), b"abc\0\0\0\0\0\0\0");
    }

    #[tokio::test]
    async fn resumed_range_writes_at_the_correct_offset() {
        let url = range_server(axum::body::Body::from("3456789"), "bytes 3-9/10").await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, b"012\0\0\0\0\0\0\0").unwrap();
        let progress = Arc::new(AtomicU64::new(3));
        let result = download_range(
            &Client::builder().no_proxy().build().unwrap(),
            &url,
            path.to_str().unwrap(),
            0,
            9,
            3,
            Arc::clone(&progress),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(matches!(result, DownloadStatus::Complete { .. }));
        assert_eq!(progress.load(Ordering::SeqCst), 10);
        assert_eq!(&std::fs::read(path).unwrap(), b"0123456789");
    }

    #[tokio::test]
    async fn excess_response_does_not_overwrite_adjacent_range() {
        let url = range_server(axum::body::Body::from("123456"), "bytes 0-3/8").await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, b"0000KEEP").unwrap();
        let progress = Arc::new(AtomicU64::new(0));
        let result = download_range(
            &Client::builder().no_proxy().build().unwrap(),
            &url,
            path.to_str().unwrap(),
            0,
            3,
            0,
            Arc::clone(&progress),
            CancellationToken::new(),
        )
        .await;
        assert!(result.is_err());
        assert!(progress.load(Ordering::SeqCst) <= 4);
        assert_eq!(&std::fs::read(path).unwrap()[4..], b"KEEP");
    }

    #[tokio::test]
    async fn cancelling_a_slow_range_flushes_its_partial_batch() {
        use axum::body::{Body, Bytes};
        use futures_util::stream::{self, StreamExt};
        use std::time::Duration;
        let first =
            stream::once(async { Ok::<Bytes, std::io::Error>(Bytes::from(vec![0x73; 8192])) });
        let body = Body::from_stream(first.chain(stream::pending()));
        let url = range_server(body, "bytes 0-2097151/2097152").await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, vec![0; 2 * 1024 * 1024]).unwrap();
        let progress = Arc::new(AtomicU64::new(0));
        let cancel = CancellationToken::new();
        let task_progress = Arc::clone(&progress);
        let task_cancel = cancel.clone();
        let task_path = path.clone();
        let task = tokio::spawn(async move {
            download_range(
                &Client::builder().no_proxy().build().unwrap(),
                &url,
                task_path.to_str().unwrap(),
                0,
                2 * 1024 * 1024 - 1,
                0,
                task_progress,
                task_cancel,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while progress.load(Ordering::SeqCst) < 8192 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("slow partial batch was not flushed");
        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            result,
            DownloadStatus::Cancelled {
                bytes_on_disk: 8192
            }
        ));
        let bytes = std::fs::read(path).unwrap();
        assert!(bytes[..8192].iter().all(|b| *b == 0x73));
        assert!(bytes[8192..].iter().all(|b| *b == 0));
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
