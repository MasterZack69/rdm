//! Turning a [`DownloadRequest`] into a file on disk.

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::chunk::Chunk;
use crate::inspect;
use crate::net;
use crate::parallel;
use crate::retry::RetryConfig;
use crate::secret_url;
use crate::ui::{self, ProgressSink, SlotState};

use super::client::shared_config;
use super::name::safe_filename;
use super::output::{resolve_existing_output, resolve_output_path};
use super::request::{DownloadRequest, Outcome, OutputDecision};
use super::streaming::{StreamIdentity, download_streaming};
use super::url::normalize_download_url;

/// Downloads one file, reporting everything through `sink`.
pub async fn download(
    req: DownloadRequest,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<Outcome> {
    let url = normalize_download_url(&req.url);
    let original_path = resolve_output_path(&url, req.output.as_deref());

    let output_path = match resolve_existing_output(
        &original_path,
        &url,
        req.resume_identity.as_deref(),
        req.policy,
    )
    .await?
    {
        OutputDecision::Use(p) => p,
        OutputDecision::AlreadyPresent => {
            sink.finish();
            return Ok(Outcome::AlreadyPresent {
                path: original_path,
            });
        }
        OutputDecision::Cancelled => {
            sink.finish();
            return Ok(Outcome::Cancelled);
        }
    };

    let user_explicitly_renamed = output_path != original_path;
    let requested_connections = req.connections.max(1);

    // A hoster that had to authenticate hands us its own session and does its
    // own addressing. Everything else — every URL a listing handed us — is
    // resolved and judged here, and fetched over a client pinned to the
    // addresses that judgement was made about.
    //
    // `url` stays what the user asked for: it names the file on disk and keys
    // the resume state. Only the fetching moves to `fetch_url`, because a
    // redirect target is a poor filename and a worse identity.
    let resolved;
    let (fetch_url, client, initial_probe) = match req.client.as_ref() {
        Some(session) => (url.clone(), session, None),
        None => {
            let policy = net::Policy::new(req.allow_private);
            let (target, response) = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    sink.finish();
                    return Ok(Outcome::Cancelled);
                }
                result = policy.probe(&url) => result?,
            };
            resolved = target;
            (resolved.url.to_string(), &resolved.client, Some(response))
        }
    };

    sink.state(SlotState::Inspecting);
    // Redacted, and never `fetch_url`. When a hoster drives this path the URL
    // it hands over is an API call carrying gdrive's `key=` parameter or
    // OneDrive's `tempauth` signature, and `detail` goes to stderr, which ends
    // up in scrollback, CI output and support captures.
    sink.detail(&format!("Inspecting: {}", secret_url::redact(&url)));

    // Policy::probe already made the same bytes=0-0 request after enforcing
    // redirect and address safety. Use its final response rather than asking
    // the same server a second time; externally supplied clients still probe.
    let info = match initial_probe {
        Some(response) => inspect::inspect_response(client, &fetch_url, response).await?,
        None => inspect::inspect_url(client, &fetch_url).await?,
    };

    // Use the server-suggested filename when the URL has no extension, but
    // only if the user didn't explicitly choose a name (via rename or -o).
    //
    // That suggestion is a *different* destination from the one cleared above,
    // so the existence check has to run again against it. Without this the
    // check had been answered for a path the download no longer used, and the
    // rename at the end of the download replaced whatever happened to be at
    // the new one.
    let output_path = match suggested_output_path(&output_path, &info, user_explicitly_renamed) {
        Some(candidate) => match resolve_existing_output(
            &candidate,
            &url,
            req.resume_identity.as_deref(),
            req.policy,
        )
        .await?
        {
            OutputDecision::Use(p) => p,
            OutputDecision::AlreadyPresent => {
                sink.finish();
                return Ok(Outcome::AlreadyPresent { path: candidate });
            }
            OutputDecision::Cancelled => {
                sink.finish();
                return Ok(Outcome::Cancelled);
            }
        },
        None => output_path,
    };

    // Unknown file size → streaming fallback.
    let file_size = match info.size {
        // An empty file is a file. There is no body to stream and no range to
        // ask for, so the whole transfer is creating it.
        Some(0) => {
            sink.detail("File size : 0 B (empty file)");
            sink.detail(&format!("Output    : {}", ui::terminal_safe(&output_path)));
            sink.total(Some(0));
            sink.state(SlotState::Finishing);
            let result = write_empty_file(&output_path).await;
            sink.finish();
            result?;
            return Ok(Outcome::Completed {
                path: output_path,
                bytes: 0,
            });
        }
        Some(s) => s,
        None => {
            sink.detail("File size : unknown (streaming)");
            // The filename can have come from a listing or from a server's
            // Content-Disposition, so it is made safe to draw.
            sink.detail(&format!("Output    : {}", ui::terminal_safe(&output_path)));
            sink.total(None);
            sink.state(SlotState::Downloading);

            let result = download_streaming(
                client,
                &fetch_url,
                &output_path,
                StreamIdentity {
                    url: &url,
                    identity: req.resume_identity.as_deref(),
                    etag: info.etag.as_deref(),
                    last_modified: info.last_modified.as_deref(),
                },
                cancel,
                Arc::clone(&sink),
            )
            .await;
            sink.finish();

            return match result {
                Ok(bytes) => Ok(Outcome::Completed {
                    path: output_path,
                    bytes,
                }),
                Err(e) => Err(e),
            };
        }
    };

    // Small files gain nothing from parallel connections.
    let adaptive_stream = file_size >= 4 * 1024 * 1024
        && info.supports_range
        && requested_connections > 1
        && adaptive_stream_enabled();

    let connections = if adaptive_stream {
        // This experimental mode is intended to compare the eight-worker
        // schedule from the R2 investigation. It remains opt-in and does not
        // change the requested count for the normal downloader.
        8
    } else if file_size < 4 * 1024 * 1024 {
        1
    } else {
        requested_connections
    };

    sink.detail(&format!("File size : {}", ui::format_size(file_size)));
    sink.detail(&format!(
        "Range     : {}",
        if info.supports_range {
            "supported"
        } else {
            "not supported"
        }
    ));
    sink.detail(&format!("Output    : {}", ui::terminal_safe(&output_path)));

    // Experimental work queue: create more bounded ranges than workers so a
    // fast connection can take another range when it finishes.
    // Keep the fixed connection-sized layout as the default/control. Existing
    // partial files always stay on their saved layout; changing the chunk
    // plan would make their per-chunk resume offsets ambiguous.
    let adaptive_ranges =
        !adaptive_stream && info.supports_range && connections > 1 && adaptive_ranges_enabled();

    let chunks = if adaptive_stream {
        plan_chunks_with_piece_size(file_size, 16 * 1024 * 1024)
    } else if adaptive_ranges {
        plan_chunks_with_max_size(file_size, 64 * 1024 * 1024, connections)
    } else if info.supports_range && connections > 1 {
        plan_chunks_with_count(file_size, connections as u32)
    } else {
        vec![Chunk {
            id: 1,
            start: 0,
            end: file_size - 1,
        }]
    };

    if !info.supports_range {
        let meta_path = crate::resume::ResumeMetadata::meta_path(&output_path);
        let part_path = format!("{}.part", output_path);
        let _ = std::fs::remove_file(&meta_path);
        let _ = std::fs::remove_file(&part_path);
    }

    sink.detail(&format!(
        "Ranges    : {}{}",
        chunks.len(),
        if adaptive_stream {
            " (adaptive stream schedule, 16 MiB pieces)"
        } else if adaptive_ranges {
            " (adaptive schedule)"
        } else {
            ""
        }
    ));

    sink.total(Some(file_size));
    sink.state(SlotState::Downloading);

    // The sink owns throttling, smoothing, and ETA maths now, so the callback
    // is just a forwarder.
    let progress_sink = Arc::clone(&sink);
    let progress_callback = move |downloaded: u64, _total: u64| {
        progress_sink.progress(downloaded);
    };

    let retry_config = RetryConfig {
        max_retries: shared_config().max_retries,
        ..RetryConfig::default()
    };

    let ctx = parallel::ParallelDownloadCtx {
        client,
        url: &url,
        fetch_url: &fetch_url,
        output_path: &output_path,
        file_size,
        workers: connections,
        chunks: &chunks,
        retry_config: &retry_config,
        cancel,
        identity: req.resume_identity.clone(),
        etag: info.etag.clone(),
        last_modified: info.last_modified.clone(),
    };

    let download_result = if adaptive_stream {
        parallel::download_parallel_adaptive_stream(&ctx, Some(progress_callback)).await
    } else {
        parallel::download_parallel(&ctx, Some(progress_callback)).await
    };

    sink.state(SlotState::Finishing);
    sink.finish();

    match download_result {
        Ok(bytes) => Ok(Outcome::Completed {
            path: output_path,
            bytes,
        }),
        Err(e) => Err(e),
    }
}

/// `RDM_ADAPTIVE_RANGES=1` opts into the experimental multi-range schedule.
/// It deliberately has no CLI default so stock behavior remains the control.
fn adaptive_ranges_enabled() -> bool {
    std::env::var("RDM_ADAPTIVE_RANGES")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

/// `RDM_ADAPTIVE_STREAM=1` opts into the experimental eight-worker schedule
/// that keeps each open-ended byte-range response alive across safely claimed
/// adjacent pieces. It is separate from the bounded queued-range experiment.
fn adaptive_stream_enabled() -> bool {
    std::env::var("RDM_ADAPTIVE_STREAM")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

/// Plans fixed-size logical pieces for ownership and resume progress. The
/// final piece may be shorter; HTTP responses are not bounded to one piece.
fn plan_chunks_with_piece_size(file_size: u64, piece_size: u64) -> Vec<Chunk> {
    debug_assert!(piece_size > 0);
    let count = file_size.div_ceil(piece_size);
    let mut chunks = Vec::with_capacity(count as usize);
    let mut start: u64 = 0;
    for id in 0..count {
        let end = start.saturating_add(piece_size).min(file_size) - 1;
        chunks.push(Chunk {
            id: id as u32 + 1,
            start,
            end,
        });
        start = end + 1;
    }
    chunks
}

/// Creates contiguous ranges no larger than `max_size`. The parallel runner
/// schedules these through its existing bounded worker queue, allowing a
/// worker that finishes early to take another range.
fn plan_chunks_with_max_size(file_size: u64, max_size: u64, workers: usize) -> Vec<Chunk> {
    debug_assert!(max_size > 0);
    let balanced_size = file_size.div_ceil(workers.max(1) as u64);
    let range_size = max_size.min(balanced_size);
    let count = file_size.div_ceil(range_size);
    let mut chunks = Vec::with_capacity(count as usize);
    let mut start: u64 = 0;
    for id in 0..count {
        let end = start.saturating_add(range_size).min(file_size) - 1;
        chunks.push(Chunk {
            id: id as u32 + 1,
            start,
            end,
        });
        start = end + 1;
    }
    chunks
}

/// Creates a zero-length file at `output_path`.
///
/// Written as `<output>.part` and renamed, so an empty file arrives the same
/// way every other download does: through a guarded open that will not follow
/// a symlink, and through the rename that refuses to replace a file which
/// appeared while we were working.
async fn write_empty_file(output_path: &str) -> Result<()> {
    use crate::safe_file::{self, Access, Existing};
    use std::path::Path;

    let temp_path = format!("{}.part", output_path);
    let temp = Path::new(&temp_path);
    let destination_existed = tokio::fs::symlink_metadata(output_path).await.is_ok();

    let file = safe_file::open_guarded(
        temp,
        Existing::Open,
        Access::ReadWrite,
        safe_file::DEFAULT_FILE_MODE,
    )
    .context("Failed to create .part file")?;
    // A `.part` left over from a previous, larger version of this file.
    file.set_len(0).context("Failed to truncate .part file")?;

    // Resume state for a file that is now empty describes something else.
    let _ = tokio::fs::remove_file(crate::resume::ResumeMetadata::meta_path(output_path)).await;

    // Published from the descriptor that was just validated, so the empty file
    // that arrives is this one rather than whatever holds the `.part` name by
    // now.
    safe_file::publish_open_file(&file, temp, Path::new(output_path), destination_existed)
        .with_context(|| format!("Failed to publish '{}' as '{}'", temp_path, output_path))
}

/// Where a server-suggested filename would move the download to, or `None` if
/// the suggestion should not be honoured at all.
///
/// Honoured only when the URL gave us no extension to work with and the user
/// did not name the file themselves. The name is re-joined onto the parent of
/// the path we already resolved, so an honoured suggestion can only ever land
/// in the directory the download was already going to — it can change the
/// filename, never the directory.
fn suggested_output_path(
    output_path: &str,
    info: &inspect::FileInfo,
    user_explicitly_renamed: bool,
) -> Option<String> {
    if user_explicitly_renamed {
        return None;
    }
    let name = info.suggested_filename.as_deref()?;
    let path = std::path::Path::new(output_path);
    if path.extension().is_some() {
        return None;
    }

    // `inspect` already sanitises, but this is the line that turns a name into
    // a path, so it does not take that on trust.
    let name = safe_filename(name)?;
    let candidate = path
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join(name)
        .to_string_lossy()
        .into_owned();

    // Nothing moved, so there is nothing to re-check.
    (candidate != output_path).then_some(candidate)
}

fn plan_chunks_with_count(file_size: u64, count: u32) -> Vec<Chunk> {
    let count = count.max(1);
    let chunk_size = file_size / count as u64;
    let remainder = file_size % count as u64;
    let mut chunks = Vec::with_capacity(count as usize);
    let mut offset: u64 = 0;
    for i in 0..count {
        let extra = if (i as u64) < remainder { 1 } else { 0 };
        let size = chunk_size + extra;
        let start = offset;
        let end = start + size - 1;
        chunks.push(Chunk {
            id: i + 1,
            start,
            end,
        });
        offset = end + 1;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn suggesting(name: Option<&str>) -> inspect::FileInfo {
        inspect::FileInfo {
            size: Some(1024),
            supports_range: true,
            suggested_filename: name.map(str::to_owned),
            etag: None,
            last_modified: None,
        }
    }

    /// `Content-Length: 0` used to be rejected outright ("Cannot download
    /// empty file"), which is a correctness bug: an empty file is a file.
    #[tokio::test]
    async fn an_empty_file_downloads_to_an_empty_file() {
        use axum::routing::get;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = axum::Router::new().route("/empty.txt", get(|| async { "" }));
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("empty.txt").to_string_lossy().into_owned();

        let outcome = download(
            DownloadRequest::new(
                format!("http://{}/empty.txt", addr),
                Some(output.clone()),
                4,
            )
            .with_allow_private(true),
            CancellationToken::new(),
            crate::ui::silent(),
        )
        .await
        .expect("an empty file is a perfectly valid download");

        assert_eq!(
            outcome,
            Outcome::Completed {
                path: output.clone(),
                bytes: 0
            }
        );
        assert_eq!(tokio::fs::metadata(&output).await.unwrap().len(), 0);
        // No `.part` and no resume state left behind.
        assert!(
            tokio::fs::metadata(format!("{}.part", output))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn scoped_probe_is_reused_but_explicit_clients_still_inspect() {
        use axum::{
            Router,
            http::{HeaderMap, StatusCode, header},
            routing::get,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        let probes = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&probes);
        let router = Router::new().route(
            "/file.bin",
            get(move |headers: HeaderMap| {
                let count = Arc::clone(&count);
                async move {
                    match headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
                        Some("bytes=0-0") => {
                            count.fetch_add(1, Ordering::SeqCst);
                            (
                                StatusCode::PARTIAL_CONTENT,
                                [(header::CONTENT_RANGE, "bytes 0-0/5")],
                                "h",
                            )
                        }
                        Some("bytes=0-4") => (
                            StatusCode::PARTIAL_CONTENT,
                            [(header::CONTENT_RANGE, "bytes 0-4/5")],
                            "hello",
                        ),
                        _ => (
                            StatusCode::OK,
                            [(header::CONTENT_RANGE, "bytes 0-4/5")],
                            "hello",
                        ),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let url = format!("http://{addr}/file.bin");
        let path = dir.path().join("policy.bin");
        let req = DownloadRequest::new(url.clone(), Some(path.to_string_lossy().into_owned()), 1)
            .with_allow_private(true);
        download(req, CancellationToken::new(), crate::ui::silent())
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello");
        assert_eq!(probes.load(Ordering::SeqCst), 1);

        let path = dir.path().join("session.bin");
        let req = DownloadRequest::new(url, Some(path.to_string_lossy().into_owned()), 1)
            .with_client(reqwest::Client::builder().no_proxy().build().unwrap());
        download(req, CancellationToken::new(), crate::ui::silent())
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello");
        assert_eq!(probes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn scoped_probe_follows_a_cross_authority_redirect_and_reuses_final_response() {
        use axum::{
            Router,
            http::{HeaderMap, StatusCode, header},
            response::Redirect,
            routing::get,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        let final_probes = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&final_probes);
        let target_router = Router::new().route(
            "/file.bin",
            get(move |headers: HeaderMap| {
                let count = Arc::clone(&count);
                async move {
                    if headers.get(header::RANGE).and_then(|v| v.to_str().ok()) == Some("bytes=0-0")
                    {
                        count.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::PARTIAL_CONTENT,
                            [(header::CONTENT_RANGE, "bytes 0-0/5")],
                            "h",
                        )
                    } else {
                        (
                            StatusCode::PARTIAL_CONTENT,
                            [(header::CONTENT_RANGE, "bytes 0-4/5")],
                            "hello",
                        )
                    }
                }
            }),
        );
        let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(target_listener, target_router).await.unwrap() });

        let redirect_to = format!("http://{target_addr}/file.bin");
        let source_router = Router::new().route(
            "/file.bin",
            get(move || {
                let redirect_to = redirect_to.clone();
                async move { Redirect::temporary(&redirect_to) }
            }),
        );
        let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_addr = source_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(source_listener, source_router).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("redirect.bin");
        let req = DownloadRequest::new(
            format!("http://{source_addr}/file.bin"),
            Some(path.to_string_lossy().into_owned()),
            1,
        )
        .with_allow_private(true);
        download(req, CancellationToken::new(), crate::ui::silent())
            .await
            .unwrap();

        assert_eq!(tokio::fs::read(path).await.unwrap(), b"hello");
        assert_eq!(final_probes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reused_probe_keeps_the_head_fallback_for_rejected_range_gets() {
        use axum::{
            Router,
            http::{HeaderMap, StatusCode, header},
            routing::{get, head},
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ranged_gets = Arc::new(AtomicUsize::new(0));
        let ranged_count = Arc::clone(&ranged_gets);
        let head_requests = Arc::new(AtomicUsize::new(0));
        let head_count = Arc::clone(&head_requests);
        let router = Router::new()
            .route(
                "/file.bin",
                get(move |headers: HeaderMap| {
                    let ranged_count = Arc::clone(&ranged_count);
                    async move {
                        if headers.get(header::RANGE).and_then(|v| v.to_str().ok())
                            == Some("bytes=0-0")
                        {
                            ranged_count.fetch_add(1, Ordering::SeqCst);
                            (StatusCode::METHOD_NOT_ALLOWED, "")
                        } else {
                            (StatusCode::OK, "hello")
                        }
                    }
                }),
            )
            .route(
                "/file.bin",
                head(move || {
                    let head_count = Arc::clone(&head_count);
                    async move {
                        head_count.fetch_add(1, Ordering::SeqCst);
                        (StatusCode::OK, [(header::CONTENT_LENGTH, "5")])
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("head.bin");
        let req = DownloadRequest::new(
            format!("http://{addr}/file.bin"),
            Some(path.to_string_lossy().into_owned()),
            1,
        )
        .with_allow_private(true);
        download(req, CancellationToken::new(), crate::ui::silent())
            .await
            .unwrap();

        assert_eq!(tokio::fs::read(path).await.unwrap(), b"hello");
        assert_eq!(ranged_gets.load(Ordering::SeqCst), 1);
        assert_eq!(head_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelling_during_the_scoped_probe_stops_before_transfer() {
        use axum::{Router, routing::get};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let requests = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&requests);
        let router = Router::new().route(
            "/file.bin",
            get(move || {
                let seen = Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    "payload"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cancelled.bin");
        let cancel = CancellationToken::new();
        let request_cancel = cancel.clone();
        let output = path.to_string_lossy().into_owned();
        let task = tokio::spawn(async move {
            download(
                DownloadRequest::new(format!("http://{addr}/file.bin"), Some(output), 1)
                    .with_allow_private(true),
                request_cancel,
                crate::ui::silent(),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while requests.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("probe request did not start");
        cancel.cancel();

        assert_eq!(task.await.unwrap().unwrap(), Outcome::Cancelled);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert!(!path.exists());
    }

    #[test]
    fn test_plan_chunks_even() {
        let chunks = plan_chunks_with_count(1000, 4);
        assert_eq!(chunks.len(), 4);
        let total: u64 = chunks.iter().map(|c| c.end - c.start + 1).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn test_plan_chunks_remainder() {
        let chunks = plan_chunks_with_count(1003, 4);
        let total: u64 = chunks.iter().map(|c| c.end - c.start + 1).sum();
        assert_eq!(total, 1003);
        for i in 1..chunks.len() {
            assert_eq!(chunks[i].start, chunks[i - 1].end + 1);
        }
    }

    #[test]
    fn adaptive_ranges_cover_the_file_with_bounded_work_units() {
        let range_size = 64 * 1024 * 1024;
        let file_size = 200 * 1024 * 1024;
        let ranges = plan_chunks_with_max_size(file_size, range_size, 2);

        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end + 1, range_size);
        assert_eq!(ranges.last().unwrap().end, file_size - 1);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end + 1, pair[1].start);
            assert!(pair[0].end - pair[0].start < range_size);
        }
        assert_eq!(
            ranges.iter().map(|r| r.end - r.start + 1).sum::<u64>(),
            file_size
        );
    }

    #[test]
    fn adaptive_ranges_never_start_with_fewer_ranges_than_workers() {
        let ranges = plan_chunks_with_max_size(32 * 1024 * 1024, 64 * 1024 * 1024, 8);
        assert_eq!(ranges.len(), 8);
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, 32 * 1024 * 1024 - 1);
    }

    #[test]
    fn adaptive_stream_piece_plan_uses_fixed_16_mib_logical_pieces() {
        let piece_size = 16 * 1024 * 1024;
        let file_size = piece_size * 2 + 17;
        let pieces = plan_chunks_with_piece_size(file_size, piece_size);

        assert_eq!(pieces.len(), 3);
        assert_eq!((pieces[0].start, pieces[0].end), (0, piece_size - 1));
        assert_eq!(
            (pieces[1].start, pieces[1].end),
            (piece_size, piece_size * 2 - 1)
        );
        assert_eq!(
            (pieces[2].start, pieces[2].end),
            (piece_size * 2, file_size - 1)
        );
        assert_eq!(
            pieces
                .iter()
                .map(|piece| piece.end - piece.start + 1)
                .sum::<u64>(),
            file_size
        );
    }

    #[test]
    fn a_suggested_name_lands_beside_the_path_we_already_resolved() {
        assert_eq!(
            suggested_output_path("/tmp/dl/download", &suggesting(Some("movie.mkv")), false)
                .as_deref(),
            Some("/tmp/dl/movie.mkv")
        );
    }

    /// The directory is ours to choose, not the server's. `inspect` sanitises
    /// too, but this is the line that turns a name into a path.
    #[test]
    fn a_suggested_name_cannot_move_the_download_to_another_directory() {
        assert_eq!(
            suggested_output_path(
                "/tmp/dl/download",
                &suggesting(Some("../../../etc/cron.d/rdm")),
                false,
            )
            .as_deref(),
            Some("/tmp/dl/rdm")
        );
        assert_eq!(
            suggested_output_path("/tmp/dl/download", &suggesting(Some("/etc/shadow")), false)
                .as_deref(),
            Some("/tmp/dl/shadow")
        );
    }

    #[test]
    fn a_name_the_user_chose_is_never_second_guessed() {
        assert_eq!(
            suggested_output_path("/tmp/dl/mine", &suggesting(Some("theirs.mkv")), true),
            None
        );
        // Nor is one the URL already gave an extension to.
        assert_eq!(
            suggested_output_path("/tmp/dl/file.zip", &suggesting(Some("theirs.mkv")), false),
            None
        );
        // Nothing suggested, nothing to reconsider.
        assert_eq!(
            suggested_output_path("/tmp/dl/download", &suggesting(None), false),
            None
        );
        // A suggestion with nothing usable in it is dropped rather than
        // turned into a guess.
        assert_eq!(
            suggested_output_path("/tmp/dl/download", &suggesting(Some("..")), false),
            None
        );
    }
}
