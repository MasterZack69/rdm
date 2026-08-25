//! Turning a [`DownloadRequest`] into a file on disk.

use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::chunk::Chunk;
use crate::inspect;
use crate::parallel;
use crate::retry::RetryConfig;
use crate::ui::{self, ProgressSink, SlotState};

use super::client::{shared_client, shared_config};
use super::output::{resolve_existing_output, resolve_output_path};
use super::request::{DownloadRequest, Outcome, OutputDecision};
use super::streaming::download_streaming;
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
    let connections = req.connections.max(1);

    // An authenticated client, if the caller had to obtain one: the session it
    // holds is the authorisation, so every request below has to go out over it
    // rather than over the shared client.
    let client = match req.client.as_ref() {
        Some(authenticated) => authenticated,
        None => shared_client()?,
    };

    sink.state(SlotState::Inspecting);
    sink.detail(&format!("Inspecting: {}", url));

    let info = inspect::inspect_url(client, &url).await?;

    // Use the server-suggested filename when the URL has no extension, but
    // only if the user didn't explicitly choose a name (via rename or -o).
    let output_path = if user_explicitly_renamed {
        output_path
    } else if let Some(ref name) = info.suggested_filename {
        let path = std::path::Path::new(&output_path);
        if path.extension().is_none() {
            let dir = path.parent().unwrap_or(std::path::Path::new("."));
            dir.join(name).to_string_lossy().to_string()
        } else {
            output_path
        }
    } else {
        output_path
    };

    // Unknown file size → streaming fallback.
    let file_size = match info.size {
        Some(0) => anyhow::bail!("Cannot download empty file (Content-Length: 0)"),
        Some(s) => s,
        None => {
            sink.detail("File size : unknown (streaming)");
            sink.detail(&format!("Output    : {}", output_path));
            sink.total(None);
            sink.state(SlotState::Downloading);

            let result =
                download_streaming(client, &url, &output_path, cancel, Arc::clone(&sink)).await;
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
    let connections = if file_size < 4 * 1024 * 1024 {
        1
    } else {
        connections
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
    sink.detail(&format!("Output    : {}", output_path));

    let chunks = if info.supports_range && connections > 1 {
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
        let part_path = format!("{}.part", &output_path);
        let _ = std::fs::remove_file(&meta_path);
        let _ = std::fs::remove_file(&part_path);
    }

    sink.detail(&format!("Chunks    : {}", chunks.len()));

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
        output_path: &output_path,
        file_size,
        chunks: &chunks,
        retry_config: &retry_config,
        cancel,
        identity: req.resume_identity.clone(),
        etag: info.etag.clone(),
        last_modified: info.last_modified.clone(),
    };

    let download_result = parallel::download_parallel(&ctx, Some(progress_callback)).await;

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
}
