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
    let connections = req.connections.max(1);

    // A hoster that had to authenticate hands us its own session and does its
    // own addressing. Everything else — every URL a listing handed us — is
    // resolved and judged here, and fetched over a client pinned to the
    // addresses that judgement was made about.
    //
    // `url` stays what the user asked for: it names the file on disk and keys
    // the resume state. Only the fetching moves to `fetch_url`, because a
    // redirect target is a poor filename and a worse identity.
    let resolved;
    let (fetch_url, client) = match req.client.as_ref() {
        Some(session) => (url.clone(), session),
        None => {
            resolved = net::Policy::new(req.allow_private)
                .resolve_target(&url)
                .await?;
            (resolved.url.to_string(), &resolved.client)
        }
    };

    sink.state(SlotState::Inspecting);
    // Redacted, and never `fetch_url`. When a hoster drives this path the URL
    // it hands over is an API call carrying gdrive's `key=` parameter or
    // OneDrive's `tempauth` signature, and `detail` goes to stderr, which ends
    // up in scrollback, CI output and support captures.
    sink.detail(&format!("Inspecting: {}", secret_url::redact(&url)));

    let info = inspect::inspect_url(client, &fetch_url).await?;

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
    sink.detail(&format!("Output    : {}", ui::terminal_safe(&output_path)));

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
        fetch_url: &fetch_url,
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
    drop(file);

    // Resume state for a file that is now empty describes something else.
    let _ = tokio::fs::remove_file(crate::resume::ResumeMetadata::meta_path(output_path)).await;

    if destination_existed {
        safe_file::rename_replacing(temp, Path::new(output_path))
    } else {
        safe_file::rename_no_replace(temp, Path::new(output_path))
    }
    .with_context(|| format!("Failed to rename '{}' to '{}'", temp_path, output_path))
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
            DownloadRequest::new(format!("http://{}/empty.txt", addr), Some(output.clone()), 4)
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
