//! The download path for servers that do not report a file size.
//!
//! There is no chunk plan here, so resume is a single ranged request. What to
//! do with the response it comes back with is decided by
//! [`resolve_resume_action`], which is where the awkward cases live.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::ui::{self, ProgressSink, SlotState};

/// Describes what the streaming download should do after the initial response.
#[derive(Debug, PartialEq)]
pub enum ResumeAction {
    /// Server confirmed the range — append to existing .part file from this offset.
    Resume(u64),
    /// Response is unusable for resume — must drop response and re-request.
    Restart,
    /// No prior partial state — consume this response from the start.
    Fresh,
    /// Response indicates failure — do not consume body.
    Fail(reqwest::StatusCode),
}

pub fn resolve_resume_action(
    status: reqwest::StatusCode,
    existing_bytes: u64,
    content_range: Option<&str>,
) -> ResumeAction {
    if existing_bytes == 0 {
        if status.is_success() {
            return ResumeAction::Fresh;
        } else {
            return ResumeAction::Fail(status);
        }
    }

    // existing_bytes > 0: we sent a Range header
    match status {
        reqwest::StatusCode::PARTIAL_CONTENT => {
            if let Some(cr) = content_range {
                let expected_prefix = format!("bytes {}-", existing_bytes);
                if cr.starts_with(&expected_prefix) {
                    ResumeAction::Resume(existing_bytes)
                } else {
                    ResumeAction::Restart
                }
            } else {
                // 206 without Content-Range — optimistic resume
                ResumeAction::Resume(existing_bytes)
            }
        }
        reqwest::StatusCode::OK => {
            // Server ignored Range header entirely
            ResumeAction::Restart
        }
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE => ResumeAction::Restart,
        _ => ResumeAction::Fail(status),
    }
}

pub fn build_streaming_request(
    client: &reqwest::Client,
    url: &str,
    existing_bytes: u64,
) -> reqwest::RequestBuilder {
    let mut req = client.get(url);
    if existing_bytes > 0 {
        req = req.header(reqwest::header::RANGE, format!("bytes={}-", existing_bytes));
    }
    req
}

pub(super) async fn download_streaming(
    client: &reqwest::Client,
    url: &str,
    output_path: &str,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<u64> {
    let temp_path = format!("{}.part", output_path);

    // Resume: check existing .part file size
    let existing_bytes = tokio::fs::metadata(&temp_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);

    // Phase 1: Build and send (possibly ranged) request
    let resp = build_streaming_request(client, url, existing_bytes)
        .send()
        .await
        .context("GET request failed")?;

    let status = resp.status();
    let content_range = resp
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    // Phase 2: Decide resume/restart/fresh/fail
    let (resume_offset, append, resp) =
        match resolve_resume_action(status, existing_bytes, content_range.as_deref()) {
            ResumeAction::Resume(offset) => {
                sink.note(&format!("Resuming from {}", ui::format_size(offset)));
                (offset, true, resp)
            }
            ResumeAction::Restart => {
                // Drop the unusable response and issue a fresh non-range GET
                drop(resp);
                if existing_bytes > 0 {
                    sink.note("Server response unusable for resume, restarting from zero");
                }
                let fresh_resp = client
                    .get(url)
                    .send()
                    .await
                    .context("Fresh GET request failed")?;
                if !fresh_resp.status().is_success() {
                    anyhow::bail!(
                        "Restart request failed with status {} {}",
                        fresh_resp.status().as_u16(),
                        fresh_resp.status().canonical_reason().unwrap_or("Unknown"),
                    );
                }
                (0u64, false, fresh_resp)
            }
            ResumeAction::Fresh => (0u64, false, resp),
            ResumeAction::Fail(code) => {
                anyhow::bail!(
                    "Server returned {} {}",
                    code.as_u16(),
                    code.canonical_reason().unwrap_or("Unknown"),
                );
            }
        };

    // A streaming response may still tell us how big it is; if so the sink can
    // draw a real bar and ETA instead of a byte counter.
    if let Some(len) = resp.content_length() {
        sink.total(Some(len + resume_offset));
    }

    // Phase 3: Open file and stream body
    let file = if append {
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(&temp_path)
            .await
            .context("Failed to open .part for append")?
    } else {
        tokio::fs::File::create(&temp_path)
            .await
            .context("Failed to create .part file")?
    };

    let mut writer = tokio::io::BufWriter::with_capacity(512 * 1024, file);
    let mut stream = resp.bytes_stream();
    let mut downloaded: u64 = resume_offset;
    let mut bytes_since_flush: u64 = 0;

    sink.progress(downloaded);

    loop {
        let chunk = tokio::select! {
            c = stream.next() => c,
            _ = cancel.cancelled() => {
                writer.flush().await.ok();
                anyhow::bail!("Download cancelled at {} bytes", downloaded);
            }
        };

        match chunk {
            Some(Ok(data)) => {
                let len = data.len() as u64;
                writer.write_all(&data).await.context("Write failed")?;
                downloaded += len;
                bytes_since_flush += len;

                if bytes_since_flush >= 4 * 1024 * 1024 {
                    writer.flush().await?;
                    bytes_since_flush = 0;
                }

                sink.progress(downloaded);
            }
            Some(Err(e)) => {
                writer.flush().await.ok();
                return Err(e).context(format!("Stream error at byte {}", downloaded));
            }
            None => break,
        }
    }

    writer.flush().await?;
    drop(writer);

    sink.state(SlotState::Finishing);

    tokio::fs::rename(&temp_path, output_path)
        .await
        .with_context(|| format!("Failed to rename '{}' to '{}'", temp_path, output_path))?;

    Ok(downloaded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_request_no_existing_bytes() {
        let client = reqwest::Client::new();
        let req = build_streaming_request(&client, "https://example.com/file.bin", 0)
            .build()
            .unwrap();
        assert!(req.headers().get(reqwest::header::RANGE).is_none());
    }

    #[test]
    fn test_build_request_with_existing_bytes() {
        let client = reqwest::Client::new();
        let req = build_streaming_request(&client, "https://example.com/file.bin", 4096)
            .build()
            .unwrap();
        let range = req.headers().get(reqwest::header::RANGE).unwrap();
        assert_eq!(range.to_str().unwrap(), "bytes=4096-");
    }

    #[test]
    fn test_resume_action_206_valid_content_range() {
        assert_eq!(
            resolve_resume_action(
                reqwest::StatusCode::PARTIAL_CONTENT,
                4096,
                Some("bytes 4096-8191/8192"),
            ),
            ResumeAction::Resume(4096),
        );
    }

    #[test]
    fn test_resume_action_206_mismatched_content_range() {
        assert_eq!(
            resolve_resume_action(
                reqwest::StatusCode::PARTIAL_CONTENT,
                4096,
                Some("bytes 0-8191/8192"),
            ),
            ResumeAction::Restart,
        );
    }

    #[test]
    fn test_resume_action_206_without_content_range() {
        assert_eq!(
            resolve_resume_action(reqwest::StatusCode::PARTIAL_CONTENT, 4096, None,),
            ResumeAction::Resume(4096),
        );
    }

    #[test]
    fn test_resume_action_200_ignores_range() {
        assert_eq!(
            resolve_resume_action(reqwest::StatusCode::OK, 4096, None),
            ResumeAction::Restart,
        );
    }

    #[test]
    fn test_resume_action_no_existing_bytes_success() {
        assert_eq!(
            resolve_resume_action(reqwest::StatusCode::OK, 0, None),
            ResumeAction::Fresh,
        );
    }

    #[test]
    fn test_resume_action_no_existing_bytes_failure() {
        assert_eq!(
            resolve_resume_action(reqwest::StatusCode::NOT_FOUND, 0, None),
            ResumeAction::Fail(reqwest::StatusCode::NOT_FOUND),
        );
    }

    #[test]
    fn test_resume_action_416_range_not_satisfiable() {
        assert_eq!(
            resolve_resume_action(reqwest::StatusCode::RANGE_NOT_SATISFIABLE, 99999, None),
            ResumeAction::Restart,
        );
    }

    #[test]
    fn test_resume_action_403_with_existing_bytes() {
        assert_eq!(
            resolve_resume_action(reqwest::StatusCode::FORBIDDEN, 4096, None),
            ResumeAction::Fail(reqwest::StatusCode::FORBIDDEN),
        );
    }

    #[test]
    fn test_resume_action_500_with_existing_bytes() {
        assert_eq!(
            resolve_resume_action(reqwest::StatusCode::INTERNAL_SERVER_ERROR, 4096, None),
            ResumeAction::Fail(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
        );
    }
}
