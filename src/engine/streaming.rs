//! The download path for servers that do not report a file size.
//!
//! There is no chunk plan here, so resume is a single ranged request. What to
//! do with the response it comes back with is decided by
//! [`resolve_resume_action`], which is where the awkward cases live.
//!
//! Two things this path cannot do, because there is no size to check against:
//! trust a resume without proof, and write until the server stops. A resume is
//! only accepted when the server states a range that agrees with what was
//! asked for, and the body is only renamed into place when every stated byte
//! arrived. Everything else restarts or fails, and the `.part` file is left
//! alone so the next run can try again.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::safe_file::{self, Access, Existing};
use crate::ui::{self, ProgressSink, SlotState};

/// The ceiling on one streaming download when the server never states a size.
///
/// Generous enough that no honest download meets it, and finite so that an
/// endpoint streaming `/dev/urandom` cannot fill the disk. `RDM_MAX_FILE_BYTES`
/// overrides it; `0` means no ceiling.
const DEFAULT_MAX_STREAM_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// How much room to leave on the filesystem rather than using every last byte.
const MIN_FREE_BYTES: u64 = 64 * 1024 * 1024;

/// How often to look at free space again while writing.
const SPACE_CHECK_INTERVAL: u64 = 64 * 1024 * 1024;

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

/// A `Content-Range` value, parsed whole rather than by its prefix.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ContentRange {
    /// First byte offset the body carries.
    pub start: u64,
    /// Last byte offset the body carries, inclusive.
    pub end: u64,
    /// Total size of the file, when the server states one rather than `*`.
    pub total: Option<u64>,
}

/// Parses `bytes <start>-<end>/<total>`, or `None` if it is not exactly that.
///
/// `None` means the response cannot be used to resume. The previous check was
/// `starts_with("bytes {offset}-")`, which read the start and nothing else: a
/// server could state any end and any total, or state a total the range did not
/// fit inside, and the bytes were appended regardless.
pub fn parse_content_range(value: &str) -> Option<ContentRange> {
    let rest = value.trim().strip_prefix("bytes ")?;
    let (range_part, total_part) = rest.split_once('/')?;
    let (start_str, end_str) = range_part.split_once('-')?;

    let start: u64 = start_str.trim().parse().ok()?;
    let end: u64 = end_str.trim().parse().ok()?;

    // An empty or inverted range carries no bytes.
    if end < start {
        return None;
    }

    let total = match total_part.trim() {
        "*" => None,
        stated => {
            let total: u64 = stated.parse().ok()?;
            // The part cannot be bigger than the whole.
            if end >= total {
                return None;
            }
            Some(total)
        }
    };

    Some(ContentRange { start, end, total })
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
            // Everything about the range has to agree before a single byte is
            // appended to a file that will then be renamed as complete.
            // Anything short of that restarts, which costs bandwidth and
            // nothing else.
            match content_range.and_then(parse_content_range) {
                Some(range) => {
                    // The exact offset that was asked for, compared as a
                    // number rather than as a prefix.
                    if range.start != existing_bytes {
                        return ResumeAction::Restart;
                    }
                    // The request was open-ended (`bytes=N-`), so an honest
                    // answer runs to the end of the file. A server offering a
                    // shorter slice is offering something that would be
                    // appended and then renamed as though it were the whole
                    // file.
                    if let Some(total) = range.total
                        && total != range.end + 1
                    {
                        return ResumeAction::Restart;
                    }
                    ResumeAction::Resume(existing_bytes)
                }
                // Missing or malformed. This was an optimistic resume, and it
                // is the case the finding is about: a 206 with no
                // Content-Range said nothing at all about which bytes were
                // being sent, and they were appended anyway.
                None => ResumeAction::Restart,
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

/// The configured per-file ceiling, or `None` when it is switched off.
fn max_stream_bytes() -> Option<u64> {
    match std::env::var("RDM_MAX_FILE_BYTES") {
        // An unparseable value is a typo, not permission to remove the limit.
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(limit) => Some(limit),
            Err(_) => Some(DEFAULT_MAX_STREAM_BYTES),
        },
        Err(_) => Some(DEFAULT_MAX_STREAM_BYTES),
    }
}

/// The directory a path sits in, for free-space questions.
fn dir_of(path: &str) -> PathBuf {
    match Path::new(path).parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

pub(super) async fn download_streaming(
    client: &reqwest::Client,
    url: &str,
    output_path: &str,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<u64> {
    let temp_path = format!("{}.part", output_path);
    let temp = Path::new(&temp_path);
    let dir = dir_of(&temp_path);

    // Whether anything is at the destination now, before the transfer starts.
    // The existence question was already asked and answered further up; this
    // only records the answer, so that the rename at the end can tell an
    // approved overwrite from a file that turned up while we were downloading.
    let destination_existed = tokio::fs::symlink_metadata(output_path).await.is_ok();

    // Resume: check existing .part file size.
    //
    // `symlink_metadata`, and only for the length: a symlink here is not a
    // partial download, and its target's size would be a lie. This is not the
    // security check — `open_guarded` below is, because any check made before
    // an open can be overtaken between the two.
    let existing_bytes = match tokio::fs::symlink_metadata(temp).await {
        Ok(meta) if meta.is_file() => meta.len(),
        _ => 0,
    };

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

    let declared_range = content_range.as_deref().and_then(parse_content_range);

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

    // How big the finished file should be, when the server has said. On a
    // resume that is the end of the range it agreed to; otherwise it is the
    // length of the body it is about to send. It is checked again at the end,
    // and the rename does not happen unless it matches.
    let declared_total: Option<u64> = if append {
        declared_range.map(|range| range.end + 1)
    } else {
        resp.content_length()
    };

    // A stated size over the limit is refused now rather than after the disk
    // has taken the first however-many gigabytes of it.
    if let (Some(total), Some(limit)) = (declared_total, max_stream_bytes())
        && total > limit
    {
        anyhow::bail!(
            "Refusing to download {}: the server states {}, over the {} limit \
             (set RDM_MAX_FILE_BYTES, or 0 for no limit)",
            ui::terminal_safe(output_path),
            ui::format_size(total),
            ui::format_size(limit),
        );
    }

    // Whichever comes first: the size the server stated, or the configured
    // ceiling. The stated size matters on its own — a server that declares ten
    // megabytes and then streams for ever is stopped at ten megabytes.
    let ceiling: Option<u64> = declared_total.or_else(max_stream_bytes);

    // Room for what is known to be coming, before anything is opened.
    // `available_bytes` answers `None` when it cannot tell, which is why the
    // ceiling above is the real protection and this is the courtesy.
    if let Some(total) = declared_total
        && let Some(free) = safe_file::available_bytes(&dir)
    {
        let needed = total.saturating_sub(resume_offset);
        if free < needed.saturating_add(MIN_FREE_BYTES) {
            anyhow::bail!(
                "Not enough space for {}: {} needed, {} free",
                ui::terminal_safe(output_path),
                ui::format_size(needed),
                ui::format_size(free),
            );
        }
    }

    // A streaming response may still tell us how big it is; if so the sink can
    // draw a real bar and ETA instead of a byte counter.
    if let Some(len) = resp.content_length() {
        sink.total(Some(len + resume_offset));
    }

    // Phase 3: Open file and stream body
    //
    // `<output>.part` is a name anyone who can write to this directory can
    // predict, and it used to be opened with calls that follow symlinks: point
    // it at a file the rdm process can write and rdm truncates or appends to
    // that file instead. `open_guarded` opens relative to the directory
    // descriptor, refuses to traverse a symlink, and fstats what it got to
    // confirm it is a regular file we own.
    //
    // The name stays predictable deliberately — resume has to find it again
    // between runs, which a randomised name could not do. Randomised temporary
    // files are used where nothing needs to find them again.
    let file = if append {
        safe_file::open_guarded(temp, Existing::Open, Access::Append, safe_file::DEFAULT_FILE_MODE)
            .context("Failed to open .part for append")?
    } else {
        // Created if absent, which is the ordinary case. The truncate happens
        // through the descriptor rather than by reopening the path, so there
        // is no second resolution for anything to slip into.
        let file = safe_file::open_guarded(
            temp,
            Existing::Open,
            Access::ReadWrite,
            safe_file::DEFAULT_FILE_MODE,
        )
        .context("Failed to create .part file")?;
        file.set_len(0).context("Failed to truncate .part file")?;
        file
    };

    let mut writer =
        tokio::io::BufWriter::with_capacity(512 * 1024, tokio::fs::File::from_std(file));
    let mut stream = resp.bytes_stream();
    let mut downloaded: u64 = resume_offset;
    let mut bytes_since_flush: u64 = 0;
    let mut bytes_since_space_check: u64 = 0;

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

                // Before the write, so the bytes over the line never reach the
                // disk at all.
                if let Some(ceiling) = ceiling
                    && downloaded + len > ceiling
                {
                    writer.flush().await.ok();
                    anyhow::bail!(
                        "Server sent more than the {} expected for {} — stopping at {}",
                        ui::format_size(ceiling),
                        ui::terminal_safe(output_path),
                        ui::format_size(downloaded),
                    );
                }

                writer.write_all(&data).await.context("Write failed")?;
                downloaded += len;
                bytes_since_flush += len;
                bytes_since_space_check += len;

                if bytes_since_flush >= 4 * 1024 * 1024 {
                    writer.flush().await?;
                    bytes_since_flush = 0;
                }

                // A stream with no stated size can outlast any up-front
                // estimate, so free space is a question worth asking again.
                if bytes_since_space_check >= SPACE_CHECK_INTERVAL {
                    bytes_since_space_check = 0;
                    if let Some(free) = safe_file::available_bytes(&dir)
                        && free < MIN_FREE_BYTES
                    {
                        writer.flush().await.ok();
                        anyhow::bail!(
                            "Stopping at {}: only {} left on the filesystem",
                            ui::format_size(downloaded),
                            ui::format_size(free),
                        );
                    }
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

    // Every byte the server said would arrive has to have arrived. Short of
    // that the `.part` file is left where it is, because a partial file that
    // can be resumed is worth more than a truncated one wearing the final
    // name.
    if let Some(total) = declared_total
        && downloaded != total
    {
        anyhow::bail!(
            "Incomplete download: {} stated, {} received — leaving the partial file in place",
            ui::format_size(total),
            ui::format_size(downloaded),
        );
    }

    sink.state(SlotState::Finishing);

    let final_path = Path::new(output_path);

    if destination_existed {
        // Something was already there when the download began, and the
        // decision to overwrite it was taken then.
        safe_file::rename_replacing(temp, final_path)
    } else {
        // Nothing was there when the download began, so anything there now
        // arrived while it ran and is not ours to replace. This is the race
        // between the existence check and the rename.
        safe_file::rename_no_replace(temp, final_path)
    }
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

    /// Was `Resume(4096)`: a 206 with no Content-Range was taken on trust and
    /// the body appended to the `.part` file. The response says nothing about
    /// which bytes it carries, so there is nothing here to resume from.
    #[test]
    fn test_resume_action_206_without_content_range() {
        assert_eq!(
            resolve_resume_action(reqwest::StatusCode::PARTIAL_CONTENT, 4096, None,),
            ResumeAction::Restart,
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

    // ---------- Content-Range parsing ----------

    #[test]
    fn a_content_range_is_parsed_whole() {
        assert_eq!(
            parse_content_range("bytes 4096-8191/8192"),
            Some(ContentRange {
                start: 4096,
                end: 8191,
                total: Some(8192),
            })
        );
        // An unknown total is legal and says nothing either way.
        assert_eq!(
            parse_content_range("bytes 0-99/*"),
            Some(ContentRange {
                start: 0,
                end: 99,
                total: None,
            })
        );
    }

    #[test]
    fn a_malformed_content_range_parses_to_nothing() {
        for value in [
            "",
            "bytes",
            "octets 0-99/100",   // wrong unit
            "bytes 0-99",        // no total
            "bytes 099/100",     // no dash
            "bytes a-99/100",    // start is not a number
            "bytes 0-b/100",     // end is not a number
            "bytes 100-99/1000", // inverted
            "bytes 0-100/100",   // the part is bigger than the whole
        ] {
            assert_eq!(
                parse_content_range(value),
                None,
                "{:?} should not parse",
                value
            );
        }
    }

    /// The prefix comparison this replaces read the start and stopped. These
    /// are the answers it would have accepted.
    #[test]
    fn a_resume_needs_the_whole_range_to_agree() {
        let resume =
            |cr: &str| resolve_resume_action(reqwest::StatusCode::PARTIAL_CONTENT, 4096, Some(cr));

        // Off by one byte at the start: the bytes would land at the wrong
        // offset and the file would be quietly corrupt.
        assert_eq!(resume("bytes 4095-8191/8192"), ResumeAction::Restart);

        // A truncated range. Appending this and renaming would produce a file
        // 3 KiB short of the real one, reported as complete.
        assert_eq!(resume("bytes 4096-5000/8192"), ResumeAction::Restart);

        // Garbage where the range should be.
        assert_eq!(resume("bytes"), ResumeAction::Restart);
        assert_eq!(resume(""), ResumeAction::Restart);

        // Right offset, and the range runs to the end of the file.
        assert_eq!(resume("bytes 4096-8191/8192"), ResumeAction::Resume(4096));

        // Right offset, total unstated. Nothing contradicts the request, so
        // this is allowed — the byte count is checked again after the body.
        assert_eq!(resume("bytes 4096-8191/*"), ResumeAction::Resume(4096));
    }

    // ---------- Limits ----------

    /// The env override exists so the ceiling can be raised or removed; an
    /// unparseable value is a typo and keeps the default rather than removing
    /// the limit.
    #[test]
    fn the_stream_ceiling_is_configurable_but_never_accidentally_removed() {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());

        unsafe {
            std::env::remove_var("RDM_MAX_FILE_BYTES");
        }
        assert_eq!(max_stream_bytes(), Some(DEFAULT_MAX_STREAM_BYTES));

        unsafe {
            std::env::set_var("RDM_MAX_FILE_BYTES", "1048576");
        }
        assert_eq!(max_stream_bytes(), Some(1024 * 1024));

        // Explicitly switched off.
        unsafe {
            std::env::set_var("RDM_MAX_FILE_BYTES", "0");
        }
        assert_eq!(max_stream_bytes(), None);

        // Not a number: keep the default.
        unsafe {
            std::env::set_var("RDM_MAX_FILE_BYTES", "lots");
        }
        assert_eq!(max_stream_bytes(), Some(DEFAULT_MAX_STREAM_BYTES));

        unsafe {
            std::env::remove_var("RDM_MAX_FILE_BYTES");
        }
    }

    #[test]
    fn a_bare_filename_looks_for_space_in_the_current_directory() {
        assert_eq!(dir_of("file.bin.part"), PathBuf::from("."));
        assert_eq!(dir_of("/tmp/dl/file.bin.part"), PathBuf::from("/tmp/dl"));
    }
}
