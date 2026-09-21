//! The download path for servers that do not report a file size.
//!
//! There is no chunk plan here, so resume is a single ranged request. What to
//! do with the response it comes back with is decided by
//! [`resolve_resume_action`], which is where the awkward cases live.
//!
//! Two things this path cannot do, because there is no size to check against:
//! trust a resume without proof, and write until the server stops. A resume is
//! only accepted when the server states a numeric total and a range that
//! agrees with what was asked for, and the body is only renamed into place
//! when every stated byte arrived. Everything else restarts or fails, and the
//! `.part` file is left alone so the next run can try again.
//!
//! The other half of "without proof" is *which file*. A `.part` file's length
//! says how many bytes are on disk and nothing about where they came from, so
//! this path keeps the same [`ResumeMetadata`](crate::resume::ResumeMetadata)
//! manifest the segmented path does — one format, one set of rules — holding
//! the strong validator the bytes were fetched under. The next run resumes
//! only when that validator is still the one the server offers, sends it back
//! as `If-Range` so the server itself can refuse, and restarts whenever
//! continuity cannot be established. Without that, a resource that changed
//! behind a stable URL had its new bytes appended to the old partial file and
//! the result renamed as though it were one download.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::resume::{self, IdentityCheck, ResumeMetadata, TransferKind};
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
///
/// A `*` total parses to `total: None`, which is legal in the header and not
/// good enough to resume on — see [`resolve_resume_action`].
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

/// What the caller knows about *which* resource is being fetched.
///
/// `url` is the address the download is keyed by, which is not always the one
/// being fetched: a signed CDN link changes every run. `identity` is the
/// durable name when a hoster has one, and the two validators are what the
/// inspection request reported.
#[derive(Debug, Clone, Copy)]
pub struct StreamIdentity<'a> {
    pub url: &'a str,
    pub identity: Option<&'a str>,
    pub etag: Option<&'a str>,
    pub last_modified: Option<&'a str>,
}

/// Whether an existing `.part` file may be continued, decided before any
/// request is sent.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamStart {
    /// Start from byte zero. `discarded` says why an existing `.part` is
    /// being thrown away, and is `None` when there was nothing there.
    Fresh { discarded: Option<&'static str> },
    /// Continue from `offset`, conditioned on `if_range`.
    Resume { offset: u64, if_range: String },
}

/// Decides whether the bytes already on disk belong to the file now on offer.
///
/// Everything here is a reason to restart except one path through it: a
/// streaming manifest, for this source, whose strong validator is still the
/// validator the server offers. Restarting costs bandwidth. The alternative
/// costs the file, silently, and only shows up when someone opens it.
pub fn plan_streaming_resume(
    existing_bytes: u64,
    saved: Option<&ResumeMetadata>,
    id: &StreamIdentity<'_>,
) -> StreamStart {
    if existing_bytes == 0 {
        return StreamStart::Fresh { discarded: None };
    }

    let discard = |reason: &'static str| StreamStart::Fresh {
        discarded: Some(reason),
    };

    // A `.part` with no manifest beside it is bytes of unknown provenance.
    // That is the state every streaming download used to be resumed from.
    let Some(meta) = saved else {
        return discard("no resume manifest beside the partial file");
    };

    if meta.transfer != TransferKind::Streaming {
        return discard("the partial file was written by a segmented transfer");
    }

    if !meta.describes_same_source(id.url, id.identity) {
        return discard("the saved state is for a different source");
    }

    match meta.compare_server_identity(id.etag, id.last_modified) {
        IdentityCheck::Changed => discard("the server's copy changed since the partial download"),
        IdentityCheck::Unknown => {
            discard("the server no longer proves this is the same file (no usable validator)")
        }
        IdentityCheck::Match => match meta.stored_validator() {
            // `Match` means both sides had one, so this cannot be `None`.
            Some(validator) => StreamStart::Resume {
                offset: existing_bytes,
                if_range: validator.to_owned(),
            },
            None => discard("the saved validator vanished between checks"),
        },
    }
}

/// The validator a response offers for its own body.
fn response_validator(resp: &reqwest::Response) -> (Option<String>, Option<String>) {
    let header = |name: reqwest::header::HeaderName| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned())
    };
    (
        header(reqwest::header::ETAG),
        header(reqwest::header::LAST_MODIFIED),
    )
}

/// Whether a `206` really is a continuation of the bytes already on disk.
///
/// The resume was conditioned on a validator; this checks that the body the
/// server actually sent still carries it. A response that offers no validator
/// of its own proves nothing, and a resume is the one place where an unproven
/// answer is worse than a slow one.
fn body_continues_the_same_file(resp: &reqwest::Response, if_range: &Option<String>) -> bool {
    let Some(expected) = if_range.as_deref() else {
        return false;
    };
    let (etag, last_modified) = response_validator(resp);
    match resume::strong_validator(etag.as_deref(), last_modified.as_deref()) {
        Some(current) => current == expected,
        None => false,
    }
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
                    // `*` is the absence of a total, not a small one. Without
                    // a number here there is nothing to check the finished
                    // file against, and the end of the range is not a
                    // substitute: `bytes 4096-5000/*` followed by exactly 905
                    // bytes satisfies every count that can be derived from
                    // the response itself, and the result is a file 3 KiB
                    // short of the real one wearing the final name.
                    let Some(total) = range.total else {
                        return ResumeAction::Restart;
                    };
                    // The request was open-ended (`bytes=N-`), so an honest
                    // answer runs to the end of the file. A server offering a
                    // shorter slice is offering something that would be
                    // appended and then renamed as though it were the whole
                    // file.
                    if total != range.end + 1 {
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

/// A response whose body will be consumed from byte zero has to actually be
/// the whole file.
///
/// `resolve_resume_action` guards the *ranged* request, but both paths that
/// bypass it consume a body from zero and then take `content_length()` as the
/// expected total. A server answering a whole-file request with `206 bytes
/// 0-5000/8192` satisfies `is_success()`, sets the expectation to 5001, sends
/// exactly that, and the 3 KiB-short file is renamed as complete. A 200 says
/// nothing about ranges and needs no check.
fn validate_whole_body(resp: &reqwest::Response) -> Result<()> {
    if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Ok(());
    }

    let value = resp
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok());

    let Some(range) = value.and_then(parse_content_range) else {
        anyhow::bail!("Server sent a 206 with no usable Content-Range for a whole-file request");
    };

    // Same reasoning as the resume path: `*` is the absence of a total, and
    // the end of the range cannot stand in for one.
    let Some(total) = range.total else {
        anyhow::bail!("Server sent a 206 with an unknown total for a whole-file request");
    };

    if range.start != 0 || range.end + 1 != total {
        anyhow::bail!(
            "Server sent bytes {}-{} of {} when the whole file was requested",
            range.start,
            range.end,
            total
        );
    }

    Ok(())
}

/// The (possibly ranged, possibly conditional) request that starts a stream.
///
/// `if_range` is the server's own chance to refuse: given a validator it no
/// longer recognises, a conforming server ignores the `Range` and answers
/// `200` with the whole body, which [`resolve_resume_action`] reads as a
/// restart. The client-side comparison stays as well, because a server that
/// ignores `If-Range` is exactly the kind that would answer `206` to anything.
pub fn build_streaming_request(
    client: &reqwest::Client,
    url: &str,
    existing_bytes: u64,
    if_range: Option<&str>,
) -> reqwest::RequestBuilder {
    let mut req = client.get(url);
    if existing_bytes > 0 {
        req = req.header(reqwest::header::RANGE, format!("bytes={}-", existing_bytes));
        if let Some(validator) = if_range {
            req = req.header(reqwest::header::IF_RANGE, validator);
        }
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
    id: StreamIdentity<'_>,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<u64> {
    let temp_path = format!("{}.part", output_path);
    let meta_path = ResumeMetadata::meta_path(output_path);
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
    let bytes_on_disk = match tokio::fs::symlink_metadata(temp).await {
        Ok(meta) if meta.is_file() => meta.len(),
        _ => 0,
    };

    // Phase 0: Decide whether those bytes are this file's bytes.
    //
    // The length of a `.part` file is a number, not a provenance. The
    // manifest beside it is what says which resource those bytes came from,
    // and it is the same manifest the segmented path writes.
    let saved = resume::load(&meta_path).await.ok();
    let (existing_bytes, if_range) =
        match plan_streaming_resume(bytes_on_disk, saved.as_ref(), &id) {
            StreamStart::Resume { offset, if_range } => (offset, Some(if_range)),
            StreamStart::Fresh { discarded } => {
                if let Some(reason) = discarded {
                    sink.note(&format!("Restarting from zero: {}", reason));
                }
                (0, None)
            }
        };

    // Phase 1: Build and send (possibly ranged, possibly conditional) request
    //
    // `without_url` on every one of these: reqwest puts the URL it was given
    // into its own error Display, `context` keeps that error in the chain, and
    // `{:#}` prints the chain. The URL is the fetch URL, which is where the
    // gdrive `key=` and every signed parameter live.
    let resp = build_streaming_request(client, url, existing_bytes, if_range.as_deref())
        .send()
        .await
        .map_err(reqwest::Error::without_url)
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
            ResumeAction::Resume(offset) if body_continues_the_same_file(&resp, &if_range) => {
                sink.note(&format!("Resuming from {}", ui::format_size(offset)));
                (offset, true, resp)
            }
            // A 206 whose own validator is not the one we conditioned on.
            // `If-Range` asked the server to refuse this itself; a server
            // that answers anyway does not get to decide.
            ResumeAction::Resume(_) => {
                drop(resp);
                sink.note("Server answered with a different version of the file, restarting from zero");
                let fresh_resp = client
                    .get(url)
                    .send()
                    .await
                    .map_err(reqwest::Error::without_url)
                    .context("Fresh GET request failed")?;

                if !fresh_resp.status().is_success() {
                    anyhow::bail!(
                        "Restart request failed with status {} {}",
                        fresh_resp.status().as_u16(),
                        fresh_resp.status().canonical_reason().unwrap_or("Unknown"),
                    );
                }
                validate_whole_body(&fresh_resp)?;
                (0u64, false, fresh_resp)
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
                    .map_err(reqwest::Error::without_url)
                    .context("Fresh GET request failed")?;

                if !fresh_resp.status().is_success() {
                    anyhow::bail!(
                        "Restart request failed with status {} {}",
                        fresh_resp.status().as_u16(),
                        fresh_resp.status().canonical_reason().unwrap_or("Unknown"),
                    );
                }
                // The restart was triggered by a server that answered the
                // ranged request badly. Accepting whatever it sends next
                // without looking hands the same trick a second chance.
                validate_whole_body(&fresh_resp)?;
                (0u64, false, fresh_resp)
            }
            ResumeAction::Fresh => {
                // No `.part`, so no Range header was sent -- which makes a 206
                // here unsolicited, and its range still decides how many bytes
                // count as the whole file.
                validate_whole_body(&resp)?;
                (0u64, false, resp)
            }
            ResumeAction::Fail(code) => {
                anyhow::bail!(
                    "Server returned {} {}",
                    code.as_u16(),
                    code.canonical_reason().unwrap_or("Unknown"),
                );
            }
        };

    // How big the finished file should be, when the server has said. On a
    // resume that is the total it stated — `resolve_resume_action` only
    // returns `Resume` for a numeric total that equals `end + 1`, so this is
    // a figure the server committed to rather than one inferred from the end
    // of the range. Otherwise it is the length of the body it is about to
    // send. It is checked again at the end, and the rename does not happen
    // unless it matches.
    let declared_total: Option<u64> = if append {
        declared_range.and_then(|range| range.total)
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

    // The manifest for the bytes about to be written. On a resume the file
    // already holds one that describes them; on a fresh body it is written
    // from the validator this very response carries, which is the one the
    // next run has to match before it appends anything.
    if !append {
        let (body_etag, body_last_modified) = response_validator(&resp);
        let meta = resume::create_streaming(
            id.url.to_owned(),
            id.identity,
            body_etag.as_deref().or(id.etag),
            body_last_modified.as_deref().or(id.last_modified),
        );
        if let Err(e) = resume::save_atomic(&meta_path, &meta).await {
            // Losing the manifest costs the next run its resume, not this
            // run its download.
            sink.note(&format!("Could not write resume state: {:#}", e));
        }
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
        safe_file::open_guarded(
            temp,
            Existing::Open,
            Access::Append,
            safe_file::DEFAULT_FILE_MODE,
        )
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
                // The stream error names the URL too.
                return Err(e.without_url())
                    .context(format!("Stream error at byte {}", downloaded));
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

    // The transfer is finished, so the state describing how to continue it is
    // not just useless but misleading.
    let _ = resume::delete(&meta_path).await;

    Ok(downloaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[test]
    fn test_build_request_no_existing_bytes() {
        let client = reqwest::Client::new();
        let req = build_streaming_request(&client, "https://example.com/file.bin", 0, None)
            .build()
            .unwrap();
        assert!(req.headers().get(reqwest::header::RANGE).is_none());
        assert!(req.headers().get(reqwest::header::IF_RANGE).is_none());
    }

    #[test]
    fn test_build_request_with_existing_bytes() {
        let client = reqwest::Client::new();
        let req = build_streaming_request(
            &client,
            "https://example.com/file.bin",
            4096,
            Some("\"v1\""),
        )
        .build()
        .unwrap();
        let range = req.headers().get(reqwest::header::RANGE).unwrap();
        assert_eq!(range.to_str().unwrap(), "bytes=4096-");
        // The server's own chance to refuse a resume across a change.
        let if_range = req.headers().get(reqwest::header::IF_RANGE).unwrap();
        assert_eq!(if_range.to_str().unwrap(), "\"v1\"");
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
        // An unknown total is legal in the header and says nothing either way.
        // Whether it is good enough to resume on is a separate question, and
        // the answer is no.
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
    }

    /// A `*` total is the absence of a size, and it used to be resumed on: the
    /// expected size was then inferred as `end + 1`, so a server could pick
    /// any end, send exactly that many bytes, and have the short file renamed
    /// as finished. There is no number in the response to catch that with, so
    /// the resume is refused instead. Restarting costs bandwidth; the
    /// alternative costs the file.
    #[test]
    fn an_unknown_total_is_never_good_enough_to_resume_on() {
        let resume =
            |cr: &str| resolve_resume_action(reqwest::StatusCode::PARTIAL_CONTENT, 4096, Some(cr));

        // The response from the finding. 905 bytes would have arrived and
        // satisfied a 5001-byte expectation derived from the header itself.
        assert_eq!(resume("bytes 4096-5000/*"), ResumeAction::Restart);

        // Even a range that looks generous proves nothing without a total.
        assert_eq!(resume("bytes 4096-8191/*"), ResumeAction::Restart);

        // The header still parses; it is the resume decision that refuses it.
        assert_eq!(
            parse_content_range("bytes 4096-5000/*").map(|r| r.total),
            Some(None)
        );
    }

    // ---------- Whose bytes are these ----------

    fn identity<'a>(etag: Option<&'a str>, last_modified: Option<&'a str>) -> StreamIdentity<'a> {
        StreamIdentity {
            url: "https://example.com/file.bin",
            identity: None,
            etag,
            last_modified,
        }
    }

    fn streaming_manifest(etag: Option<&str>, last_modified: Option<&str>) -> ResumeMetadata {
        resume::create_streaming(
            "https://example.com/file.bin".into(),
            None,
            etag,
            last_modified,
        )
    }

    #[test]
    fn nothing_on_disk_is_simply_a_fresh_download() {
        assert_eq!(
            plan_streaming_resume(0, None, &identity(Some("\"v1\""), None)),
            StreamStart::Fresh { discarded: None }
        );
    }

    /// The finding: the streaming path resumed from the length of the `.part`
    /// file alone. Bytes with no manifest have no provenance, and appending
    /// to them can splice two different files together.
    #[test]
    fn a_partial_file_with_no_manifest_is_never_appended_to() {
        assert!(matches!(
            plan_streaming_resume(4096, None, &identity(Some("\"v1\""), None)),
            StreamStart::Fresh {
                discarded: Some(_)
            }
        ));
    }

    #[test]
    fn a_matching_validator_resumes_and_conditions_the_request() {
        let saved = streaming_manifest(Some("\"v1\""), None);
        assert_eq!(
            plan_streaming_resume(4096, Some(&saved), &identity(Some("\"v1\""), None)),
            StreamStart::Resume {
                offset: 4096,
                if_range: "\"v1\"".into(),
            }
        );
    }

    /// The resource changed behind a stable URL. This is the case where the
    /// old code produced a file made of two different downloads.
    #[test]
    fn a_changed_validator_restarts() {
        let saved = streaming_manifest(Some("\"v1\""), None);
        assert!(matches!(
            plan_streaming_resume(4096, Some(&saved), &identity(Some("\"v2\""), None)),
            StreamStart::Fresh {
                discarded: Some(_)
            }
        ));
    }

    /// Continuity that cannot be established is not continuity: a server
    /// that has stopped offering a validator is not confirming anything.
    #[test]
    fn a_vanished_or_absent_validator_restarts() {
        let saved = streaming_manifest(Some("\"v1\""), None);
        assert!(matches!(
            plan_streaming_resume(4096, Some(&saved), &identity(None, None)),
            StreamStart::Fresh {
                discarded: Some(_)
            }
        ));

        let no_validator = streaming_manifest(None, None);
        assert!(matches!(
            plan_streaming_resume(4096, Some(&no_validator), &identity(Some("\"v1\""), None)),
            StreamStart::Fresh {
                discarded: Some(_)
            }
        ));
    }

    #[test]
    fn a_manifest_for_another_source_restarts() {
        let saved = resume::create_streaming(
            "https://elsewhere.example/other.bin".into(),
            None,
            Some("\"v1\""),
            None,
        );
        assert!(matches!(
            plan_streaming_resume(4096, Some(&saved), &identity(Some("\"v1\""), None)),
            StreamStart::Fresh {
                discarded: Some(_)
            }
        ));
    }

    /// The two paths share a manifest format, so each has to recognise the
    /// other's and decline it: a segmented `.part` is a preallocated file
    /// with holes, not a prefix.
    #[test]
    fn a_segmented_manifest_is_not_a_streaming_one() {
        let mut saved = streaming_manifest(Some("\"v1\""), None);
        saved.transfer = TransferKind::Segmented;
        assert!(matches!(
            plan_streaming_resume(4096, Some(&saved), &identity(Some("\"v1\""), None)),
            StreamStart::Fresh {
                discarded: Some(_)
            }
        ));
    }

    /// A server that ignores `If-Range` and answers 206 anyway is checked
    /// again on this side.
    #[test]
    fn a_206_that_carries_another_version_is_not_a_continuation() {
        fn response(etag: Option<&'static str>) -> reqwest::Response {
            use axum::http;
            let mut builder = http::Response::builder().status(reqwest::StatusCode::PARTIAL_CONTENT);
            if let Some(etag) = etag {
                builder = builder.header("etag", etag);
            }
            reqwest::Response::from(builder.body(String::new()).unwrap())
        }

        let expected = Some("\"v1\"".to_owned());
        assert!(body_continues_the_same_file(&response(Some("\"v1\"")), &expected));
        assert!(!body_continues_the_same_file(
            &response(Some("\"v2\"")),
            &expected
        ));
        // Nothing to compare against is not a pass.
        assert!(!body_continues_the_same_file(&response(None), &expected));
        assert!(!body_continues_the_same_file(&response(Some("\"v1\"")), &None));
    }

    // ---------- End to end ----------

    async fn serve(router: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{}", addr)
    }

    /// The finding, end to end: a `.part` file left by version one of a
    /// resource, and a server now holding version two at the same URL.
    ///
    /// The old code sent `Range: bytes=5-`, saw a well-formed `206`, and
    /// appended the tail of the new file to the head of the old one. The
    /// result was renamed as a finished download and nothing ever said
    /// otherwise.
    #[tokio::test]
    async fn a_changed_resource_is_never_spliced_onto_the_old_partial_file() {
        use axum::http::header;
        use axum::routing::get;

        // Version two, served whole to anyone who asks, and served as a
        // range to anyone who asks for one — a server that ignores
        // `If-Range`, which is the case the client-side check is for.
        let body = "NEW-CONTENT-ENTIRELY";
        let base = serve(axum::Router::new().route(
            "/file.bin",
            get(move |headers: header::HeaderMap| async move {
                match headers.get(header::RANGE) {
                    Some(_) => (
                        reqwest::StatusCode::PARTIAL_CONTENT,
                        [
                            (header::CONTENT_RANGE, format!("bytes 5-19/{}", body.len())),
                            (header::ETAG, "\"v2\"".to_owned()),
                        ],
                        &body[5..],
                    )
                        .into_response(),
                    None => (
                        reqwest::StatusCode::OK,
                        [(header::ETAG, "\"v2\"".to_owned())],
                        body,
                    )
                        .into_response(),
                }
            }),
        ))
        .await;
        let url = format!("{}/file.bin", base);

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file.bin").to_string_lossy().into_owned();

        // What version one left behind: five bytes, and a manifest saying
        // which file they came from.
        tokio::fs::write(format!("{}.part", output), b"OLD-1")
            .await
            .unwrap();
        let old_meta = resume::create_streaming(url.clone(), None, Some("\"v1\""), None);
        resume::save_atomic(&ResumeMetadata::meta_path(&output), &old_meta)
            .await
            .unwrap();

        let written = download_streaming(
            &reqwest::Client::builder().no_proxy().build().unwrap(),
            &url,
            &output,
            StreamIdentity {
                url: &url,
                identity: None,
                etag: Some("\"v2\""),
                last_modified: None,
            },
            CancellationToken::new(),
            crate::ui::silent(),
        )
        .await
        .expect("the download itself should succeed, from zero");

        let on_disk = tokio::fs::read_to_string(&output).await.unwrap();
        assert_eq!(on_disk, body, "two versions were spliced together");
        assert_eq!(written, body.len() as u64);

        // Finished, so the resume state is gone rather than left to mislead.
        assert!(
            tokio::fs::metadata(ResumeMetadata::meta_path(&output))
                .await
                .is_err()
        );
    }

    /// The other half: unchanged content really does resume, and the manifest
    /// is what makes that safe rather than lucky.
    #[tokio::test]
    async fn an_unchanged_resource_resumes_from_the_partial_file() {
        use axum::http::header;
        use axum::routing::get;

        let body = "0123456789ABCDEF";
        let base = serve(axum::Router::new().route(
            "/file.bin",
            get(move |headers: header::HeaderMap| async move {
                let range = headers
                    .get(header::RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("bytes="))
                    .and_then(|v| v.split('-').next())
                    .and_then(|v| v.parse::<usize>().ok());

                match range {
                    Some(start) => (
                        reqwest::StatusCode::PARTIAL_CONTENT,
                        [
                            (
                                header::CONTENT_RANGE,
                                format!("bytes {}-{}/{}", start, body.len() - 1, body.len()),
                            ),
                            (header::ETAG, "\"v1\"".to_owned()),
                        ],
                        &body[start..],
                    )
                        .into_response(),
                    None => (
                        reqwest::StatusCode::OK,
                        [(header::ETAG, "\"v1\"".to_owned())],
                        body,
                    )
                        .into_response(),
                }
            }),
        ))
        .await;
        let url = format!("{}/file.bin", base);

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file.bin").to_string_lossy().into_owned();

        tokio::fs::write(format!("{}.part", output), &body.as_bytes()[..6])
            .await
            .unwrap();
        let meta = resume::create_streaming(url.clone(), None, Some("\"v1\""), None);
        resume::save_atomic(&ResumeMetadata::meta_path(&output), &meta)
            .await
            .unwrap();

        let written = download_streaming(
            &reqwest::Client::builder().no_proxy().build().unwrap(),
            &url,
            &output,
            StreamIdentity {
                url: &url,
                identity: None,
                etag: Some("\"v1\""),
                last_modified: None,
            },
            CancellationToken::new(),
            crate::ui::silent(),
        )
        .await
        .expect("an unchanged file should resume");

        assert_eq!(tokio::fs::read_to_string(&output).await.unwrap(), body);
        // Only the tail came over the wire.
        assert_eq!(written, body.len() as u64);
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

    fn response_with(
        status: reqwest::StatusCode,
        content_range: Option<&str>,
    ) -> reqwest::Response {
        use axum::http;

        let mut builder = http::Response::builder().status(status);
        if let Some(cr) = content_range {
            builder = builder.header("content-range", cr);
        }
        reqwest::Response::from(builder.body(String::new()).unwrap())
    }

    /// The gap the ranged check left open: after a bad resume is correctly
    /// refused, the restart consumed whatever came back from byte zero and
    /// took its length as the total.
    #[test]
    fn a_whole_file_request_answered_with_a_slice_is_refused() {
        use reqwest::StatusCode;

        // A plain 200 makes no range claim.
        assert!(validate_whole_body(&response_with(StatusCode::OK, None)).is_ok());

        // A 206 that really is the whole file is fine.
        assert!(
            validate_whole_body(&response_with(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 0-8191/8192")
            ))
            .is_ok()
        );

        for bad in [
            // The finding: 5001 bytes would become the expected total.
            Some("bytes 0-5000/8192"),
            // Starts somewhere else entirely.
            Some("bytes 4096-8191/8192"),
            // No total to check the tail against.
            Some("bytes 0-5000/*"),
            // A 206 claiming nothing at all.
            None,
        ] {
            assert!(
                validate_whole_body(&response_with(StatusCode::PARTIAL_CONTENT, bad)).is_err(),
                "{bad:?} was accepted for a whole-file request"
            );
        }
    }
}
