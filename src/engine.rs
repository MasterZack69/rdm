//! The download engine.
//!
//! Formerly `cli.rs`, which was a misnomer: argument parsing lives in
//! `args.rs` and the command dispatch lives in `main.rs`. This module knows
//! how to turn a URL into a file on disk and nothing else.
//!
//! It never prints. Callers hand it a [`ProgressSink`] and receive an
//! [`Outcome`]; a single download passes a [`ui::SoloBar`], the queue passes a
//! lane of its live board. That is what makes per-file progress lines
//! possible during parallel runs.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::chunk::Chunk;
use crate::inspect;
use crate::parallel;
use crate::retry::RetryConfig;
use crate::ui::{self, ProgressSink, SlotState};

static SHARED_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn shared_client() -> Result<&'static reqwest::Client> {
    if let Some(c) = SHARED_CLIENT.get() {
        return Ok(c);
    }
    let client = reqwest::Client::builder()
        .user_agent("rdm")
        .connect_timeout(Duration::from_secs(10))
        .build()
        .context("Failed to build HTTP client")?;
    Ok(SHARED_CLIENT.get_or_init(|| client))
}

static SHARED_CONFIG: OnceLock<crate::config::Config> = OnceLock::new();

fn shared_config() -> &'static crate::config::Config {
    SHARED_CONFIG.get_or_init(crate::config::Config::load)
}

// ── Request / outcome ───────────────────────────────────────────────────

/// What to do when the target file already exists on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingPolicy {
    /// Prompt the user to overwrite / rename / cancel. Interactive only.
    Ask,
    /// Treat the existing file as done. Used by every batch path, so a queue
    /// of 400 files never blocks on a hidden prompt behind the progress board.
    Reuse,
    /// Remove the file (and any `.part` / `.rdm` state) and download again.
    Overwrite,
}

#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub output: Option<String>,
    pub connections: usize,
    pub policy: ExistingPolicy,
    /// The client to download with, when it has to be a particular one.
    ///
    /// Normally `None`, and the shared client is used along with its connection
    /// pool. A hoster that had to authenticate passes its own client instead,
    /// because some authorisation cannot be expressed in a URL: a
    /// password-protected Dropbox share is authorised by the cookies in a jar,
    /// so the download has to go out over the client holding that jar.
    ///
    /// Keeping it on the request is what lets such a hoster stay a URL rewrite
    /// instead of growing a downloader: ranges, chunking, resume and retries
    /// all still come from this module.
    pub client: Option<reqwest::Client>,
    /// A stable identity for the remote content, when the URL is not one.
    ///
    /// The same reason `client` exists: a signed URL is a credential, not a
    /// name. A OneDrive download URL carries a fresh `tempauth` per run, so
    /// resume state keyed on it is discarded and the file restarts from zero.
    /// A hoster that knows something durable — a drive item id — puts it here.
    pub resume_identity: Option<String>,
}

impl DownloadRequest {
    pub fn new(url: String, output: Option<String>, connections: usize) -> Self {
        Self {
            url,
            output,
            connections,
            policy: ExistingPolicy::Ask,
            client: None,
            resume_identity: None,
        }
    }

    pub fn with_policy(mut self, policy: ExistingPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Downloads over `client` rather than the shared one, carrying whatever
    /// session it holds.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Identifies the remote content for resume purposes, when the URL cannot.
    pub fn with_resume_identity(mut self, identity: String) -> Self {
        self.resume_identity = Some(identity);
        self
    }
}

/// How a download ended. Errors are still returned as `Err`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Completed { path: String, bytes: u64 },
    AlreadyPresent { path: String },
    Cancelled,
}

/// Result of reconciling the requested output path with what is on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputDecision {
    Use(String),
    AlreadyPresent,
    Cancelled,
}

// ── Streaming resume helpers ─────────────────────────────────────────────

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

// ── Main download entry point ────────────────────────────────────────────

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

/// Single-download convenience wrapper: renders its own progress bar and
/// prints a summary line. Used by `rdm <url>` and `rdm download`.
pub async fn run_download(
    url: String,
    output: Option<String>,
    connections: usize,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    run(url, output, connections, None, None, cancel, quiet).await
}

/// The same, over a client the caller has already authenticated.
///
/// For a source whose authorisation is a session rather than part of the URL —
/// a password-protected Dropbox share, whose cookies live in that client's jar.
pub async fn run_download_with_client(
    url: String,
    output: Option<String>,
    connections: usize,
    client: reqwest::Client,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    run(url, output, connections, Some(client), None, cancel, quiet).await
}

/// The same, for a source whose URL is a credential rather than a name.
pub async fn run_download_with_identity(
    url: String,
    output: Option<String>,
    connections: usize,
    identity: String,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    run(
        url,
        output,
        connections,
        None,
        Some(identity),
        cancel,
        quiet,
    )
    .await
}

async fn run(
    url: String,
    output: Option<String>,
    connections: usize,
    client: Option<reqwest::Client>,
    identity: Option<String>,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    let name = output
        .clone()
        .map(|o| o.rsplit('/').next().unwrap_or(&o).to_owned())
        .or_else(|| extract_filename_from_url(&url))
        .unwrap_or_else(|| "download".to_owned());

    let bar = if quiet {
        None
    } else {
        Some(ui::SoloBar::new(&name))
    };
    let sink: Arc<dyn ProgressSink> = match &bar {
        Some(b) => Arc::clone(b) as Arc<dyn ProgressSink>,
        None => ui::silent(),
    };

    let request = DownloadRequest::new(url, output, connections);
    let request = match client {
        Some(authenticated) => request.with_client(authenticated),
        None => request,
    };
    let request = match identity {
        Some(identity) => request.with_resume_identity(identity),
        None => request,
    };

    let result = download(request, cancel, sink).await;
    let elapsed = bar.as_ref().map(|b| b.elapsed()).unwrap_or_default();

    match result {
        Ok(Outcome::Completed { path, bytes }) => {
            if !quiet {
                let secs = elapsed.as_secs_f64();
                let avg = if secs > 0.1 {
                    Some((bytes as f64 / secs) as u64)
                } else {
                    None
                };
                eprintln!("  \u{2705} Download complete: {}", path);
                eprintln!(
                    "  {} in {} ({})",
                    ui::format_size(bytes),
                    ui::format_duration(elapsed.as_secs()),
                    ui::format_speed(avg),
                );
            }
            Ok(())
        }
        Ok(Outcome::AlreadyPresent { path }) => {
            if !quiet {
                eprintln!("  \u{2713} Already downloaded: {}", path);
            }
            Ok(())
        }
        Ok(Outcome::Cancelled) => {
            if !quiet {
                eprintln!("  Download cancelled.");
            }
            Ok(())
        }
        Err(e) => {
            if !quiet {
                eprintln!("  \u{274d} Download failed.");
                eprintln!("  Progress saved. Resume by running the same command again.");
            }
            Err(e)
        }
    }
}

async fn download_streaming(
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

/// Decides what to do about an already-existing output file.
///
/// A resumable download (a `.part` file, or valid `.rdm` metadata) always wins
/// over the policy — there is nothing to ask about, we just continue.
pub async fn resolve_existing_output(
    path: &str,
    url: &str,
    identity: Option<&str>,
    policy: ExistingPolicy,
) -> Result<OutputDecision> {
    use std::io::{BufRead, IsTerminal, Write};

    if !std::path::Path::new(path).exists() {
        return Ok(OutputDecision::Use(path.to_owned()));
    }

    let part_path = format!("{}.part", path);
    if std::path::Path::new(&part_path).exists() {
        return Ok(OutputDecision::Use(path.to_owned()));
    }

    let meta_path = crate::resume::ResumeMetadata::meta_path(path);
    if let Ok(meta) = crate::resume::load(&meta_path).await {
        let chunks: Vec<crate::chunk::Chunk> = meta
            .chunks
            .iter()
            .map(|c| crate::chunk::Chunk {
                id: c.id,
                start: c.start,
                end: c.end,
            })
            .collect();
        if crate::resume::validate_against(&meta, url, identity, meta.file_size, &chunks) {
            return Ok(OutputDecision::Use(path.to_owned()));
        }
    }

    match policy {
        // Batch runs must never block on stdin.
        ExistingPolicy::Reuse => return Ok(OutputDecision::AlreadyPresent),
        ExistingPolicy::Overwrite => {
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_file(&part_path);
            let _ = std::fs::remove_file(&meta_path);
            return Ok(OutputDecision::Use(path.to_owned()));
        }
        ExistingPolicy::Ask => {}
    }

    if !std::io::stdin().is_terminal() {
        anyhow::bail!(
            "File already exists: {}\n  Use -o to specify a different output path.",
            path
        );
    }

    let parent = std::path::Path::new(path)
        .parent()
        .unwrap_or(std::path::Path::new(""));

    eprintln!("  \u{26a0} File already exists: {}", path);
    eprintln!();
    eprintln!("  1) Overwrite");
    eprintln!("  2) Rename");
    eprintln!("  3) Cancel");

    loop {
        eprint!("  Choice [1/2/3]: ");
        std::io::stderr().flush()?;

        let mut input = String::new();
        std::io::stdin().lock().read_line(&mut input)?;

        match input.trim() {
            "1" => {
                let _ = std::fs::remove_file(path);
                let _ = std::fs::remove_file(&part_path);
                let _ = std::fs::remove_file(&meta_path);
                return Ok(OutputDecision::Use(path.to_owned()));
            }
            "2" => loop {
                eprint!("  New filename: ");
                std::io::stderr().flush()?;
                let mut name = String::new();
                std::io::stdin().lock().read_line(&mut name)?;
                let trimmed = name.trim();
                if trimmed.is_empty() {
                    eprintln!("  Filename cannot be empty.");
                    continue;
                }
                let new_path = if parent.as_os_str().is_empty() {
                    trimmed.to_owned()
                } else {
                    parent.join(trimmed).to_string_lossy().to_string()
                };
                return Ok(OutputDecision::Use(new_path));
            },
            "3" => return Ok(OutputDecision::Cancelled),
            _ => eprintln!("  Invalid choice. Enter 1, 2, or 3."),
        }
    }
}

fn resolve_output_path(url: &str, output: Option<&str>) -> String {
    if let Some(provided) = output {
        return provided.to_owned();
    }
    extract_filename_from_url(url).unwrap_or_else(|| "download.bin".to_owned())
}

pub fn extract_filename_from_url(url: &str) -> Option<String> {
    let normalized = normalize_download_url(url);
    let without_fragment = normalized.split('#').next()?;
    let path = without_fragment.split('?').next()?;
    let segment = path.rsplit('/').next()?;
    let decoded = percent_decode(segment);
    let trimmed = decoded.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return None;
    }
    Some(trimmed.to_owned())
}

pub fn normalize_download_url(url: &str) -> String {
    let Ok(mut parsed) = reqwest::Url::parse(url) else {
        return url.to_owned();
    };

    let Some(fragment) = parsed.fragment() else {
        return url.to_owned();
    };

    let route = fragment.split('?').next().unwrap_or(fragment);
    let route = route.trim_start_matches('/').to_owned();
    let mut route_segments = route.split('/');
    if route_segments.next() != Some("download") {
        return url.to_owned();
    }

    let rest: Vec<String> = route_segments
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    let Some(last) = rest.last() else {
        return url.to_owned();
    };
    if !last.contains('.') {
        return url.to_owned();
    }

    parsed.set_fragment(None);
    parsed.set_query(None);

    let mut base_dir = parsed.path().to_owned();
    if !base_dir.ends_with('/') {
        if let Some(pos) = base_dir.rfind('/') {
            base_dir.truncate(pos + 1);
        } else {
            base_dir.clear();
            base_dir.push('/');
        }
    }

    let new_path = format!("{}download/{}", base_dir, rest.join("/"));
    parsed.set_path(&new_path);
    parsed.to_string()
}

pub fn percent_decode(input: &str) -> String {
    let mut bytes = Vec::with_capacity(input.len());
    let mut chars = input.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next();
            let lo = chars.next();
            if let (Some(h), Some(l)) = (hi, lo) {
                if let Ok(s) = std::str::from_utf8(&[h, l])
                    && let Ok(decoded) = u8::from_str_radix(s, 16)
                {
                    bytes.push(decoded);
                    continue;
                }
                // Failed decode — push all three bytes back
                bytes.push(b'%');
                bytes.push(h);
                bytes.push(l);
            } else {
                // Incomplete sequence — push what we have
                bytes.push(b'%');
                if let Some(h) = hi {
                    bytes.push(h);
                }
            }
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|_| input.to_owned())
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

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_filename_simple() {
        assert_eq!(
            extract_filename_from_url("https://example.com/path/file.zip"),
            Some("file.zip".into())
        );
    }
    #[test]
    fn test_extract_filename_query() {
        assert_eq!(
            extract_filename_from_url("https://example.com/file.tar.gz?t=1"),
            Some("file.tar.gz".into())
        );
    }
    #[test]
    fn test_extract_filename_hash_download_route() {
        assert_eq!(
            extract_filename_from_url(
                "https://mobdisc.com/dwbfc3e38e/download.html?lang=en#/download/8189-DOOM-3-v1-1-0-22-cache1.zip"
            ),
            Some("8189-DOOM-3-v1-1-0-22-cache1.zip".into())
        );
    }
    #[test]
    fn test_normalize_hash_download_route() {
        assert_eq!(
            normalize_download_url(
                "https://mobdisc.com/dwbfc3e38e/download.html?lang=en#/download/8189-DOOM-3-v1-1-0-22-cache1.zip"
            ),
            "https://mobdisc.com/dwbfc3e38e/download/8189-DOOM-3-v1-1-0-22-cache1.zip"
        );
    }
    #[test]
    fn test_normalize_ignores_regular_fragment() {
        let url = "https://example.com/file.zip#section";
        assert_eq!(normalize_download_url(url), url);
    }
    #[test]
    fn test_extract_filename_percent() {
        assert_eq!(
            extract_filename_from_url("https://example.com/my%20file.zip"),
            Some("my file.zip".into())
        );
    }
    #[test]
    fn test_extract_filename_trailing() {
        assert_eq!(extract_filename_from_url("https://example.com/"), None);
    }
    #[test]
    fn test_resolve_explicit() {
        assert_eq!(
            resolve_output_path("https://example.com/f.zip", Some("out.zip")),
            "out.zip"
        );
    }
    #[test]
    fn test_resolve_from_url() {
        assert_eq!(
            resolve_output_path("https://example.com/data.tar.gz", None),
            "data.tar.gz"
        );
    }
    #[test]
    fn test_resolve_fallback() {
        assert_eq!(
            resolve_output_path("https://example.com/", None),
            "download.bin"
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

    // ── Request builder ──

    #[test]
    fn request_defaults_to_asking_about_existing_files() {
        let req = DownloadRequest::new("https://example.com/f.zip".into(), None, 8);
        assert_eq!(req.policy, ExistingPolicy::Ask);
        assert_eq!(
            req.with_policy(ExistingPolicy::Reuse).policy,
            ExistingPolicy::Reuse
        );
    }

    /// A session cannot be expressed in a URL, so it travels on the request.
    /// Default is the shared client, which is what keeps the pool useful.
    #[test]
    fn a_request_uses_the_shared_client_unless_given_one() {
        let req = DownloadRequest::new("https://example.com/f.zip".into(), None, 8);
        assert!(req.client.is_none());

        let authenticated = DownloadRequest::new("https://example.com/f.zip".into(), None, 8)
            .with_client(reqwest::Client::new());
        assert!(authenticated.client.is_some());
    }

    // ── Existing-output policy ──

    #[tokio::test]
    async fn missing_file_is_always_used() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.bin").to_string_lossy().to_string();
        assert_eq!(
            resolve_existing_output(
                &path,
                "https://example.com/nope.bin",
                None,
                ExistingPolicy::Ask
            )
            .await
            .unwrap(),
            OutputDecision::Use(path),
        );
    }

    #[tokio::test]
    async fn reuse_policy_reports_existing_file_instead_of_prompting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("done.bin");
        std::fs::write(&path, b"payload").unwrap();
        let path = path.to_string_lossy().to_string();

        assert_eq!(
            resolve_existing_output(
                &path,
                "https://example.com/done.bin",
                None,
                ExistingPolicy::Reuse
            )
            .await
            .unwrap(),
            OutputDecision::AlreadyPresent,
        );
    }

    #[tokio::test]
    async fn overwrite_policy_clears_the_file_and_its_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stale.bin");
        std::fs::write(&path, b"old").unwrap();
        let path = path.to_string_lossy().to_string();

        assert_eq!(
            resolve_existing_output(
                &path,
                "https://example.com/stale.bin",
                None,
                ExistingPolicy::Overwrite
            )
            .await
            .unwrap(),
            OutputDecision::Use(path.clone()),
        );
        assert!(!std::path::Path::new(&path).exists());
    }

    #[tokio::test]
    async fn a_partial_download_resumes_regardless_of_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("half.bin");
        std::fs::write(&path, b"old").unwrap();
        std::fs::write(dir.path().join("half.bin.part"), b"partial").unwrap();
        let path = path.to_string_lossy().to_string();

        assert_eq!(
            resolve_existing_output(
                &path,
                "https://example.com/half.bin",
                None,
                ExistingPolicy::Reuse
            )
            .await
            .unwrap(),
            OutputDecision::Use(path),
        );
    }

    // ── Streaming resume helper tests ──

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

    // ── percent_decode fix tests ──

    #[test]
    fn test_percent_decode_valid() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
    }

    #[test]
    fn test_percent_decode_invalid_hex() {
        // %GH is not valid hex — all three bytes should be preserved
        assert_eq!(percent_decode("test%GHvalue"), "test%GHvalue");
    }

    #[test]
    fn test_percent_decode_truncated_at_end() {
        // trailing %2 with no second hex char
        assert_eq!(percent_decode("test%2"), "test%2");
    }

    #[test]
    fn test_percent_decode_bare_percent() {
        assert_eq!(percent_decode("test%"), "test%");
    }
}
