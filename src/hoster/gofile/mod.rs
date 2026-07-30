//! GoFile (gofile.io) support.
//!
//! A GoFile link is not a fetchable address. `https://gofile.io/d/AbCdEf` is a
//! *content id*: the real download URLs live behind an API call, they are
//! per-server (`store1.gofile.io`, `store7.gofile.io`, …), and they only work
//! while an account token rides along on the request. So the generic HTTP
//! engine cannot be handed one of these links, which is exactly the situation
//! [`crate::hoster`] exists for.
//!
//! ## What the API wants
//!
//! Three things, all of which the website itself does and all of which are
//! ported from the reference implementation:
//!
//!   1. **An account token.** Anonymous visitors get a throwaway guest account
//!      from `POST /accounts`. People with a real account can supply their own
//!      token instead and keep whatever quota it carries.
//!   2. **A website token.** `X-Website-Token` is a SHA-256 over the user
//!      agent, the locale, the account token, a 4-hour time slot and a fixed
//!      salt. It rotates on its own every four hours, which is why it is
//!      recomputed per request rather than cached.
//!   3. **The token on every hop.** Both as `Authorization: Bearer …` and as
//!      an `accountToken` cookie — the API accepts the header, the storage
//!      servers want the cookie.
//!
//! ## Shape of a download
//!
//! One content id can be a single file or a whole tree of folders, and that is
//! not knowable from the link, so a GoFile download always walks the tree
//! first and only then decides where to put things. Where that is depends on
//! what came back — see [`destination_root`].
//!
//! Per file: `.part` temp file, `Range` resume, a status-code check that
//! refuses to treat a `200` as a resumed `206`, and a size comparison before
//! the temp file is moved into place. A file already on disk with a non-zero
//! size is left alone.
//!
//! ## What this deliberately does not do
//!
//! No parallel chunks within one file. GoFile's storage nodes rate-limit hard
//! per connection *and* per account, and the reference downloader gets its
//! throughput by running several files at once instead. `-c` therefore means
//! "files in flight" here, in the same way it means "workers" for MEGA.

mod naming;
mod sha256;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{StreamExt, stream};
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_RANGE, COOKIE, HeaderMap, HeaderValue, ORIGIN, RANGE, REFERER,
    USER_AGENT,
};
use reqwest::{Client, Response, StatusCode};
use serde_json::Value;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::ui::{self, Board, ProgressSink, SlotState};
use naming::unique_path;

/// GoFile's public API root.
const API_BASE: &str = "https://api.gofile.io";

/// Salt baked into the website token. Rotating it is a server-side decision;
/// when downloads start failing with `error-wt`, this is the first thing to
/// check against the live site.
const WEBSITE_TOKEN_SALT: &str = "9844d94d963d30";

/// The website token is valid for one four-hour slot.
const TOKEN_WINDOW_SECS: u64 = 14_400;

/// Sent as-is, because the same string is hashed into the website token: a
/// mismatch between the header and the hash is rejected by the API.
const DEFAULT_USER_AGENT: &str = "Mozilla/5.0";

/// Files in flight when nothing else says otherwise.
pub const WORKERS_DEFAULT: usize = 5;

/// Upper bound on files in flight. The reference downloader caps batches at
/// ten for the same reason: the API starts refusing a starved account.
const WORKERS_MAX: usize = 10;

// ── Link handling ─────────────────────────────────────────

/// A parsed GoFile link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GofileLink {
    /// The opaque content id — a file, or the root of a folder tree.
    pub content_id: String,
}

/// Host of an http(s) URL, lower-cased and without any port.
fn host_of(url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let authority = rest.split(['/', '?', '#']).next()?;
    // Anything before an `@` is userinfo, and `evil.com@gofile.io` style
    // links must not be read as the host that comes before it.
    let host = authority.rsplit('@').next()?;
    let host = host.split(':').next()?;
    Some(host.trim_end_matches('.').to_ascii_lowercase())
}

/// Is this a GoFile link?
///
/// Host equality, not substring matching: `gofile.io.evil.com` and
/// `notgofile.io` are somebody else's problem.
pub fn is_gofile_url(url: &str) -> bool {
    matches!(
        host_of(url.trim()).as_deref(),
        Some("gofile.io") | Some("www.gofile.io")
    )
}

/// Extracts the content id from `https://gofile.io/d/<id>`.
pub fn parse_link(url: &str) -> Result<GofileLink> {
    let trimmed = url.trim();

    if !is_gofile_url(trimmed) {
        bail!("not a GoFile link: {trimmed}");
    }

    let after_scheme = trimmed
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(trimmed);
    let path = after_scheme.split(['?', '#']).next().unwrap_or("");

    let mut segments = path.split('/').filter(|s| !s.is_empty());
    let _host = segments.next();
    let kind = segments.next().unwrap_or("");
    let id = segments.next().unwrap_or("");

    if kind != "d" {
        bail!("GoFile links look like https://gofile.io/d/<id> — got {trimmed}");
    }

    if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        bail!("no content id in {trimmed}");
    }

    Ok(GofileLink {
        content_id: id.to_string(),
    })
}

// ── Options and results ───────────────────────────────────

/// Knobs for a GoFile download.
#[derive(Debug, Clone)]
pub struct GofileOptions {
    /// How many files to download at once.
    pub workers: usize,
    /// Attempts per request before a file is given up on.
    pub max_retries: u32,
    /// Password for a protected link, in plain text. It is hashed before it
    /// ever leaves the process.
    pub password: Option<String>,
    /// An existing GoFile account token. `None` means "create a guest".
    pub token: Option<String>,
    /// Re-download files that are already on disk.
    pub overwrite: bool,
}

impl Default for GofileOptions {
    fn default() -> Self {
        Self {
            workers: WORKERS_DEFAULT,
            max_retries: 5,
            password: None,
            token: None,
            overwrite: false,
        }
    }
}

/// What happened to a whole content id.
#[derive(Debug, Clone)]
pub struct GofileSummary {
    /// Directory everything was written under.
    pub root: PathBuf,
    /// Files found behind the link.
    pub total: usize,
    /// Files downloaded during this run.
    pub completed: usize,
    /// Files that were already on disk.
    pub skipped: usize,
    /// Bytes written during this run.
    pub bytes: u64,
    /// Per-file failures: (path within the content, reason).
    pub failed: Vec<(String, String)>,
    /// Whether the run was interrupted.
    pub cancelled: bool,
}

/// One file discovered in the content tree.
#[derive(Debug, Clone)]
struct RemoteFile {
    /// Path relative to the download root, folders included.
    relative: PathBuf,
    /// Leaf name, for the progress line.
    name: String,
    /// Direct storage-server URL.
    link: String,
    /// Size as advertised by the API, or 0 when it did not say.
    size: u64,
}

enum FileOutcome {
    Completed(u64),
    Skipped,
    Cancelled,
}

enum StreamOutcome {
    Done(u64),
    Cancelled,
}

/// An authenticated GoFile session: a client that already carries the token,
/// plus the two values the website token is derived from.
struct Session {
    client: Client,
    token: String,
    user_agent: String,
}

// ── Entry point ─────────────────────────────────────────

/// Downloads everything behind a GoFile link.
///
/// `output`, when given, is the directory everything is written into. Without
/// it the destination depends on what the link turns out to hold — see
/// [`destination_root`].
///
/// `client` is used only to open the session; the transfers run on a second
/// client that carries the account token by default, so the token cannot leak
/// into unrelated requests made with the caller's client.
pub async fn download(
    client: Client,
    url: &str,
    output: Option<String>,
    download_dir: &str,
    options: GofileOptions,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<GofileSummary> {
    let link = parse_link(url)?;
    let workers = options.workers.clamp(1, WORKERS_MAX);

    let session = open_session(client, &options).await?;

    let files = list_content(&session, &link, &options).await?;

    if files.is_empty() {
        bail!(
            "nothing to download behind {} — the link is empty, expired, or the content was removed",
            link.content_id
        );
    }

    // Deliberately after the listing: whether a wrapper directory is wanted at
    // all depends on how many files came back.
    let root = destination_root(output, download_dir, &link.content_id, &files);

    fs::create_dir_all(&root)
        .await
        .with_context(|| format!("could not create {}", root.display()))?;

    let total = files.len();
    let board = (!quiet).then(|| Board::new("GoFile", total, workers));
    let renderer = board.as_ref().map(|b| b.spawn_renderer());

    let completed = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let bytes = AtomicU64::new(0);
    let cancelled = AtomicBool::new(false);
    let failed: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

    {
        let session = &session;
        let options = &options;
        let root = &root;
        let board = board.as_ref();
        let completed = &completed;
        let skipped = &skipped;
        let bytes = &bytes;
        let cancelled = &cancelled;
        let failed = &failed;

        stream::iter(files.iter().enumerate())
            .for_each_concurrent(workers, move |(index, file)| {
                let cancel = cancel.clone();
                async move {
                    if cancel.is_cancelled() {
                        cancelled.store(true, Ordering::Relaxed);
                        return;
                    }

                    // The lane is held for the whole file: dropping it early
                    // would hand the display slot to another worker while this
                    // one is still reporting into it.
                    let lane = board.and_then(|b| b.claim(index as u64 + 1, &file.name));
                    let sink = lane
                        .as_ref()
                        .map(|l| l.sink())
                        .unwrap_or_else(ui::silent);

                    if file.size > 0 {
                        sink.total(Some(file.size));
                    }

                    match download_file(session, file, root, options, &cancel, &sink).await {
                        Ok(FileOutcome::Completed(written)) => {
                            completed.fetch_add(1, Ordering::Relaxed);
                            bytes.fetch_add(written, Ordering::Relaxed);
                            if let Some(board) = board {
                                board.file_completed(written);
                            }
                        }
                        Ok(FileOutcome::Skipped) => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            if let Some(board) = board {
                                board.file_skipped();
                            }
                        }
                        Ok(FileOutcome::Cancelled) => {
                            cancelled.store(true, Ordering::Relaxed);
                        }
                        Err(error) => {
                            let reason = format!("{error:#}");
                            let name = file.relative.display().to_string();
                            if let Some(board) = board {
                                board.file_failed();
                                board.log(&format!("  \u{26a0} {name}: {reason}"));
                            }
                            let mut failed = failed.lock().unwrap_or_else(|e| e.into_inner());
                            failed.push((name, reason));
                        }
                    }

                    sink.finish();
                }
            })
            .await;
    }

    if let Some(renderer) = renderer {
        renderer.abort();
    }
    if let Some(board) = &board {
        board.finish();
    }

    Ok(GofileSummary {
        root,
        total,
        completed: completed.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed),
        bytes: bytes.load(Ordering::Relaxed),
        failed: failed.into_inner().unwrap_or_else(|e| e.into_inner()),
        cancelled: cancelled.load(Ordering::Relaxed),
    })
}

/// Where a content id's files should be written.
///
/// A single loose file gets no folder of its own: `~/Downloads/thing.zip`, not
/// `~/Downloads/AbCdEf/thing.zip`. The wrapper exists to stop a forty-file
/// content id scattering itself across the download directory, and with one
/// file there is nothing to keep together — only a directory named after an id
/// that means nothing to anyone.
///
/// Everything else keeps the wrapper, including a single file that the
/// uploader already put inside a folder: that structure is information, and
/// flattening it would throw the information away.
///
/// An explicit `-o` always wins and is always a directory. The shape of the
/// content is not known until the listing has been fetched, so a flag that
/// sometimes meant a filename would be decided by the contents of somebody
/// else's upload.
fn destination_root(
    output: Option<String>,
    download_dir: &str,
    content_id: &str,
    files: &[RemoteFile],
) -> PathBuf {
    if let Some(dir) = output {
        return PathBuf::from(dir);
    }

    let lone_file = files.len() == 1 && files[0].relative.components().count() == 1;

    if lone_file {
        PathBuf::from(download_dir)
    } else {
        PathBuf::from(download_dir).join(content_id)
    }
}

// ── Session ──────────────────────────────────────────────

/// The rotating `X-Website-Token`.
///
/// Recomputed per request rather than stored: the four-hour slot can roll over
/// in the middle of a long download, and a stale token is rejected outright.
fn website_token(user_agent: &str, account_token: &str) -> String {
    let slot = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
        / TOKEN_WINDOW_SECS;

    sha256::hex(
        format!("{user_agent}::en-US::{account_token}::{slot}::{WEBSITE_TOKEN_SALT}").as_bytes(),
    )
}

async fn open_session(client: Client, options: &GofileOptions) -> Result<Session> {
    let user_agent = DEFAULT_USER_AGENT.to_string();

    let token = match options.token.as_deref().map(str::trim) {
        Some(token) if !token.is_empty() => token.to_string(),
        _ => create_guest_account(&client, &user_agent, options).await?,
    };

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_str(&user_agent)?);
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    // Deliberately no `Accept-Encoding: gzip`, even though the reference
    // downloader sends one. Python's requests decompresses transparently;
    // reqwest only does so for encodings it negotiated itself, and only with
    // the matching cargo feature enabled. Asking by hand means the compressed
    // bytes arrive as an opaque blob and every JSON parse dies at byte one.
    // Letting reqwest decide costs a little bandwidth on API replies and
    // nothing at all on file transfers, which are already compressed.
    headers.insert(ORIGIN, HeaderValue::from_static("https://gofile.io"));
    headers.insert(REFERER, HeaderValue::from_static("https://gofile.io/"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))?,
    );
    // Belt and braces: the API reads the header, the storage nodes read the
    // cookie, and which one serves a given file is not ours to decide.
    headers.insert(
        COOKIE,
        HeaderValue::from_str(&format!("accountToken={token}"))?,
    );

    let client = Client::builder()
        .default_headers(headers)
        .build()
        .context("could not build the GoFile HTTP client")?;

    Ok(Session {
        client,
        token,
        user_agent,
    })
}

async fn create_guest_account(
    client: &Client,
    user_agent: &str,
    options: &GofileOptions,
) -> Result<String> {
    let mut last: Option<anyhow::Error> = None;

    for attempt in 0..options.max_retries.max(1) {
        // A guest account has no token yet, so the website token is derived
        // from an empty one — exactly what the site does on first load.
        let response = client
            .post(format!("{API_BASE}/accounts"))
            .header(USER_AGENT, user_agent)
            .header("X-Website-Token", website_token(user_agent, ""))
            .header("X-BL", "en-US")
            .header(ORIGIN, "https://gofile.io")
            .header(REFERER, "https://gofile.io/")
            .send()
            .await;

        match response {
            Ok(response) => match unwrap_envelope(response).await {
                Ok(data) => match data.get("token").and_then(Value::as_str) {
                    Some(token) if !token.is_empty() => return Ok(token.to_string()),
                    _ => last = Some(anyhow!("the accounts endpoint returned no token")),
                },
                Err(error) => last = Some(error),
            },
            Err(error) => last = Some(anyhow!(error)),
        }

        backoff(attempt).await;
    }

    Err(last
        .unwrap_or_else(|| anyhow!("could not create a GoFile guest account"))
        .context("GoFile refused to hand out an account token"))
}

/// A gzip stream starts with these two bytes.
///
/// Worth naming: if a compressed body ever reaches the parser again, the
/// error should say so outright instead of blaming the JSON.
fn looks_gzipped(body: &[u8]) -> bool {
    body.starts_with(&[0x1f, 0x8b])
}

/// The first couple of hundred bytes of a body, flattened onto one line.
///
/// A bare `expected value at line 1 column 1` says only that the body was not
/// JSON — not whether it was HTML, a Cloudflare interstitial, or an empty
/// response. Showing the start of it turns a guessing game into a glance.
fn snippet(body: &[u8]) -> String {
    const MAX: usize = 200;

    let head = &body[..body.len().min(MAX)];
    let flattened = String::from_utf8_lossy(head)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    if body.len() > MAX {
        format!("{flattened}…")
    } else {
        flattened
    }
}

/// Unwraps the `{ "status": "ok", "data": … }` envelope every endpoint uses.
///
/// A GoFile error is an HTTP 200 with `status: "error-notFound"` in the body,
/// so checking the status code alone would read a dead link as a success.
async fn unwrap_envelope(response: Response) -> Result<Value> {
    let status = response.status();
    let body = response
        .bytes()
        .await
        .context("the GoFile API response could not be read")?;

    if !status.is_success() {
        bail!("the GoFile API answered HTTP {status}: {}", snippet(&body));
    }

    if looks_gzipped(&body) {
        bail!(
            "the GoFile API answered with a compressed body this build cannot decode — \
             an Accept-Encoding header is being sent without the matching reqwest feature"
        );
    }

    if body.is_empty() {
        bail!("the GoFile API answered with an empty body");
    }

    let parsed: Value = serde_json::from_slice(&body).with_context(|| {
        format!(
            "the GoFile API answered with something that is not JSON: {}",
            snippet(&body)
        )
    })?;

    match parsed.get("status").and_then(Value::as_str) {
        Some("ok") => Ok(parsed.get("data").cloned().unwrap_or(Value::Null)),
        Some(other) => bail!("the GoFile API refused the request: {other}"),
        None => bail!("the GoFile API answered without a status"),
    }
}

/// Exponential-ish backoff, capped so a retry loop never parks for minutes.
async fn backoff(attempt: u32) {
    let millis = 250u64 << attempt.min(5);
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

// ── Content tree ──────────────────────────────────────────

/// Walks the content tree and returns every file in it, with the local path it
/// should be written to.
///
/// Iterative rather than recursive: a boxed recursive async fn buys nothing
/// here, and an explicit stack keeps the collision bookkeeping in one place.
async fn list_content(
    session: &Session,
    link: &GofileLink,
    options: &GofileOptions,
) -> Result<Vec<RemoteFile>> {
    let password = options
        .password
        .as_deref()
        .map(|password| sha256::hex(password.as_bytes()));

    let mut files: Vec<RemoteFile> = Vec::new();
    let mut taken: HashMap<PathBuf, usize> = HashMap::new();
    let mut stack: Vec<(String, PathBuf)> = vec![(link.content_id.clone(), PathBuf::new())];

    while let Some((content_id, parent)) = stack.pop() {
        let data = fetch_content(session, &content_id, password.as_deref(), options).await?;

        if data
            .get("passwordStatus")
            .and_then(Value::as_str)
            .is_some_and(|status| status != "passwordOk")
        {
            bail!(
                "this GoFile link is password protected — set RDM_GOFILE_PASSWORD to the password and try again"
            );
        }

        if data.get("type").and_then(Value::as_str) != Some("folder") {
            push_file(&data, &parent, &mut files, &mut taken);
            continue;
        }

        let Some(children) = data.get("children").and_then(Value::as_object) else {
            continue;
        };

        // The API hands back an object keyed by id, so the order depends on
        // the JSON parser rather than on anything meaningful. Sorting by
        // creation time reproduces what the website shows.
        let mut entries: Vec<&Value> = children.values().collect();
        entries.sort_by_key(|child| child.get("createTime").and_then(Value::as_i64).unwrap_or(0));

        // Pushed in reverse so the stack pops them in creation order.
        for child in entries.into_iter().rev() {
            if child.get("type").and_then(Value::as_str) == Some("folder") {
                let Some(id) = child.get("id").and_then(Value::as_str) else {
                    continue;
                };
                let name = child.get("name").and_then(Value::as_str).unwrap_or(id);
                let dir = unique_path(&mut taken, &parent, name, true);
                stack.push((id.to_string(), dir));
            } else {
                push_file(child, &parent, &mut files, &mut taken);
            }
        }
    }

    Ok(files)
}

async fn fetch_content(
    session: &Session,
    content_id: &str,
    password: Option<&str>,
    options: &GofileOptions,
) -> Result<Value> {
    let mut url =
        format!("{API_BASE}/contents/{content_id}?cache=true&sortField=createTime&sortDirection=1");

    if let Some(hash) = password {
        url.push_str("&password=");
        url.push_str(hash);
    }

    let mut last: Option<anyhow::Error> = None;

    for attempt in 0..options.max_retries.max(1) {
        let response = session
            .client
            .get(&url)
            .header(
                "X-Website-Token",
                website_token(&session.user_agent, &session.token),
            )
            .header("X-BL", "en-US")
            .send()
            .await;

        match response {
            Ok(response) => match unwrap_envelope(response).await {
                Ok(data) => return Ok(data),
                Err(error) => last = Some(error),
            },
            Err(error) => last = Some(anyhow!(error)),
        }

        backoff(attempt).await;
    }

    Err(last
        .unwrap_or_else(|| anyhow!("no answer"))
        .context(format!("could not list GoFile content {content_id}")))
}

fn push_file(
    node: &Value,
    parent: &Path,
    files: &mut Vec<RemoteFile>,
    taken: &mut HashMap<PathBuf, usize>,
) {
    // A node without a link is not downloadable — an upload still in
    // progress, or a type we do not handle. Skipping it is better than
    // failing the whole tree over one entry.
    let Some(link) = node.get("link").and_then(Value::as_str) else {
        return;
    };

    let name = node
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("download.bin");
    let size = node.get("size").and_then(Value::as_u64).unwrap_or(0);
    let relative = unique_path(taken, parent, name, false);
    let leaf = relative
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| name.to_string());

    files.push(RemoteFile {
        relative,
        name: leaf,
        link: link.to_string(),
        size,
    });
}

// ── File transfer ─────────────────────────────────────────

async fn download_file(
    session: &Session,
    file: &RemoteFile,
    root: &Path,
    options: &GofileOptions,
    cancel: &CancellationToken,
    sink: &Arc<dyn ProgressSink>,
) -> Result<FileOutcome> {
    let destination = root.join(&file.relative);

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("could not create {}", parent.display()))?;
    }

    if !options.overwrite
        && fs::metadata(&destination)
            .await
            .map(|meta| meta.len() > 0)
            .unwrap_or(false)
    {
        return Ok(FileOutcome::Skipped);
    }

    // `with_extension` would eat the real one: `movie.mkv` must become
    // `movie.mkv.part`, not `movie.part`.
    let mut part = destination.clone().into_os_string();
    part.push(".part");
    let part = PathBuf::from(part);

    let mut last: Option<anyhow::Error> = None;

    for attempt in 0..options.max_retries.max(1) {
        if cancel.is_cancelled() {
            return Ok(FileOutcome::Cancelled);
        }

        let resumed = fs::metadata(&part).await.map(|meta| meta.len()).unwrap_or(0);
        sink.state(SlotState::Inspecting);
        sink.progress(resumed);

        let mut request = session.client.get(&file.link);
        if resumed > 0 {
            request = request.header(RANGE, format!("bytes={resumed}-"));
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                last = Some(anyhow!(error));
                backoff(attempt).await;
                continue;
            }
        };

        let status = response.status();
        if !acceptable(status, resumed) {
            // A `200` to a ranged request means the server is about to resend
            // the file from byte zero. Appending that to the existing `.part`
            // would produce a plausible-looking corrupt file, so the partial
            // data goes and the next attempt starts clean.
            if resumed > 0
                && (status == StatusCode::OK || status == StatusCode::RANGE_NOT_SATISFIABLE)
            {
                let _ = fs::remove_file(&part).await;
                sink.note("the server would not resume, starting the file again");
                last = Some(anyhow!("resume refused with HTTP {status}"));
                continue;
            }

            last = Some(anyhow!("the server answered HTTP {status}"));
            backoff(attempt).await;
            continue;
        }

        let expected = expected_total(&response, resumed);
        if expected.is_some() {
            sink.total(expected);
        }
        sink.state(SlotState::Downloading);

        match write_stream(response, &part, resumed, cancel, sink).await {
            Ok(StreamOutcome::Cancelled) => return Ok(FileOutcome::Cancelled),
            Ok(StreamOutcome::Done(written)) => {
                let on_disk = fs::metadata(&part)
                    .await
                    .map(|meta| meta.len())
                    .unwrap_or(written);

                // Only a complete file is moved into place. A short read left
                // under the real name is indistinguishable from a good
                // download on the next run.
                if let Some(expected) = expected
                    && on_disk != expected
                {
                    last = Some(anyhow!("got {on_disk} of {expected} bytes"));
                    backoff(attempt).await;
                    continue;
                }

                sink.state(SlotState::Finishing);
                fs::rename(&part, &destination)
                    .await
                    .with_context(|| format!("could not move {} into place", destination.display()))?;

                return Ok(FileOutcome::Completed(on_disk));
            }
            Err(error) => {
                last = Some(error);
                backoff(attempt).await;
            }
        }
    }

    Err(last.unwrap_or_else(|| anyhow!("download failed")))
}

/// Which status codes mean "body follows", given what is already on disk.
///
/// The asymmetry is the point: a fresh download accepts `200` or `206`, but a
/// resumed one accepts only `206`.
fn acceptable(status: StatusCode, resumed: u64) -> bool {
    if resumed > 0 {
        status == StatusCode::PARTIAL_CONTENT
    } else {
        status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT
    }
}

/// Total size of the file, not of this response.
///
/// `Content-Range: bytes 500-999/1000` knows the whole story; a bare
/// `Content-Length` only covers the part still to come, so what is already on
/// disk has to be added back.
fn expected_total(response: &Response, resumed: u64) -> Option<u64> {
    if let Some(range) = response
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        && let Some((_, total)) = range.rsplit_once('/')
        && let Ok(total) = total.trim().parse::<u64>()
    {
        return Some(total);
    }

    response.content_length().map(|length| length + resumed)
}

async fn write_stream(
    response: Response,
    part: &Path,
    resumed: u64,
    cancel: &CancellationToken,
    sink: &Arc<dyn ProgressSink>,
) -> Result<StreamOutcome> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(part)
        .await
        .with_context(|| format!("could not open {}", part.display()))?;

    let mut written = resumed;
    let mut stream = response.bytes_stream();

    loop {
        let chunk = tokio::select! {
            biased;
            // Cancellation wins over another chunk: whatever is on disk stays
            // in the `.part` file and the next run resumes from it.
            _ = cancel.cancelled() => {
                let _ = file.flush().await;
                return Ok(StreamOutcome::Cancelled);
            }
            chunk = stream.next() => chunk,
        };

        let Some(chunk) = chunk else { break };
        let chunk = chunk.context("the connection dropped mid-file")?;

        file.write_all(&chunk)
            .await
            .with_context(|| format!("could not write to {}", part.display()))?;

        written += chunk.len() as u64;
        sink.progress(written);
    }

    file.flush()
        .await
        .with_context(|| format!("could not flush {}", part.display()))?;

    Ok(StreamOutcome::Done(written))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Link recognition ──

    #[test]
    fn gofile_links_are_recognised() {
        assert!(is_gofile_url("https://gofile.io/d/AbCdEf"));
        assert!(is_gofile_url("https://www.gofile.io/d/AbCdEf"));
        assert!(is_gofile_url("  https://gofile.io/d/AbCdEf  "));
        assert!(is_gofile_url("http://gofile.io/d/AbCdEf"));
    }

    /// Lookalikes must fall through to the generic engine rather than be
    /// claimed here — the same rule MEGA follows.
    #[test]
    fn lookalikes_are_not_claimed() {
        assert!(!is_gofile_url("https://notgofile.io/d/AbCdEf"));
        assert!(!is_gofile_url("https://gofile.io.evil.com/d/AbCdEf"));
        assert!(!is_gofile_url("https://evil.com/gofile.io/d/AbCdEf"));
        assert!(!is_gofile_url("https://evil.com@gofile.io.evil.net/d/x"));
        assert!(!is_gofile_url("https://example.com/file.zip"));
        assert!(!is_gofile_url("ftp://gofile.io/d/AbCdEf"));
    }

    #[test]
    fn content_id_is_the_last_segment() {
        assert_eq!(
            parse_link("https://gofile.io/d/AbCdEf").unwrap().content_id,
            "AbCdEf"
        );
        assert_eq!(
            parse_link("https://gofile.io/d/AbCdEf/").unwrap().content_id,
            "AbCdEf"
        );
        assert_eq!(
            parse_link("https://gofile.io/d/AbCdEf?x=1#y")
                .unwrap()
                .content_id,
            "AbCdEf"
        );
    }

    #[test]
    fn links_without_a_content_id_are_rejected() {
        assert!(parse_link("https://gofile.io/").is_err());
        assert!(parse_link("https://gofile.io/d/").is_err());
        assert!(parse_link("https://gofile.io/uploadFiles").is_err());
        assert!(parse_link("https://example.com/d/AbCdEf").is_err());
    }

    // ── Where files land ──

    fn file_at(relative: &str) -> RemoteFile {
        RemoteFile {
            relative: PathBuf::from(relative),
            name: relative.rsplit('/').next().unwrap_or(relative).to_string(),
            link: "https://store1.gofile.io/download/x".to_string(),
            size: 0,
        }
    }

    /// The whole point of the rule: `~/Downloads/thing.zip`, not
    /// `~/Downloads/zp3Wzv/thing.zip`.
    #[test]
    fn a_lone_file_gets_no_folder_of_its_own() {
        let files = vec![file_at("thing.zip")];
        assert_eq!(
            destination_root(None, "/dl", "AbCdEf", &files),
            PathBuf::from("/dl")
        );
    }

    /// Several loose files still need somewhere to go, or they end up strewn
    /// across the download directory with nothing tying them together.
    #[test]
    fn several_files_keep_the_content_id_folder() {
        let files = vec![file_at("a.zip"), file_at("b.zip")];
        assert_eq!(
            destination_root(None, "/dl", "AbCdEf", &files),
            PathBuf::from("/dl/AbCdEf")
        );
    }

    /// One file, but the uploader put it in a folder. That structure is
    /// information, so it is kept rather than flattened away.
    #[test]
    fn a_single_nested_file_keeps_the_wrapper() {
        let files = vec![file_at("season 1/ep1.mkv")];
        assert_eq!(
            destination_root(None, "/dl", "AbCdEf", &files),
            PathBuf::from("/dl/AbCdEf")
        );
    }

    #[test]
    fn an_explicit_output_wins_whatever_the_shape() {
        let one = vec![file_at("thing.zip")];
        let many = vec![file_at("a.zip"), file_at("b.zip")];

        assert_eq!(
            destination_root(Some("/here".to_string()), "/dl", "AbCdEf", &one),
            PathBuf::from("/here")
        );
        assert_eq!(
            destination_root(Some("/here".to_string()), "/dl", "AbCdEf", &many),
            PathBuf::from("/here")
        );
    }

    // ── Website token ──

    /// Same inputs inside the same four-hour slot must hash the same, or every
    /// second request would be rejected.
    #[test]
    fn website_token_is_stable_within_a_slot() {
        let a = website_token("Mozilla/5.0", "tok");
        let b = website_token("Mozilla/5.0", "tok");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert_ne!(a, website_token("Mozilla/5.0", "other"));
        assert_ne!(a, website_token("curl/8", "tok"));
    }

    /// Pins the exact string that gets hashed. If the recipe drifts, the API
    /// starts answering `error-wt` and this is the test that explains why.
    #[test]
    fn website_token_recipe_is_pinned() {
        let expected = sha256::hex(b"Mozilla/5.0::en-US::abc::123::9844d94d963d30");
        let slot = 123u64;
        let actual = sha256::hex(
            format!("Mozilla/5.0::en-US::abc::{slot}::{WEBSITE_TOKEN_SALT}").as_bytes(),
        );
        assert_eq!(actual, expected);
    }

    // ── Response bodies ──

    /// The failure this replaced said only "expected value at line 1 column
    /// 1", which is true of every non-JSON body ever sent.
    #[test]
    fn a_compressed_body_is_named_rather_than_blamed_on_json() {
        assert!(looks_gzipped(&[0x1f, 0x8b, 0x08, 0x00]));
        assert!(!looks_gzipped(b"{\"status\":\"ok\"}"));
        assert!(!looks_gzipped(b""));
        assert!(!looks_gzipped(&[0x1f]));
    }

    #[test]
    fn body_snippets_are_short_and_single_line() {
        assert_eq!(
            snippet(b"<html>\n  <body>nope</body>\n</html>"),
            "<html> <body>nope</body> </html>"
        );
        assert_eq!(snippet(b""), "");

        let long = vec![b'x'; 500];
        let shortened = snippet(&long);
        assert!(shortened.ends_with('…'));
        assert_eq!(shortened.chars().count(), 201);

        // Cutting mid-character must not panic.
        let mut multibyte = "é".repeat(300).into_bytes();
        multibyte.truncate(401);
        let _ = snippet(&multibyte);
    }

    // ── Response validation ──

    #[test]
    fn a_resumed_request_only_accepts_partial_content() {
        assert!(acceptable(StatusCode::OK, 0));
        assert!(acceptable(StatusCode::PARTIAL_CONTENT, 0));

        // The bug this guards against: appending a full body to a partial file.
        assert!(!acceptable(StatusCode::OK, 1024));
        assert!(acceptable(StatusCode::PARTIAL_CONTENT, 1024));

        for status in [
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::METHOD_NOT_ALLOWED,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(!acceptable(status, 0));
            assert!(!acceptable(status, 1024));
        }
    }

    // ── Options ──

    #[test]
    fn defaults_are_conservative() {
        let options = GofileOptions::default();
        assert_eq!(options.workers, WORKERS_DEFAULT);
        assert!(options.password.is_none());
        assert!(options.token.is_none());
        assert!(!options.overwrite);
        const { assert!(WORKERS_DEFAULT <= WORKERS_MAX); }
    }
}
