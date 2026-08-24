//! pixeldrain (pixeldrain.com) support.
//!
//! pixeldrain is the friendliest host in here by some distance. The API is
//! documented, reading it needs no credential, and `/api/file/<id>` is an
//! ordinary ranged HTTPS URL that keeps working: no signature, no expiry, no
//! redeem step. So this module does very little itself. It turns a link into a
//! name and a URL, and [`crate::engine`] does the transfer — the same trade
//! Dropbox and OneDrive make.
//!
//! What it cannot do is hand the link over untouched. `pixeldrain.com/u/<id>`
//! is a page for a viewer, so the generic engine would save an HTML document
//! under a plausible filename rather than failing anywhere the user would
//! notice.
//!
//! ## Shape of a link
//!
//! Two shapes, and unlike every other host here the link says which it is:
//!
//!   * `/u/<id>` is one file.
//!   * `/l/<id>` is a list — pixeldrain's word for an album — which expands
//!     into a flat set of files.
//!
//! That is why [`crate::hoster::Kind::link_kind`] can give a real answer for
//! pixeldrain, where for GoFile it has to assume and for OneDrive it has to
//! defer to the API.
//!
//! The `/api/file/<id>` and `/api/list/<id>` forms are accepted too, because
//! anyone who reads the API documentation will eventually paste one.
//!
//! ## The API key
//!
//! Optional, and it buys speed rather than access: pixeldrain caps anonymous
//! transfers and lifts the cap for an account. See [`api::build_client`] for
//! where it goes and why it goes there rather than into the URL.
//!
//! ## What this deliberately does not do
//!
//! No throughput sampling. The reference downloader watches speed over a
//! rolling window, abandons a download it judges too slow, and starts it again
//! with the API key. That is a lot of machinery to infer something the API
//! states outright: `download_speed_limit` says whether this file is capped
//! before a single byte moves. So a capped file with no key configured gets a
//! line saying so (see [`speed_limit_note`]) and then downloads at whatever
//! speed it downloads at, rather than being fetched, judged and thrown away.
//!
//! No integrity check, yet. `/info` publishes `hash_sha256`, and this crate
//! already contains a SHA-256, but verifying it means reading the finished
//! file back off disk and the engine has no hook for that. A digest checked by
//! a second full read of a 40 GB file is a different feature from one checked
//! as the bytes go past, and it deserves to be built as that one.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, stream};
use reqwest::Client;
use serde::Deserialize;
use tokio::fs;
use tokio_util::sync::CancellationToken;

use crate::engine::{self, DownloadRequest, ExistingPolicy, Outcome};
use crate::ui::{self, Board, ProgressSink};

mod api;
mod naming;

#[cfg(test)]
mod tests;

/// Where the API lives. Every request this module makes is a GET against it.
const API_BASE: &str = "https://pixeldrain.com/api";

/// Hosts a pixeldrain link arrives on.
///
/// Only the two this module has actually been written against. A mirror or a
/// vanity domain that turns out to serve something subtly different is worse
/// than one that falls through to the generic engine, which at least fails
/// where somebody can see it.
const HOSTS: [&str; 2] = ["pixeldrain.com", "www.pixeldrain.com"];

/// Files of a list to download at once when nothing else says otherwise.
///
/// Deliberately modest. An anonymous transfer allowance is shared across
/// connections, so extra workers mostly divide the same bandwidth; they earn
/// their keep on a list of many small files, where round trips rather than
/// throughput are the cost.
pub const WORKERS_DEFAULT: usize = 4;

/// Upper bound on files in flight.
const WORKERS_MAX: usize = 10;

// ── Link handling ─────────────────────────────────────────

/// Host of an http(s) URL, lower-cased.
fn host_of(url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(url.trim()).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    Some(parsed.host_str()?.trim_end_matches('.').to_ascii_lowercase())
}

/// Is this a pixeldrain link?
///
/// Host equality against a parsed URL, so `notpixeldrain.com`,
/// `pixeldrain.com.evil.com` and `https://evil.com@pixeldrain.com.evil.net/u/x`
/// all belong to somebody else.
pub fn is_pixeldrain_url(url: &str) -> bool {
    host_of(url).is_some_and(|host| HOSTS.contains(&host.as_str()))
}

/// What a pixeldrain link addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
    /// One file: `/u/<id>`, or `/api/file/<id>`.
    File(String),
    /// A list of files: `/l/<id>`, or `/api/list/<id>`.
    List(String),
}

/// Reads a link as a file id or a list id.
///
/// Purely syntactic — no network access — which is what lets
/// [`crate::hoster::Kind::link_kind`] answer for pixeldrain while arguments
/// are still being parsed.
pub fn parse_link(url: &str) -> Result<Link> {
    // The host check belongs here and not only in the callers: everything
    // below is interpolated into an API path, and an id lifted from some other
    // site's URL has no business being asked about.
    if !is_pixeldrain_url(url) {
        bail!("not a pixeldrain link: {}", url.trim());
    }

    let parsed =
        reqwest::Url::parse(url.trim()).context("the link could not be parsed as a URL")?;
    let mut segments = parsed
        .path_segments()
        .into_iter()
        .flatten()
        .filter(|segment| !segment.is_empty());

    match (segments.next(), segments.next(), segments.next()) {
        (Some("u"), Some(id), None) => Ok(Link::File(checked_id(id)?)),
        (Some("l"), Some(id), None) => Ok(Link::List(checked_id(id)?)),
        // The API forms. A fourth segment is ignored rather than rejected on a
        // file, because `/api/file/<id>/info` and `/api/file/<id>/thumbnail`
        // both name the same file.
        (Some("api"), Some("file"), Some(id)) => Ok(Link::File(checked_id(id)?)),
        (Some("api"), Some("list"), Some(id)) => Ok(Link::List(checked_id(id)?)),
        _ => bail!(
            "a pixeldrain link is /u/<id> for a file or /l/<id> for a list, and '{}' is neither",
            parsed.path()
        ),
    }
}

/// Accepts a file or list id, and nothing that could mean something else.
///
/// Ids go straight into an API path, so this is the only thing standing
/// between a pasted link and a request somewhere else entirely. pixeldrain ids
/// are short base64url strings, so anything outside that alphabet is not one.
///
/// The comma gets its own message because it is the one plausible mistake:
/// `/info` reads `a,b` as a request for two files and answers with an array,
/// which is a shape nothing here expects, and "not a pixeldrain id" would be a
/// poor account of that.
fn checked_id(id: &str) -> Result<String> {
    const MAX: usize = 64;

    if id.contains(',') {
        bail!(
            "'{id}' names several files at once, which is not supported — pass them one at a time"
        );
    }

    if id.is_empty()
        || id.len() > MAX
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!("'{id}' is not a pixeldrain id");
    }

    Ok(id.to_owned())
}

/// Does this link address a list rather than a single file?
///
/// Infallible on purpose, for callers that only want to route: a link this
/// cannot parse is not a list, and [`resolve`] is where it gets to say why.
pub fn is_list_link(url: &str) -> bool {
    matches!(parse_link(url), Ok(Link::List(_)))
}

/// The fetchable URL for a file id.
///
/// `?download` asks for an attachment instead of an inline render, which makes
/// pixeldrain send a `Content-Disposition` naming the file. The engine reads
/// that, so even a file whose name had to fall back to its id still lands
/// under its real one.
fn download_url(id: &str) -> String {
    format!("{API_BASE}/file/{id}?download")
}

/// The fetchable URL for a file link, without asking the API anything.
///
/// This is what makes a pixeldrain file queueable where a OneDrive share is
/// not: the result carries no signature and no expiry, so it is still valid an
/// hour later at the front of the queue, and the response names the file
/// itself, so there is no filename to pin either.
///
/// A list has no single URL, and is refused here rather than quietly reduced
/// to one of its files.
pub fn direct_url(url: &str) -> Result<String> {
    match parse_link(url)? {
        Link::File(id) => Ok(download_url(&id)),
        Link::List(_) => bail!(
            "a pixeldrain list has no single download URL — run `rdm <list link>` to download all of it"
        ),
    }
}

// ── API shapes ─────────────────────────────────────────────

/// The fields of a file this module uses, from `/file/<id>/info` or from the
/// `files` array of a list — pixeldrain uses one shape for both.
///
/// Every one of them is optional. pixeldrain does not promise partial entries,
/// but a list that fails to parse because one of forty files is odd is a worse
/// outcome than a list that skips it and says so.
///
/// `success` is not among them: it rides on every response, and on a failure
/// the status code carries the same news less ambiguously.
#[derive(Debug, Deserialize)]
struct FileInfo {
    id: Option<String>,
    name: Option<String>,
    size: Option<u64>,
    /// Empty when the file downloads normally. Otherwise it holds a download
    /// error code and `availability_message` explains it in a sentence.
    #[serde(default)]
    availability: String,
    #[serde(default)]
    availability_message: String,
    /// Cap in bytes per second, `0` meaning none.
    #[serde(default)]
    download_speed_limit: u64,
}

/// A list and the files in it.
///
/// pixeldrain lists are flat and arrive whole, so unlike a OneDrive folder
/// there is no tree to walk and no continuation token to follow. One request
/// is the entire listing, names and sizes included.
#[derive(Debug, Deserialize)]
struct ListInfo {
    title: Option<String>,
    #[serde(default)]
    files: Vec<FileInfo>,
}

// ── Options and results ─────────────────────────────────────

/// Knobs for a pixeldrain download.
///
/// No `Debug`: `api_key` is a credential, and a type that can print one has a
/// way of ending up inside an error message.
#[derive(Clone)]
pub struct PixeldrainOptions {
    /// How many files of a list to download at once.
    pub workers: usize,
    /// Attempts per API call before giving up. Per-file retries belong to the
    /// engine, which is the thing doing the transfer.
    pub max_retries: u32,
    /// An account API key. `None` means anonymous, which works, and is slower.
    pub api_key: Option<String>,
    /// Re-download files that are already on disk.
    pub overwrite: bool,
}

impl Default for PixeldrainOptions {
    fn default() -> Self {
        Self {
            workers: WORKERS_DEFAULT,
            max_retries: 5,
            api_key: None,
            overwrite: false,
        }
    }
}

/// What happened to a list.
#[derive(Debug, Clone)]
pub struct PixeldrainSummary {
    /// Directory everything was written under.
    pub root: PathBuf,
    /// Files found in the list.
    pub total: usize,
    /// Files downloaded during this run.
    pub completed: usize,
    /// Files that were already on disk.
    pub skipped: usize,
    /// Entries the API described without an id, so there was nothing to fetch.
    /// Counted rather than dropped silently, because a caller about to report
    /// "12 of 12" needs to know its picture of the list is incomplete.
    pub skipped_entries: usize,
    /// Bytes written during this run.
    pub bytes: u64,
    /// Per-file failures: (name, reason).
    pub failed: Vec<(String, String)>,
    /// Whether the run was interrupted.
    pub cancelled: bool,
}

/// A link, once the API has said what is behind it.
pub enum Resolved {
    /// One file, at a URL the engine can fetch as it stands.
    File(FileLink),
    /// A list, and everything in it.
    List(ListDownload),
}

/// A single pixeldrain file.
///
/// No `Debug`: `client` holds the API key in its default headers.
pub struct FileLink {
    /// Ranged HTTPS URL, valid for as long as the file exists.
    pub url: String,
    /// The name pixeldrain has for it, reduced to one safe path component.
    pub name: String,
    /// This file's own cap in bytes per second, `0` for none. See
    /// [`speed_limit_note`].
    pub speed_limit: u64,
    /// The client the download has to go out over, because the API key lives
    /// in it rather than in `url`.
    pub client: Client,
}

/// A list, ready to download.
///
/// No `Debug`, for the same reason as [`FileLink`].
pub struct ListDownload {
    client: Client,
    title: Option<String>,
    files: Vec<RemoteFile>,
    skipped: usize,
}

impl ListDownload {
    /// The list's own title, when it had one.
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Every file in the list, in the order pixeldrain returned them.
    pub fn files(&self) -> &[RemoteFile] {
        &self.files
    }

    /// Entries with no id behind them. See
    /// [`PixeldrainSummary::skipped_entries`].
    pub fn skipped(&self) -> usize {
        self.skipped
    }
}

/// One file of a list.
#[derive(Debug, Clone)]
pub struct RemoteFile {
    /// pixeldrain's id for it, which is all a download URL needs.
    pub id: String,
    /// The name it will be saved under: already one safe path component, and
    /// already made unique within the list.
    pub name: String,
    /// Bytes, as the listing reported them. `None` only matters to a caller
    /// comparing against a local copy.
    pub size: Option<u64>,
}

// ── Entry points ──────────────────────────────────────────

/// Asks the API what a link points at.
///
/// One GET either way. A file needs its name, which only `/info` knows; a list
/// needs its contents, which arrive whole in one response.
pub async fn resolve(url: &str, options: &PixeldrainOptions) -> Result<Resolved> {
    let link = parse_link(url)?;
    let client = api::build_client(options.api_key.as_deref())?;

    match link {
        Link::File(id) => {
            let info: FileInfo = api::fetch_json(
                &client,
                options.max_retries,
                "could not look up the pixeldrain file",
                &format!("{API_BASE}/file/{id}/info"),
            )
            .await?;

            refuse_if_unavailable(&info)?;

            Ok(Resolved::File(FileLink {
                url: download_url(&id),
                name: naming::choose(info.name.as_deref(), &id),
                speed_limit: info.download_speed_limit,
                client,
            }))
        }

        Link::List(id) => {
            let info: ListInfo = api::fetch_json(
                &client,
                options.max_retries,
                "could not look up the pixeldrain list",
                &format!("{API_BASE}/list/{id}"),
            )
            .await?;

            let mut taken: HashSet<String> = HashSet::new();
            let mut files = Vec::with_capacity(info.files.len());
            let mut skipped = 0usize;

            for entry in &info.files {
                // No id means no URL, so there is nothing to fetch. Skipped
                // rather than fatal: one odd entry is no reason to abandon the
                // other forty.
                let Some(id) = entry.id.as_deref() else {
                    skipped += 1;
                    continue;
                };

                // An entry that is present but unavailable is kept rather than
                // refused here. It will fail in the engine and be reported as
                // one line of the summary, which is the same bargain every
                // folder walk in this crate makes, and better than abandoning
                // thirty-nine good files over one blocked one.
                files.push(RemoteFile {
                    id: id.to_owned(),
                    name: naming::unique(&mut taken, naming::choose(entry.name.as_deref(), id)),
                    size: entry.size,
                });
            }

            Ok(Resolved::List(ListDownload {
                client,
                title: info.title.as_deref().and_then(naming::safe_component),
                files,
                skipped,
            }))
        }
    }
}

/// Refuses a file pixeldrain has already said it will not serve.
///
/// The one failure that arrives as HTTP 200: `availability` holds a download
/// error code — an abuse block, or the file's bandwidth share being spent —
/// and `availability_message` explains it. Checking it here means the refusal
/// names the reason, instead of the engine reporting a bare 403 from a URL it
/// was told was fine.
fn refuse_if_unavailable(info: &FileInfo) -> Result<()> {
    let code = info.availability.trim();
    if code.is_empty() {
        return Ok(());
    }

    let message = info.availability_message.trim();
    if message.is_empty() {
        bail!("pixeldrain will not serve this file: {code}");
    }

    bail!("pixeldrain will not serve this file: {message} ({code})");
}

/// Downloads everything in a list.
///
/// `output`, when given, is the directory the files are written into; without
/// it the list keeps its own title under `download_dir`.
pub async fn download_list(
    list: ListDownload,
    output: Option<String>,
    download_dir: &str,
    options: PixeldrainOptions,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<PixeldrainSummary> {
    let ListDownload {
        client,
        title,
        files,
        skipped,
    } = list;

    let root = destination_root(output, download_dir, title.as_deref());
    fs::create_dir_all(&root)
        .await
        .with_context(|| format!("could not create {}", root.display()))?;

    let mut summary = download_files(&client, &files, &root, &options, cancel, quiet).await?;
    summary.skipped_entries = skipped;
    Ok(summary)
}

/// Downloads an explicit list of files under `root`.
///
/// Every file goes through [`crate::engine`], which is where `.part` files,
/// ranged resume, retries and the already-downloaded check already live. This
/// function only decides what to fetch, where to put it, and how many at once.
pub async fn download_files(
    client: &Client,
    files: &[RemoteFile],
    root: &Path,
    options: &PixeldrainOptions,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<PixeldrainSummary> {
    let total = files.len();

    if files.is_empty() {
        // An empty list is a result, not a failure: the directory is there and
        // there is nothing to put in it.
        return Ok(PixeldrainSummary {
            root: root.to_path_buf(),
            total: 0,
            completed: 0,
            skipped: 0,
            skipped_entries: 0,
            bytes: 0,
            failed: Vec::new(),
            cancelled: false,
        });
    }

    let workers = options.workers.clamp(1, WORKERS_MAX);
    let board = (!quiet).then(|| Board::new("pixeldrain", total, workers));
    let renderer = board.as_ref().map(|board| board.spawn_renderer());

    let completed = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let bytes = AtomicU64::new(0);
    let cancelled = AtomicBool::new(false);
    let failed: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

    {
        let board = board.as_ref();
        let completed = &completed;
        let skipped = &skipped;
        let bytes = &bytes;
        let cancelled = &cancelled;
        let failed = &failed;
        let overwrite = options.overwrite;

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
                    let lane = board.and_then(|board| board.claim(index as u64 + 1, &file.name));
                    let sink: Arc<dyn ProgressSink> = match lane.as_ref() {
                        Some(lane) => lane.sink(),
                        None => ui::silent(),
                    };

                    let destination = root.join(&file.name);
                    let request = DownloadRequest::new(
                        download_url(&file.id),
                        Some(destination.to_string_lossy().into_owned()),
                        // One connection per file. The parallelism here is
                        // files at once, and multiplying the two would point
                        // workers × chunks sockets at one host.
                        1,
                    )
                    // The key lives in the client, and a list of forty files
                    // is where it matters most.
                    .with_client(client.clone())
                    .with_policy(if overwrite {
                        ExistingPolicy::Overwrite
                    } else {
                        // Never `Ask`: a list of four hundred files must not
                        // stop on a prompt hidden behind the progress board.
                        ExistingPolicy::Reuse
                    });

                    match engine::download(request, cancel, Arc::clone(&sink)).await {
                        Ok(Outcome::Completed { bytes: written, .. }) => {
                            completed.fetch_add(1, Ordering::Relaxed);
                            bytes.fetch_add(written, Ordering::Relaxed);
                            if let Some(board) = board {
                                board.file_completed(written);
                            }
                        }
                        Ok(Outcome::AlreadyPresent { .. }) => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            if let Some(board) = board {
                                board.file_skipped();
                            }
                        }
                        Ok(Outcome::Cancelled) => {
                            cancelled.store(true, Ordering::Relaxed);
                        }
                        Err(error) => {
                            let reason = format!("{error:#}");
                            if let Some(board) = board {
                                board.file_failed();
                                board.log(&format!("  \u{26a0} {}: {reason}", file.name));
                            }
                            failed
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .push((file.name.clone(), reason));
                            // The engine closes the sink on every outcome it
                            // returns; an error is the one way out that leaves
                            // the lane still drawing.
                            sink.finish();
                        }
                    }
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

    Ok(PixeldrainSummary {
        root: root.to_path_buf(),
        total,
        completed: completed.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed),
        // Filled in by `download_list`, which is the caller that knows.
        skipped_entries: 0,
        bytes: bytes.load(Ordering::Relaxed),
        failed: failed
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        cancelled: cancelled.load(Ordering::Relaxed),
    })
}

/// Where a list's files land.
///
/// An explicit `-o` wins and is always a directory: a list is many files, and
/// there is no single file for a filename to name. Otherwise the list keeps
/// its own title, which is the one thing about it that means anything to the
/// person who was sent the link.
pub fn destination_root(
    output: Option<String>,
    download_dir: &str,
    title: Option<&str>,
) -> PathBuf {
    if let Some(dir) = output {
        return PathBuf::from(dir);
    }

    let label = title
        .and_then(naming::safe_component)
        .unwrap_or_else(|| "pixeldrain".to_owned());

    PathBuf::from(download_dir).join(label)
}

/// A line worth printing when pixeldrain has capped this file.
///
/// `download_speed_limit` is the API stating the cap up front, which is why
/// this module does not do what the reference downloader does — sample
/// throughput, give up, and start again with the key. There is nothing to
/// infer here, so there is nothing to throw away.
///
/// Says nothing when a key is already configured: the cap is then either
/// already lifted or not something the user can act on.
pub fn speed_limit_note(limit: u64, has_api_key: bool) -> Option<String> {
    if limit == 0 || has_api_key {
        return None;
    }

    Some(format!(
        "pixeldrain is capping this file at {}/s — set pixeldrain_api_key in the config, or \
         RDM_PIXELDRAIN_API_KEY in the environment, to download at your account's speed",
        ui::format_size(limit)
    ))
}
