//! OneDrive (1drv.ms, onedrive.live.com) support.
//!
//! A OneDrive share link is not a fetchable address, and it fails in a nastier
//! way than a MEGA or GoFile one: it *is* a real page, so handing it to the
//! generic engine saves an HTML preview under a plausible filename rather than
//! failing outright. Only the API knows what is behind it.
//!
//! ## What the API wants
//!
//! Three things, all of them what the web player itself does:
//!
//!   1. **A badger token.** `POST https://api-badgerp.svc.ms/v1.0/token` with
//!      a known app id hands one back. No account, no consent screen: it is
//!      the anonymous credential a share link is meant to be opened with, and
//!      it rides along as `Authorization: Badger …`.
//!   2. **The link, encoded.** A share is addressed as `u!` followed by the
//!      link in unpadded base64url — Microsoft's own "encoding sharing URLs"
//!      recipe.
//!   3. **A redeem.** `Prefer: autoredeem` on a POST to `/shares/u!…/driveitem`
//!      accepts the share on that token's behalf, which is what makes the
//!      `/drives/…/items/…` calls afterwards work at all.
//!
//! ## Shape of a download
//!
//! The root item answers the one question the link cannot: an item with a
//! `@content.downloadUrl` is a single file, and anything else is a folder.
//!
//! A file is the easy half. That download URL is an ordinary ranged HTTPS URL
//! with its authorisation baked in, so this module hands it straight back to
//! [`crate::engine`] and gets chunking, resume, retries and a progress bar for
//! free — the same trade Dropbox makes.
//!
//! A folder is walked one folder at a time through `children`, following
//! `@odata.nextLink` while the pages last, and every file in it goes through
//! the engine too, several at a time. So `-c` means chunks-per-file for a file
//! share and files-in-flight for a folder share, the way it already means
//! workers for MEGA and GoFile.
//!
//! ## What this deliberately does not do
//!
//! No integrity check. The API can hand over a `quickXorHash`, but nothing in
//! this crate can compute one, and a digest that cannot be recomputed is
//! decoration.
//!
//! No refreshing of a download URL mid-run. They are signed and they do
//! expire, which only becomes a real risk on a folder big enough that the walk
//! outlives the signature. When it happens that one file fails, the rest of
//! the run carries on, and running the same command again picks up what is
//! missing — finished files are skipped and half-finished ones resume.
//!
//! No password-protected or people-specific shares. A badger token is
//! anonymous, so those come back as a refusal, and [`explain`] says so in as
//! many words instead of printing a bare `401`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::{StreamExt, stream};
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::fs;
use tokio_util::sync::CancellationToken;

use crate::engine::{self, DownloadRequest, ExistingPolicy, Outcome};
use crate::ui::{self, Board};

#[cfg(test)]
mod tests;

/// Where personal shares are served from.
const API_BASE: &str = "https://my.microsoftpersonalcontent.com/_api/v2.0";

/// Where the anonymous token comes from.
const BADGER_ENDPOINT: &str = "https://api-badgerp.svc.ms/v1.0/token";

/// The app id the token is requested under. Not a secret and not ours: the
/// endpoint only issues tokens for app ids it recognises, and this is the one
/// the web player uses.
const BADGER_APP_ID: &str = "5cbed6ac-a083-4e14-b191-b4ba07653de2";

/// Hosts a personal OneDrive share arrives on.
const HOSTS: [&str; 3] = ["1drv.ms", "onedrive.live.com", "www.onedrive.live.com"];

/// Files in flight when nothing else says otherwise.
pub const WORKERS_DEFAULT: usize = 5;

/// Upper bound on files in flight, matching the reference downloader's cap.
const WORKERS_MAX: usize = 15;

// ── Link handling ─────────────────────────────────────────

/// Host of an http(s) URL, lower-cased.
fn host_of(url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(url.trim()).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    Some(parsed.host_str()?.trim_end_matches('.').to_ascii_lowercase())
}

/// Is this a OneDrive share link?
///
/// Host equality against a parsed URL, so `1drv.ms.evil.com`, `not1drv.ms` and
/// `https://evil.com@1drv.ms.evil.net/x` all belong to somebody else.
///
/// SharePoint and OneDrive for Business shares (`*.sharepoint.com`) are
/// deliberately left unclaimed. They authenticate against a tenant rather than
/// against an anonymous token, so claiming them would swap a download that
/// sometimes works for a failure that always does.
pub fn is_onedrive_url(url: &str) -> bool {
    host_of(url).is_some_and(|host| HOSTS.contains(&host.as_str()))
}

/// The `u!` share id for a link.
///
/// The link is encoded as it was given, trimmed but never rewritten: the id has
/// to describe the share somebody actually handed out, and a normalised path or
/// a reordered query is a different string and therefore a different share.
fn share_id(url: &str) -> String {
    format!("u!{}", URL_SAFE_NO_PAD.encode(url.trim()))
}

/// The listing URL for a folder.
///
/// An item id carries its drive in front of the `!`, and the API wants both.
fn children_url(item_id: &str) -> String {
    let drive_id = item_id.split('!').next().unwrap_or(item_id);
    format!(
        "{API_BASE}/drives/{drive_id}/items/{item_id}?select=children&expand=children(select=name,@content.downloadUrl,id)"
    )
}

// ── API shapes ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct BadgerToken {
    token: String,
}

/// The fields of a driveItem this module asks for.
#[derive(Debug, Deserialize)]
struct DriveItem {
    id: Option<String>,
    name: Option<String>,
    /// Present on a file, absent on a folder — which is the whole file/folder
    /// test. Asking for the `folder` facet as well would be a second way of
    /// learning the same thing, and two answers can disagree.
    #[serde(rename = "@content.downloadUrl")]
    download_url: Option<String>,
}

/// One page of a folder's children.
///
/// The first response nests them under `children`, because they arrive as an
/// expanded property of the folder itself. Following a next link asks for that
/// property directly, so the items come back as a plain collection under
/// `value` and the continuation moves from `children@odata.nextLink` to
/// `@odata.nextLink`. Accepting both shapes is what makes paging work past the
/// first page: the reference downloader greps for `"children":` and quietly
/// stops at the end of page one.
#[derive(Debug, Deserialize)]
struct ChildrenPage {
    #[serde(default)]
    children: Vec<DriveItem>,
    #[serde(default)]
    value: Vec<DriveItem>,
    #[serde(rename = "children@odata.nextLink")]
    children_next: Option<String>,
    #[serde(rename = "@odata.nextLink")]
    next: Option<String>,
}

impl ChildrenPage {
    /// The items on this page, wherever the API chose to put them.
    fn items(&self) -> impl Iterator<Item = &DriveItem> {
        self.children.iter().chain(&self.value)
    }

    fn next_link(&self) -> Option<&str> {
        self.children_next.as_deref().or(self.next.as_deref())
    }
}

/// What a child of a folder turned out to be.
enum Child<'a> {
    File { id: &'a str, url: &'a str },
    Folder { id: &'a str },
}

/// Reads a child as one or the other.
///
/// A download URL means a file. Without one, an id means a folder to walk into.
/// An entry with neither is skipped rather than failing the whole tree — an
/// upload still in progress is no reason to abandon the other forty files, and
/// the reference downloader skips them too.
fn classify(item: &DriveItem) -> Option<Child<'_>> {
    if let Some(url) = item.download_url.as_deref() {
        return Some(Child::File {
            id: item.id.as_deref()?,
            url,
        });
    }
    item.id.as_deref().map(|id| Child::Folder { id })
}

// ── Options and results ───────────────────────────────────

/// Knobs for a OneDrive download.
#[derive(Debug, Clone)]
pub struct OneDriveOptions {
    /// How many files of a folder share to download at once.
    pub workers: usize,
    /// Attempts per API call before giving up. Per-file retries belong to the
    /// engine, which is the thing doing the transfer.
    pub max_retries: u32,
    /// Re-download files that are already on disk.
    pub overwrite: bool,
}

impl Default for OneDriveOptions {
    fn default() -> Self {
        Self {
            workers: WORKERS_DEFAULT,
            max_retries: 5,
            overwrite: false,
        }
    }
}

/// What happened to a folder share.
#[derive(Debug, Clone)]
pub struct OneDriveSummary {
    /// Directory everything was written under.
    pub root: PathBuf,
    /// Files found in the folder.
    pub total: usize,
    /// Files downloaded during this run.
    pub completed: usize,
    /// Files that were already on disk.
    pub skipped: usize,
    /// Bytes written during this run.
    pub bytes: u64,
    /// Per-file failures: (path within the share, reason).
    pub failed: Vec<(String, String)>,
    /// Whether the run was interrupted.
    pub cancelled: bool,
}

/// A share link, once the API has said what it points at.
pub enum Resolved {
    /// One file, at a URL the engine can fetch as it stands.
    File(DirectLink),
    /// A folder, along with the session allowed to walk it.
    Folder(Folder),
}

/// A single file behind a share link.
#[derive(Debug, Clone)]
pub struct DirectLink {
    /// Ranged HTTPS URL, authorisation included.
    pub url: String,
    /// The name the file has on OneDrive, already safe to use as a filename.
    pub name: String,
    /// What the file is, durably, when the URL is not: a drive item id.
    pub id: String,
}

/// A folder share, ready to be walked.
///
/// No `Debug`, here or on [`Session`]: the token is a credential, and a type
/// that prints one has a way of ending up inside an error message.
pub struct Folder {
    session: Session,
    id: String,
    name: Option<String>,
}

/// A client that already carries the badger token.
struct Session {
    client: Client,
}

/// One file to fetch.
struct RemoteFile {
    /// Path relative to the download root, folders included.
    relative: PathBuf,
    /// Leaf name, for the progress line.
    name: String,
    /// Signed, ranged, and good for about an hour.
    url: String,
    /// What the file is, durably, when the URL is not: a drive item id.
    id: String,
}

// ── Entry points ──────────────────────────────────────────

/// Asks the API what a share link points at.
///
/// One POST, which is also the request that redeems the share for the
/// anonymous token, so this is the first thing any OneDrive download does.
pub async fn resolve(client: Client, url: &str, options: &OneDriveOptions) -> Result<Resolved> {
    if !is_onedrive_url(url) {
        bail!("not a OneDrive link: {}", url.trim());
    }

    let session = open_session(&client, options).await?;
    let root = root_item(&session, url, options).await?;

    if let Some(download) = root.download_url {
        let name = safe_component(root.name.as_deref().unwrap_or_default());
        return Ok(Resolved::File(DirectLink {
            url: download,
            name,
            id: root.id.unwrap_or_default(),
        }));
    }

    let Some(id) = root.id else {
        bail!(
            "the share has neither a download URL nor an item id, so there is nothing behind it to fetch"
        );
    };

    Ok(Resolved::Folder(Folder {
        session,
        id,
        name: root.name,
    }))
}

/// Downloads everything in a folder share.
///
/// `output`, when given, is the directory the tree is written into; without it
/// the folder keeps its own name under `download_dir`.
///
/// Every file goes through [`crate::engine`], which is where `.part` files,
/// ranged resume, retries and the already-downloaded check already live. This
/// function only decides what to fetch, where to put it, and how many at once.
pub async fn download_folder(
    folder: Folder,
    output: Option<String>,
    download_dir: &str,
    options: OneDriveOptions,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<OneDriveSummary> {
    let workers = options.workers.clamp(1, WORKERS_MAX);
    let (files, dirs) = walk(&folder.session, &folder.id, &options).await?;
    let root = destination_root(output, download_dir, folder.name.as_deref());

    fs::create_dir_all(&root)
        .await
        .with_context(|| format!("could not create {}", root.display()))?;

    // Every directory the walk saw, empty ones included: an empty folder is
    // still part of the structure the share had. Doing it here also means every
    // file's parent exists before any transfer starts, so nothing below has to
    // create anything.
    for dir in &dirs {
        let path = root.join(dir);
        fs::create_dir_all(&path)
            .await
            .with_context(|| format!("could not create {}", path.display()))?;
    }

    if files.is_empty() {
        // A folder share with nothing downloadable in it is a result, not a
        // failure: the directories are on disk and there is nothing to fetch.
        return Ok(OneDriveSummary {
            root,
            total: 0,
            completed: 0,
            skipped: 0,
            bytes: 0,
            failed: Vec::new(),
            cancelled: false,
        });
    }

    let total = files.len();
    let board = (!quiet).then(|| Board::new("OneDrive", total, workers));
    let renderer = board.as_ref().map(|b| b.spawn_renderer());

    let completed = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let bytes = AtomicU64::new(0);
    let cancelled = AtomicBool::new(false);
    let failed: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

    {
        let root = &root;
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
                    let lane = board.and_then(|b| b.claim(index as u64 + 1, &file.name));
                    let sink = lane.as_ref().map(|l| l.sink()).unwrap_or_else(ui::silent);

                    let destination = root.join(&file.relative);
                    let request = DownloadRequest::new(
                        file.url.clone(),
                        Some(destination.to_string_lossy().into_owned()),
                        // One connection per file. The parallelism here is
                        // files at once, and multiplying the two would point
                        // workers × chunks sockets at one CDN.
                        1,
                    )
                    .with_policy(if overwrite {
                        ExistingPolicy::Overwrite
                    } else {
                        // Never `Ask`: a folder of four hundred files must not
                        // stop on a prompt hidden behind the progress board.
                        ExistingPolicy::Reuse
                    })
                    .with_resume_identity(format!("onedrive:{}", file.id));

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
                            let name = file.relative.to_string_lossy().into_owned();
                            if let Some(board) = board {
                                board.file_failed();
                                board.log(&format!("  \u{26a0} {name}: {reason}"));
                            }
                            failed
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .push((name, reason));
                            // The engine closes the sink on every outcome it
                            // returns; an error is the one way out that leaves
                            // the lane open.
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

    Ok(OneDriveSummary {
        root,
        total,
        completed: completed.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed),
        bytes: bytes.load(Ordering::Relaxed),
        failed: failed.into_inner().unwrap_or_else(|e| e.into_inner()),
        cancelled: cancelled.load(Ordering::Relaxed),
    })
}

/// Where a folder share's files land.
///
/// An explicit `-o` wins and is always a directory: a folder share is a tree,
/// and there is no single file for a filename to name. Otherwise the folder
/// keeps its own name, which is the one thing about a share that means anything
/// to the person who was sent it.
fn destination_root(
    output: Option<String>,
    download_dir: &str,
    folder_name: Option<&str>,
) -> PathBuf {
    if let Some(dir) = output {
        return PathBuf::from(dir);
    }

    let label = folder_name
        .map(safe_component)
        .filter(|label| label != "download.bin")
        .unwrap_or_else(|| "onedrive".to_owned());

    PathBuf::from(download_dir).join(label)
}

// ── Session ───────────────────────────────────────────────

/// Mints a badger token and builds the client that carries it.
///
/// `client` is used for that one request. The transfers run on the engine's own
/// client, which is what keeps the token off requests that have no business
/// carrying it.
async fn open_session(client: &Client, options: &OneDriveOptions) -> Result<Session> {
    let minted: BadgerToken = fetch_json(
        options.max_retries,
        "could not get an anonymous OneDrive token",
        || {
            client
                .post(BADGER_ENDPOINT)
                .json(&serde_json::json!({ "appId": BADGER_APP_ID }))
        },
    )
    .await?;

    let token = minted.token.trim();
    if token.is_empty() {
        bail!("the OneDrive token endpoint answered with an empty token");
    }

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Badger {token}"))?,
    );

    let client = Client::builder()
        .user_agent("rdm")
        .connect_timeout(Duration::from_secs(10))
        .default_headers(headers)
        .build()
        .context("could not build the OneDrive HTTP client")?;

    Ok(Session { client })
}

/// Fetches the item a share link points at.
///
/// A POST rather than a GET, and that is not incidental: `Prefer: autoredeem`
/// is what accepts the share on the token's behalf, and it is that redeem which
/// makes the later `/drives/…` calls work.
async fn root_item(session: &Session, url: &str, options: &OneDriveOptions) -> Result<DriveItem> {
    let endpoint = format!(
        "{API_BASE}/shares/{}/driveitem?select=@content.downloadUrl,id,name",
        share_id(url)
    );

    fetch_json(
        options.max_retries,
        "could not open the OneDrive share",
        || session.client.post(&endpoint).header("Prefer", "autoredeem"),
    )
    .await
}

// ── Folder walk ───────────────────────────────────────────

/// Every file under a folder, plus every directory seen on the way.
///
/// An explicit stack rather than recursion: an async fn that calls itself has to
/// be boxed, and the stack keeps the collision bookkeeping in one place.
async fn walk(
    session: &Session,
    root: &str,
    options: &OneDriveOptions,
) -> Result<(Vec<RemoteFile>, Vec<PathBuf>)> {
    let mut files: Vec<RemoteFile> = Vec::new();
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut taken: HashSet<PathBuf> = HashSet::new();
    let mut stack: Vec<(String, PathBuf)> = vec![(root.to_owned(), PathBuf::new())];

    while let Some((id, parent)) = stack.pop() {
        let mut endpoint = children_url(&id);

        loop {
            let page: ChildrenPage = fetch_json(
                options.max_retries,
                "could not list a OneDrive folder",
                || session.client.get(&endpoint),
            )
            .await?;

            for item in page.items() {
                let name = safe_component(item.name.as_deref().unwrap_or_default());

                match classify(item) {
                    Some(Child::File { id, url }) => {
                        let relative = unique(&mut taken, parent.join(&name));
                        // The leaf rather than the remote name, so a numbered
                        // collision shows up on the progress line as the file
                        // it is actually being written to.
                        let name = relative
                            .file_name()
                            .map(|leaf| leaf.to_string_lossy().into_owned())
                            .unwrap_or(name);
                        files.push(RemoteFile {
                            relative,
                            name,
                            id: id.to_owned(),
                            url: url.to_owned(),
                        });
                    }
                    Some(Child::Folder { id }) => {
                        let dir = unique(&mut taken, parent.join(&name));
                        dirs.push(dir.clone());
                        stack.push((id.to_owned(), dir));
                    }
                    None => {}
                }
            }

            match page.next_link() {
                Some(next) => endpoint = next.to_owned(),
                None => break,
            }
        }
    }

    Ok((files, dirs))
}

/// Turns a remote name into exactly one safe path component.
///
/// Names come off the network, and a folder called `../../.ssh` is somebody
/// else's idea of a joke: everything before the last separator is dropped,
/// separators that arrive percent-encoded are decoded first so they cannot
/// smuggle one past, and a name that trims away to nothing becomes
/// `download.bin`.
///
/// OneDrive itself forbids the characters Windows objects to (`:`, `*`, `?`,
/// `"`, `<`, `>`, `|`), so only the separators, control characters and the dot
/// names are worth defending against here.
///
/// Nothing decodes `\uXXXX` escapes. The reference downloader has to, because
/// it reads JSON with grep; serde has already done it by the time a name
/// reaches this function.
fn safe_component(name: &str) -> String {
    let decoded = engine::percent_decode(name);
    let leaf = decoded.rsplit(['/', '\\']).next().unwrap_or_default();
    let cleaned: String = leaf.chars().filter(|c| !c.is_control()).collect();
    // Trailing dots go too: `.` and `..` trim away to nothing and are caught
    // below, and Windows drops them from real names anyway.
    let trimmed = cleaned.trim().trim_end_matches('.').trim_end();

    if trimmed.is_empty() {
        "download.bin".to_owned()
    } else {
        trimmed.to_owned()
    }
}

/// Keeps two remote names that sanitise to the same thing from becoming the
/// same file on disk.
///
/// Only reachable through names a normal drive would not contain, but the
/// failure it prevents is a silent one: one file overwriting another, and both
/// looking downloaded afterwards.
fn unique(taken: &mut HashSet<PathBuf>, candidate: PathBuf) -> PathBuf {
    if taken.insert(candidate.clone()) {
        return candidate;
    }

    let stem = candidate
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_default();
    let extension = candidate
        .extension()
        .map(|extension| extension.to_string_lossy().into_owned());
    let parent = candidate.parent().unwrap_or(Path::new(""));
    let mut copy = 2u32;

    loop {
        let mut name = format!("{stem} ({copy})");
        if let Some(extension) = &extension {
            name.push('.');
            name.push_str(extension);
        }

        let numbered = parent.join(name);
        if taken.insert(numbered.clone()) {
            return numbered;
        }

        copy += 1;
    }
}

// ── Requests ──────────────────────────────────────────────

/// Sends a request until it answers with JSON that parses, `retries` attempts at
/// most.
///
/// The error handling is worth the space: an expired share, a private one and a
/// link that never existed all come back as a bare status code, and the body is
/// often the only thing that says which.
async fn fetch_json<T, F>(retries: u32, what: &'static str, build: F) -> Result<T>
where
    T: DeserializeOwned,
    F: Fn() -> reqwest::RequestBuilder,
{
    let mut last: Option<anyhow::Error> = None;

    for attempt in 0..retries.max(1) {
        match build().send().await {
            Ok(response) => {
                let status = response.status();

                match response.bytes().await {
                    Ok(body) if status.is_success() => match serde_json::from_slice(&body) {
                        Ok(parsed) => return Ok(parsed),
                        Err(error) => {
                            last = Some(anyhow!(error).context(format!(
                                "OneDrive answered with something other than the JSON asked for: {}",
                                snippet(&body)
                            )));
                        }
                    },
                    Ok(body) => {
                        last = Some(anyhow!("{}", explain(status, &body)));
                        // A refusal is a decision, not a hiccup: asking again
                        // with the same credential gets the same answer.
                        if is_final(status) {
                            break;
                        }
                    }
                    Err(error) => {
                        last = Some(anyhow!(error).context("the response could not be read"));
                    }
                }
            }
            Err(error) => last = Some(anyhow!(error)),
        }

        backoff(attempt).await;
    }

    Err(last.unwrap_or_else(|| anyhow!("no answer")).context(what))
}

/// Statuses there is no point asking about twice.
fn is_final(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::NOT_FOUND
            | StatusCode::GONE
    )
}

/// Turns a refusal into something worth reading.
fn explain(status: StatusCode, body: &[u8]) -> String {
    let hint = match status {
        StatusCode::NOT_FOUND | StatusCode::GONE => {
            " \u{2014} the share has expired, been deleted, or the link is wrong"
        }
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            " \u{2014} the share needs a password or is restricted to specific people, and an anonymous token cannot open either"
        }
        StatusCode::TOO_MANY_REQUESTS => " \u{2014} OneDrive is rate limiting this token",
        _ => "",
    };

    format!("OneDrive answered HTTP {status}{hint}: {}", snippet(body))
}

/// The start of a response body, flattened onto one line.
///
/// A bare `expected value at line 1 column 1` says only that the body was not
/// JSON, not whether it was HTML, a sign-in page, or nothing at all.
fn snippet(body: &[u8]) -> String {
    const MAX: usize = 200;

    let head = &body[..body.len().min(MAX)];
    let flattened = String::from_utf8_lossy(head)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    if body.len() > MAX {
        format!("{flattened}\u{2026}")
    } else {
        flattened
    }
}

/// Exponential-ish backoff, capped so a retry loop never parks for minutes.
async fn backoff(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(250u64 << attempt.min(5))).await;
}
