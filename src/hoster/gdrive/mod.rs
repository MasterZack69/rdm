//! Google Drive (drive.google.com, docs.google.com) support.
//!
//! No Drive link is a fetchable address, and that is what makes the host worth
//! a module: hand any of them to the generic engine and it saves the page it
//! was served under a plausible filename.
//!
//! Three shapes arrive here, and they are unfetchable in three different ways:
//!
//!   1. **A file** — `/file/d/<id>/view`, `/uc?id=<id>`, `/open?id=<id>`. A
//!      small one redirects straight to the bytes. Anything Drive declines to
//!      virus-scan answers with a warning page instead, and the real URL is
//!      the `action` of the form on it, along with the `uuid` and `at`
//!      parameters its hidden inputs carry. Following that form is the whole
//!      trick behind every "download a big file from Drive" recipe.
//!   2. **A Google Doc** — `docs.google.com/<kind>/d/<id>`. There is no file
//!      at all: a Doc is rendered on request, so the link is swapped for an
//!      export endpoint and a format.
//!   3. **A folder** — `/drive/folders/<id>`. The listing lives behind the
//!      Drive API, which wants an API key. Anonymous access can fetch a file
//!      whose link you already hold, but it cannot enumerate anything.
//!
//! ## What this asks for, and what it does not
//!
//! No login, no OAuth, no consent screen. An API key is optional and buys two
//! things: folder listings, and real filenames without a round trip through
//! the warning page. It is a quota identity rather than a credential, so a
//! restricted share stays unreadable either way.
//!
//! Transfers go through [`crate::engine`], so chunking, ranged resume, retries
//! and the progress bar are the ones every other hoster gets. Resume is keyed
//! on the Drive id rather than the URL, because a confirmed download URL
//! carries a short-lived `at` token: the same bytes come back under a
//! different URL on the next run.
//!
//! No integrity check. The API can hand over an `md5Checksum` for a binary
//! file, but nothing in this crate can compute one, and a digest that cannot
//! be recomputed is decoration.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{StreamExt, stream};
use reqwest::header::CONTENT_DISPOSITION;
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::fs;
use tokio_util::sync::CancellationToken;

use crate::engine::{self, DownloadRequest, ExistingPolicy, Outcome};
use crate::ui::{self, Board, ProgressSink};

#[cfg(test)]
mod tests;

/// Where an anonymous file download starts.
const UC_ENDPOINT: &str = "https://drive.google.com/uc";

/// Drive API v3, for everything that needs a key.
const API_FILES: &str = "https://www.googleapis.com/drive/v3/files";

/// Where a Google Doc is rendered on demand.
const DOCS_BASE: &str = "https://docs.google.com";

/// Hosts whose links this module claims.
const HOSTS: [&str; 6] = [
    "drive.google.com",
    "www.drive.google.com",
    "docs.google.com",
    "www.docs.google.com",
    "colab.research.google.com",
    "www.colab.research.google.com",
];

/// The mimeType prefix every Google-native item shares.
const APPS_PREFIX: &str = "application/vnd.google-apps.";

/// The one Google-native mimeType that is a container rather than a document.
const FOLDER_MIME: &str = "application/vnd.google-apps.folder";

/// Files in flight when nothing else says otherwise.
pub const WORKERS_DEFAULT: usize = 5;

/// Upper bound on files in flight. Drive's quota is per key and per second
/// rather than per connection, so more than this trades throughput for 403s.
const WORKERS_MAX: usize = 15;

// ── Link handling ─────────────────────────────────────────

/// Host of an http(s) URL, lower-cased.
fn host_of(url: &str) -> Option<String> {
    let parsed = Url::parse(url.trim()).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    Some(parsed.host_str()?.trim_end_matches('.').to_ascii_lowercase())
}

/// Is this a Google Drive link?
///
/// Host equality against a parsed URL, so `notdrive.google.com`,
/// `drive.google.com.evil.net` and `https://evil.com@drive.google.com.evil.net`
/// all belong to somebody else.
///
/// `drive.usercontent.google.com` is deliberately not claimed: that is where a
/// confirmed download already lives, so there is nothing left to resolve and
/// the generic engine can have it as it stands.
pub fn is_gdrive_url(url: &str) -> bool {
    host_of(url).is_some_and(|host| HOSTS.contains(&host.as_str()))
}

/// What a Drive link points at, decided without a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
    /// A file uploaded as bytes.
    File { id: String },
    /// A Google-native document, which has to be exported to become a file.
    Doc { kind: DocKind, id: String },
    /// A folder, whose listing needs the API.
    Folder { id: String },
}

/// The Google-native document types that can be exported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocKind {
    Document,
    Spreadsheet,
    Presentation,
    Drawing,
}

impl DocKind {
    /// The path segment Docs addresses this kind by, which is not always the
    /// mimeType's noun: a spreadsheet lives under `/spreadsheets`.
    fn segment(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Spreadsheet => "spreadsheets",
            Self::Presentation => "presentation",
            Self::Drawing => "drawings",
        }
    }

    fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "document" => Some(Self::Document),
            "spreadsheets" => Some(Self::Spreadsheet),
            "presentation" => Some(Self::Presentation),
            "drawings" => Some(Self::Drawing),
            _ => None,
        }
    }

    /// The kind behind an `application/vnd.google-apps.*` mimeType.
    fn from_mime(mime: &str) -> Option<Self> {
        match mime.strip_prefix(APPS_PREFIX)? {
            "document" => Some(Self::Document),
            "spreadsheet" => Some(Self::Spreadsheet),
            "presentation" => Some(Self::Presentation),
            "drawing" => Some(Self::Drawing),
            _ => None,
        }
    }

    /// Formats this kind can be exported as: the extension it lands under, and
    /// the mimeType Drive wants asked for.
    ///
    /// Not every export Drive offers is here. These are the ones worth having
    /// a name for, PDF first and the natural non-PDF choice second, which is
    /// what makes the `office` alias below a lookup rather than a table of its
    /// own.
    fn formats(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Document => &[
                ("pdf", "application/pdf"),
                (
                    "docx",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ),
                ("odt", "application/vnd.oasis.opendocument.text"),
                ("rtf", "application/rtf"),
                ("txt", "text/plain"),
                ("md", "text/markdown"),
                ("epub", "application/epub+zip"),
            ],
            Self::Spreadsheet => &[
                ("pdf", "application/pdf"),
                (
                    "xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ),
                ("ods", "application/vnd.oasis.opendocument.spreadsheet"),
                ("csv", "text/csv"),
                ("tsv", "text/tab-separated-values"),
            ],
            Self::Presentation => &[
                ("pdf", "application/pdf"),
                (
                    "pptx",
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                ),
                ("odp", "application/vnd.oasis.opendocument.presentation"),
                ("txt", "text/plain"),
            ],
            Self::Drawing => &[
                ("pdf", "application/pdf"),
                ("png", "image/png"),
                ("jpg", "image/jpeg"),
                ("svg", "image/svg+xml"),
            ],
        }
    }

    /// The extension and mimeType a requested format resolves to.
    ///
    /// `office` is the one alias, and the reason a bare extension is not
    /// enough on its own: "whatever Microsoft opens" is a different format for
    /// every kind, and there is no Microsoft anything for a drawing.
    ///
    /// An unrecognised format falls back to PDF rather than failing. A folder
    /// of forty documents must not die because one of them cannot be exported
    /// as `xlsx`, and the format is a config value shared by every kind.
    fn export_as(self, format: &str) -> (&'static str, &'static str) {
        let formats = self.formats();
        let wanted = format.trim().trim_start_matches('.').to_ascii_lowercase();

        if wanted == "office" {
            return formats.get(1).copied().unwrap_or(formats[0]);
        }

        formats
            .iter()
            .find(|(ext, _)| *ext == wanted)
            .copied()
            .unwrap_or(formats[0])
    }
}

/// Does this look like a Drive id?
///
/// Ids are opaque, but they are always URL-safe base64-ish and never short.
/// Checking that keeps `/drive/my-drive` from being read as a file called
/// `my-drive`, and keeps a quote out of the `q` parameter a folder listing is
/// built with.
fn is_id(candidate: &str) -> bool {
    candidate.len() >= 10
        && candidate
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Reads a link without asking Google anything.
///
/// Every shape below is one Drive hands out itself. A `/u/<n>/` account prefix
/// is ignored rather than parsed: it records which signed-in browser profile
/// produced the link, which is nothing a download needs to know.
pub fn parse_link(url: &str) -> Result<Link> {
    let trimmed = url.trim();
    if !is_gdrive_url(trimmed) {
        bail!("not a Google Drive link: {trimmed}");
    }

    let parsed = Url::parse(trimmed).with_context(|| format!("not a URL: {trimmed}"))?;
    let host = host_of(trimmed).unwrap_or_default();
    let segments: Vec<&str> = parsed
        .path_segments()
        .map(|segments| segments.filter(|segment| !segment.is_empty()).collect())
        .unwrap_or_default();
    let query_id = parsed
        .query_pairs()
        .find(|(key, _)| key == "id")
        .map(|(_, value)| value.into_owned())
        .filter(|id| is_id(id));

    // A folder, addressed by path or by the old `folderview` query.
    if let Some(index) = segments.iter().position(|segment| *segment == "folders")
        && let Some(id) = segments.get(index + 1).filter(|id| is_id(id))
    {
        return Ok(Link::Folder {
            id: (*id).to_owned(),
        });
    }
    if segments.last() == Some(&"folderview")
        && let Some(id) = query_id.clone()
    {
        return Ok(Link::Folder { id });
    }

    // `<kind>/d/<id>`, the shape every editor and every file preview uses.
    if let Some(index) = segments.iter().position(|segment| *segment == "d")
        && index > 0
        && let Some(id) = segments.get(index + 1).filter(|id| is_id(id))
    {
        let id = (*id).to_owned();
        let kind = segments[index - 1];
        if kind == "file" {
            return Ok(Link::File { id });
        }
        return match DocKind::from_segment(kind) {
            Some(kind) => Ok(Link::Doc { kind, id }),
            // Forms, Sites and Jamboards are real Drive items with no export
            // behind them, so there is nothing to fetch and nothing to write.
            None => bail!(
                "a Google {kind} cannot be downloaded \u{2014} files, folders, Docs, Sheets, Slides and Drawings can"
            ),
        };
    }

    // A Colab notebook is an ordinary Drive file with an editor of its own.
    if matches!(
        host.as_str(),
        "colab.research.google.com" | "www.colab.research.google.com"
    ) && segments.first() == Some(&"drive")
        && let Some(id) = segments.get(1).filter(|id| is_id(id))
    {
        return Ok(Link::File {
            id: (*id).to_owned(),
        });
    }

    // `/uc`, `/open`, `/u/0/uc` and the rest keep the id in the query.
    if let Some(id) = query_id {
        return Ok(Link::File { id });
    }

    bail!(
        "no Drive id in the link \u{2014} they look like /file/d/<id>/view, /uc?id=<id> or /drive/folders/<id>"
    )
}

/// Is this a folder link? Decided from the link alone, no request.
pub fn is_folder_link(url: &str) -> bool {
    matches!(parse_link(url), Ok(Link::Folder { .. }))
}

// ── Options and results ───────────────────────────────────

/// Knobs for a Google Drive download.
#[derive(Debug, Clone)]
pub struct GdriveOptions {
    /// How many files of a folder to download at once.
    pub workers: usize,
    /// Attempts per API call before giving up. Per-file retries belong to the
    /// engine, which is the thing doing the transfer.
    pub max_retries: u32,
    /// A Drive API key. Optional for a file, required for a folder.
    pub api_key: Option<String>,
    /// What a Google Doc is exported as: an extension, or `office`.
    pub doc_format: String,
    /// Re-download files that are already on disk.
    pub overwrite: bool,
}

impl Default for GdriveOptions {
    fn default() -> Self {
        Self {
            workers: WORKERS_DEFAULT,
            max_retries: 5,
            api_key: None,
            doc_format: "pdf".to_owned(),
            overwrite: false,
        }
    }
}

impl GdriveOptions {
    /// The API key, if there is one worth sending.
    ///
    /// A blank key is not a key: it comes from an unset config field or an
    /// empty environment variable, and sending it turns every call into a 400
    /// instead of falling back to anonymous access.
    fn key(&self) -> Option<&str> {
        self.api_key
            .as_deref()
            .map(str::trim)
            .filter(|key| !key.is_empty())
    }
}

/// What happened to a folder download.
#[derive(Debug, Clone)]
pub struct GdriveSummary {
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
    /// Items nothing could be done with: shortcuts, which point at something
    /// else, and Apps Script projects, which have no export. Counted rather
    /// than dropped silently, because a caller comparing the folder against a
    /// local copy needs to know its picture of it is short.
    pub unsupported: usize,
    /// Per-file failures: (path within the folder, reason).
    pub failed: Vec<(String, String)>,
    /// Whether the run was interrupted.
    pub cancelled: bool,
}

/// A link, once it is known what can be fetched for it.
pub enum Resolved {
    /// One file, at a URL the engine can take as it stands.
    File(DirectLink),
    /// A folder, along with the key allowed to list it.
    Folder(Folder),
}

/// A single file behind a link.
#[derive(Debug, Clone)]
pub struct DirectLink {
    /// Ranged HTTPS URL, authorisation included.
    pub url: String,
    /// What to call the result. Drive keeps names in metadata rather than in
    /// URLs, and an exported Doc has no name at all until a format is chosen.
    pub name: String,
    /// What the file is, durably, when the URL is not: its Drive id.
    pub id: String,
}

/// A folder, ready to be walked.
///
/// No `Debug`, here or on [`Session`]: an API key is a billable identity, and
/// a type that prints one has a way of ending up inside an error message.
pub struct Folder {
    session: Session,
    id: String,
    name: Option<String>,
}

impl Folder {
    /// The folder's own name, when the API gave one.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

/// A client and the key its requests carry.
struct Session {
    client: Client,
    api_key: String,
}

/// What a walk of a folder found.
struct Listing {
    /// Every file under the folder, in walk order.
    files: Vec<RemoteFile>,
    /// Every directory seen, empty ones included.
    dirs: Vec<PathBuf>,
    /// Children with no downloadable form.
    unsupported: usize,
}

/// One file to fetch.
struct RemoteFile {
    /// Path relative to the download root, folders included.
    relative: PathBuf,
    /// Leaf name, for the progress line.
    name: String,
    /// An API URL, with the key already on it.
    url: String,
    /// Drive id, which is what resume is keyed on.
    id: String,
}

// ── API shapes ────────────────────────────────────────────

/// The fields of a Drive file this module asks for.
#[derive(Debug, Deserialize)]
struct FileMeta {
    id: Option<String>,
    name: Option<String>,
    /// The one thing that says what can be done with an item: bytes to fetch,
    /// a document to render, a folder to walk, or none of those.
    #[serde(rename = "mimeType")]
    mime_type: Option<String>,
}

/// One page of a folder's children.
#[derive(Debug, Deserialize)]
struct FileList {
    #[serde(default)]
    files: Vec<FileMeta>,
    #[serde(rename = "nextPageToken")]
    next: Option<String>,
}

// ── Entry points ──────────────────────────────────────────

/// Works out what a link points at, and what can be fetched for it.
///
/// A folder costs one call, for its name; walking it is [`download_folder`]'s
/// job. A file or a Doc comes back as a URL and a filename, which is one
/// request with a key and up to three without.
pub async fn resolve(client: Client, url: &str, options: &GdriveOptions) -> Result<Resolved> {
    match (parse_link(url)?, options.key()) {
        (Link::Folder { id }, Some(key)) => {
            let session = Session {
                client,
                api_key: key.to_owned(),
            };
            let name = folder_name(&session, &id, options).await?;
            Ok(Resolved::Folder(Folder { session, id, name }))
        }

        (Link::Folder { .. }, None) => bail!(
            "listing a Google Drive folder needs an API key \u{2014} set gdrive_api_key in the config, \
             or RDM_GDRIVE_API_KEY in the environment. Anonymous access can fetch a file whose link \
             you already hold, but it cannot enumerate a folder"
        ),

        // With a key both shapes take the same path: the metadata says whether
        // there are bytes to fetch or a document to render, which is more than
        // the link said. A `/file/d/` link to a Sheet is a link people really
        // do send.
        (Link::File { id } | Link::Doc { id, .. }, Some(key)) => {
            let session = Session {
                client,
                api_key: key.to_owned(),
            };
            let meta = metadata(&session, &id, options).await?;
            Ok(Resolved::File(direct_link(&session, &id, &meta, options)?))
        }

        (Link::File { id }, None) => Ok(Resolved::File(anonymous_file(&client, &id, options).await?)),

        (Link::Doc { kind, id }, None) => {
            let (ext, _) = kind.export_as(&options.doc_format);
            let url = docs_export_url(kind, &id, ext)?;
            // The export names the file in its `Content-Disposition`, and that
            // is the only place the document's title appears without a key.
            let name = suggested_name(&client, url.as_str())
                .await
                .unwrap_or_else(|| format!("{}.{ext}", fallback_name(&id)));

            Ok(Resolved::File(DirectLink {
                url: url.into(),
                name,
                id,
            }))
        }
    }
}

/// Downloads everything in a folder.
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
    options: GdriveOptions,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<GdriveSummary> {
    let listing = walk(&folder.session, &folder.id, &options).await?;
    let root = destination_root(output, download_dir, folder.name());

    create_tree(&root, &listing.dirs).await?;
    download_files(&listing, &root, &options, cancel, quiet).await
}

/// Where a folder's files land.
///
/// An explicit `-o` wins and is always a directory: a folder is a tree, and
/// there is no single file for a filename to name. Otherwise the folder keeps
/// its own name, which is the one thing about it that means anything to the
/// person who was sent the link.
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
        .unwrap_or_else(|| "gdrive".to_owned());

    PathBuf::from(download_dir).join(label)
}

/// Creates the download root and every directory the walk saw.
///
/// Empty ones included: an empty folder is still part of the structure that
/// was shared. Doing it up front also means every file's parent exists before
/// any transfer starts, so nothing below has to create anything.
async fn create_tree(root: &Path, dirs: &[PathBuf]) -> Result<()> {
    fs::create_dir_all(root)
        .await
        .with_context(|| format!("could not create {}", root.display()))?;

    for dir in dirs {
        let path = root.join(dir);
        fs::create_dir_all(&path)
            .await
            .with_context(|| format!("could not create {}", path.display()))?;
    }

    Ok(())
}

/// Downloads a walked listing under `root`, several files at a time.
async fn download_files(
    listing: &Listing,
    root: &Path,
    options: &GdriveOptions,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<GdriveSummary> {
    let total = listing.files.len();

    if total == 0 {
        // A folder with nothing downloadable in it is a result, not a failure:
        // the directories are on disk and there is nothing to fetch.
        return Ok(GdriveSummary {
            root: root.to_path_buf(),
            total: 0,
            completed: 0,
            skipped: 0,
            bytes: 0,
            unsupported: listing.unsupported,
            failed: Vec::new(),
            cancelled: false,
        });
    }

    let workers = options.workers.clamp(1, WORKERS_MAX);
    let board = (!quiet).then(|| Board::new("Google Drive", total, workers));
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

        stream::iter(listing.files.iter().enumerate())
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
                    let sink: Arc<dyn ProgressSink> = match lane.as_ref() {
                        Some(lane) => lane.sink(),
                        None => ui::silent(),
                    };

                    let destination = root.join(&file.relative);
                    let request = DownloadRequest::new(
                        file.url.clone(),
                        Some(destination.to_string_lossy().into_owned()),
                        // One connection per file. The parallelism here is
                        // files at once, and multiplying the two would point
                        // workers \u{00d7} chunks sockets at one API key.
                        1,
                    )
                    .with_policy(if overwrite {
                        ExistingPolicy::Overwrite
                    } else {
                        // Never `Ask`: a folder of four hundred files must not
                        // stop on a prompt hidden behind the progress board.
                        ExistingPolicy::Reuse
                    })
                    .with_resume_identity(format!("gdrive:{}", file.id));

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
        board.finish