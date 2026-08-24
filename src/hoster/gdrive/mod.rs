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
//!      Drive API, which wants an API key. Without one there is still the page
//!      Drive hands websites to embed, which lists a folder's children as
//!      links: enough to walk an ordinary share, capped by the page rather
//!      than paged to the end, and with no sizes on it.
//!
//! ## What this asks for, and what it does not
//!
//! No login, no OAuth, no consent screen. An API key is optional and buys
//! three things: a folder listing paged to the end rather than capped by a
//! page, a size per file, and real filenames without a round trip through the
//! warning page. It is a quota identity rather than a credential, so a
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
//!
//! ## Layout
//!
//! Split by what each part talks to: [`link`] answers questions offline, `api`
//! is the only thing that speaks to Google, and `transfer` writes a walked
//! folder to disk. This file holds the vocabulary all three share.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use reqwest::Client;
use tokio_util::sync::CancellationToken;

use crate::engine;

mod api;
pub mod link;
mod transfer;

#[cfg(test)]
mod tests;

pub use link::{DocKind, Link, is_folder_link, is_gdrive_url, parse_link};
pub use transfer::{create_tree, download_files};

/// Where an anonymous file download starts.
const UC_ENDPOINT: &str = "https://drive.google.com/uc";

/// Drive API v3, for everything that needs a key.
const API_FILES: &str = "https://www.googleapis.com/drive/v3/files";

/// Where a Google Doc is rendered on demand.
const DOCS_BASE: &str = "https://docs.google.com";

/// The mimeType prefix every Google-native item shares.
const APPS_PREFIX: &str = "application/vnd.google-apps.";

/// The one Google-native mimeType that is a container rather than a document.
const FOLDER_MIME: &str = "application/vnd.google-apps.folder";

/// Files in flight when nothing else says otherwise.
pub const WORKERS_DEFAULT: usize = 5;

/// Upper bound on files in flight. Drive's quota is per key and per second
/// rather than per connection, so more than this trades throughput for 403s.
const WORKERS_MAX: usize = 15;

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
    /// rather than falling back to anonymous access.
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
    /// than dropped silently, because anyone comparing the folder against a
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
    source: FolderSource,
    id: String,
    name: Option<String>,
}

impl Folder {
    /// The folder's own name, when the API gave one.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

enum FolderSource {
    /// The Drive API, with the key allowed to list it: paged to the end, with
    /// every child's mimeType.
    Api(Session),
    /// The page Drive hands websites to embed, when there is no key: one page
    /// per folder, capped by whatever it chooses to render.
    Page(Client),
}

/// A client and the key its requests carry.
struct Session {
    client: Client,
    api_key: String,
}

/// What a walk of a folder found.
pub struct Listing {
    /// Every file under the folder, in walk order.
    pub files: Vec<RemoteFile>,
    /// Every directory seen, empty ones included.
    pub dirs: Vec<PathBuf>,
    /// Children with no downloadable form.
    pub unsupported: usize,
}

/// One file to fetch.
pub struct RemoteFile {
    /// Path relative to the download root, folders included.
    pub relative: PathBuf,
    /// Leaf name, for the progress line.
    pub name: String,
    /// Where the bytes come from: an API URL with the key on it, a document's
    /// export, or a confirmed anonymous download.
    pub url: String,
    /// Bytes, when something said so. The API states a size for a stored file
    /// and never for a Google document; the folder page states none at all. A
    /// mirror with no size to compare cannot tell stale from current.
    pub size: Option<u64>,
    /// Drive id, which is what resume is keyed on.
    pub id: String,
}

/// Where a folder download reports progress.
pub enum Progress {
    /// Its own board, for a download that owns the terminal.
    Board,
    /// Nothing at all.
    Quiet,
    /// One line on somebody else's board, for a folder running as a single
    /// queue item. The queue owns the display, so nothing here may build a
    /// second one.
    Lane(std::sync::Arc<dyn crate::ui::ProgressSink>),
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
            let name = api::folder_name(&session, &id, options).await?;
            Ok(Resolved::Folder(Folder {
                source: FolderSource::Api(session),
                id,
                name,
            }))
        }

        // No key, so the folder page is the only listing there is. It is
        // written for an iframe rather than for a caller, and can only be
        // trusted as far as it goes — `api::scrape_folder` says so out loud
        // when a listing comes back at the length the page stops at.
        //
        (Link::Folder { id }, None) => {
            let name = api::scrape_folder_name(&client, &id, options).await?;
            Ok(Resolved::Folder(Folder {
                source: FolderSource::Page(client),
                id,
                name,
            }))
        }

        // With a key both shapes take the same path: the metadata says whether
        // there are bytes to fetch or a document to render, which is more than
        // the link said. A `/file/d/` link to a Sheet is one people really do
        // send.
        (Link::File { id } | Link::Doc { id, .. }, Some(key)) => {
            let session = Session {
                client,
                api_key: key.to_owned(),
            };
            let meta = api::metadata(&session, &id, options).await?;
            Ok(Resolved::File(api::direct_link(
                &session, &id, &meta, options,
            )?))
        }

        (Link::File { id }, None) => Ok(Resolved::File(
            api::anonymous_file(&client, &id, options).await?,
        )),

        (Link::Doc { kind, id }, None) => {
            let (ext, _) = kind.export_as(&options.doc_format);
            let url = api::docs_export_url(kind, &id, ext)?;
            // The export names the file in its `Content-Disposition`, and that
            // is the only place a document's title appears without a key.
            let name = api::suggested_name(&client, url.as_str())
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
/// only decides what to fetch, where to put it, and how many at once.
/// Lists everything under a folder, without fetching any of it.
///
/// The two sources differ in what they can promise: the API pages to the end
/// and states each file's size, while the folder page renders one batch and
/// states nothing. `api::scrape_folder` warns on stderr when a listing comes
/// back at the length the page stops at.
pub async fn list_folder(folder: &Folder, options: &GdriveOptions) -> Result<Listing> {
    match &folder.source {
        FolderSource::Api(session) => api::walk(session, &folder.id, options).await,
        FolderSource::Page(client) => api::scrape_folder(client, &folder.id, options).await,
    }
}

/// Downloads everything in a folder.
///
/// `output`, when given, is the directory the tree is written into; without it
/// the folder keeps its own name under `download_dir`.
///
/// Every file goes through [`crate::engine`], which is where `.part` files,
/// ranged resume, retries and the already-downloaded check already live. This
/// only decides what to fetch, where to put it, and how many at once.
pub async fn download_folder(
    folder: Folder,
    output: Option<String>,
    download_dir: &str,
    options: GdriveOptions,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<GdriveSummary> {
    let listing = list_folder(&folder, &options).await?;
    let root = destination_root(output, download_dir, folder.name());

    create_tree(&root, &listing.dirs).await?;
    let progress = if quiet {
        Progress::Quiet
    } else {
        Progress::Board
    };
    let mut summary = download_files(&listing.files, &root, &options, cancel, progress).await?;
    // The transfer sees a slice of files rather than the listing they came
    // from, so the count of what could not be fetched is added back here.
    summary.unsupported = listing.unsupported;

    Ok(summary)
}

// ── Naming ───────────────────────────────────────────────

/// Where a folder's files land.
///
/// An explicit `-o` wins and is always a directory: a folder is a tree, and
/// there is no single file for a filename to name. Otherwise the folder keeps
/// its own name, which is the one thing about it that means anything to
/// whoever was sent the link.
pub fn destination_root(
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

/// A name for a file Drive would not name.
///
/// Deliberately without an extension: the engine fills in the name the server
/// suggests when the output path has none, so this is a floor rather than a
/// guess.
fn fallback_name(id: &str) -> String {
    format!("gdrive-{id}")
}

/// Adds an export format's extension, unless the name already ends in it.
///
/// A Doc's title has no extension, but somebody's spreadsheet really is called
/// `budget.xlsx`, and `budget.xlsx.xlsx` is nobody's idea of a result.
fn with_extension(name: &str, ext: &str) -> String {
    if name.to_ascii_lowercase().ends_with(&format!(".{ext}")) {
        return name.to_owned();
    }
    format!("{name}.{ext}")
}

/// Turns a remote name into one safe component of a local path.
///
/// Drive allows `/` in a filename, so a name is a name and never a path: the
/// leaf is taken and the rest discarded, which is what keeps a folder listing
/// from writing outside the directory it was asked for.
fn safe_component(name: &str) -> String {
    let decoded = engine::percent_decode(name);
    let leaf = decoded.rsplit(['/', '\\']).next().unwrap_or_default();
    let cleaned: String = leaf
        .chars()
        .filter(|c| !c.is_control() && !matches!(c, ':' | '*' | '?' | '"' | '<' | '>' | '|'))
        .collect();
    let trimmed = cleaned.trim().trim_end_matches('.').trim();

    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        return "download.bin".to_owned();
    }
    trimmed.to_owned()
}

/// Reserves a path, numbering it when the tree already holds that name.
///
/// Drive lets two children of one folder share a name, and a case-insensitive
/// filesystem makes near-misses collide too. Numbering happens while walking
/// rather than while downloading, so the name a file is announced under is the
/// name it is written to.
fn unique(taken: &mut HashSet<PathBuf>, candidate: PathBuf) -> PathBuf {
    if taken.insert(candidate.clone()) {
        return candidate;
    }

    let parent = candidate
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    let stem = candidate
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_default();
    let ext = candidate
        .extension()
        .map(|ext| ext.to_string_lossy().into_owned());

    for n in 2usize.. {
        let name = match &ext {
            Some(ext) => format!("{stem} ({n}).{ext}"),
            None => format!("{stem} ({n})"),
        };
        let next = parent.join(name);
        if taken.insert(next.clone()) {
            return next;
        }
    }

    candidate
}
