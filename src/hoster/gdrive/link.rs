//! What a Drive link points at, decided offline.
//!
//! Nothing here makes a request. Everything Google needs to be asked about is
//! in `api`.

use anyhow::{Context, Result, bail};
use reqwest::Url;

use super::APPS_PREFIX;

/// Hosts whose links this module claims.
const HOSTS: [&str; 6] = [
    "drive.google.com",
    "www.drive.google.com",
    "docs.google.com",
    "www.docs.google.com",
    "colab.research.google.com",
    "www.colab.research.google.com",
];

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
/// `drive.google.com.evil.net` and `https://drive.google.com@evil.net` all
/// belong to somebody else.
///
/// `drive.usercontent.google.com` is deliberately not claimed: that is where a
/// confirmed download already lives, so there is nothing left to resolve and
/// the generic engine can have it as it stands.
pub fn is_gdrive_url(url: &str) -> bool {
    host_of(url).is_some_and(|host| HOSTS.contains(&host.as_str()))
}

/// What a Drive link points at.
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
    pub(super) fn segment(self) -> &'static str {
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
    pub(super) fn from_mime(mime: &str) -> Option<Self> {
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
    /// Not every export Drive offers is here — these are the ones worth having
    /// a name for. PDF first, then the natural alternative, which is what makes
    /// the `office` alias below a lookup rather than a table of its own.
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
    /// `office` is the one alias, and the reason a bare extension is not enough
    /// on its own: "whatever Microsoft opens" is a different format for every
    /// kind, and there is no Microsoft anything for a drawing.
    ///
    /// An unrecognised format falls back to PDF rather than failing. The format
    /// is one config value shared by every kind, and a folder of forty
    /// documents must not die because one of them cannot be a spreadsheet.
    pub(super) fn export_as(self, format: &str) -> (&'static str, &'static str) {
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

/// Reads a link.
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
