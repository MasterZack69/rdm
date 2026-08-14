//! Dropbox (dropbox.com): share links rewritten into direct downloads.
//!
//! Dropbox is the plainest hoster here, and deliberately the smallest module:
//! there is no API to call, no token to mint and nothing to decrypt. A share
//! link already *is* the file — it is only wearing a preview page. Ask for it
//! with `dl=1` instead of `dl=0` and the server redirects to its CDN, which
//! serves the bytes, honours `Range` and names the file in a
//! `Content-Disposition` header.
//!
//! So this module owns the rewrite and nothing else. Chunking, resuming,
//! retrying and drawing the bar stay in [`crate::engine`], because a second
//! copy of that machinery which happened to know the word "dropbox" would be
//! strictly worse than the first one.
//!
//! ## What the rewrite has to get right
//!
//!   1. **The query string is load-bearing.** A current link carries `rlkey`
//!      and `st`; drop either and Dropbox answers 404. So `dl` is replaced
//!      within the query rather than the query being rebuilt out of the two
//!      parameters we happen to recognise.
//!   2. **`dl` is usually already there.** Appending blindly gives
//!      `?dl=0&dl=1`, and Dropbox reads the first value — the preview page.
//!      The old flag is dropped, then exactly one `dl=1` is added.
//!   3. **A folder share is one zip, not a listing.** There are no per-file
//!      URLs to discover and nothing to fan out over, so it stays a single
//!      download — see [`Share::Folder`] and
//!      [`crate::hoster::Kind::link_kind`].
//!   4. **A name off the network is untrusted.** A percent-encoded `/`
//!      survives decoding, and `..%2F..%2Fetc` joined onto the download
//!      directory is how a download manager gets talked into writing outside
//!      it.
//!
//! Only the four documented share shapes are claimed. Anything else on the
//! host — `/home`, a Paper doc — and every `dl.dropboxusercontent.com` link,
//! which is already direct, falls through to the generic engine that handles
//! it perfectly well.

use anyhow::{Context, Result};
use reqwest::Url;

/// Hosts this module answers for, compared whole and case-insensitively:
/// `notdropbox.com` is not Dropbox, and neither is `dropbox.com.evil.com`.
const HOSTS: [&str; 2] = ["dropbox.com", "www.dropbox.com"];

/// The query flag that picks between the preview page (`0`) and the file
/// itself (`1`).
const DOWNLOAD_FLAG: &str = "dl";

/// What a recognised share link points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Share {
    /// One file, which names itself in the link's own path.
    File,
    /// One folder, which Dropbox zips on the fly. Still a single download.
    Folder,
}

/// A share link rewritten into something the generic engine can fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectLink {
    /// The same link with `dl=1`, which redirects to the file itself.
    pub url: String,
    /// What is on the other end of it.
    pub share: Share,
    /// What to call the download when the user did not name it.
    ///
    /// A file share carries its real name in its own path, so that is used
    /// verbatim. A folder share carries nothing usable — `/scl/fo/<id>/<hash>`
    /// says nothing about the folder — so it falls back to the share id with a
    /// `.zip` suffix. That suffix is a fact rather than a guess: Dropbox packs
    /// a folder share before serving it.
    pub fallback_name: String,
}

/// Parses `url`, keeping it only if a Dropbox host owns it.
fn dropbox_url(url: &str) -> Option<Url> {
    let parsed = Url::parse(url.trim()).ok()?;

    // Bound to a `bool` first: `host_str` borrows `parsed`, and the borrow has
    // to end before `parsed` can be handed back.
    let claimed = parsed
        .host_str()
        .is_some_and(|host| HOSTS.iter().any(|known| host.eq_ignore_ascii_case(known)));

    claimed.then_some(parsed)
}

/// Non-empty path segments, in order.
fn segments(parsed: &Url) -> impl Iterator<Item = &str> {
    parsed.path().split('/').filter(|segment| !segment.is_empty())
}

/// Which share shape this is, if it is one we know.
///
/// Purely syntactic — nothing goes out over the network — so it is safe to
/// call while parsing arguments.
///
/// ```text
/// /scl/fi/<id>/<name>   file    (current links)
/// /scl/fo/<id>/<hash>   folder  (current links)
/// /s/<id>/<name>        file    (legacy, still live)
/// /sh/<id>/<hash>       folder  (legacy, still live)
/// ```
pub fn share_kind(url: &str) -> Option<Share> {
    let parsed = dropbox_url(url)?;
    let mut path = segments(&parsed);

    let share = match path.next()? {
        "scl" => match path.next()? {
            "fi" => Share::File,
            "fo" => Share::Folder,
            _ => return None,
        },
        "s" => Share::File,
        "sh" => Share::Folder,
        _ => return None,
    };

    // Every shape carries an id after its prefix. Without one there is nothing
    // to download: `/scl/fi/` on its own is a Dropbox error page, not a share.
    path.next()?;

    Some(share)
}

/// Does a Dropbox share link live behind this URL?
pub fn is_dropbox_url(url: &str) -> bool {
    share_kind(url).is_some()
}

/// Is this the folder-share shape?
///
/// Mirrors `mega::folder::is_folder_link` so callers can ask the same question
/// of every host. Note that a `true` here still means one download: it is a
/// zip, not a tree.
pub fn is_folder_link(url: &str) -> bool {
    share_kind(url) == Some(Share::Folder)
}

/// Rewrites a share link into a direct download.
pub fn resolve(url: &str) -> Result<DirectLink> {
    let share = share_kind(url)
        .context("Not a Dropbox share link — expected /scl/fi/, /scl/fo/, /s/ or /sh/")?;
    let parsed = dropbox_url(url).context("Not a Dropbox link")?;

    let fallback_name = match share {
        // The path name is the one the owner sees, so it is worth preferring.
        Share::File => file_name(&parsed).unwrap_or_else(|| share_handle(&parsed, None)),
        Share::Folder => share_handle(&parsed, Some("zip")),
    };

    Ok(DirectLink {
        url: with_download_flag(&parsed),
        share,
        fallback_name,
    })
}

/// The name a file share carries in its own path.
fn file_name(parsed: &Url) -> Option<String> {
    let mut path = segments(parsed);

    // Skip the shape prefix and the id; whatever follows is the name.
    let name = match path.next()? {
        "scl" => {
            path.next()?;
            path.next()?;
            path.next()?
        }
        _ => {
            path.next()?;
            path.next()?
        }
    };

    sanitize(&crate::engine::percent_decode(name))
}

/// Reduces a name to something that can only ever be a filename.
///
/// A path segment cannot hold a literal separator, but a percent-encoded one
/// survives decoding, so the last component is taken and the two traversal
/// names are refused outright.
fn sanitize(name: &str) -> Option<String> {
    let last = name.rsplit(['/', '\\']).next()?.trim();

    if last.is_empty() || last == "." || last == ".." {
        return None;
    }

    Some(last.to_owned())
}

/// A stand-in name for a link that does not carry one, with the extension we
/// know the response will have, if we know it.
fn share_handle(parsed: &Url, extension: Option<&str>) -> String {
    let id = share_id(parsed).unwrap_or("download");

    match extension {
        Some(ext) => format!("dropbox-{id}.{ext}"),
        None => format!("dropbox-{id}"),
    }
}

/// The opaque id Dropbox gives the share.
fn share_id(parsed: &Url) -> Option<&str> {
    let mut path = segments(parsed);

    match path.next()? {
        "scl" => {
            path.next()?;
            path.next()
        }
        _ => path.next(),
    }
}

/// Returns `parsed` with exactly one `dl=1`, every other parameter kept.
fn with_download_flag(parsed: &Url) -> String {
    let preserved: Vec<(String, String)> = parsed
        .query_pairs()
        .filter_map(|(key, value)| {
            let key = key.into_owned();
            (key != DOWNLOAD_FLAG).then(|| (key, value.into_owned()))
        })
        .collect();

    let mut direct = parsed.clone();
    {
        let mut query = direct.query_pairs_mut();
        query.clear();
        for (key, value) in &preserved {
            query.append_pair(key, value);
        }
        query.append_pair(DOWNLOAD_FLAG, "1");
    }

    String::from(direct)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_share_shapes_are_claimed() {
        assert_eq!(
            share_kind("https://www.dropbox.com/scl/fi/abc123/report.pdf?rlkey=k&dl=0"),
            Some(Share::File)
        );
        assert_eq!(
            share_kind("https://www.dropbox.com/scl/fo/abc123/h?rlkey=k&dl=0"),
            Some(Share::Folder)
        );
    }

    /// Old links never stopped working, so they are still recognised.
    #[test]
    fn legacy_share_shapes_are_claimed_too() {
        assert_eq!(
            share_kind("https://www.dropbox.com/s/abc123/report.pdf?dl=0"),
            Some(Share::File)
        );
        assert_eq!(
            share_kind("https://dropbox.com/sh/abc123/AABBCC?dl=0"),
            Some(Share::Folder)
        );
    }

    /// Lookalike hosts, Dropbox pages that are not downloads, and links that
    /// are already direct: all of them must fall through untouched.
    #[test]
    fn everything_else_is_left_alone() {
        for url in [
            "https://notdropbox.com/scl/fi/abc123/report.pdf",
            "https://www.dropbox.com.evil.com/scl/fi/abc123/report.pdf",
            "https://example.com/dropbox.com/s/abc123/report.pdf",
            "https://www.dropbox.com/home",
            "https://www.dropbox.com/scl/fi/",
            "https://www.dropbox.com/s/",
            // Already the CDN: rewriting this would be rewriting a URL that
            // needs no rewrite.
            "https://dl.dropboxusercontent.com/cd/0/get/abc/file.zip",
        ] {
            assert!(!is_dropbox_url(url), "should not claim {url}");
        }
    }

    #[test]
    fn folder_shares_are_told_apart_without_a_request() {
        assert!(is_folder_link("https://www.dropbox.com/scl/fo/abc123/h?rlkey=k"));
        assert!(is_folder_link("https://www.dropbox.com/sh/abc123/AABBCC"));
        assert!(!is_folder_link(
            "https://www.dropbox.com/scl/fi/abc123/report.pdf"
        ));
    }

    #[test]
    fn the_preview_flag_becomes_the_download_flag() {
        let link =
            resolve("https://www.dropbox.com/scl/fi/abc123/report.pdf?rlkey=k&st=t&dl=0").unwrap();

        assert!(link.url.contains("dl=1"));
        assert!(!link.url.contains("dl=0"));

        // Lose either of these and the server answers 404.
        assert!(link.url.contains("rlkey=k"));
        assert!(link.url.contains("st=t"));
    }

    /// `?dl=0&dl=1` resolves to the preview page, so a second flag is worse
    /// than none.
    #[test]
    fn the_download_flag_is_never_duplicated() {
        for url in [
            "https://www.dropbox.com/scl/fi/abc123/report.pdf?dl=0",
            "https://www.dropbox.com/scl/fi/abc123/report.pdf?dl=1",
            "https://www.dropbox.com/scl/fi/abc123/report.pdf",
        ] {
            let link = resolve(url).unwrap();
            assert_eq!(link.url.matches("dl=").count(), 1, "for {url}");
            assert!(link.url.ends_with("dl=1"), "for {url}");
        }
    }

    #[test]
    fn a_file_share_is_named_by_its_own_path() {
        let link =
            resolve("https://www.dropbox.com/scl/fi/abc123/holiday%20photos.zip?dl=0").unwrap();

        assert_eq!(link.share, Share::File);
        assert_eq!(link.fallback_name, "holiday photos.zip");
    }

    /// A folder link says nothing about the folder, and what arrives is always
    /// a zip.
    #[test]
    fn a_folder_share_falls_back_to_the_share_id() {
        let link = resolve("https://www.dropbox.com/scl/fo/abc123/h?rlkey=k&dl=0").unwrap();

        assert_eq!(link.share, Share::Folder);
        assert_eq!(link.fallback_name, "dropbox-abc123.zip");
    }

    /// The name comes off the network, so it must not be able to point outside
    /// the download directory.
    ///
    /// Encoded separators are the case worth testing, because they are the
    /// case that reaches us: URL parsing folds away a double-dot segment
    /// before we ever see it, `%2E%2E` included — that spelling turns
    /// `/scl/fi/<id>/%2E%2E` into `/scl/`, which is not a share link at all
    /// and is refused as one. `%2F` survives parsing and only becomes a
    /// separator when the name is decoded, which is where `sanitize` earns its
    /// keep.
    #[test]
    fn a_hostile_name_cannot_escape_the_download_directory() {
        // A path in a name is reduced to its last component.
        let traversal =
            resolve("https://www.dropbox.com/scl/fi/abc123/..%2F..%2Fetc%2Fpasswd?dl=0").unwrap();
        assert_eq!(traversal.fallback_name, "passwd");

        // Nothing after the last separator: the name is unusable, so the share
        // id stands in for it.
        let trailing = resolve("https://www.dropbox.com/scl/fi/abc123/..%2F..%2F?dl=0").unwrap();
        assert_eq!(trailing.fallback_name, "dropbox-abc123");

        // A last component of `..` is refused rather than joined onto the
        // download directory.
        let dots = resolve("https://www.dropbox.com/scl/fi/abc123/photos%2F..?dl=0").unwrap();
        assert_eq!(dots.fallback_name, "dropbox-abc123");
    }

    #[test]
    fn a_link_we_do_not_recognise_is_refused_with_a_reason() {
        let err = resolve("https://www.dropbox.com/home").expect_err("expected a refusal");
        assert!(err.to_string().contains("share link"), "got: {err}");
    }
}
