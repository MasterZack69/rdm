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
//!
//! ## Password-protected shares
//!
//! These are the one case that cannot be handled by rewriting a URL, because
//! the authorisation lives in a session rather than in the link. Left alone, a
//! protected share answers `dl=1` with its password page, and rdm would write
//! that HTML to disk under the name of the file the user wanted.
//!
//! [`open`] performs the handshake and hands back a client holding the
//! authenticated cookies, which [`crate::engine`] then downloads with — so
//! resume, ranges and parallel chunks still come from the engine rather than
//! being reimplemented here. See [`content_id`] for the part Dropbox makes
//! awkward.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine as _;
use reqwest::Client;
use reqwest::Url;
use reqwest::cookie::{CookieStore, Jar};

/// Hosts this module answers for, compared whole and case-insensitively:
/// `notdropbox.com` is not Dropbox, and neither is `dropbox.com.evil.com`.
const HOSTS: [&str; 2] = ["dropbox.com", "www.dropbox.com"];

/// The query flag that picks between the preview page (`0`) and the file
/// itself (`1`).
const DOWNLOAD_FLAG: &str = "dl";

/// Where a share password is posted.
const AUTH_ENDPOINT: &str = "https://www.dropbox.com/sm/auth";

/// What `/sm/auth` answers when the password was right.
const AUTHED: &str = "authed";

/// The JavaScript call the share page hides its prefetched state inside.
const PREFETCH_CALL: &str = "registerStreamedPrefetch";

/// What marks the prefetched blob that describes the password form.
const PASSWORD_MARKER: &str = "/sm/password";

/// The environment variable a share password is read from.
///
/// An environment variable rather than a flag, matching `RDM_GOFILE_PASSWORD`:
/// a password on the command line ends up in shell history and in `ps` output
/// for every other user on the machine.
pub const PASSWORD_ENV: &str = "RDM_DROPBOX_PASSWORD";

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
    parsed
        .path()
        .split('/')
        .filter(|segment| !segment.is_empty())
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

/// Returns `parsed` with exactly one `dl=<value>`, every other parameter kept.
fn with_flag(parsed: &Url, value: &str) -> String {
    let preserved: Vec<(String, String)> = parsed
        .query_pairs()
        .filter_map(|(key, existing)| {
            let key = key.into_owned();
            (key != DOWNLOAD_FLAG).then(|| (key, existing.into_owned()))
        })
        .collect();

    let mut rewritten = parsed.clone();
    {
        let mut query = rewritten.query_pairs_mut();
        query.clear();
        for (key, existing) in &preserved {
            query.append_pair(key, existing);
        }
        query.append_pair(DOWNLOAD_FLAG, value);
    }

    String::from(rewritten)
}

/// The link that serves the file.
fn with_download_flag(parsed: &Url) -> String {
    with_flag(parsed, "1")
}

/// The link that serves the preview page.
///
/// Worth being explicit about rather than reusing whatever the user pasted: if
/// they hand us a link that already says `dl=1`, fetching it as-is to look for
/// a password form would download the entire file into a `String`.
fn with_preview_flag(parsed: &Url) -> String {
    with_flag(parsed, "0")
}

// ── Password-protected shares ───────────────────────────────────────────

/// Reads the share password from the environment, if it is set to anything.
pub fn password_from_env() -> Option<String> {
    std::env::var(PASSWORD_ENV)
        .ok()
        .filter(|password| !password.trim().is_empty())
}

/// A client that keeps cookies, plus the jar so the CSRF token can be read
/// back out of it.
///
/// Matches the engine's own client settings, since whatever this returns may
/// end up doing the download.
fn session() -> Result<(Client, Arc<Jar>)> {
    let jar = Arc::new(Jar::default());
    let client = Client::builder()
        .user_agent("rdm")
        .connect_timeout(Duration::from_secs(10))
        .cookie_provider(Arc::clone(&jar))
        .build()
        .context("Failed to build HTTP client")?;

    Ok((client, jar))
}

/// Opens a share, authenticating first if it turns out to be password-
/// protected.
///
/// `Ok(None)` means the share is public and the caller should download it with
/// the engine's own client, so nothing is spent on a session it does not need.
/// `Ok(Some(client))` carries the authenticated cookies and must be used for
/// the download itself.
///
/// This costs one small HTML GET on every Dropbox download, public ones
/// included. That buys the difference between a clear "this share needs a
/// password" and silently saving a password page under the name of the file
/// the user asked for, which is worth far more than one request against a
/// download measured in megabytes.
pub async fn open(url: &str, password: Option<&str>) -> Result<Option<Client>> {
    let parsed = dropbox_url(url).context("Not a Dropbox link")?;
    let (client, jar) = session()?;

    let page = client
        .get(with_preview_flag(&parsed))
        .send()
        .await
        .context("Failed to open the Dropbox share page")?
        .text()
        .await
        .context("Failed to read the Dropbox share page")?;

    let Some(content_id) = content_id(&page) else {
        return Ok(None);
    };

    let password = password.with_context(|| {
        format!("This Dropbox share is password-protected — set {PASSWORD_ENV} to its password")
    })?;

    let token = csrf_token(&jar, &parsed)
        .context("Dropbox did not set the CSRF cookie that its password form needs")?;

    // Dropbox wants the link without its scheme or host.
    let relative = relative_link(&parsed);

    let answer = client
        .post(AUTH_ENDPOINT)
        .form(&[
            ("is_xhr", "true"),
            ("t", token.as_str()),
            ("content_id", content_id.as_str()),
            ("password", password),
            ("url", relative.as_str()),
        ])
        .send()
        .await
        .context("Failed to send the share password to Dropbox")?;

    let status = answer.status();
    if !status.is_success() {
        anyhow::bail!(
            "Dropbox refused the password request with status {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown"),
        );
    }

    // A wrong password is a 200 with a different status field, so it is the
    // body that decides, not the code.
    let answer: serde_json::Value = answer
        .json()
        .await
        .context("Dropbox's answer to the password was not JSON")?;
    let outcome = answer
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("no status");

    if outcome != AUTHED {
        anyhow::bail!("Dropbox rejected the password for this share (answered \"{outcome}\")");
    }

    Ok(Some(client))
}

/// The `content_id` of the password form, if this page is asking for one.
///
/// Doubles as the "is this share protected?" test, because a page that is not
/// asking for a password has no password form to identify.
///
/// Dropbox does not put it in the markup. The page carries base64 blobs of
/// prefetched state, so they are decoded and the one describing the password
/// form — the one mentioning `/sm/password` — is the one that holds the id.
/// Later blobs supersede earlier ones, hence the reverse iteration.
fn content_id(page: &str) -> Option<String> {
    prefetched(page)
        .iter()
        .rev()
        .filter(|blob| blob.contains(PASSWORD_MARKER))
        .find_map(|blob| content_id_in(blob))
}

/// Every prefetched blob on the page, decoded, in document order.
///
/// A blob that will not decode is skipped rather than fatal: the page is full
/// of them, we are looking for one, and Dropbox is free to change the rest.
fn prefetched(page: &str) -> Vec<String> {
    page.split(PREFETCH_CALL)
        .skip(1)
        .filter_map(|call| {
            // registerStreamedPrefetch("<key>", "<payload>") — stop at the
            // closing paren so a call without a payload cannot reach forward
            // into unrelated markup for its second string.
            let head = call.split(')').next()?;
            let payload = nth_quoted(head, 1)?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(payload)
                .ok()?;

            Some(String::from_utf8_lossy(&bytes).into_owned())
        })
        .collect()
}

/// The `n`th double-quoted string in `text`, counting from zero.
///
/// Sound for the JavaScript this reads, whose arguments are base64 and cannot
/// contain a quote or an escape.
fn nth_quoted(text: &str, n: usize) -> Option<&str> {
    text.split('"').nth(n * 2 + 1)
}

/// Pulls `content_id=<value>` out of a decoded blob.
fn content_id_in(blob: &str) -> Option<String> {
    let value: String = blob
        .split_once("content_id=")?
        .1
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '+' | '=' | '/' | '-' | '_'))
        .collect();

    (!value.is_empty()).then_some(value)
}

/// The CSRF token Dropbox expects echoed back in the form body.
///
/// Read out of the jar rather than off the response, because Dropbox is free
/// to set it on any hop of a redirect chain and only the last response's
/// headers are visible afterwards.
fn csrf_token(jar: &Jar, url: &Url) -> Option<String> {
    let header = jar.cookies(url)?;

    cookie_value(header.to_str().ok()?, "t")
}

/// Picks one cookie out of a `Cookie:` header value.
fn cookie_value(header: &str, name: &str) -> Option<String> {
    header
        .split(';')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| key.trim() == name)
        .map(|(_, value)| value.trim().to_owned())
}

/// The share link as `/path?query`, which is the shape `/sm/auth` expects.
fn relative_link(parsed: &Url) -> String {
    match parsed.query() {
        Some(query) => format!("{}?{}", parsed.path(), query),
        None => parsed.path().to_owned(),
    }
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
        assert!(is_folder_link(
            "https://www.dropbox.com/scl/fo/abc123/h?rlkey=k"
        ));
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

    /// The page has to be fetched as a page. A link that already says `dl=1`
    /// would otherwise be read into a `String` in full.
    #[test]
    fn the_share_page_is_always_asked_for_as_a_preview() {
        let parsed =
            dropbox_url("https://www.dropbox.com/scl/fi/abc123/report.pdf?rlkey=k&dl=1").unwrap();
        let preview = with_preview_flag(&parsed);

        assert!(preview.ends_with("dl=0"));
        assert_eq!(preview.matches("dl=").count(), 1);
        assert!(preview.contains("rlkey=k"));
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

    // ── Password-protected shares ──

    /// Builds a page carrying `payload` the way Dropbox does: base64, inside a
    /// `registerStreamedPrefetch` call.
    fn page_with(payloads: &[&str]) -> String {
        let mut page = String::from("<!doctype html><html><body>");
        for payload in payloads {
            let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
            page.push_str(&format!(
                "<script>registerStreamedPrefetch(\"key\", \"{encoded}\");</script>"
            ));
        }
        page.push_str("</body></html>");

        page
    }

    /// The id is the whole point of decoding the blobs, and its character set
    /// is wider than it looks: it has to stop at the `&`, not at the `+`, `/`
    /// or `=`.
    #[test]
    fn the_password_form_is_found_inside_a_prefetched_blob() {
        let page = page_with(&[
            "{\"route\":\"/sharing/view\"}",
            "{\"form\":\"/sm/password?content_id=AbC-1.2+3/4_5=&next=/home\"}",
        ]);

        assert_eq!(content_id(&page).as_deref(), Some("AbC-1.2+3/4_5="));
    }

    /// A public share has no password form, which is exactly how it is
    /// recognised as public.
    #[test]
    fn a_page_without_a_password_form_needs_no_password() {
        let page = page_with(&[
            "{\"route\":\"/sharing/view\"}",
            "{\"preview\":\"/scl/fi/abc123/report.pdf\"}",
        ]);

        assert_eq!(content_id(&page), None);
        assert_eq!(content_id("<html>nothing prefetched here</html>"), None);
    }

    /// The page is full of blobs meant for the browser, and Dropbox owes us no
    /// stability in any of them. One that will not decode, or a call with no
    /// payload, must not stop us finding the one we came for.
    #[test]
    fn unreadable_blobs_are_skipped_rather_than_fatal() {
        let good = base64::engine::general_purpose::STANDARD
            .encode("{\"form\":\"/sm/password?content_id=OK123\"}");
        let page = format!(
            "<script>registerStreamedPrefetch(\"key\", \"not!valid!base64!\");</script>\
             <script>registerStreamedPrefetch(\"key\");</script>\
             <script>registerStreamedPrefetch(\"key\", \"{good}\");</script>\
             <p>content_id=NOT_FROM_A_BLOB</p>"
        );

        assert_eq!(content_id(&page).as_deref(), Some("OK123"));
    }

    /// Markup outside a prefetched blob is not a source of ids, even when it
    /// contains the words we are looking for.
    #[test]
    fn the_id_is_only_taken_from_a_decoded_blob() {
        let page = "<p>/sm/password?content_id=FROM_THE_MARKUP</p>";

        assert_eq!(content_id(page), None);
    }

    #[test]
    fn the_csrf_token_is_read_out_of_the_jar() {
        let url = Url::parse("https://www.dropbox.com/scl/fi/abc123/report.pdf").unwrap();
        let jar = Jar::default();
        jar.add_cookie_str("gvc=99; Path=/", &url);
        jar.add_cookie_str("t=tok123; Path=/", &url);

        assert_eq!(csrf_token(&jar, &url).as_deref(), Some("tok123"));
    }

    #[test]
    fn a_jar_without_the_token_yields_nothing() {
        let url = Url::parse("https://www.dropbox.com/scl/fi/abc123/report.pdf").unwrap();
        let jar = Jar::default();
        jar.add_cookie_str("gvc=99; Path=/", &url);

        assert_eq!(csrf_token(&jar, &url), None);
    }

    #[test]
    fn one_cookie_is_picked_out_of_many() {
        assert_eq!(
            cookie_value("gvc=99; t=tok123; locale=en", "t").as_deref(),
            Some("tok123")
        );
        assert_eq!(cookie_value("t=tok123", "t").as_deref(), Some("tok123"));
        // A name that merely ends in the one we want is a different cookie.
        assert_eq!(cookie_value("st=nope", "t"), None);
        assert_eq!(cookie_value("", "t"), None);
    }

    #[test]
    fn the_endpoint_is_given_the_link_without_its_host() {
        let parsed =
            dropbox_url("https://www.dropbox.com/scl/fi/abc123/report.pdf?rlkey=k&dl=0").unwrap();
        assert_eq!(
            relative_link(&parsed),
            "/scl/fi/abc123/report.pdf?rlkey=k&dl=0"
        );

        let bare = dropbox_url("https://www.dropbox.com/s/abc123/report.pdf").unwrap();
        assert_eq!(relative_link(&bare), "/s/abc123/report.pdf");
    }

    /// An unset or blank variable means "no password", not an empty one.
    #[test]
    fn a_blank_password_variable_counts_as_unset() {
        assert_eq!(PASSWORD_ENV, "RDM_DROPBOX_PASSWORD");

        // SAFETY: single-threaded test, and the variable is read back here
        // rather than by anything else.
        unsafe {
            std::env::set_var(PASSWORD_ENV, "   ");
        }
        assert_eq!(password_from_env(), None);

        unsafe {
            std::env::set_var(PASSWORD_ENV, "hunter2");
        }
        assert_eq!(password_from_env().as_deref(), Some("hunter2"));

        unsafe {
            std::env::remove_var(PASSWORD_ENV);
        }
        assert_eq!(password_from_env(), None);
    }
}
