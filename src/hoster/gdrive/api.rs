//! Everything that talks to Google: the Drive API when there is a key, and
//! the `uc` endpoint plus its virus-scan warning page when there is not.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Response, StatusCode, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::{
    API_FILES, APPS_PREFIX, DOCS_BASE, DirectLink, DocKind, FOLDER_MIME, GdriveOptions, Listing,
    RemoteFile, Session, UC_ENDPOINT, fallback_name, safe_component, unique, with_extension,
};

/// The fields of a Drive file this module asks for.
#[derive(Debug, Deserialize)]
pub(super) struct FileMeta {
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

// ── URLs ─────────────────────────────────────────────────

/// `files/<id>[/<action>]?key=…`, which every API URL here starts as.
///
/// The id goes in through `path_segments_mut` rather than into a formatted
/// string, so an id that is not what it claimed to be cannot walk out of the
/// path it was given.
fn files_url(api_key: &str, id: &str, action: Option<&str>) -> Result<Url> {
    let mut url = Url::parse(API_FILES)?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow!("the Drive API base URL cannot take a path"))?;
        path.push(id);
        if let Some(action) = action {
            path.push(action);
        }
    }
    url.query_pairs_mut().append_pair("key", api_key);
    Ok(url)
}

/// What one item is, without fetching it.
fn metadata_url(api_key: &str, id: &str) -> Result<Url> {
    let mut url = files_url(api_key, id, None)?;
    url.query_pairs_mut()
        .append_pair("fields", "id,name,mimeType")
        .append_pair("supportsAllDrives", "true");
    Ok(url)
}

/// The bytes of an uploaded file.
fn media_url(api_key: &str, id: &str) -> Result<Url> {
    let mut url = files_url(api_key, id, None)?;
    url.query_pairs_mut()
        .append_pair("alt", "media")
        .append_pair("supportsAllDrives", "true");
    Ok(url)
}

/// A Google-native document, rendered into a real format.
fn api_export_url(api_key: &str, id: &str, mime: &str) -> Result<Url> {
    let mut url = files_url(api_key, id, Some("export"))?;
    url.query_pairs_mut().append_pair("mimeType", mime);
    Ok(url)
}

/// The same export, through the endpoint the Docs \u{201c}File \u{2192} Download\u{201d} menu
/// uses, which needs no key.
///
/// Slides and Drawings take the format as a path segment while Docs and Sheets
/// take it as a query parameter. Nobody's mistake, just two generations of one
/// endpoint, and both are still what the menu links to.
pub(super) fn docs_export_url(kind: DocKind, id: &str, ext: &str) -> Result<Url> {
    let mut url = Url::parse(DOCS_BASE)?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow!("the Docs base URL cannot take a path"))?;
        path.push(kind.segment()).push("d").push(id).push("export");
        if matches!(kind, DocKind::Presentation | DocKind::Drawing) {
            path.push(ext);
        }
    }
    if matches!(kind, DocKind::Document | DocKind::Spreadsheet) {
        url.query_pairs_mut().append_pair("format", ext);
    }
    Ok(url)
}

// ── With a key ────────────────────────────────────────────

/// What the API says about one item.
pub(super) async fn metadata(
    session: &Session,
    id: &str,
    options: &GdriveOptions,
) -> Result<FileMeta> {
    let url = metadata_url(&session.api_key, id)?;
    fetch_json(
        &session.client,
        url.as_str(),
        options.max_retries,
        "could not read the Drive item",
    )
    .await
}

/// The folder's own name, for the directory the download lands in.
pub(super) async fn folder_name(
    session: &Session,
    id: &str,
    options: &GdriveOptions,
) -> Result<Option<String>> {
    let meta = metadata(session, id, options).await?;
    Ok(meta.name.as_deref().map(safe_component))
}

/// The URL and filename to download one item by, given what the API said.
pub(super) fn direct_link(
    session: &Session,
    id: &str,
    meta: &FileMeta,
    options: &GdriveOptions,
) -> Result<DirectLink> {
    let name = meta
        .name
        .as_deref()
        .map(safe_component)
        .unwrap_or_else(|| fallback_name(id));
    let mime = meta.mime_type.as_deref().unwrap_or_default();

    if let Some((ext, export_mime)) = DocKind::from_mime(mime).map(|kind| kind.export_as(&options.doc_format))
    {
        return Ok(DirectLink {
            url: api_export_url(&session.api_key, id, export_mime)?.into(),
            name: with_extension(&name, ext),
            id: id.to_owned(),
        });
    }

    if mime.starts_with(APPS_PREFIX) {
        bail!(
            "'{name}' is a {mime}, which has nothing to download \u{2014} a shortcut points at \
             another item, and an Apps Script project is not a file"
        );
    }

    Ok(DirectLink {
        url: media_url(&session.api_key, id)?.into(),
        name,
        id: id.to_owned(),
    })
}

/// Everything under a folder, plus every directory seen on the way.
///
/// An explicit stack rather than recursion: a self-calling async fn has to be
/// boxed, and the stack keeps the collision bookkeeping in one place. Names are
/// made unique here rather than at download time, so the name a file is
/// announced under is the name it is written to.
pub(super) async fn walk(
    session: &Session,
    root: &str,
    options: &GdriveOptions,
) -> Result<Listing> {
    let mut files: Vec<RemoteFile> = Vec::new();
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut taken: HashSet<PathBuf> = HashSet::new();
    let mut unsupported = 0;
    let mut stack = vec![(root.to_owned(), PathBuf::new())];

    while let Some((folder, parent)) = stack.pop() {
        let mut page_token: Option<String> = None;

        loop {
            let mut url = Url::parse(API_FILES)?;
            {
                let mut query = url.query_pairs_mut();
                query
                    // Trashed children still list, and they are not there as
                    // far as anyone opening the folder is concerned.
                    .append_pair("q", &format!("'{folder}' in parents and trashed = false"))
                    .append_pair("fields", "nextPageToken,files(id,name,mimeType)")
                    .append_pair("pageSize", "1000")
                    .append_pair("supportsAllDrives", "true")
                    .append_pair("includeItemsFromAllDrives", "true")
                    .append_pair("key", &session.api_key);
                if let Some(token) = &page_token {
                    query.append_pair("pageToken", token);
                }
            }

            let page: FileList = fetch_json(
                &session.client,
                url.as_str(),
                options.max_retries,
                "could not list a Drive folder",
            )
            .await?;

            for item in &page.files {
                let Some(id) = item.id.as_deref() else {
                    unsupported += 1;
                    continue;
                };
                let name = safe_component(item.name.as_deref().unwrap_or_default());
                let mime = item.mime_type.as_deref().unwrap_or_default();

                if mime == FOLDER_MIME {
                    let dir = unique(&mut taken, parent.join(&name));
                    stack.push((id.to_owned(), dir.clone()));
                    dirs.push(dir);
                    continue;
                }

                let (url, filename) =
                    match DocKind::from_mime(mime).map(|kind| kind.export_as(&options.doc_format)) {
                        Some((ext, export_mime)) => (
                            api_export_url(&session.api_key, id, export_mime)?,
                            with_extension(&name, ext),
                        ),
                        // Every other Google-native type: a shortcut, which
                        // points somewhere else, or an Apps Script project,
                        // which the API will not export.
                        None if mime.starts_with(APPS_PREFIX) => {
                            unsupported += 1;
                            continue;
                        }
                        None => (media_url(&session.api_key, id)?, name),
                    };

                let relative = unique(&mut taken, parent.join(&filename));
                files.push(RemoteFile {
                    name: relative
                        .file_name()
                        .map(|leaf| leaf.to_string_lossy().into_owned())
                        .unwrap_or(filename),
                    relative,
                    url: url.into(),
                    id: id.to_owned(),
                });
            }

            match page.next {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }
    }

    Ok(Listing {
        files,
        dirs,
        unsupported,
    })
}

// ── Without a key ──────────────────────────────────────────

/// Resolves a file link anonymously.
///
/// Drive answers `uc?export=download` with the bytes for a small file and with
/// a virus-scan warning page for anything it will not scan. That page is not an
/// error and not a redirect: the real URL is the form's `action`, and the
/// `uuid` and `at` parameters in its hidden inputs are what make it work.
pub(super) async fn anonymous_file(
    client: &Client,
    id: &str,
    options: &GdriveOptions,
) -> Result<DirectLink> {
    let mut url = Url::parse(UC_ENDPOINT)?;
    url.query_pairs_mut()
        .append_pair("export", "download")
        .append_pair("id", id);

    let response = fetch(client, url.as_str(), options.max_retries).await?;
    let status = response.status();
    // Where the redirects ended, which for a small file is already the bytes.
    let landed = response.url().clone();

    if !status.is_success() {
        let body = response.bytes().await.unwrap_or_default();
        bail!("{}", explain(status, &body));
    }

    // Taken from this response rather than by asking the URL again: Drive
    // names the file in the answer it redirects to, and then answers a second,
    // ranged request for the same URL with the bytes and no
    // `Content-Disposition` at all. Asking twice is how a file ends up named
    // after its id.
    let disposition = crate::inspect::filename_from_content_disposition(response.headers());

    if let Some(name) = disposition {
        // Dropping the response unread is what keeps this from streaming the
        // whole file once to learn its name and again to save it.
        drop(response);
        return Ok(DirectLink {
            name: safe_component(&name),
            url: landed.into(),
            id: id.to_owned(),
        });
    }

    let page = response
        .text()
        .await
        .context("the Drive warning page could not be read")?;
    let confirmed = confirm_url(&page).ok_or_else(|| {
        anyhow!(
            "Drive served a page instead of a file, and it holds no download form \u{2014} the file \
             is over its share quota, needs sign-in, or is no longer shared"
        )
    })?;

    // The warning page prints the name of the file it is warning about, so the
    // confirmed URL is only asked when the page did not say.
    let name = match name_from_page(&page) {
        Some(name) => name,
        None => suggested_name(client, confirmed.as_str())
            .await
            .unwrap_or_else(|| fallback_name(id)),
    };

    Ok(DirectLink {
        url: confirmed.into(),
        name,
        id: id.to_owned(),
    })
}

/// The filename a URL says it serves, if it says.
///
/// The last thing tried before naming a file after its id, and the only one
/// that costs a request \u{2014} a single ranged byte, through [`crate::inspect`],
/// because a second `Content-Disposition` parser living here would be a copy of
/// tested code for no gain.
pub(super) async fn suggested_name(client: &Client, url: &str) -> Option<String> {
    let info = crate::inspect::inspect_url(client, url).await.ok()?;
    info.suggested_filename
        .as_deref()
        .map(safe_component)
        .filter(|name| name != "download.bin")
}

// ── The warning page ───────────────────────────────────────

/// A `<form>` on the page: where it goes, and the markup inside it.
struct Form<'a> {
    action: String,
    body: &'a str,
}

/// The URL the warning page's download form points at, hidden inputs and all.
///
/// Two shapes, in the order they stopped working. Today's form posts to
/// `drive.usercontent.google.com` and carries `id`, `export`, `confirm`, `uuid`
/// and `at` as hidden inputs, so the action alone is not enough; the older page
/// put everything in a plain link instead.
pub(super) fn confirm_url(page: &str) -> Option<Url> {
    if let Some(form) = download_form(page) {
        let mut url = Url::parse(&form.action).ok()?;
        let present: HashSet<String> = url
            .query_pairs()
            .map(|(name, _)| name.into_owned())
            .collect();

        let mut query = url.query_pairs_mut();
        for (name, value) in hidden_inputs(form.body) {
            // An older action already spells out `confirm` and `id`. Adding
            // them twice is how a download turns into a 400.
            if !present.contains(&name) {
                query.append_pair(&name, &value);
            }
        }
        drop(query);

        return Some(url);
    }

    let href = unescape(&anchor_href(page)?);
    if href.starts_with("http") {
        return Url::parse(&href).ok();
    }
    Url::parse(UC_ENDPOINT).ok()?.join(&href).ok()
}

/// The download form, preferring the one Drive labels as such.
///
/// The warning page also carries a search form, so the first `<form>` on it is
/// not a safe guess: the label `download-form` is what Drive has used for
/// years, and an action carrying `confirm=` is the older shape of the same
/// thing.
fn download_form(page: &str) -> Option<Form<'_>> {
    let mut fallback: Option<Form<'_>> = None;

    for candidate in page.split("<form").skip(1) {
        let Some((tag, rest)) = candidate.split_once('>') else {
            continue;
        };
        let Some(action) = attribute(tag, "action") else {
            continue;
        };
        let form = Form {
            action: unescape(&action),
            body: rest.split("</form").next().unwrap_or(rest),
        };

        if attribute(tag, "id").as_deref() == Some("download-form") {
            return Some(form);
        }
        if fallback.is_none() && form.action.contains("confirm=") {
            fallback = Some(form);
        }
    }

    fallback
}

/// Every named input of a form, in the order the page lists them.
///
/// Not filtered by `type="hidden"`, unlike the recipes this follows: Drive's
/// form has nothing visible in it, and an input that omits its type would
/// otherwise take `at` \u{2014} the one parameter the download will not work without \u{2014}
/// out of the query.
fn hidden_inputs(body: &str) -> Vec<(String, String)> {
    body.split("<input")
        .skip(1)
        .filter_map(|candidate| {
            let tag = candidate.split_once('>').map_or(candidate, |(tag, _)| tag);
            let name = attribute(tag, "name").filter(|name| !name.is_empty())?;
            Some((name, attribute(tag, "value").unwrap_or_default()))
        })
        .collect()
}

/// The `href` of the old `uc-download-link` anchor.
fn anchor_href(page: &str) -> Option<String> {
    page.split("<a")
        .skip(1)
        .filter_map(|candidate| candidate.split_once('>').map(|(tag, _)| tag))
        .find(|tag| tag.contains("uc-download-link"))
        .and_then(|tag| attribute(tag, "href"))
}

/// The filename Drive prints on its warning page.
///
/// Only reached when the confirmed URL did not name the file either, which
/// happens on the older page. Cheap to keep, and the alternative is naming a
/// file after its id.
pub(super) fn name_from_page(page: &str) -> Option<String> {
    let after_span = page.split_once("uc-name-size")?.1;
    let after_anchor = after_span.split_once("<a")?.1;
    let text = after_anchor.split_once('>')?.1.split('<').next()?;
    let name = unescape(text.trim());

    (!name.is_empty()).then(|| safe_component(&name))
}

/// The value of one attribute of a start tag.
///
/// Quoted values only, and the name has to start an attribute rather than end
/// one, so `data-action="…"` is not mistaken for `action`. Drive's markup is
/// machine-written and always quotes; an unquoted-attribute parser is the point
/// at which writing one of these by hand stops being reasonable.
fn attribute(tag: &str, name: &str) -> Option<String> {
    for quote in ['"', '\''] {
        if let Some((_, after)) = tag.split_once(&format!(" {name}={quote}")) {
            return Some(after.split(quote).next().unwrap_or_default().to_owned());
        }
    }
    None
}

/// Undoes the entity escaping in an HTML attribute.
///
/// `&amp;` goes last on purpose: unescaping it first would turn a literal
/// `&amp;lt;` in a filename into `<`.
fn unescape(text: &str) -> String {
    text.replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

// ── Requests ──────────────────────────────────────────────

/// Sends a GET until it answers, `retries` attempts at most.
///
/// A refusal comes back rather than being raised: only the caller knows whether
/// the body is an error worth quoting or a page worth reading.
async fn fetch(client: &Client, url: &str, retries: u32) -> Result<Response> {
    let mut last: Option<anyhow::Error> = None;

    for attempt in 0..retries.max(1) {
        match client.get(url).send().await {
            Ok(response) => {
                let status = response.status();
                if status.is_success() || is_final(status, &[]) {
                    return Ok(response);
                }
                last = Some(anyhow!("Google Drive answered HTTP {status}"));
            }
            Err(error) => last = Some(anyhow!(error)),
        }

        backoff(attempt).await;
    }

    Err(last
        .unwrap_or_else(|| anyhow!("no answer"))
        .context("could not reach Google Drive"))
}

/// Sends a GET until it answers with JSON that parses.
///
/// The error handling earns its space: a key whose project has the Drive API
/// switched off, a file nobody shared and a link that never existed all arrive
/// as a bare status code, and the body is usually the only thing that says
/// which.
async fn fetch_json<T>(client: &Client, url: &str, retries: u32, what: &'static str) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut last: Option<anyhow::Error> = None;

    for attempt in 0..retries.max(1) {
        match client.get(url).send().await {
            Ok(response) => {
                let status = response.status();
                match response.bytes().await {
                    Ok(body) if status.is_success() => match serde_json::from_slice(&body) {
                        Ok(parsed) => return Ok(parsed),
                        Err(error) => {
                            last = Some(anyhow!(error).context(format!(
                                "the Drive API answered with something other than the JSON asked for: {}",
                                snippet(&body)
                            )));
                        }
                    },
                    Ok(body) => {
                        let final_answer = is_final(status, &body);
                        last = Some(anyhow!("{}", explain(status, &body)));
                        if final_answer {
                            break;
                        }
                    }
                    Err(error) => {
                        last = Some(anyhow!(error).context("the answer could not be read"));
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
///
/// 403 is the awkward one: Drive uses it both for "this key may not" and for
/// "slow down", and only the reason in the body tells them apart.
fn is_final(status: StatusCode, body: &[u8]) -> bool {
    if status == StatusCode::FORBIDDEN {
        return !String::from_utf8_lossy(body).contains("ateLimitExceeded");
    }

    matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::UNAUTHORIZED
            | StatusCode::NOT_FOUND
            | StatusCode::GONE
    )
}

/// Turns a refusal into something worth reading.
fn explain(status: StatusCode, body: &[u8]) -> String {
    let hint = match status {
        StatusCode::BAD_REQUEST => " \u{2014} the API key looks malformed",
        StatusCode::UNAUTHORIZED => " \u{2014} the API key was rejected",
        StatusCode::FORBIDDEN => {
            " \u{2014} the file is over its download quota, the share is restricted, or the key's project has the Drive API switched off"
        }
        StatusCode::NOT_FOUND | StatusCode::GONE => {
            " \u{2014} no such item, or it is not shared with everyone holding the link"
        }
        StatusCode::TOO_MANY_REQUESTS => " \u{2014} Google is rate limiting this key",
        _ => "",
    };

    format!("Google Drive answered HTTP {status}{hint}: {}", snippet(body))
}

/// As much of a body as belongs in an error message.
fn snippet(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");

    if flat.is_empty() {
        return "<empty response>".to_owned();
    }
    match flat.char_indices().nth(200) {
        Some((cut, _)) => format!("{}\u{2026}", &flat[..cut]),
        None => flat,
    }
}

/// Waits before the next attempt, a little longer each time.
async fn backoff(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(250 << attempt.min(5))).await;
}
