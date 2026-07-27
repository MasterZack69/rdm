//! Directory scraper.
//!
//! Discovers files served by a "directory listing" style HTTP endpoint
//! (Apache/nginx autoindex, S3 web index, etc.) by recursively parsing
//! anchor links. Hardened against:
//!   - Path traversal (decoded *and* encoded)
//!   - SSRF via private/loopback/link-local addresses
//!   - Redirects that escape the base scope
//!   - Oversized response bodies
//!   - HTML parser false positives (script/style/comments, attribute boundaries)
//!   - Cross-directory duplicates
//!   - Windows-specific filename quirks (drive letters, reserved names, trailing dots)
//!
//! Progress is reported through a single [`ui::ScanSpinner`] line rather than
//! one `Scanning ...` line per directory.
//!
//! Public API preserved:
//!   - `pub struct DiscoveredFile { pub url: String, pub relative_path: String }`
//!   - `pub async fn discover_files(url: &str, wrap_in_folder: bool, allow_private: bool)
//!         -> Result<Option<Vec<DiscoveredFile>>>`

use anyhow::{Context, Result};
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

use reqwest::Url; // change to `use url::Url;` if your reqwest version doesn't re-export it.

use crate::engine;
use crate::ui;

// ---------- Public types ----------

pub struct DiscoveredFile {
    pub url: String,
    pub relative_path: String,
}

// ---------- Tunables ----------

const MAX_DEPTH: u32 = 10;
const MAX_DIRS: usize = 500;
const MAX_FILES: usize = 10_000;
const CONCURRENCY: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// (5) Cap how much HTML we'll buffer per directory.
const MAX_HTML_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

// (1, 2) Filename / path sanitization caps.
const MAX_PATH_COMPONENT_LEN: usize = 255;
const MAX_RELATIVE_PATH_LEN: usize = 4096;

// ---------- Entry point ----------

pub async fn discover_files(
    url: &str,
    wrap_in_folder: bool,
    allow_private: bool,
) -> Result<Option<Vec<DiscoveredFile>>> {
    let base_url = parse_and_validate_url(url, allow_private).context("Invalid base URL")?;
    let base_url = ensure_trailing_slash(base_url);

    let client = reqwest::Client::builder()
        .user_agent("rdm")
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .context("Failed to build HTTP client")?;

    let folder_name = derive_folder_name(&base_url);

    let base_str = base_url.as_str().to_string();

    let mut files: Vec<DiscoveredFile> = Vec::new();
    let mut seen_files: HashSet<String> = HashSet::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut current_level: Vec<(Url, u32)> = vec![(base_url.clone(), 0)];
    let sem = Arc::new(Semaphore::new(CONCURRENCY));

    // One live line for the whole scan instead of a line per directory.
    let spinner = ui::ScanSpinner::new();

    let mut hard_stopped = false;

    while !current_level.is_empty() && !hard_stopped {
        let mut tasks = tokio::task::JoinSet::new();

        for (dir_url, depth) in current_level.drain(..) {
            if depth > MAX_DEPTH {
                continue;
            }
            let key = dir_url.as_str().to_string();
            if !visited.insert(key) {
                continue;
            }

            if visited.len() > MAX_DIRS {
                spinner.note(&format!(
                    "   \u{26a0} Directory limit reached ({}), stopping scan",
                    MAX_DIRS
                ));
                hard_stopped = true;
                break;
            }

            spinner.dir(&directory_label(&base_url, &dir_url));

            let client_c = client.clone();
            let sem_c = sem.clone();
            let base_c = base_url.clone();

            tasks.spawn(async move {
                let _permit = match sem_c.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        return (
                            Err(anyhow::anyhow!("semaphore closed")) as Result<_>,
                            depth,
                            dir_url,
                        );
                    }
                };
                let res = fetch_and_parse(&client_c, &dir_url, &base_c).await;
                (res, depth, dir_url)
            });
        }

        let mut next_level: Vec<(Url, u32)> = Vec::new();
        let mut next_seen: HashSet<String> = HashSet::new();

        while let Some(joined) = tasks.join_next().await {
            let (result, depth, dir_url) = match joined {
                Ok(v) => v,
                Err(e) => {
                    spinner.note(&format!("   \u{26a0} task join error: {}", e));
                    continue;
                }
            };

            let (found_files, found_dirs) = match result {
                Ok(Some(r)) => r,
                Ok(None) => continue,
                Err(e) => {
                    spinner.note(&format!("   \u{26a0} failed to scan {}: {:#}", dir_url, e));
                    continue;
                }
            };

            for file_url in found_files {
                if !seen_files.insert(file_url.clone()) {
                    continue;
                }

                let raw_relative = match file_url.strip_prefix(&base_str) {
                    Some(s) => s.to_string(),
                    None => continue,
                };

                let safe = match sanitize_relative_path(&raw_relative) {
                    Some(s) => s,
                    None => continue,
                };

                let final_rel = if wrap_in_folder {
                    format!("{}/{}", folder_name, safe)
                } else {
                    safe
                };

                files.push(DiscoveredFile {
                    url: file_url,
                    relative_path: final_rel,
                });
                spinner.add_files(1);

                if files.len() >= MAX_FILES {
                    spinner.note(&format!(
                        "   \u{26a0} File limit reached ({}), stopping scan",
                        MAX_FILES
                    ));
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    hard_stopped = true;
                    break;
                }
            }
            if hard_stopped {
                break;
            }

            for sub in found_dirs {
                let k = sub.as_str().to_string();
                if !visited.contains(&k) && next_seen.insert(k) {
                    next_level.push((sub, depth + 1));
                }
            }
        }

        current_level = next_level;
    }

    spinner.finish();

    if files.is_empty() {
        return Ok(None);
    }
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(Some(files))
}

// ---------- URL parsing & scope ----------

fn parse_and_validate_url(s: &str, allow_private: bool) -> Result<Url> {
    let url = Url::parse(s).context("URL parse failed")?;
    match url.scheme() {
        "http" | "https" => {}
        other => anyhow::bail!("Unsupported URL scheme: {}", other),
    }
    let host_str = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("URL has no host"))?;

    let skip_private_check = allow_private || std::env::var_os("RDM_ALLOW_PRIVATE").is_some();
    if !skip_private_check
        && let Some(ip) = parse_host_as_ip(host_str)
        && is_disallowed_ip(ip)
    {
        anyhow::bail!("Refusing to scan private/internal address: {}", ip);
    }
    Ok(url)
}

fn parse_host_as_ip(host: &str) -> Option<IpAddr> {
    let trimmed = host.trim_start_matches('[').trim_end_matches(']');
    trimmed.parse::<IpAddr>().ok()
}

fn is_disallowed_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.octets()[0] == 0
                || v4.octets()[0] >= 224 // multicast / reserved
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
        }
    }
}

fn ensure_trailing_slash(mut url: Url) -> Url {
    url.set_fragment(None);
    if !url.path().ends_with('/') {
        let new_path = format!("{}/", url.path());
        url.set_path(&new_path);
    }
    url
}

fn is_under_base(url: &Url, base: &Url) -> bool {
    if url.scheme() != base.scheme() {
        return false;
    }
    if url.host_str() != base.host_str() {
        return false;
    }
    if url.port_or_known_default() != base.port_or_known_default() {
        return false;
    }
    // base.path() ends with '/' by construction.
    url.path().starts_with(base.path())
}

fn derive_folder_name(base: &Url) -> String {
    let path = base.path().trim_end_matches('/');
    let last = path.rsplit('/').next().unwrap_or("");
    let candidate = if last.is_empty() {
        base.host_str().unwrap_or("download").to_string()
    } else {
        engine::percent_decode(last)
    };
    sanitize_path_component(&candidate).unwrap_or_else(|| "download".to_string())
}

fn directory_label(base: &Url, dir: &Url) -> String {
    let rel = dir
        .as_str()
        .strip_prefix(base.as_str())
        .unwrap_or(dir.as_str());
    let decoded = engine::percent_decode(rel);
    let trimmed = decoded.trim_end_matches('/');
    let last = trimmed.rsplit('/').next().unwrap_or("");
    if last.is_empty() {
        dir.as_str().to_string()
    } else {
        last.to_string()
    }
}

// ---------- Path sanitization ----------

// (1) Decode first, then validate every component. Reject:
//   - empty / overly long paths
//   - null bytes / control chars
//   - backslashes (Windows separator), absolute paths, drive letters, UNC
//   - any "." or ".." component (post-decode)
//   - Windows reserved device names
//   - components that are only dots/spaces (Windows quirks)
fn sanitize_relative_path(raw: &str) -> Option<String> {
    let decoded = engine::percent_decode(raw);
    if decoded.is_empty() || decoded.len() > MAX_RELATIVE_PATH_LEN {
        return None;
    }
    if decoded.contains('\0') || decoded.contains('\\') {
        return None;
    }
    if decoded.starts_with('/') {
        return None;
    }
    if has_windows_drive_or_unc(&decoded) {
        return None;
    }

    let mut clean: Vec<String> = Vec::new();
    for comp in decoded.split('/') {
        if comp.is_empty() || comp == "." || comp == ".." {
            return None;
        }
        if comp.len() > MAX_PATH_COMPONENT_LEN {
            return None;
        }
        if comp.chars().any(|c| (c as u32) < 0x20 || c == '\u{7f}') {
            return None;
        }
        if is_windows_reserved(comp) {
            return None;
        }
        let stripped = comp.trim_end_matches(['.', ' ']);
        if stripped.is_empty() {
            return None;
        }
        clean.push(comp.to_string());
    }
    if clean.is_empty() {
        None
    } else {
        Some(clean.join("/"))
    }
}

fn sanitize_path_component(s: &str) -> Option<String> {
    if s.contains('/') {
        return None;
    }
    sanitize_relative_path(s)
}

fn has_windows_drive_or_unc(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return true;
    }
    s.starts_with("\\\\") // UNC; covered by '\\' check too, but explicit
}

fn is_windows_reserved(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name).to_ascii_uppercase();
    matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL"
        | "COM1" | "COM2" | "COM3" | "COM4" | "COM5"
        | "COM6" | "COM7" | "COM8" | "COM9"
        | "LPT1" | "LPT2" | "LPT3" | "LPT4" | "LPT5"
        | "LPT6" | "LPT7" | "LPT8" | "LPT9"
    )
}

// ---------- HTTP fetch ----------

async fn fetch_and_parse(
    client: &reqwest::Client,
    url: &Url,
    base: &Url,
) -> Result<Option<(Vec<String>, Vec<Url>)>> {
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("Failed to fetch {}", url))?;

    if !response.status().is_success() {
        anyhow::bail!("non-success status {}", response.status());
    }

    // (21) Reject redirected responses that escape the base scope.
    let final_url = response.url().clone();
    if !is_under_base(&final_url, base) {
        anyhow::bail!("redirect escaped base scope: {}", final_url);
    }

    // (20) Accept text/html and application/xhtml+xml; match the *essence* type.
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(extract_mime_essence)
        .unwrap_or_default();

    if content_type != "text/html" && content_type != "application/xhtml+xml" {
        return Ok(None);
    }

    // (5) Bound the body size.
    let body = read_capped_body(response, MAX_HTML_BYTES).await?;
    let body_text = String::from_utf8_lossy(&body);

    let final_url = ensure_trailing_slash(final_url);
    Ok(Some(parse_links(&body_text, &final_url, base)))
}

fn extract_mime_essence(ct: &str) -> String {
    ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase()
}

async fn read_capped_body(mut response: reqwest::Response, max: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    while let Some(chunk) = response.chunk().await.context("reading response body")? {
        if buf.len().saturating_add(chunk.len()) > max {
            anyhow::bail!("response body exceeds {} bytes", max);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

// ---------- Link parsing ----------

fn parse_links(html: &str, page_url: &Url, base: &Url) -> (Vec<String>, Vec<Url>) {
    let mut files: Vec<String> = Vec::new();
    let mut dirs: Vec<Url> = Vec::new();
    let mut seen_files: HashSet<String> = HashSet::new();
    let mut seen_dirs: HashSet<String> = HashSet::new();

    for raw_href in extract_hrefs(html) {
        // (13) Decode the most common HTML entities in href values.
        let href_owned = decode_html_entities(&raw_href);
        let href = href_owned.trim();

        if href.is_empty()
            || href == "../"
            || href == "./"
            || href == ".."
            || href == "."
            || href == "/"
            || href.starts_with('#')
            || href.starts_with('?')
        {
            continue;
        }

        let lower = href.to_ascii_lowercase();
        if lower.starts_with("javascript:")
            || lower.starts_with("mailto:")
            || lower.starts_with("data:")
            || lower.starts_with("file:")
            || lower.starts_with("ftp:")
        {
            continue;
        }

        // (16, 18) Use real URL resolution. Handles absolute, protocol-relative,
        // root-relative and path-relative hrefs correctly.
        let mut resolved = match page_url.join(href) {
            Ok(u) => u,
            Err(_) => continue,
        };
        resolved.set_fragment(None);

        if resolved.scheme() != "http" && resolved.scheme() != "https" {
            continue;
        }

        // (2) Proper containment check (origin + path prefix).
        if !is_under_base(&resolved, base) {
            continue;
        }

        if resolved.path().ends_with('/') {
            let key = resolved.as_str().to_string();
            if seen_dirs.insert(key) {
                dirs.push(resolved);
            }
        } else {
            let s = resolved.as_str().to_string();
            if seen_files.insert(s.clone()) {
                files.push(s);
            }
        }
    }

    files.sort();
    dirs.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    (files, dirs)
}

// (10-14) Robust state-machine href extractor:
//   - Skips <script>, <style>, and HTML comments
//   - Requires an attribute boundary before `href` (rejects `data-href`)
//   - Handles all HTML whitespace (space, tab, CR, LF, FF)
//   - Supports double-quoted, single-quoted, and unquoted values
//   - Doesn't read past EOF
fn extract_hrefs(html: &str) -> Vec<String> {
    let bytes = html.as_bytes();
    let len = bytes.len();
    let mut hrefs = Vec::new();
    let mut i = 0;
    let mut in_tag = false;
    let mut tag_start: usize = 0;
    let mut in_script = false;
    let mut in_style = false;
    let mut in_comment = false;

    while i < len {
        if in_comment {
            if i + 3 <= len && &bytes[i..i + 3] == b"-->" {
                in_comment = false;
                i += 3;
            } else {
                i += 1;
            }
            continue;
        }

        if !in_tag {
            // Detect comment start
            if i + 4 <= len && &bytes[i..i + 4] == b"<!--" {
                in_comment = true;
                i += 4;
                continue;
            }
            if bytes[i] == b'<' {
                // Inside <script>/<style>, only look for end tags.
                if in_script {
                    if i + 9 <= len && bytes[i..i + 9].eq_ignore_ascii_case(b"</script>") {
                        in_script = false;
                        i += 9;
                        continue;
                    }
                    i += 1;
                    continue;
                }
                if in_style {
                    if i + 8 <= len && bytes[i..i + 8].eq_ignore_ascii_case(b"</style>") {
                        in_style = false;
                        i += 8;
                        continue;
                    }
                    i += 1;
                    continue;
                }

                // Detect <script ...> / <style ...>
                if i + 7 < len
                    && bytes[i + 1..i + 7].eq_ignore_ascii_case(b"script")
                    && (bytes[i + 7] == b'>' || is_html_whitespace(bytes[i + 7]))
                {
                    in_script = true;
                }
                if i + 6 < len
                    && bytes[i + 1..i + 6].eq_ignore_ascii_case(b"style")
                    && (bytes[i + 6] == b'>' || is_html_whitespace(bytes[i + 6]))
                {
                    in_style = true;
                }

                in_tag = true;
                tag_start = i;
                i += 1;
                continue;
            }
            i += 1;
            continue;
        }

        // in_tag == true
        if in_script || in_style {
            // We're past the opening `<` but inside the script/style tag content.
            if bytes[i] == b'>' {
                in_tag = false;
            }
            i += 1;
            continue;
        }

        if bytes[i] == b'>' {
            in_tag = false;
            i += 1;
            continue;
        }

        // (12) Require an attribute boundary before `href`. The byte immediately
        // before must be whitespace, `<`, or `/` (for self-closing variations).
        let boundary_ok = i > tag_start
            && (is_html_whitespace(bytes[i - 1]) || bytes[i - 1] == b'<' || bytes[i - 1] == b'/');

        if boundary_ok
            && i + 4 <= len
            && bytes[i..i + 4].eq_ignore_ascii_case(b"href")
        {
            let mut j = i + 4;
            while j < len && is_html_whitespace(bytes[j]) {
                j += 1;
            }
            if j < len && bytes[j] == b'=' {
                j += 1;
                while j < len && is_html_whitespace(bytes[j]) {
                    j += 1;
                }
                if j >= len {
                    break;
                }
                let (val_bytes, end) = if bytes[j] == b'"' || bytes[j] == b'\'' {
                    let quote = bytes[j];
                    j += 1;
                    let start = j;
                    while j < len && bytes[j] != quote {
                        j += 1;
                    }
                    if j >= len {
                        break;
                    }
                    (&bytes[start..j], j + 1)
                } else {
                    // (10) Unquoted attribute value: read until whitespace or `>`.
                    let start = j;
                    while j < len && !is_html_whitespace(bytes[j]) && bytes[j] != b'>' {
                        j += 1;
                    }
                    (&bytes[start..j], j)
                };
                let s = match std::str::from_utf8(val_bytes) {
                    Ok(s) => s.to_string(),
                    Err(_) => String::from_utf8_lossy(val_bytes).into_owned(),
                };
                hrefs.push(s);
                i = end;
                continue;
            }
        }

        i += 1;
    }

    hrefs
}

fn is_html_whitespace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

// (13) Minimal HTML entity decoder (handles the common cases that show up in
// hrefs: &amp; &lt; &gt; &quot; &apos; and numeric &#nn; / &#xNN;).
fn decode_html_entities(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'&' {
            let scan_end = (i + 12).min(bytes.len());
            if let Some(rel) = bytes[i + 1..scan_end].iter().position(|&b| b == b';') {
                let entity = &s[i + 1..i + 1 + rel];
                let replacement: Option<char> = match entity {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    e if e.starts_with("#x") || e.starts_with("#X") => {
                        u32::from_str_radix(&e[2..], 16).ok().and_then(char::from_u32)
                    }
                    e if e.starts_with('#') => {
                        e[1..].parse::<u32>().ok().and_then(char::from_u32)
                    }
                    _ => None,
                };
                if let Some(ch) = replacement {
                    out.push(ch);
                    i = i + 1 + rel + 1;
                    continue;
                }
            }
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn test_ensure_trailing_slash() {
        assert_eq!(ensure_trailing_slash(u("http://x.com/dir")).as_str(), "http://x.com/dir/");
        assert_eq!(ensure_trailing_slash(u("http://x.com/dir/")).as_str(), "http://x.com/dir/");
        // Query is preserved (unlike old behavior).
        assert_eq!(
            ensure_trailing_slash(u("http://x.com/dir?q=1")).as_str(),
            "http://x.com/dir/?q=1"
        );
        // Fragment is dropped.
        assert_eq!(
            ensure_trailing_slash(u("http://x.com/dir#frag")).as_str(),
            "http://x.com/dir/"
        );
    }

    #[test]
    fn test_is_under_base() {
        let base = ensure_trailing_slash(u("http://x.com/files/"));
        assert!(is_under_base(&u("http://x.com/files/a.zip"), &base));
        assert!(is_under_base(&u("http://x.com/files/sub/a.zip"), &base));
        assert!(!is_under_base(&u("http://x.com/other/a.zip"), &base));
        assert!(!is_under_base(&u("https://x.com/files/a.zip"), &base)); // scheme differs
        assert!(!is_under_base(&u("http://y.com/files/a.zip"), &base)); // host differs
    }

    #[test]
    fn test_parse_files_and_dirs() {
        let html = r#"
        <a href="../">../</a>
        <a href="movie.mkv">movie.mkv</a>
        <a href="subdir/">subdir/</a>
        <a href="photo.png">photo.png</a>
        "#;
        let page = u("http://x.com/root/");
        let (files, dirs) = parse_links(html, &page, &page);
        assert_eq!(files, vec![
            "http://x.com/root/movie.mkv".to_string(),
            "http://x.com/root/photo.png".to_string(),
        ]);
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].as_str(), "http://x.com/root/subdir/");
    }

    #[test]
    fn test_parse_nested_dirs() {
        let html = r#"
        <a href="../">../</a>
        <a href="deep/">deep/</a>
        <a href="file.mp4">file.mp4</a>
        "#;
        let page = u("http://x.com/a/b/");
        let (files, dirs) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/a/b/file.mp4".to_string()]);
        assert_eq!(dirs[0].as_str(), "http://x.com/a/b/deep/");
    }

    #[test]
    fn test_skips_external() {
        let html = r#"
        <a href="http://evil.com/malware.exe">bad</a>
        <a href="good.zip">good</a>
        "#;
        let page = u("http://safe.com/d/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://safe.com/d/good.zip".to_string()]);
    }

    #[test]
    fn test_absolute_path_under_base() {
        let html = r#"<a href="/files/sub/">sub</a><a href="/files/a.mkv">a</a>"#;
        let page = u("http://x.com/files/");
        let (files, dirs) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/files/a.mkv".to_string()]);
        assert_eq!(dirs[0].as_str(), "http://x.com/files/sub/");
    }

    #[test]
    fn test_absolute_path_outside_base_rejected() {
        let html = r#"<a href="/other/secret.zip">bad</a><a href="good.zip">good</a>"#;
        let page = u("http://x.com/files/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/files/good.zip".to_string()]);
    }

    #[test]
    fn test_percent_encoded_names() {
        let html = r#"<a href="file%201.mp4">f</a><a href="sub%20dir/">s</a>"#;
        let page = u("http://x.com/d/");
        let (files, dirs) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/d/file%201.mp4".to_string()]);
        assert_eq!(dirs[0].as_str(), "http://x.com/d/sub%20dir/");
    }

    #[test]
    fn test_single_quotes() {
        let html = "<a href='video.mp4'>video</a>";
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/d/video.mp4".to_string()]);
    }

    #[test]
    fn test_unquoted_href() {
        // (10) Unquoted attribute values are valid HTML5.
        let html = "<a href=video.mp4>video</a>";
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/d/video.mp4".to_string()]);
    }

    #[test]
    fn test_empty_html_returns_empty() {
        let page = u("http://x.com/");
        let (files, dirs) = parse_links("<html></html>", &page, &page);
        assert!(files.is_empty());
        assert!(dirs.is_empty());
    }

    // (1) Sanitization tests.
    #[test]
    fn test_sanitize_relative_path_safe() {
        assert_eq!(sanitize_relative_path("file.mkv").as_deref(), Some("file.mkv"));
        assert_eq!(sanitize_relative_path("sub/file.mkv").as_deref(), Some("sub/file.mkv"));
        assert_eq!(sanitize_relative_path("a/b/c/d.mp4").as_deref(), Some("a/b/c/d.mp4"));
    }

    #[test]
    fn test_sanitize_relative_path_unsafe() {
        assert!(sanitize_relative_path("").is_none());
        assert!(sanitize_relative_path("/etc/passwd").is_none());
        assert!(sanitize_relative_path("../../.ssh/keys").is_none());
        assert!(sanitize_relative_path("sub/../../etc/passwd").is_none());
        assert!(sanitize_relative_path("..").is_none());
        // Encoded traversal is decoded then rejected.
        assert!(sanitize_relative_path("%2e%2e/%2e%2e/etc").is_none());
        // Backslash separators (Windows traversal).
        assert!(sanitize_relative_path("..\\..\\windows\\system32").is_none());
        // Drive letter.
        assert!(sanitize_relative_path("C:/Windows/x").is_none());
        // Null byte.
        assert!(sanitize_relative_path("good\0bad.txt").is_none());
        // Reserved Windows name.
        assert!(sanitize_relative_path("CON").is_none());
        assert!(sanitize_relative_path("Lpt1.txt").is_none());
        // Trailing dot/space-only component.
        assert!(sanitize_relative_path("foo/.").is_none());
        assert!(sanitize_relative_path("foo/...").is_none());
    }

    #[test]
    fn test_path_traversal_in_href_rejected() {
        // (1, 2) Traversal hrefs are now resolved + scope-checked, then rejected.
        let html = r#"
        <a href="../../etc/passwd">sneaky</a>
        <a href="legit.mp4">legit</a>
        "#;
        let page = u("http://x.com/files/sub/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/files/sub/legit.mp4".to_string()]);
    }

    #[test]
    fn test_encoded_traversal_in_href_rejected() {
        let html = r#"<a href="%2e%2e/%2e%2e/etc/passwd">x</a>"#;
        let page = u("http://x.com/files/sub/");
        let (files, _) = parse_links(html, &page, &page);
        assert!(files.is_empty());
    }

    #[test]
    fn test_data_href_attribute_not_matched() {
        // (12) `data-href` must not be parsed as `href`.
        let html = r#"<a data-href="evil.exe" href="good.txt">x</a>"#;
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/d/good.txt".to_string()]);
    }

    #[test]
    fn test_href_inside_script_ignored() {
        let html = r#"<script>var x = '<a href="evil.exe">';</script>
                      <a href="good.txt">good</a>"#;
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/d/good.txt".to_string()]);
    }

    #[test]
    fn test_href_inside_comment_ignored() {
        let html = r#"<!-- <a href="evil.exe">x</a> -->
                      <a href="good.txt">good</a>"#;
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/d/good.txt".to_string()]);
    }

    #[test]
    fn test_protocol_relative_href() {
        // (16) Protocol-relative URLs resolve against the page's scheme.
        let html = r#"<a href="//x.com/files/a.mp4">a</a>"#;
        let page = u("http://x.com/files/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/files/a.mp4".to_string()]);
    }

    #[test]
    fn test_html_entities_in_href() {
        let html = r#"<a href="a&amp;b.mp4">x</a>"#;
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);
        // url::Url::join re-encodes `&` as needed for paths.
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("a&b.mp4"));
    }

    #[test]
    fn test_mixed_case_attr_and_whitespace() {
        let html = "<a HREF =\n\"video.mp4\">v</a>";
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);
        assert_eq!(files, vec!["http://x.com/d/video.mp4".to_string()]);
    }

    #[test]
    fn test_extract_mime_essence() {
        assert_eq!(extract_mime_essence("text/html; charset=utf-8"), "text/html");
        assert_eq!(extract_mime_essence("APPLICATION/XHTML+XML"), "application/xhtml+xml");
        assert_eq!(extract_mime_essence(""), "");
    }

    #[test]
    fn test_validate_url_rejects_bad_schemes() {
        assert!(parse_and_validate_url("file:///etc/passwd", false).is_err());
        assert!(parse_and_validate_url("ftp://x.com/", false).is_err());
        assert!(parse_and_validate_url("javascript:alert(1)", false).is_err());
    }

    #[test]
    fn test_validate_url_allows_private_when_flag_set() {
        unsafe { std::env::remove_var("RDM_ALLOW_PRIVATE"); }
        assert!(parse_and_validate_url("http://10.214.89.214:8000/", true).is_ok());
        assert!(parse_and_validate_url("http://192.168.1.1/", true).is_ok());
        assert!(parse_and_validate_url("http://127.0.0.1/", true).is_ok());
    }

    #[test]
    fn test_validate_url_rejects_private_ips() {
        unsafe {
            std::env::remove_var("RDM_ALLOW_PRIVATE");
        }
        assert!(parse_and_validate_url("http://127.0.0.1/", false).is_err());
        assert!(parse_and_validate_url("http://10.0.0.1/", false).is_err());
        assert!(parse_and_validate_url("http://169.254.169.254/", false).is_err());
        assert!(parse_and_validate_url("http://[::1]/", false).is_err());
    }
}
