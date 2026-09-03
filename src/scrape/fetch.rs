//! HTTP fetching, with redirects followed by hand.
//!
//! The client is built with `Policy::none()` precisely so that this module
//! gets to decide. reqwest's own following made the request first and left
//! the caller to notice from `response.url()` that the answer came from
//! somewhere else — by which point the internal service had been contacted
//! and had replied, which is the entire SSRF.
//!
//! Every URL quoted in an error here is redacted first. A listing URL can
//! carry a token in its query, these messages travel up an anyhow chain into
//! a spinner note, and that note goes to stderr.

use anyhow::{Context, Result};
use reqwest::Url;

use crate::net::redirect_location;
use crate::secret_url;

use super::limits::{MAX_HTML_BYTES, MAX_REDIRECTS};
use super::parse::parse_links;
use super::scope::ScopeGuard;
use super::url_util::{ensure_trailing_slash, is_under_base};

/// GETs `url`, following redirects by hand.
///
/// The client is built with `Policy::none()` precisely so that this function
/// gets to decide. reqwest's own following made the request first and left the
/// caller to notice from `response.url()` that the answer came from somewhere
/// else — by which point the internal service had been contacted and had
/// replied, which is the entire SSRF. Refusing the body afterwards does not
/// un-send the request.
///
/// Every hop is therefore scheme-checked, scope-checked and address-checked
/// *before* it is issued.
async fn fetch_following_redirects(
    client: &reqwest::Client,
    url: &Url,
    base: &Url,
    guard: &ScopeGuard,
) -> Result<(reqwest::Response, Url)> {
    let mut current = url.clone();

    for _ in 0..=MAX_REDIRECTS {
        // The base host is pinned in the client to addresses checked before
        // the crawl began, so a hop back to it needs no second lookup — and a
        // second lookup is the window a rebinding attack wants. Anything else
        // is resolved and checked here. `is_under_base` means that branch is
        // unreachable today; it is what keeps this correct if that ever
        // loosens.
        if current.host_str() != base.host_str() {
            guard.resolve(&current).await?;
        }

        let response = client
            .get(current.clone())
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("Failed to fetch {}", secret_url::redact(current.as_str())))?;

        let Some(location) = redirect_location(&response) else {
            return Ok((response, current));
        };

        let mut next = current.join(&location).with_context(|| {
            format!(
                "Unparseable redirect from {}",
                secret_url::redact(current.as_str())
            )
        })?;

        next.set_fragment(None);

        if next.scheme() != "http" && next.scheme() != "https" {
            anyhow::bail!("redirect to unsupported scheme: {}", next.scheme());
        }

        if !is_under_base(&next, base) {
            anyhow::bail!(
                "redirect escaped base scope: {}",
                secret_url::redact(next.as_str())
            );
        }

        current = next;
    }

    anyhow::bail!(
        "too many redirects starting at {}",
        secret_url::redact(url.as_str())
    )
}

pub(super) async fn fetch_and_parse(
    client: &reqwest::Client,
    url: &Url,
    base: &Url,
    guard: &ScopeGuard,
) -> Result<Option<(Vec<String>, Vec<Url>)>> {
    let (response, final_url) = fetch_following_redirects(client, url, base, guard).await?;

    if !response.status().is_success() {
        anyhow::bail!("non-success status {}", response.status());
    }

    // (21) Every hop was checked before it was taken, so this cannot fail.
    // Kept as the statement of that invariant.
    debug_assert!(is_under_base(&final_url, base));

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
    ct.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

async fn read_capped_body(mut response: reqwest::Response, max: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    // Same reason as the send above: a mid-body transport error names the URL
    // it was reading, and this one is the fetch URL.
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)
        .context("reading response body")?
    {
        if buf.len().saturating_add(chunk.len()) > max {
            anyhow::bail!("response body exceeds {} bytes", max);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn test_extract_mime_essence() {
        assert_eq!(
            extract_mime_essence("text/html; charset=utf-8"),
            "text/html"
        );
        assert_eq!(
            extract_mime_essence("APPLICATION/XHTML+XML"),
            "application/xhtml+xml"
        );
        assert_eq!(extract_mime_essence(""), "");
    }

    /// The shape every error in this module quotes a URL through. A listing
    /// behind a signed URL puts the signature in the query, and these
    /// messages end up on stderr.
    #[test]
    fn a_url_quoted_in_an_error_carries_no_credentials() {
        let quoted = secret_url::redact(
            u("https://files.example.com/pub/?key=AIzaSyReal&tempauth=sig#frag").as_str(),
        );
        assert!(!quoted.contains("AIzaSyReal"), "got: {}", quoted);
        assert!(!quoted.contains("sig"), "got: {}", quoted);
        assert!(!quoted.contains("frag"), "got: {}", quoted);
        // Still identifies which listing failed.
        assert!(quoted.contains("files.example.com"), "got: {}", quoted);
        assert!(quoted.contains("/pub/"), "got: {}", quoted);
    }

    // ---------- Redirects ----------

    #[test]
    fn every_redirect_status_is_recognised_as_one() {
        use axum::http;
        use reqwest::StatusCode;

        fn response(status: StatusCode, location: Option<&str>) -> reqwest::Response {
            let mut builder = http::Response::builder().status(status);
            if let Some(location) = location {
                builder = builder.header("location", location);
            }
            reqwest::Response::from(builder.body(String::new()).unwrap())
        }

        for status in [
            StatusCode::MOVED_PERMANENTLY,
            StatusCode::FOUND,
            StatusCode::SEE_OTHER,
            StatusCode::TEMPORARY_REDIRECT,
            StatusCode::PERMANENT_REDIRECT,
        ] {
            assert_eq!(
                redirect_location(&response(status, Some("http://x.com/next/"))).as_deref(),
                Some("http://x.com/next/"),
                "{} is a redirect and must be followed by hand",
                status
            );
        }

        // A plain response is the terminal one.
        assert!(redirect_location(&response(StatusCode::OK, None)).is_none());
        // 304 is not a redirect to somewhere else.
        assert!(redirect_location(&response(StatusCode::NOT_MODIFIED, None)).is_none());
        // A redirect with no Location is not actionable.
        assert!(redirect_location(&response(StatusCode::FOUND, None)).is_none());
    }

    /// The scope rule a redirect hop is held to before it is issued. Each of
    /// these was previously fetched first and rejected afterwards, by which
    /// point the request had already reached the internal service.
    #[test]
    fn a_redirect_off_the_base_is_out_of_scope_before_it_is_taken() {
        let base = ensure_trailing_slash(u("http://files.example.com/pub/"));

        for target in [
            "http://127.0.0.1:8080/",
            "http://169.254.169.254/latest/meta-data/",
            "http://10.0.0.5/admin/",
            "http://files.example.com/etc/",
            "https://files.example.com/pub/",
            "http://evil.example.com/pub/",
        ] {
            assert!(
                !is_under_base(&u(target), &base),
                "{} must be refused before the hop is made",
                target
            );
        }

        // A hop deeper into the mirror is the legitimate case and still works.
        assert!(is_under_base(
            &u("http://files.example.com/pub/sub/"),
            &base
        ));
    }

    /// Pins the `without_url` idiom both send sites use. It exercises the
    /// shape rather than calling `fetch_following_redirects`, which would
    /// need a live listing server and a constructed `ScopeGuard`; what it
    /// actually guards is that the raw error never reaches the chain.
    #[tokio::test]
    async fn a_failed_fetch_does_not_name_the_url_it_failed_on() {
        let client = reqwest::Client::new();
        let url = "http://127.0.0.1:1/pub/?key=SUPERSECRETKEY&tempauth=sig";

        let err = client
            .get(url)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("Failed to fetch {}", secret_url::redact(url)))
            .unwrap_err();

        let chain = format!("{err:#}");
        assert!(!chain.contains("SUPERSECRETKEY"), "got: {chain}");
        assert!(!chain.contains("sig"), "got: {chain}");
        // Still says which listing failed.
        assert!(chain.contains("127.0.0.1"), "got: {chain}");
    }
}
