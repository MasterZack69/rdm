//! One HTTP policy, used by everything that fetches something a listing named.
//!
//! The scraper already refused private addresses, pinned the base host's
//! addresses against a rebind, and checked every redirect before issuing it.
//! What it could not do is stop a file URL from leaving its client: the sync
//! verification pass built a plain `reqwest::Client`, the download engine
//! shared one, and neither had ever heard of `allow_private`. A listing on a
//! public host could therefore hand back a same-origin `file.bin` whose
//! endpoint answered `302 http://169.254.169.254/latest/meta-data/`, or whose
//! name rebound to `127.0.0.1` after the scan, and the request that fetched it
//! went there.
//!
//! So the judgement lives here, and it is made per destination rather than per
//! client: [`Policy::probe`] walks the redirect chain itself, resolving and
//! judging each hop *before* the request that would follow it, and hands back a
//! client pinned to the addresses it just cleared. A transfer running on that
//! client cannot leave the authority it was built for.
//!
//! Two things are needed to make that pinning mean anything. The client must
//! not hand the connection to a system proxy, which would resolve the name
//! again on the other side; and nothing here may print a URL, because by this
//! point it can carry an API key or a signed token.

use anyhow::{Context, Result, bail};
use reqwest::{Url, header};
use std::net::SocketAddr;
use std::time::Duration;

use super::scope::{ScopeGuard, env_flag, parse_and_validate_url, parse_host_as_ip};
use crate::secret_url;

/// How many hops a chain may take before we stop believing it. The scraper's
/// own budget, for the same reason.
pub const MAX_REDIRECTS: usize = 10;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

/// A destination whose every hop has been resolved and judged, and the client
/// that is permitted to talk to it.
#[derive(Clone)]
pub struct Target {
    /// Where the bytes actually are, after redirects.
    ///
    /// Never name the file on disk from this, and never store it as resume
    /// identity: a signed CDN URL differs every run, and the name the user
    /// asked for is the one they typed.
    ///
    /// Never print it either. This is the fetch URL, so it is the one carrying
    /// `key=` or a `tempauth` signature; [`Target::display_url`] is the form
    /// that is safe to show.
    pub url: Url,
    /// Pinned to the addresses cleared for `url`'s host, and unwilling to
    /// follow a redirect off it.
    pub client: reqwest::Client,
}

impl Target {
    /// The destination in a form that can be shown to a person.
    pub fn display_url(&self) -> String {
        secret_url::redact(self.url.as_str())
    }
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redacted, because `{:?}` on a struct holding this ends up in error
        // reports and log lines like anything else.
        f.debug_struct("Target").field("url", &self.display_url()).finish_non_exhaustive()
    }
}

/// Whether a destination is acceptable, and the clients that enforce the answer.
#[derive(Clone, Copy)]
pub struct Policy {
    guard: ScopeGuard,
    allow_private: bool,
}

impl Policy {
    pub fn new(allow_private: bool) -> Self {
        Self {
            guard: ScopeGuard::new(allow_private),
            allow_private,
        }
    }

    /// Resolves `url` to the destination the bytes will come from.
    pub async fn resolve_target(&self, url: &str) -> Result<Target> {
        self.probe(url).await.map(|(target, _)| target)
    }

    /// [`Policy::resolve_target`], keeping the answer the last hop gave.
    ///
    /// The walk has to issue a request per hop anyway, so a caller that only
    /// wants headers — sync's verification pass — reads them from this instead
    /// of asking twice.
    pub async fn probe(&self, url: &str) -> Result<(Target, reqwest::Response)> {
        let mut current = parse_and_validate_url(url, self.allow_private)?;
        let mut hops = 0usize;

        loop {
            // Before the request, not after: a redirect that has been sent has
            // already reached whatever it named.
            let addrs = self.guard.resolve(&current).await?;
            let client = self.client_for(&current, &addrs)?;

            // The same one-byte ranged GET the download itself opens with, so a
            // server that redirects only for real range requests is followed
            // here as well.
            let response = client
                .get(current.clone())
                .header(header::RANGE, "bytes=0-0")
                .timeout(PROBE_TIMEOUT)
                .send()
                .await
                // Redacted: a failure to reach a signed CDN URL would
                // otherwise print the signature, and this error is displayed.
                .with_context(|| format!("Failed to reach {}", secret_url::redact(current.as_str())))?;

            let Some(location) = redirect_location(&response) else {
                return Ok((
                    Target {
                        // Same-authority hops the client followed for us moved
                        // the URL, so take it from the response, not from
                        // `current`.
                        url: response.url().clone(),
                        client,
                    },
                    response,
                ));
            };

            hops += 1;
            if hops > MAX_REDIRECTS {
                bail!("Too many redirects starting at {}", secret_url::redact(url));
            }

            // The policy below answers a hop off the authority with `stop`, so
            // this redirect was not followed and `response.url()` is the hop
            // that answered it.
            let mut next = response
                .url()
                .join(&location)
                .with_context(|| {
                    format!(
                        "Unparseable redirect from {}",
                        secret_url::redact(response.url().as_str())
                    )
                })?;
            next.set_fragment(None);

            match next.scheme() {
                "http" | "https" => {}
                other => bail!("Refusing to follow a redirect to scheme {}", other),
            }

            current = next;
        }
    }

    /// A client pinned to `addrs` that will not leave `url`'s authority.
    fn client_for(&self, url: &Url, addrs: &[SocketAddr]) -> Result<reqwest::Client> {
        let host = url.host_str().map(str::to_owned);
        let port = url.port_or_known_default();
        let scheme = url.scheme().to_owned();

        // No total timeout: this client carries multi-hour transfers. The probe
        // puts its leash on the request instead.
        let mut builder = reqwest::Client::builder()
            .user_agent("rdm")
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() > MAX_REDIRECTS {
                    return attempt.error("Too many redirects");
                }

                let next = attempt.url();
                let same_authority = next.host_str() == host.as_deref()
                    && next.port_or_known_default() == port
                    && next.scheme() == scheme;

                if same_authority {
                    // The addresses are pinned and were judged, so a hop that
                    // stays here cannot reach anywhere new.
                    attempt.follow()
                } else {
                    // Anywhere else has not been judged. `stop` returns the 3xx
                    // rather than following it, so the request that would have
                    // gone there is never sent; `probe` picks the hop up and
                    // judges it properly.
                    attempt.stop()
                }
            }));

        // Everything below this line pins the host to addresses that were
        // judged — and a proxy would make that decide nothing at all, because
        // the connection goes to the proxy and the *proxy* resolves the name.
        // Its answer can differ from ours through split DNS, through a rebind
        // on its side, or simply because it sits inside a network we cannot
        // see. reqwest reads HTTP_PROXY, HTTPS_PROXY and ALL_PROXY unless it
        // is told not to, so it is told not to.
        if proxies_allowed() {
            warn_about_proxies();
        } else {
            builder = builder.no_proxy();
        }

        // Pin the name to the addresses just cleared. Without this the
        // connection performs its own lookup, and a record with a one-second
        // TTL can answer publicly for the check and privately for the connect.
        // A literal-IP host is already its address, and reqwest's override does
        // not apply to it.
        if let Some(host) = url.host_str()
            && parse_host_as_ip(host).is_none()
        {
            builder = builder.resolve_to_addrs(host, addrs);
        }

        builder.build().context("Failed to build HTTP client")
    }
}

/// Whether the operator has asked, by name, for system proxies to be honoured.
///
/// Off by default, and it cannot be turned on by accident: this gives up the
/// guarantee that the address checks above are worth anything, so the variable
/// has to say `true` rather than merely exist.
fn proxies_allowed() -> bool {
    env_flag("RDM_ALLOW_PROXY")
}

/// Says once, out loud, what has been given up.
fn warn_about_proxies() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        eprintln!(
            "  \u{26a0} RDM_ALLOW_PROXY is set: requests go through the system proxy, which \
             resolves hostnames itself \u{2014} rdm cannot enforce where they finally land."
        );
    });
}

/// The `Location` of a redirect response, if that is what this is.
///
/// Lives here rather than in the scraper because the scraper is no longer the
/// only thing that has to look at a redirect before following it.
pub fn redirect_location(response: &reqwest::Response) -> Option<String> {
    if !matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
        return None;
    }

    response
        .headers()
        .get(header::LOCATION)?
        .to_str()
        .ok()
        .map(str::to_owned)
}

/// The total size a one-byte ranged probe implies, when it states one.
///
/// A 206 or a 416 states the total after the slash in `Content-Range`; a server
/// that ignored the range answers 200 and states it outright. A 206's
/// `Content-Length` is 1 and must never be read as the total, which is why the
/// two cases are kept apart.
pub fn probed_size(response: &reqwest::Response) -> Option<u64> {
    let status = response.status();
    let headers = response.headers();

    if status == reqwest::StatusCode::PARTIAL_CONTENT
        || status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    {
        let value = headers.get(header::CONTENT_RANGE)?.to_str().ok()?;
        return value.rsplit('/').next()?.trim().parse().ok();
    }

    if status.is_success() {
        return headers.get(header::CONTENT_LENGTH)?.to_str().ok()?.parse().ok();
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::response::{IntoResponse, Redirect};
    use axum::routing::get;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{}", addr)
    }

    /// Serialises the tests that touch process-wide environment variables.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[tokio::test]
    async fn a_private_destination_is_refused_before_a_request_is_made() {
        let err = Policy::new(false)
            .resolve_target("http://169.254.169.254/latest/meta-data/")
            .await
            .expect_err("the metadata service is not a download");
        assert!(err.to_string().contains("private/internal"), "{err}");
    }

    /// The ports differ, so this is a different authority from the one the
    /// client was built for: it has to come back through the guard rather than
    /// be followed inside reqwest.
    #[tokio::test]
    async fn a_redirect_off_the_authority_is_judged_before_it_is_followed() {
        let cdn = serve(Router::new().route("/real.bin", get(|| async { "payload" }))).await;
        let to = format!("{}/real.bin", cdn);
        let listing = serve(Router::new().route(
            "/file.bin",
            get(move || {
                let to = to.clone();
                async move { Redirect::temporary(&to) }
            }),
        ))
        .await;

        let target = Policy::new(true)
            .resolve_target(&format!("{}/file.bin", listing))
            .await
            .expect("a cleared redirect is fine");

        assert_eq!(target.url.as_str(), format!("{}/real.bin", cdn));
    }

    #[tokio::test]
    async fn a_redirect_to_another_scheme_is_refused() {
        let listing = serve(Router::new().route(
            "/file.bin",
            get(|| async { Redirect::temporary("file:///etc/passwd") }),
        ))
        .await;

        let err = Policy::new(true)
            .resolve_target(&format!("{}/file.bin", listing))
            .await
            .expect_err("a download is http or nothing");
        assert!(err.to_string().contains("scheme"), "{err}");
    }

    /// The chunk requests happen long after resolution, so a server that
    /// behaves for the probe and redirects afterwards must still fail to move
    /// the transfer anywhere.
    #[tokio::test]
    async fn a_transfer_cannot_be_redirected_off_the_authority_it_was_pinned_to() {
        let hits = Arc::new(AtomicUsize::new(0));
        let seen = hits.clone();
        let listing = serve(Router::new().route(
            "/file.bin",
            get(move || {
                let seen = seen.clone();
                async move {
                    if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                        "payload".into_response()
                    } else {
                        Redirect::temporary("http://169.254.169.254/latest/meta-data/")
                            .into_response()
                    }
                }
            }),
        ))
        .await;

        let target = Policy::new(true)
            .resolve_target(&format!("{}/file.bin", listing))
            .await
            .expect("the first answer is honest");

        let response = target.client.get(target.url.clone()).send().await.unwrap();
        assert_eq!(response.status().as_u16(), 307, "the hop must not be followed");
    }

    /// Pinning a name to judged addresses decides nothing if the connection is
    /// handed to a proxy, because the proxy resolves the name itself and can
    /// reach a different machine than the one that was cleared. A proxy that
    /// answers everything with `PROXIED` stands in for that: if a guarded
    /// request can be made to say `PROXIED`, it went somewhere this crate did
    /// not choose.
    #[tokio::test]
    async fn a_guarded_request_ignores_a_system_proxy() {
        let _lock = env_lock();

        let proxy = serve(Router::new().fallback(|| async { "PROXIED" })).await;
        let origin = serve(Router::new().route("/file.bin", get(|| async { "payload" }))).await;
        let file = format!("{}/file.bin", origin);

        unsafe {
            std::env::set_var("HTTP_PROXY", &proxy);
            std::env::set_var("ALL_PROXY", &proxy);
            std::env::remove_var("RDM_ALLOW_PROXY");
        }

        // reqwest reads the system proxy list once per process and caches it,
        // so if an earlier client here already read it the variables set above
        // are not live and there is nothing to prove. Establish that they *are*
        // live with an unguarded client before asserting anything about a
        // guarded one; otherwise this test would pass by doing nothing.
        let unguarded = reqwest::Client::builder()
            .build()
            .unwrap()
            .get(&file)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        if unguarded == "PROXIED" {
            let target = Policy::new(true)
                .resolve_target(&file)
                .await
                .expect("the origin answers");

            let body = target
                .client
                .get(target.url.clone())
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();

            assert_eq!(
                body, "payload",
                "a guarded client must reach the pinned address, not the proxy"
            );
        }

        unsafe {
            std::env::remove_var("HTTP_PROXY");
            std::env::remove_var("ALL_PROXY");
        }
    }

    /// The fetch URL is the one carrying `key=` and `tempauth`, and `{:?}` on
    /// anything holding a Target ends up in logs.
    #[test]
    fn a_target_never_debug_prints_its_query() {
        let target = Target {
            url: Url::parse("https://www.googleapis.com/drive/v3/files/ID?alt=media&key=SECRET")
                .unwrap(),
            client: reqwest::Client::new(),
        };

        let shown = format!("{:?}", target);
        assert!(!shown.contains("SECRET"), "got: {}", shown);
        assert!(shown.contains("googleapis.com"), "got: {}", shown);
    }
}
