//! URL validation and address scope.
//!
//! Two halves of one rule: [`parse_and_validate_url`] judges what the URL
//! says, and [`ScopeGuard`] judges what the host actually resolves to. The
//! second half is the one that matters, because a name is not a literal IP.

use anyhow::{Context, Result};
use reqwest::Url;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Scheme and literal-address checks, which need no network.
///
/// This is only half the check. A host that is a *name* cannot be judged here
/// at all — that is [`ScopeGuard::resolve`]'s job, and skipping it is what
/// let a domain pointed at 127.0.0.1 through.
pub(super) fn parse_and_validate_url(s: &str, allow_private: bool) -> Result<Url> {
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

/// Decides whether an address may be contacted, and resolves names so that
/// the decision is made about the address actually being dialled.
#[derive(Clone, Copy)]
pub(super) struct ScopeGuard {
    allow_private: bool,
}

impl ScopeGuard {
    pub(super) fn new(allow_private: bool) -> Self {
        Self {
            allow_private: allow_private || std::env::var_os("RDM_ALLOW_PRIVATE").is_some(),
        }
    }

    /// Resolves `url`'s host and returns its addresses, or refuses.
    ///
    /// Every address has to pass, not merely the first. A name that answers
    /// with a public address *and* 127.0.0.1 is a deliberate attempt to have
    /// the check look at one and `connect` pick the other, and there is no
    /// legitimate directory listing that needs it.
    pub(super) async fn resolve(&self, url: &Url) -> Result<Vec<SocketAddr>> {
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("URL has no host"))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| anyhow::anyhow!("URL has no port"))?;

        // A literal address needs no lookup; `parse_and_validate_url` has
        // already judged it, and this keeps the two paths agreeing.
        if let Some(ip) = parse_host_as_ip(host) {
            self.check(ip, host)?;
            return Ok(vec![SocketAddr::new(ip, port)]);
        }

        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
            .await
            .with_context(|| format!("Could not resolve host: {}", host))?
            .collect();

        if addrs.is_empty() {
            anyhow::bail!("Host resolved to no addresses: {}", host);
        }

        for addr in &addrs {
            self.check(addr.ip(), host)?;
        }

        Ok(addrs)
    }

    fn check(&self, ip: IpAddr, host: &str) -> Result<()> {
        if self.allow_private || !is_disallowed_ip(ip) {
            return Ok(());
        }

        if host == ip.to_string() {
            anyhow::bail!("Refusing to scan private/internal address: {}", ip);
        }

        anyhow::bail!(
            "Refusing to scan private/internal address: {} resolves to {}",
            host,
            ip
        )
    }
}

pub(super) fn parse_host_as_ip(host: &str) -> Option<IpAddr> {
    let trimmed = host.trim_start_matches('[').trim_end_matches(']');
    trimmed.parse::<IpAddr>().ok()
}

fn is_disallowed_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_disallowed_v4(v4),
        IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() {
                return true;
            }

            // An IPv4 address wearing an IPv6 hat. ::ffff:127.0.0.1 reaches
            // the same loopback interface as 127.0.0.1, so it has to be
            // judged by the IPv4 rules instead of passing as an ordinary
            // global v6 address.
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_disallowed_v4(mapped);
            }

            v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
        }
    }
}

fn is_disallowed_v4(v4: Ipv4Addr) -> bool {
    let o = v4.octets();
    v4.is_loopback()
        || v4.is_private()
        // 169.254.0.0/16, which is also where every cloud provider parks its
        // metadata service (169.254.169.254).
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_documentation()
        || v4.is_unspecified()
        || o[0] == 0
        || o[0] >= 224 // multicast / reserved
        // 100.64.0.0/10, carrier-grade NAT. Not private by the RFC1918
        // definition, and routinely where a network puts its own equipment.
        || (o[0] == 100 && (64..128).contains(&o[1]))
        // 198.18.0.0/15, reserved for benchmarking. Nothing on the public
        // internet legitimately answers here.
        || (o[0] == 198 && (o[1] == 18 || o[1] == 19))
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// Built directly rather than through `new` so the tests do not depend on
    /// whether RDM_ALLOW_PRIVATE happens to be set in the environment.
    fn guard(allow_private: bool) -> ScopeGuard {
        ScopeGuard { allow_private }
    }

    // ---------- URL validation ----------

    #[test]
    fn test_validate_url_rejects_bad_schemes() {
        assert!(parse_and_validate_url("file:///etc/passwd", false).is_err());
        assert!(parse_and_validate_url("ftp://x.com/", false).is_err());
        assert!(parse_and_validate_url("javascript:alert(1)", false).is_err());
    }

    #[test]
    fn test_validate_url_allows_private_when_flag_set() {
        unsafe {
            std::env::remove_var("RDM_ALLOW_PRIVATE");
        }
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

    // ---------- Address classification ----------

    #[test]
    fn ordinary_public_addresses_are_allowed() {
        assert!(!is_disallowed_ip(ip("93.184.216.34")));
        assert!(!is_disallowed_ip(ip("1.1.1.1")));
        assert!(!is_disallowed_ip(ip("2606:4700:4700::1111")));
    }

    /// Every cloud provider serves instance credentials from this address, so
    /// it is the single most valuable target of an SSRF. It falls inside
    /// 169.254.0.0/16 and is caught by the link-local rule; this says so out
    /// loud, because it is the assertion most worth never regressing.
    #[test]
    fn the_cloud_metadata_address_is_refused() {
        assert!(is_disallowed_ip(ip("169.254.169.254")));
    }

    #[test]
    fn loopback_and_rfc1918_are_refused() {
        assert!(is_disallowed_ip(ip("127.0.0.1")));
        assert!(is_disallowed_ip(ip("127.9.9.9")));
        assert!(is_disallowed_ip(ip("10.0.0.1")));
        assert!(is_disallowed_ip(ip("172.16.0.1")));
        assert!(is_disallowed_ip(ip("192.168.1.1")));
        assert!(is_disallowed_ip(ip("0.0.0.0")));
        assert!(is_disallowed_ip(ip("::1")));
        assert!(is_disallowed_ip(ip("fd00::1")));
        assert!(is_disallowed_ip(ip("fe80::1")));
    }

    /// `::ffff:127.0.0.1` reaches the same loopback interface as
    /// `127.0.0.1`, so judging it by the v6 rules alone let it through as an
    /// ordinary global address.
    #[test]
    fn an_ipv4_address_wearing_an_ipv6_hat_is_judged_as_ipv4() {
        assert!(is_disallowed_ip(ip("::ffff:127.0.0.1")));
        assert!(is_disallowed_ip(ip("::ffff:10.0.0.1")));
        assert!(is_disallowed_ip(ip("::ffff:169.254.169.254")));
        assert!(!is_disallowed_ip(ip("::ffff:93.184.216.34")));
    }

    /// 100.64.0.0/10 is not RFC1918, so `is_private` says nothing about it,
    /// but it is where a provider parks its own equipment.
    #[test]
    fn carrier_grade_nat_space_is_refused() {
        assert!(is_disallowed_ip(ip("100.64.0.1")));
        assert!(is_disallowed_ip(ip("100.127.255.255")));
        // Either side of the range is ordinary public space.
        assert!(!is_disallowed_ip(ip("100.63.255.255")));
        assert!(!is_disallowed_ip(ip("100.128.0.1")));
    }

    #[test]
    fn benchmarking_space_is_refused() {
        assert!(is_disallowed_ip(ip("198.18.0.1")));
        assert!(is_disallowed_ip(ip("198.19.255.255")));
        assert!(!is_disallowed_ip(ip("198.20.0.1")));
    }

    // ---------- Resolution ----------

    /// The bypass this guard exists for. `localhost` is a name, not a literal
    /// address, so the literal-IP-only check waved it through and the scan
    /// went ahead and connected to loopback.
    #[tokio::test]
    async fn a_hostname_that_resolves_to_loopback_is_refused() {
        let err = guard(false)
            .resolve(&u("http://localhost:8080/"))
            .await
            .expect_err("a name pointing at loopback must not pass");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("private/internal"),
            "expected a scope refusal, got: {}",
            msg
        );
    }

    #[tokio::test]
    async fn allow_private_still_permits_a_name_pointing_inward() {
        assert!(guard(true)
            .resolve(&u("http://localhost:8080/"))
            .await
            .is_ok());
    }

    /// A literal address is judged without a lookup, and the port comes from
    /// the URL so the pinned addresses match what will be dialled.
    #[tokio::test]
    async fn a_literal_address_resolves_to_itself_on_the_url_port() {
        let addrs = guard(false)
            .resolve(&u("http://93.184.216.34:8000/"))
            .await
            .unwrap();
        assert_eq!(addrs, vec!["93.184.216.34:8000".parse().unwrap()]);

        // And the default port is filled in when the URL omits it.
        let addrs = guard(false)
            .resolve(&u("https://93.184.216.34/"))
            .await
            .unwrap();
        assert_eq!(addrs, vec!["93.184.216.34:443".parse().unwrap()]);
    }

    #[tokio::test]
    async fn a_literal_private_address_is_refused_without_a_lookup() {
        assert!(guard(false).resolve(&u("http://127.0.0.1/")).await.is_err());
        assert!(guard(false).resolve(&u("http://[::1]/")).await.is_err());
    }

    /// A name that answers with a public address *and* a private one is the
    /// attack, not an accident: the check would look at one and `connect`
    /// would be free to pick the other. Every answer has to pass.
    #[test]
    fn one_bad_address_in_a_record_set_condemns_the_whole_set() {
        let g = guard(false);
        let answers = [ip("93.184.216.34"), ip("127.0.0.1")];
        assert!(
            answers
                .iter()
                .any(|a| g.check(*a, "evil.example.com").is_err()),
            "a mixed record set must not be accepted"
        );
    }

    #[test]
    fn a_refusal_names_the_host_and_the_address_it_pointed_at() {
        let msg = format!(
            "{:#}",
            guard(false)
                .check(ip("169.254.169.254"), "files.example.com")
                .unwrap_err()
        );
        assert!(msg.contains("files.example.com"), "got: {}", msg);
        assert!(msg.contains("169.254.169.254"), "got: {}", msg);
    }
}
