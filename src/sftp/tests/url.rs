use std::path::Path;

use crate::sftp::{SftpUrl, is_sftp_url};
use crate::sftp::download::retryable;
use crate::sftp::session::verify_host;

#[test]
fn urls_preserve_decoded_paths_and_explicit_ports() {
    let url = SftpUrl::parse("sftp://alice@example.test:2222/music/a%20b.flac").unwrap();
    assert_eq!(url.username(), "alice");
    assert_eq!(url.host(), "example.test");
    assert_eq!(url.port(), 2222);
    assert_eq!(url.path(), Path::new("/music/a b.flac"));
    assert_eq!(SftpUrl::parse("sftp://alice@example.test/").unwrap().port(), 22);
    let ipv6 = SftpUrl::parse("sftp://alice@[::1]:2222/a").unwrap();
    assert_eq!(ipv6.host(), "::1");
    assert_eq!(ipv6.port(), 2222);
}

#[test]
fn untrusted_paths_and_url_credentials_are_rejected() {
    for url in [
        "sftp://alice:do-not-log@example.test/a",
        "sftp://alice:@example.test/a",
        "sftp://example.test/a",
        "sftp://alice@example.test:0/a",
        "sftp://alice@example.test/../a",
        "sftp://alice@example.test/%2e%2e/a",
        "sftp://alice@example.test/a/%2Fetc",
        "sftp://alice@example.test/%5Cescape",
        "sftp://alice@example.test/%00a",
        "sftp://alice@example.test/%1Ba",
        "sftp://alice@example.test/%FF",
        "sftp://alice@example.test/%zz",
        "sftp://alice@example.test/.rdm-sftp/a",
        "sftp://alice@example.test/a?password=do-not-log",
        "sftp://alice@example.test/a#fragment",
        "https://example.test/a",
    ] {
        let error = SftpUrl::parse(url).unwrap_err();
        assert!(!format!("{error:#}").contains("do-not-log"));
    }
}

#[test]
fn filename_encoding_round_trips_without_double_decoding() {
    let base = SftpUrl::parse("sftp://alice@example.test/music/").unwrap();
    for path in ["/music/a%2Fb", "/music/a ?#.flac", "/music/हिन्दी.flac"] {
        let child = base.with_path(Path::new(path)).unwrap();
        assert_eq!(SftpUrl::parse(child.as_str()).unwrap().path(), Path::new(path));
    }
    let literal = SftpUrl::parse("sftp://alice@example.test/%252Fetc").unwrap();
    assert_eq!(literal.path(), Path::new("/%2Fetc"));
    assert!(literal.as_str().contains("%252F"));
}

#[test]
fn known_hosts_match_the_host_port_and_key() {
    let session = ssh2::Session::new().unwrap();
    let mut hosts = session.known_hosts().unwrap();
    let key = b"opaque-test-host-key";
    hosts.add("[example.test]:2222", key, "test", ssh2::HostKeyType::Ed25519.into()).unwrap();
    let correct = SftpUrl::parse("sftp://alice@example.test:2222/a").unwrap();
    assert!(verify_host(&hosts, &correct, key, ssh2::HostKeyType::Ed25519).is_ok());
    assert!(verify_host(&hosts, &correct, b"changed-key", ssh2::HostKeyType::Ed25519).is_err());
    let other_port = SftpUrl::parse("sftp://alice@example.test/a").unwrap();
    assert!(verify_host(&hosts, &other_port, key, ssh2::HostKeyType::Ed25519).is_err());
    let other_host = SftpUrl::parse("sftp://alice@other.test:2222/a").unwrap();
    assert!(verify_host(&hosts, &other_host, key, ssh2::HostKeyType::Ed25519).is_err());
}

#[test]
fn authentication_and_permission_failures_are_not_retried() {
    assert!(!retryable(&ssh2::Error::from_errno(ssh2::ErrorCode::Session(-18)).into()));
    assert!(!retryable(&ssh2::Error::from_errno(ssh2::ErrorCode::SFTP(3)).into()));
    assert!(retryable(&ssh2::Error::from_errno(ssh2::ErrorCode::Session(-9)).into()));
    assert!(retryable(&ssh2::Error::from_errno(ssh2::ErrorCode::SFTP(7)).into()));
}

#[test]
fn clap_accepts_sftp_on_all_download_commands() {
    use clap::Parser;
    let url = "sftp://alice@example.test/music/";
    for args in [
        vec!["rdm", url],
        vec!["rdm", "download", url],
        vec!["rdm", "queue", "add", url],
        vec!["rdm", "sync", url],
    ] {
        assert!(crate::args::Cli::try_parse_from(args).is_ok());
    }
    assert!(is_sftp_url("SFTP://alice@example.test/a"));
    assert!(!is_sftp_url("https://example.test/sftp://a"));
    assert!(crate::args::parse_url("sftp://alice:do-not-log@example.test/a").is_err());
}
