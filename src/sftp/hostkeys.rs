//! Fail closed on unsupported known_hosts markers, including revocations.

use anyhow::{Context, Result, ensure};
use ssh2::{KnownHostFileKind, KnownHosts};
use std::io::Read;
use std::path::Path;

/// One RSA key blob backs three signature algorithms, so a line written as
/// `ssh-rsa` verifies an `rsa-sha2-*` negotiation: the blob compared by
/// `check_port` is identical either way.
const PREFERENCE: [(&str, &str); 5] = [
    ("ssh-ed25519", "ssh-ed25519"),
    ("ecdsa-sha2-nistp256", "ecdsa-sha2-nistp256"),
    ("ecdsa-sha2-nistp384", "ecdsa-sha2-nistp384"),
    ("ecdsa-sha2-nistp521", "ecdsa-sha2-nistp521"),
    ("ssh-rsa", "rsa-sha2-512,rsa-sha2-256,ssh-rsa"),
];

pub(super) fn read(path: &Path) -> Result<String> {
    const MAX_BYTES: u64 = 4 * 1024 * 1024;
    let file = std::fs::File::open(path)
        .context("Cannot read SSH known_hosts; verify and enrol the server key first")?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= MAX_BYTES, "SSH known_hosts exceeds 4 MiB");
    let text = std::str::from_utf8(&bytes).context("SSH known_hosts is not UTF-8")?;
    for line in entries(text) {
        // libssh2 is not OpenSSH's certificate/revocation policy engine.
        ensure!(
            !line.starts_with('@'),
            "Marked known_hosts entries are unsupported; use RDM_SFTP_KNOWN_HOSTS with a dedicated, verified ordinary-key file"
        );
    }
    Ok(text.to_owned())
}

pub(super) fn load(hosts: &mut KnownHosts, text: &str) -> Result<()> {
    for line in entries(text) {
        hosts.read_str(line, KnownHostFileKind::OpenSSH)
            .context("Cannot parse SSH known_hosts")?;
    }
    Ok(())
}

/// Host-key algorithms enrolled for `host:port`, strongest first.
///
/// `None` means nothing could be matched literally — hashed or wildcard
/// entries — in which case the negotiation must stay unrestricted rather
/// than narrow to an empty set.
pub(super) fn algorithms(text: &str, host: &str, port: u16) -> Option<String> {
    let mut enrolled: Vec<&str> = Vec::new();
    for line in entries(text) {
        let Some((patterns, rest)) = line.split_once(char::is_whitespace) else { continue };
        if !matches_target(patterns, host, port) {
            continue;
        }
        let Some(kind) = rest.split_whitespace().next() else { continue };
        if let Some((_, preference)) = PREFERENCE.iter().find(|(name, _)| *name == kind)
            && !enrolled.contains(preference)
        {
            enrolled.push(preference);
        }
    }
    // Emit in PREFERENCE order, not file order: known_hosts line order says
    // nothing about key strength.
    let ordered: Vec<&str> = PREFERENCE.iter()
        .map(|(_, preference)| *preference)
        .filter(|preference| enrolled.contains(preference))
        .collect();
    (!ordered.is_empty()).then(|| ordered.join(","))
}

fn entries(text: &str) -> impl Iterator<Item = &str> {
    text.lines().map(str::trim).filter(|line| !line.is_empty() && !line.starts_with('#'))
}

fn matches_target(patterns: &str, host: &str, port: u16) -> bool {
    patterns.split(',').any(|pattern| {
        let pattern = pattern.trim();
        // Hashed and wildcard patterns cannot be compared literally. Skipping
        // them only costs the optimisation, never correctness: verification
        // still goes through libssh2's own matching.
        if pattern.starts_with('|') || pattern.contains('*') || pattern.contains('?') {
            return false;
        }
        match pattern.strip_prefix('[').and_then(|rest| rest.split_once("]:")) {
            Some((name, listed)) => name == host && listed.parse::<u16>().is_ok_and(|listed| listed == port),
            None => port == 22 && pattern == host,
        }
    })
}
