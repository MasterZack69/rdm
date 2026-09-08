//! Fail closed on unsupported known_hosts markers, including revocations.

use anyhow::{Context, Result, ensure};
use ssh2::{KnownHostFileKind, KnownHosts};
use std::io::Read;
use std::path::Path;

pub(super) fn load(hosts: &mut KnownHosts, path: &Path) -> Result<()> {
    const MAX_BYTES: u64 = 4 * 1024 * 1024;
    let file = std::fs::File::open(path)
        .context("Cannot read SSH known_hosts; verify and enrol the server key first")?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= MAX_BYTES, "SSH known_hosts exceeds 4 MiB");
    let text = std::str::from_utf8(&bytes).context("SSH known_hosts is not UTF-8")?;
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') { continue; }
        // libssh2 is not OpenSSH's certificate/revocation policy engine. Never
        // silently ignore @revoked or accept a certificate as an ordinary key.
        ensure!(
            !line.starts_with('@'),
            "Marked known_hosts entries are unsupported; use RDM_SFTP_KNOWN_HOSTS with a dedicated, verified ordinary-key file"
        );
        hosts.read_str(line, KnownHostFileKind::OpenSSH)
            .context("Cannot parse SSH known_hosts")?;
    }
    Ok(())
}
