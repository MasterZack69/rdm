use std::io::Write;

#[test]
fn marked_known_hosts_entries_are_not_silently_ignored() {
    for marker in ["@revoked", "@cert-authority"] {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "{marker} example.test ssh-ed25519 ignored-test-key").unwrap();
        let session = ssh2::Session::new().unwrap();
        let mut hosts = session.known_hosts().unwrap();
        assert!(crate::sftp::hostkeys::load(&mut hosts, file.path()).is_err());
    }
}

#[test]
fn an_empty_known_hosts_file_trusts_nobody() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let session = ssh2::Session::new().unwrap();
    let mut hosts = session.known_hosts().unwrap();
    crate::sftp::hostkeys::load(&mut hosts, file.path()).unwrap();
    let target = crate::sftp::SftpUrl::parse("sftp://alice@example.test/file").unwrap();
    assert!(crate::sftp::session::verify_host(&hosts, &target, b"test-key").is_err());
}
