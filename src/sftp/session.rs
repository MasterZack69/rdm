//! Blocking libssh2 operations never run on Tokio's executor threads.

use anyhow::{Context, Result, ensure};
use ssh2::{CheckResult, KnownHosts, Session, Sftp};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::{SftpUrl, options::Authentication};

const TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub(crate) struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SFTP operation cancelled")
    }
}

impl std::error::Error for Cancelled {}

pub(crate) fn check_cancel(cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() { Err(Cancelled.into()) } else { Ok(()) }
}

struct Interrupt {
    socket: TcpStream,
    cancel: CancellationToken,
}

impl Drop for Interrupt {
    fn drop(&mut self) {
        self.cancel.cancel();
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

pub(crate) async fn with_session<T, F>(
    target: SftpUrl,
    authentication: Arc<Authentication>,
    allow_private: bool,
    cancel: CancellationToken,
    operation: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&Session, &Sftp, &SftpUrl, &CancellationToken) -> Result<T> + Send + 'static,
{
    let guard = crate::net::ScopeGuard::new(allow_private);
    let addresses = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Cancelled.into()),
        result = tokio::time::timeout(TIMEOUT, guard.resolve(target.address_url())) =>
            result.context("SFTP DNS lookup timed out")??,
    };
    // Dial only vetted addresses: a second DNS lookup would permit rebinding.
    let tcp = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Cancelled.into()),
        result = tokio::time::timeout(TIMEOUT, tokio::net::TcpStream::connect(addresses.as_slice())) =>
            result.context("SFTP connection timed out")?.context("Cannot connect to SFTP server")?,
    };
    let tcp = tcp.into_std().context("Cannot configure SSH socket")?;
    tcp.set_nonblocking(false)?;
    let worker_cancel = cancel.child_token();
    let interrupt = Interrupt { socket: tcp.try_clone()?, cancel: worker_cancel.clone() };
    let mut worker = tokio::task::spawn_blocking(move || {
        check_cancel(&worker_cancel)?;
        let mut session = Session::new().context("Cannot initialise SSH")?;
        session.set_timeout(15_000);
        session.set_tcp_stream(tcp);
        session.handshake().context("SSH handshake failed")?;
        let mut hosts = session.known_hosts().context("Cannot initialise host-key verification")?;
        super::hostkeys::load(&mut hosts, &authentication.known_hosts)?;
        let (key, _) = session.host_key().context("SSH server supplied no host key")?;
        verify_host(&hosts, &target, key)?;
        // Verification MUST precede every authentication attempt.
        check_cancel(&worker_cancel)?;
        authenticate(&session, &target, &authentication, &worker_cancel)?;
        let sftp = session.sftp().context("Server does not provide an SFTP subsystem")?;
        check_cancel(&worker_cancel)?;
        operation(&session, &sftp, &target, &worker_cancel)
    });
    let result = tokio::select! {
        biased;
        result = &mut worker => result.context("SFTP worker panicked")?,
        _ = cancel.cancelled() => {
            let _ = interrupt.socket.shutdown(Shutdown::Both);
            // Aborting a JoinHandle cannot stop spawn_blocking. Drain it so a
            // skipped queue item cannot write behind its replacement.
            let _ = worker.await;
            Err(Cancelled.into())
        }
    };
    drop(interrupt);
    result.map_err(|error| {
        if let Some(ssh) = error.downcast_ref::<ssh2::Error>() {
            // Preserve the typed code, not an untrusted server-supplied error
            // message that could contain terminal escapes or echoed secrets.
            anyhow::Error::new(ssh2::Error::from_errno(ssh.code()))
                .context("SSH/SFTP operation failed")
        } else {
            error
        }
    })
}

pub(crate) fn verify_host(hosts: &KnownHosts, target: &SftpUrl, key: &[u8]) -> Result<()> {
    match hosts.check_port(target.host(), target.port(), key) {
        CheckResult::Match => Ok(()),
        CheckResult::Mismatch => anyhow::bail!("SSH host key changed; refusing authentication"),
        CheckResult::NotFound => anyhow::bail!(
            "SSH host key is not trusted; verify its fingerprint independently and enrol it in known_hosts"
        ),
        CheckResult::Failure => anyhow::bail!("SSH host-key verification failed"),
    }
}

fn authenticate(
    session: &Session,
    target: &SftpUrl,
    auth: &Authentication,
    cancel: &CancellationToken,
) -> Result<()> {
    if let Some(identity) = &auth.identity {
        session.userauth_pubkey_file(target.username(), None, identity, auth.passphrase.as_deref())
            .context("SSH identity authentication failed")?;
    } else if let Some(password) = &auth.password {
        session.userauth_password(target.username(), password)
            .context("SSH password authentication failed")?;
    } else {
        let mut agent = session.agent().context("Cannot initialise SSH agent")?;
        agent.connect().context(
            "Cannot connect to SSH agent; set RDM_SFTP_IDENTITY_FILE or RDM_SFTP_PASSWORD",
        )?;
        agent.list_identities().context("Cannot list SSH agent identities")?;
        for identity in agent.identities()?.iter().take(16) {
            check_cancel(cancel)?;
            if agent.userauth(target.username(), identity).is_ok() && session.authenticated() {
                return Ok(());
            }
        }
    }
    ensure!(session.authenticated(), "SSH authentication failed (at most 16 agent identities are tried)");
    Ok(())
}
