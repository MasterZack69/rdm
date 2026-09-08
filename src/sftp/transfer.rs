//! One read-only SFTP transfer, with a checked resume identity.

use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use ssh2::{Session, Sftp};
use std::fs::FileTimes;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

use crate::engine::{ExistingPolicy, Outcome};
use crate::ui::{ProgressSink, SlotState};
use super::checkpoint::Partial;
use super::local::Destination;
use super::session::check_cancel;
use super::stamp::{FileStamp, Identity};
use super::SftpUrl;

#[derive(Clone)]
pub(crate) struct Transfer {
    pub target: SftpUrl,
    pub destination: Destination,
    pub expected: Option<FileStamp>,
    pub policy: ExistingPolicy,
}

impl Transfer {
    pub fn run(
        &self,
        session: &Session,
        sftp: &Sftp,
        cancel: &CancellationToken,
        sink: &Arc<dyn ProgressSink>,
    ) -> Result<Outcome> {
        check_cancel(cancel)?;
        sink.state(SlotState::Inspecting);
        ensure!(
            sftp.realpath(self.target.path())?.as_path() == self.target.path(),
            "SFTP paths must be canonical; remote symlinks are not followed"
        );
        let stamp = FileStamp::from_remote(&sftp.lstat(self.target.path())?)?;
        if let Some(expected) = &self.expected {
            ensure!(*expected == stamp, "Remote file changed since the SFTP listing; rerun the scan");
        }
        let limit = size_limit()?;
        ensure!(limit == 0 || stamp.size <= limit, "SFTP file exceeds RDM_MAX_FILE_BYTES");
        let mut remote = sftp.open(self.target.path()).context("Cannot open remote SFTP file")?;
        ensure!(FileStamp::from_remote(&remote.stat()?)? == stamp, "Remote file changed while opening it");
        self.destination.prepare()?;
        let path = self.destination.display_path()?;
        if self.destination.metadata()?.is_some() {
            match self.policy {
                ExistingPolicy::Reuse => return Ok(Outcome::AlreadyPresent { path }),
                ExistingPolicy::Ask => anyhow::bail!(
                    "SFTP output already exists; choose a different -o path or use sync to refresh it"
                ),
                ExistingPolicy::Overwrite => {}
            }
        }
        let (key, _) = session.host_key().context("SSH server supplied no host key")?;
        let identity = Identity {
            url: self.target.as_str().to_owned(),
            host_key: STANDARD_NO_PAD.encode(key),
            stamp: stamp.clone(),
        };
        let mut partial = Partial::open(self.destination.clone(), identity)?;
        remote.seek(SeekFrom::Start(partial.offset))?;
        sink.total(Some(stamp.size));
        sink.progress(partial.offset);
        sink.state(SlotState::Downloading);
        let copied = copy_data(&mut remote, &mut partial, stamp.size, cancel, sink.as_ref());
        // Persist even a short successful write before a disk/network error.
        let committed = partial.file.stream_position()?;
        partial.checkpoint(committed)?;
        copied?;
        if FileStamp::from_remote(&remote.stat()?)? != stamp
            || FileStamp::from_remote(&sftp.lstat(self.target.path())?)? != stamp
        {
            partial.invalidate()?;
            anyhow::bail!("Remote file changed during the SFTP transfer");
        }
        check_cancel(cancel)?;
        if let Some(seconds) = stamp.modified {
            let time = UNIX_EPOCH.checked_add(Duration::from_secs(seconds))
                .context("Remote modification time is out of range")?;
            partial.file.set_times(FileTimes::new().set_modified(time))?;
        }
        sink.state(SlotState::Finishing);
        check_cancel(cancel)?;
        partial.publish(self.policy == ExistingPolicy::Overwrite)?;
        Ok(Outcome::Completed { path, bytes: stamp.size })
    }
}

pub(crate) fn copy_data<R: Read>(
    remote: &mut R,
    partial: &mut Partial,
    size: u64,
    cancel: &CancellationToken,
    sink: &dyn ProgressSink,
) -> Result<()> {
    let mut written = partial.offset;
    let mut buffer = [0_u8; 64 * 1024];
    while written < size {
        check_cancel(cancel)?;
        let wanted = usize::try_from((size - written).min(buffer.len() as u64))?;
        let read = remote.read(&mut buffer[..wanted]).map_err(RemoteRead)?;
        if read == 0 {
            return Err(RemoteRead(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof, "SFTP file ended before its advertised size",
            )).into());
        }
        partial.file.write_all(&buffer[..read]).context("Cannot write SFTP partial file")?;
        written += read as u64;
        sink.progress(written);
        if written - partial.offset >= 8 * 1024 * 1024 {
            partial.checkpoint(written)?;
        }
    }
    check_cancel(cancel)?;
    ensure!(remote.read(&mut [0_u8; 1]).map_err(RemoteRead)? == 0, "SFTP file grew during transfer");
    Ok(())
}

#[derive(Debug)]
pub(crate) struct RemoteRead(pub std::io::Error);

impl std::fmt::Display for RemoteRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // libssh2's Read adapter erases its numeric code into io::Error.
        // Report the stable kind, never a server-controlled message.
        write!(f, "SFTP read failed ({:?})", self.0.kind())
    }
}

impl std::error::Error for RemoteRead {}

fn size_limit() -> Result<u64> {
    match std::env::var("RDM_MAX_FILE_BYTES") {
        Ok(value) => value.parse().context("RDM_MAX_FILE_BYTES must be an unsigned integer"),
        Err(std::env::VarError::NotPresent) => Ok(64 * 1024 * 1024 * 1024),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Invalid RDM_MAX_FILE_BYTES"),
    }
}
