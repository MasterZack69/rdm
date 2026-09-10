//! Durable checkpoints and atomic publication. The old output survives until
//! its replacement is complete; HTTP .part/.rdm files are never reused.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::safe_file::{self, Access, Existing, DEFAULT_FILE_MODE, PRIVATE_FILE_MODE};
use super::local::{Destination, lock_output, state_paths, try_lock_output};
use super::stamp::Identity;

const MAX_METADATA: u64 = 32 * 1024;

#[derive(Serialize, Deserialize)]
struct Record {
    version: u8,
    identity: Identity,
    committed: u64,
}

pub(crate) struct Partial {
    pub file: File,
    pub offset: u64,
    destination: Destination,
    identity: Identity,
    directory: PathBuf,
    part: PathBuf,
    metadata: PathBuf,
    _lock: File,
}

impl Partial {
    pub fn open(destination: Destination, identity: Identity) -> Result<Self> {
        destination.prepare()?;
        let (directory, key) = state_paths(&destination.relative)?;
        let lock = lock_output(&destination.root, &destination.relative)?;
        let part = directory.join(format!("{key}.part"));
        let metadata = directory.join(format!("{key}.json"));
        let file = safe_file::open_beneath(
            &destination.root, &part, Existing::Open, Access::ReadWrite, DEFAULT_FILE_MODE,
        )?;
        let mut meta = safe_file::open_beneath(
            &destination.root, &metadata, Existing::Open, Access::ReadWrite, PRIVATE_FILE_MODE,
        )?;
        let record = if meta.metadata()?.len() <= MAX_METADATA {
            let mut bytes = Vec::new();
            (&mut meta).take(MAX_METADATA + 1).read_to_end(&mut bytes)?;
            if bytes.len() as u64 <= MAX_METADATA {
                serde_json::from_slice::<Record>(&bytes).ok()
            } else {
                None
            }
        } else {
            None
        };
        drop(meta);
        let length = file.metadata()?.len();
        let offset = record.filter(|record| {
            record.version == 1 && identity.can_resume(&record.identity)
                && record.committed <= length && record.committed <= identity.stamp.size
        }).map_or(0, |record| record.committed);
        let mut partial = Self {
            file, offset, destination, identity, directory, part, metadata, _lock: lock,
        };
        // Discard writes beyond the last fsynced checkpoint after a crash.
        partial.file.set_len(offset)?;
        partial.file.seek(SeekFrom::Start(offset))?;
        partial.checkpoint(offset)?;
        Ok(partial)
    }

    pub fn checkpoint(&mut self, committed: u64) -> Result<()> {
        self.file.sync_data().context("Cannot flush SFTP partial file")?;
        let record = Record { version: 1, identity: self.identity.clone(), committed };
        let temp = self.directory.join(format!("{}.tmp", safe_file::random_token()));
        let result = (|| {
            let mut file = safe_file::open_beneath(
                &self.destination.root, &temp, Existing::Reject, Access::ReadWrite, PRIVATE_FILE_MODE,
            )?;
            serde_json::to_writer(&mut file, &record).context("Cannot encode SFTP checkpoint")?;
            file.flush()?;
            file.sync_all()?;
            safe_file::rename_beneath(&self.destination.root, &temp, &self.metadata, true)
        })();
        if result.is_err() {
            let _ = safe_file::unlink_beneath(&self.destination.root, &temp);
        }
        result?;
        self.offset = committed;
        Ok(())
    }

    pub fn invalidate(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.checkpoint(0)
    }

    pub fn publish(self, replace: bool) -> Result<()> {
        self.file.sync_all().context("Cannot flush completed SFTP file")?;
        safe_file::rename_beneath(
            &self.destination.root, &self.part, &self.destination.relative, replace,
        )?;
        // The payload is already published. A leftover checkpoint is harmless
        // and will be reset if its part file is no longer there.
        let _ = safe_file::unlink_beneath(&self.destination.root, &self.metadata);
        Ok(())
    }
}

/// Removes the transfer state of a destination that already matches the
/// server, and reports the bytes reclaimed.
///
/// A cancelled or failed transfer keeps its partial payload deliberately, and
/// the published file is never replaced until that payload is complete. Once
/// the published file matches the listing again, those bytes are unreachable:
/// nothing schedules the file, so `Partial::open` is never reached to resume
/// them, and they would be discarded anyway as soon as the remote size or
/// timestamp changed. State for a file that is *still* stale is left alone,
/// which is the case resume exists for.
pub(crate) fn discard_state(destination: &Destination) -> Result<u64> {
    let (directory, key) = state_paths(&destination.relative)?;
    let part = directory.join(format!("{key}.part"));
    // A file that was simply already current has no state at all, so the
    // common case takes no lock and creates no state directory for it.
    if std::fs::symlink_metadata(destination.root.join(&part)).is_err() {
        return Ok(0);
    }
    let Some(_lock) = try_lock_output(&destination.root, &destination.relative)? else {
        // Another transfer owns this destination, so its state is in use.
        return Ok(0);
    };
    let reclaimed = match std::fs::symlink_metadata(destination.root.join(&part)) {
        Ok(meta) if meta.is_file() => meta.len(),
        // Anything else is not ours to account for. The unlink below still
        // refuses to follow it.
        _ => 0,
    };
    if safe_file::unlink_beneath(&destination.root, &part).is_err() {
        return Ok(0);
    }
    let _ = safe_file::unlink_beneath(&destination.root, &directory.join(format!("{key}.json")));
    Ok(reclaimed)
}
