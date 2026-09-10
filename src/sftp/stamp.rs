//! SFTP v3 exposes size and second-resolution mtime, not a content digest.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::fs::Metadata;
use std::time::UNIX_EPOCH;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileStamp {
    pub size: u64,
    pub modified: Option<u64>,
}

/// How an existing local file is judged against a remote listing.
///
/// The HTTP mirror compares sizes and nothing else, so sizes decide here as
/// well. Also requiring the timestamps to match meant that a library written
/// by anything else - another tool, an earlier rdm, a restored backup - was
/// stale in its entirety, because its mtimes record when it was written
/// locally rather than what the server reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Compare {
    /// Same size, same file. A differing timestamp is repaired, not fetched.
    Size,
    /// Same size and same mtime, within the modify window.
    SizeAndTime,
}

impl Compare {
    /// `RDM_SFTP_SYNC_COMPARE`: `size` (default) or `size+mtime`.
    pub fn from_env() -> Result<Self> {
        let value = match std::env::var("RDM_SFTP_SYNC_COMPARE") {
            Err(std::env::VarError::NotPresent) => return Ok(Self::Size),
            Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Invalid RDM_SFTP_SYNC_COMPARE"),
            Ok(value) => value,
        };
        let value = value.trim();
        if value.eq_ignore_ascii_case("size") {
            Ok(Self::Size)
        } else if value.eq_ignore_ascii_case("size+mtime") {
            Ok(Self::SizeAndTime)
        } else {
            anyhow::bail!("RDM_SFTP_SYNC_COMPARE must be 'size' or 'size+mtime'")
        }
    }
}

/// Timestamp slack, in whole seconds.
///
/// FAT and exFAT store mtimes in two-second units, and SMB and some NFS
/// mounts round as well, so exact equality would make those mirrors
/// re-download their whole tree on every run. 0 requires exact equality.
pub(crate) fn modify_window() -> Result<u64> {
    match std::env::var("RDM_SFTP_MODIFY_WINDOW") {
        Err(std::env::VarError::NotPresent) => Ok(2),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Invalid RDM_SFTP_MODIFY_WINDOW"),
        Ok(value) => value.trim().parse()
            .context("RDM_SFTP_MODIFY_WINDOW must be a whole number of seconds"),
    }
}

/// Why a local file is not the remote one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Difference {
    Missing,
    /// Changed between being inspected and being repaired.
    Replaced,
    Size { local: u64 },
    Modified { local: Option<u64> },
}

/// What sync should do with a local file that is already there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    Current,
    /// The same bytes as far as this protocol can tell. Adopting the server's
    /// timestamp costs no transfer and lets later runs decide by equality.
    Retime { seconds: u64 },
    /// Another transfer owns the destination. Only the repair path reports
    /// this; a comparison alone cannot observe it.
    Busy,
    Stale(Difference),
}

impl FileStamp {
    pub fn from_remote(stat: &ssh2::FileStat) -> Result<Self> {
        ensure!(stat.is_file(), "SFTP target is not a regular file");
        Ok(Self {
            size: stat.size.context("SFTP server did not report the file size")?,
            modified: stat.mtime,
        })
    }

    /// The local mtime in the resolution the server reports.
    fn local_modified(meta: &Metadata) -> Option<u64> {
        meta.modified().ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|time| time.as_secs())
    }

    pub fn compare_local(&self, meta: &Metadata, compare: Compare, window: u64) -> Verdict {
        if meta.len() != self.size {
            return Verdict::Stale(Difference::Size { local: meta.len() });
        }
        let local = Self::local_modified(meta);
        if let (Some(remote), Some(local)) = (self.modified, local)
            && remote.abs_diff(local) <= window
        {
            return Verdict::Current;
        }
        match (compare, self.modified) {
            (Compare::Size, Some(seconds)) => Verdict::Retime { seconds },
            // The sizes agree and there is no server timestamp to adopt.
            (Compare::Size, None) => Verdict::Current,
            (Compare::SizeAndTime, _) => Verdict::Stale(Difference::Modified { local }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub url: String,
    pub host_key: String,
    pub stamp: FileStamp,
}

impl Identity {
    /// Resume asks a different question from freshness: these partial bytes
    /// have to belong to exactly the file still on the server, so this stays
    /// an exact match with no window and no repair.
    pub fn can_resume(&self, saved: &Self) -> bool {
        self == saved && self.stamp.modified.is_some()
    }
}
