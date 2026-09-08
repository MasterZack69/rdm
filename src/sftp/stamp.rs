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

impl FileStamp {
    pub fn from_remote(stat: &ssh2::FileStat) -> Result<Self> {
        ensure!(stat.is_file(), "SFTP target is not a regular file");
        Ok(Self {
            size: stat.size.context("SFTP server did not report the file size")?,
            modified: stat.mtime,
        })
    }

    pub fn matches_local(&self, meta: &Metadata) -> bool {
        let modified = meta.modified().ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|time| time.as_secs());
        meta.is_file() && meta.len() == self.size
            && self.modified.is_some() && self.modified == modified
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub url: String,
    pub host_key: String,
    pub stamp: FileStamp,
}

impl Identity {
    pub fn can_resume(&self, saved: &Self) -> bool {
        self == saved && self.stamp.modified.is_some()
    }
}
