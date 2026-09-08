//! Every network-derived component is walked beneath an explicit trust root.

use anyhow::{Context, Result, ensure};
use std::fs::{File, Metadata};
use std::os::fd::AsRawFd;
use std::path::{Component, Path, PathBuf};

use crate::safe_file::{self, Access, Existing, PRIVATE_FILE_MODE};

pub(crate) const STATE_DIR: &str = ".rdm-sftp";

#[derive(Clone, Debug)]
pub(crate) struct Destination {
    pub root: PathBuf,
    pub relative: PathBuf,
}

impl Destination {
    pub fn beneath(root: &Path, relative: &str) -> Result<Self> {
        ensure!(!relative.is_empty(), "Destination must name a file");
        for name in relative.split('/') {
            super::url::validate_name(name)?;
        }
        Ok(Self {
            root: std::path::absolute(root)?,
            relative: PathBuf::from(relative),
        })
    }

    pub fn from_output(output: &Path, configured_root: &Path) -> Result<Self> {
        let output = std::path::absolute(output)?;
        let configured_root = std::path::absolute(configured_root)?;
        // A queued absolute output does not retain its original trust root.
        // Outside download_dir, walk from / rather than trusting an arbitrary
        // parent which may have come from a directory listing.
        let root = if output.starts_with(&configured_root) {
            configured_root
        } else {
            PathBuf::from("/")
        };
        let relative = output.strip_prefix(&root)?.to_path_buf();
        ensure!(
            !relative.as_os_str().is_empty()
                && relative.components().all(|part| matches!(part, Component::Normal(_))),
            "Destination must be a normal file path without '..'"
        );
        ensure!(
            !relative.iter().any(|name| name == STATE_DIR),
            "The .rdm-sftp directory is reserved for transfer state"
        );
        Ok(Self { root, relative })
    }

    pub fn path(&self) -> PathBuf {
        self.root.join(&self.relative)
    }

    pub fn prepare(&self) -> Result<()> {
        std::fs::create_dir_all(&self.root).context("Cannot create SFTP download root")?;
        safe_file::create_dirs_beneath(
            &self.root,
            self.relative.parent().unwrap_or(Path::new("")),
        )
    }

    pub fn metadata(&self) -> Result<Option<Metadata>> {
        safe_file::verify_dir_beneath(
            &self.root,
            self.relative.parent().unwrap_or(Path::new("")),
        )?;
        match std::fs::symlink_metadata(self.path()) {
            Ok(meta) => {
                ensure!(meta.is_file(), "SFTP destination is not a regular file");
                Ok(Some(meta))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).context("Cannot inspect SFTP destination"),
        }
    }

    pub fn display_path(&self) -> Result<String> {
        Ok(path_text(&self.path())?.to_owned())
    }
}

pub(crate) fn path_text(path: &Path) -> Result<&str> {
    path.to_str().context("RDM requires UTF-8 output paths")
}

pub(crate) fn state_paths(relative: &Path) -> Result<(PathBuf, String)> {
    use sha2::{Digest, Sha256};
    let name = relative.file_name().context("Missing output filename")?;
    let key = format!("{:x}", Sha256::digest(name.as_encoded_bytes()));
    let directory = relative.parent().unwrap_or(Path::new("")).join(STATE_DIR);
    Ok((directory, key))
}

pub(crate) fn lock_output(root: &Path, relative: &Path) -> Result<File> {
    let (directory, key) = state_paths(relative)?;
    safe_file::create_dirs_beneath(root, &directory)?;
    lock_file(root, &directory.join(format!("{key}.lock")))
}

/// Keep the lock file's inode alive. Removing it would allow a second process
/// to lock a replacement inode while the first process still owns this one.
pub(crate) fn lock_file(root: &Path, relative: &Path) -> Result<File> {
    let file = safe_file::open_beneath(
        root, relative, Existing::Open, Access::ReadWrite, PRIVATE_FILE_MODE,
    )?;
    // SAFETY: file owns a valid descriptor for the duration of flock and the
    // returned File holds the lock until it is dropped. No raw pointer is used.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .context("Another SFTP transfer or sync owns this destination");
    }
    Ok(file)
}
