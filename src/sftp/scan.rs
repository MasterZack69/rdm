//! Bounded SFTP directory enumeration, not HTML or textual `ls` parsing.

use anyhow::{Context, Result, ensure};
use ssh2::{ErrorCode, Sftp};
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use super::session::{check_cancel, with_session};
use super::stamp::FileStamp;
use super::url::validate_name;
use super::{SftpOptions, SftpUrl};

const MAX_ENTRIES: usize = 200_000;
const MAX_FILES: usize = 100_000;
const MAX_DIRS: usize = 10_000;
const MAX_DEPTH: usize = 64;
const MAX_DURATION: Duration = Duration::from_secs(600);
// ssh2::File::readdir reports end-of-directory as LIBSSH2_ERROR_FILE.
const DIRECTORY_EOF: i32 = -16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub url: String,
    pub relative_path: String,
    pub size: u64,
    pub modified: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listing {
    pub root_url: String,
    pub files: Vec<RemoteFile>,
    /// Symlinks and special files are never followed. A nonzero count forbids
    /// sync deletion because their local counterparts are not known orphans.
    pub skipped: usize,
}

/// None means a regular file. Some(empty) is a successfully listed empty
/// directory. Errors and partial listings must never collapse into None.
pub async fn list(
    url: &str,
    options: &SftpOptions,
    allow_private: bool,
    cancel: CancellationToken,
) -> Result<Option<Listing>> {
    with_session(
        SftpUrl::parse(url)?, Arc::clone(&options.authentication), allow_private, cancel,
        |_, sftp, target, cancel| scan(sftp, target, cancel),
    ).await
}

/// Adapter for RDM's existing directory-discovery interface.
pub async fn discover_files(
    url: &str,
    wrap_in_folder: bool,
    allow_private: bool,
) -> Result<Option<Vec<crate::scrape::DiscoveredFile>>> {
    let cfg = crate::config::Config::load();
    let options = SftpOptions::from_config(&cfg)?;
    let target = SftpUrl::parse(url)?;
    let Some(listing) = list(url, &options, allow_private, CancellationToken::new()).await? else {
        return Ok(None);
    };
    ensure!(listing.skipped == 0, "SFTP discovery omitted symlinks or special files; refusing an incomplete listing");
    Ok(Some(listing.files.into_iter().map(|file| {
        crate::scrape::DiscoveredFile {
            url: file.url,
            relative_path: if wrap_in_folder {
                format!("{}/{}", target.folder_name(), file.relative_path)
            } else {
                file.relative_path
            },
        }
    }).collect()))
}

fn scan(sftp: &Sftp, target: &SftpUrl, cancel: &CancellationToken) -> Result<Option<Listing>> {
    check_cancel(cancel)?;
    ensure!(sftp.realpath(target.path())?.as_path() == target.path(), "SFTP paths must be canonical; remote symlinks are not followed");
    let root_stat = sftp.lstat(target.path()).context("Cannot inspect SFTP source")?;
    if root_stat.is_file() {
        return Ok(None);
    }
    ensure!(root_stat.is_dir(), "SFTP source is not a regular file or directory");
    let started = Instant::now();
    let mut pending = VecDeque::from([(PathBuf::new(), 0_usize)]);
    let mut seen = HashSet::new();
    let mut files = Vec::new();
    let mut skipped = 0;
    let mut entries = 0;
    let mut directories = 0;
    while let Some((relative, depth)) = pending.pop_front() {
        check_cancel(cancel)?;
        directories += 1;
        ensure!(directories <= MAX_DIRS && depth <= MAX_DEPTH, "SFTP directory scan limit exceeded");
        ensure!(started.elapsed() <= MAX_DURATION, "SFTP scan exceeded ten minutes");
        let path = target.path().join(&relative);
        ensure!(sftp.realpath(&path)? == path, "SFTP directory changed to a symlink during scanning");
        let before = sftp.lstat(&path)?;
        ensure!(before.is_dir(), "SFTP directory changed while scanning");
        let mut directory = sftp.opendir(&path).context("Cannot open SFTP directory")?;
        loop {
            check_cancel(cancel)?;
            ensure!(started.elapsed() <= MAX_DURATION, "SFTP scan exceeded ten minutes");
            let (name, _) = match directory.readdir() {
                Ok(entry) => entry,
                Err(error) if error.code() == ErrorCode::Session(DIRECTORY_EOF) => break,
                Err(error) => return Err(error).context("SFTP directory enumeration failed"),
            };
            entries += 1;
            ensure!(entries <= MAX_ENTRIES, "SFTP entry scan limit exceeded");
            let name = name.to_str().context("Non-UTF-8 SFTP filename")?;
            if name == "." || name == ".." { continue; }
            validate_name(name)?;
            let child = relative.join(name);
            ensure!(seen.insert(child.clone()), "SFTP server returned a duplicate directory entry");
            let child_url = target.with_path(&target.path().join(&child))?;
            let stat = sftp.lstat(child_url.path()).context("Cannot inspect SFTP directory entry")?;
            if stat.is_dir() {
                ensure!(depth < MAX_DEPTH && pending.len() + directories < MAX_DIRS, "SFTP directory scan limit exceeded");
                pending.push_back((child, depth + 1));
            } else if stat.is_file() {
                let stamp = FileStamp::from_remote(&stat)?;
                ensure!(files.len() < MAX_FILES, "SFTP file scan limit exceeded");
                files.push(RemoteFile {
                    url: child_url.as_str().to_owned(),
                    relative_path: super::local::path_text(&child)?.to_owned(),
                    size: stamp.size,
                    modified: stamp.modified,
                });
            } else {
                skipped += 1;
            }
        }
        directory.close().context("Cannot close SFTP directory")?;
        let after = sftp.lstat(&path)?;
        ensure!(after.is_dir() && before.mtime == after.mtime, "SFTP directory changed during scanning; rerun the command");
    }
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(Some(Listing { root_url: target.as_str().to_owned(), files, skipped }))
}
