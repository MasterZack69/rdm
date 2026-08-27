//! Creating the files that hold credentials.
//!
//! Three of the files rdm writes carry something worth stealing:
//!
//! - `config.toml`: the GoFile account token, the pixeldrain API key, the
//!   Google Drive API key.
//! - `queue.json`: every queued URL, which for a private share *is* the
//!   credential.
//! - `<file>.rdm`: the URL a download is resuming from. A MEGA link carries
//!   the decryption key in its fragment, and a OneDrive direct link carries a
//!   `tempauth` signature.
//!
//! All three used to be created with a plain [`std::fs::File::create`], which
//! requests mode 0666 and leaves the rest to the umask. The common 022 turns
//! that into 0644, so on a multi-user host every other account can read them.
//!
//! Everything here is a normal create on non-Unix platforms. Windows has no
//! umask and files inherit an ACL from the user profile directory, which is
//! already private; there is nothing equivalent to set.

use std::io;
use std::path::Path;

/// Owner read/write, nothing for group or other.
#[cfg(unix)]
pub const OWNER_ONLY_FILE: u32 = 0o600;

/// Owner rwx, nothing for group or other. Directory mode needs the execute
/// bit or the owner cannot traverse into it.
#[cfg(unix)]
pub const OWNER_ONLY_DIR: u32 = 0o700;

/// Creates or truncates `path` so that only the owner can read it.
///
/// The mode is applied twice on purpose. [`OpenOptions::mode`] only affects a
/// file this call actually creates, so a `config.toml` left at 0644 by an
/// older build would keep those permissions for the rest of its life. Setting
/// the mode after opening repairs that on the next write.
///
/// [`OpenOptions::mode`]: std::os::unix::fs::OpenOptionsExt::mode
pub fn create(path: &Path) -> io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(OWNER_ONLY_FILE);
    }

    let file = options.open(path)?;
    harden(&file)?;
    Ok(file)
}

/// The async twin of [`create`], for the callers already inside a runtime.
///
/// Built from the blocking open rather than from `tokio::fs::OpenOptions` so
/// that the mode handling lives in exactly one place.
pub async fn create_async(path: &Path) -> io::Result<tokio::fs::File> {
    let path = path.to_owned();
    let file = tokio::task::spawn_blocking(move || create(&path))
        .await
        .map_err(io::Error::other)??;
    Ok(tokio::fs::File::from_std(file))
}

/// Restricts an already-open file to its owner.
#[cfg(unix)]
pub fn harden(file: &std::fs::File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(OWNER_ONLY_FILE))
}

#[cfg(not(unix))]
pub fn harden(_file: &std::fs::File) -> io::Result<()> {
    Ok(())
}

/// Creates `path` and every missing parent, then restricts `path` itself to
/// its owner.
///
/// Only the final component is tightened. The parents are things like
/// `~/.config`, which belong to the user rather than to rdm and may well be
/// shared with other tools that expect to traverse them.
///
/// A failure to tighten is not fatal: the directory exists and the files
/// inside it are created 0600 regardless, so the run can continue.
pub fn create_dir_all(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(OWNER_ONLY_DIR));
    }

    Ok(())
}

/// Writes `data` to `path` in one call, owner-readable only.
pub fn write(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write;

    let mut file = create(path)?;
    file.write_all(data)?;
    file.flush()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    /// The whole point: a 022 umask must not be able to widen this.
    #[test]
    fn a_created_file_is_readable_only_by_its_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        write(&path, b"gofile_token = \"secret\"").unwrap();

        assert_eq!(mode_of(&path), OWNER_ONLY_FILE);
    }

    /// `OpenOptions::mode` is ignored for a file that already exists, so
    /// without the explicit `set_permissions` a config written by an older
    /// build would stay world-readable through every later save.
    #[test]
    fn an_existing_world_readable_file_is_repaired_on_the_next_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");

        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(mode_of(&path), 0o644);

        write(&path, b"{\"items\":[]}").unwrap();

        assert_eq!(mode_of(&path), OWNER_ONLY_FILE);
    }

    #[test]
    fn writing_replaces_rather_than_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        write(&path, b"aaaaaaaaaaaaaaaaaaaa").unwrap();
        write(&path, b"bb").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"bb");
    }

    #[test]
    fn a_created_directory_is_traversable_only_by_its_owner() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("rdm");

        create_dir_all(&nested).unwrap();

        assert_eq!(mode_of(&nested), OWNER_ONLY_DIR);
    }

    #[test]
    fn creating_a_directory_that_already_exists_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("rdm");

        create_dir_all(&nested).unwrap();
        create_dir_all(&nested).unwrap();

        assert_eq!(mode_of(&nested), OWNER_ONLY_DIR);
    }

    #[tokio::test]
    async fn the_async_twin_agrees_with_the_blocking_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resume.rdm");

        {
            use tokio::io::AsyncWriteExt;
            let mut file = create_async(&path).await.unwrap();
            file.write_all(b"{}").await.unwrap();
            file.flush().await.unwrap();
        }

        assert_eq!(mode_of(&path), OWNER_ONLY_FILE);
    }
}
