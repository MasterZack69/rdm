//! Filesystem operations that resist symlink swaps and TOCTOU races.
//!
//! ## The problem
//!
//! Every temporary path rdm writes is derived from the output path, so it is
//! entirely predictable:
//!
//! ```text
//! <output>.part        streaming download in progress
//! <output>.rdm.tmp     resume metadata being rewritten
//! ```
//!
//! They were opened with `File::create` and `OpenOptions::open`, both of which
//! follow symlinks. Anyone able to create entries in the download directory
//! could therefore drop a symlink at one of those names and have rdm truncate
//! or append to whatever it points at. The parallel path made it worse by
//! doing a `metadata()` check and *then* opening and resizing the path, and
//! the publish step checked whether the destination existed and *then*
//! renamed onto it. Both gaps are races: a `symlink_metadata` followed by an
//! `open` proves nothing about the file that `open` actually reached.
//!
//! This matters most exactly where rdm is most useful — a shared `/downloads`
//! on a NAS, a seedbox, a container running as root, a systemd unit.
//!
//! ## The approach
//!
//! Split the path into its parent directory and its final component. Open the
//! parent with `O_DIRECTORY | O_CLOEXEC`, then open the final component
//! *relative to that descriptor* with `openat2` and
//! `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`. The kernel then refuses a symlink
//! at the final component, refuses `..`, and refuses an absolute path,
//! atomically, with no window between the decision and the open.
//!
//! The parent directory is resolved normally: the user chose it (via config or
//! `-o`) and it is not attacker-supplied, so following a symlink to reach it
//! is intended. What must not be followed is the last component, which is the
//! part an attacker can create.
//!
//! Where `openat2` is unavailable — pre-5.6 kernels, or a seccomp filter that
//! rejects it — we fall back to `openat` with `O_NOFOLLOW`, which covers the
//! symlink-at-final-component case (the actual attack) even though it cannot
//! express `RESOLVE_BENEATH`. Since the name is a single component with no
//! separators, that is nearly the same guarantee.
//!
//! Every descriptor is then validated with `fstat` — on the descriptor, never
//! on the path, so there is nothing left to race.

use anyhow::{Context, Result, bail};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

/// Default permissions for a downloaded file: owner read/write, group and
/// other read. Mirrors what `File::create` produces under a normal umask.
pub const DEFAULT_FILE_MODE: u32 = 0o644;

/// Permissions for anything that might contain a credential.
pub const PRIVATE_FILE_MODE: u32 = 0o600;

/// How a [`open_guarded`] call should treat an existing file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Existing {
    /// Fail with `AlreadyExists` if anything is already there. `O_EXCL`.
    ///
    /// The right choice for a freshly created temp file: it means we know we
    /// created it, so nothing else can have prepared it for us.
    Reject,
    /// Open the existing file, or create it if absent. Never follows a
    /// symlink either way.
    Open,
    /// Open the existing file and truncate it. Fails if absent.
    Truncate,
}

/// How the file should be positioned for writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Read and write, positioned at the start. Used by the parallel writer,
    /// which seeks to each chunk's offset.
    ReadWrite,
    /// Append only. Used when resuming a `.part`, so a bad offset cannot
    /// overwrite bytes already verified.
    Append,
}

/// Opens `path` without following a symlink at its final component.
///
/// The returned descriptor is validated: it is a regular file owned by the
/// current effective uid.
pub fn open_guarded(path: &Path, existing: Existing, access: Access, mode: u32) -> Result<File> {
    let file = open_impl(path, existing, access, mode)
        .with_context(|| format!("Failed to safely open {}", path.display()))?;

    validate_regular_owned(&file)
        .with_context(|| format!("Refusing to write to {}", path.display()))?;

    Ok(file)
}

/// Creates a randomly named temp file in `dir`, returning it and its path.
///
/// `<output>.part` is guessable, so an attacker knows the name to plant a
/// symlink at before rdm starts. A random suffix removes that, and `O_EXCL`
/// means a lucky guess still fails rather than being silently reused.
pub fn create_temp_in(dir: &Path, prefix: &str, mode: u32) -> Result<(File, PathBuf)> {
    let mut last_err = None;

    // Retries cover an actual random collision, which is vanishingly rare, and
    // an attacker pre-creating guessed names, which is not worth many attempts.
    for _ in 0..8 {
        let candidate = dir.join(format!("{}.{}.part", prefix, random_token()));

        match open_impl(&candidate, Existing::Reject, Access::ReadWrite, mode) {
            Ok(file) => {
                validate_regular_owned(&file).with_context(|| {
                    format!("Refusing to write to {}", candidate.display())
                })?;
                return Ok((file, candidate));
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => last_err = Some(e),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("Failed to create a temporary file in {}", dir.display())
                });
            }
        }
    }

    Err(last_err.unwrap_or_else(|| io::Error::other("exhausted temp name attempts")))
        .with_context(|| format!("Failed to create a temporary file in {}", dir.display()))
}

/// Renames `from` to `to`, failing if `to` already exists.
///
/// The publish step used to check whether the destination existed and then
/// rename, which replaces a file created in between. `RENAME_NOREPLACE` makes
/// the check and the rename one operation.
///
/// Use [`rename_replacing`] only where the user has actually approved an
/// overwrite (`--force`, or an explicit redownload).
pub fn rename_no_replace(from: &Path, to: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        match linux::renameat2_noreplace(from, to) {
            Ok(()) => return Ok(()),
            Err(e) if is_unsupported(&e) => {
                // Older kernel or an exotic filesystem: fall through to the
                // link/unlink emulation below.
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("Failed to move {} into place", from.display())
                });
            }
        }
    }

    // `link` fails with EEXIST if the destination exists, which gives the same
    // no-clobber guarantee atomically. Then drop the temp name.
    std::fs::hard_link(from, to).with_context(|| {
        format!(
            "Failed to move {} into place (destination may already exist)",
            from.display()
        )
    })?;

    if let Err(e) = std::fs::remove_file(from) {
        // The file is published; a leftover temp name is cosmetic.
        debug_assert!(false, "failed to unlink temp after publish: {e}");
    }

    Ok(())
}

/// Renames `from` over `to`, replacing it. Only for an approved overwrite.
pub fn rename_replacing(from: &Path, to: &Path) -> Result<()> {
    std::fs::rename(from, to)
        .with_context(|| format!("Failed to move {} into place", from.display()))
}

/// Requires the descriptor to be a regular file owned by the current user.
///
/// Checked on the descriptor rather than the path, so unlike
/// `symlink_metadata` there is no window in which the answer can change.
/// Rejecting non-regular files stops rdm being pointed at a FIFO (which would
/// hang) or a device node (which would be far worse).
pub fn validate_regular_owned(file: &File) -> Result<()> {
    #[cfg(unix)]
    {
        let stat = fstat(file.as_raw_fd())?;

        if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            bail!("Destination is not a regular file");
        }

        // SAFETY: geteuid cannot fail and takes no arguments.
        let uid = unsafe { libc::geteuid() };
        if stat.st_uid != uid {
            bail!(
                "Destination is owned by uid {} but rdm runs as uid {}",
                stat.st_uid,
                uid
            );
        }

        // More than one link means the same inode is reachable under another
        // name, which is how a hard-link swap survives an O_NOFOLLOW open.
        if stat.st_nlink > 1 {
            bail!(
                "Destination has {} hard links; refusing to write through it",
                stat.st_nlink
            );
        }

        Ok(())
    }

    #[cfg(not(unix))]
    {
        let meta = file.metadata().context("Failed to stat destination")?;
        if !meta.is_file() {
            bail!("Destination is not a regular file");
        }
        Ok(())
    }
}

/// Free space available to this user on the filesystem holding `dir`.
///
/// `None` when it cannot be determined, so callers must treat the check as
/// advisory and never as permission to skip a byte ceiling.
pub fn available_bytes(dir: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_dir = CString::new(dir.as_os_str().as_bytes()).ok()?;
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };

        // SAFETY: c_dir is a valid NUL-terminated string and stat is a valid
        // writable statvfs.
        let rc = unsafe { libc::statvfs(c_dir.as_ptr(), &mut stat) };
        if rc != 0 {
            return None;
        }

        // f_bavail is what an unprivileged user may actually use, which is the
        // number that matters; f_bfree includes the reserved blocks.
        let block = if stat.f_frsize > 0 {
            stat.f_frsize as u64
        } else {
            stat.f_bsize as u64
        };

        Some((stat.f_bavail as u64).saturating_mul(block))
    }

    #[cfg(not(unix))]
    {
        let _ = dir;
        None
    }
}

/// 128 bits of hex from the OS CSPRNG, for temp file names.
pub fn random_token() -> String {
    let bytes = random_bytes();
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

fn random_bytes() -> [u8; 16] {
    let mut buf = [0u8; 16];

    #[cfg(unix)]
    {
        use std::io::Read;

        if let Ok(mut urandom) = File::open("/dev/urandom")
            && urandom.read_exact(&mut buf).is_ok()
        {
            return buf;
        }
    }

    // Fallback: the std hasher is seeded from the OS. Weaker than urandom, but
    // this only runs if /dev/urandom is unavailable, and it is still far less
    // predictable than a fixed ".part" suffix.
    use std::hash::{BuildHasher, Hash, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    let a = hasher.finish().to_le_bytes();

    let b = std::collections::hash_map::RandomState::new().hash_one(a).to_le_bytes();

    buf[..8].copy_from_slice(&a);
    buf[8..].copy_from_slice(&b);
    buf
}

// ---------------------------------------------------------------------------
// Unix implementation
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn open_impl(path: &Path, existing: Existing, access: Access, mode: u32) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let (dir, name) = split_parent(path)?;

    let dir_fd = OwnedFd::open_dir(&dir)?;

    let c_name = CString::new(name.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "filename contains NUL"))?;

    let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW;

    flags |= match access {
        Access::ReadWrite => libc::O_RDWR,
        Access::Append => libc::O_WRONLY | libc::O_APPEND,
    };

    flags |= match existing {
        Existing::Reject => libc::O_CREAT | libc::O_EXCL,
        Existing::Open => libc::O_CREAT,
        Existing::Truncate => libc::O_TRUNC,
    };

    #[cfg(target_os = "linux")]
    {
        match linux::openat2(dir_fd.fd, &c_name, flags, mode) {
            Ok(fd) => {
                // SAFETY: openat2 returned a fresh owned descriptor.
                return Ok(unsafe { File::from_raw_fd(fd) });
            }
            Err(e) if is_unsupported(&e) => {
                // Pre-5.6 kernel, or seccomp. Fall through to openat, which
                // still carries O_NOFOLLOW.
            }
            Err(e) => return Err(e),
        }
    }

    // SAFETY: dir_fd is open, c_name is NUL-terminated, and mode is only read
    // when O_CREAT is set.
    let fd = unsafe { libc::openat(dir_fd.fd, c_name.as_ptr(), flags, mode as libc::c_uint) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: openat returned a fresh owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn split_parent(path: &Path) -> io::Result<(PathBuf, PathBuf)> {
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "path has no final component",
        )
    })?;

    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        // A bare filename is relative to the process's cwd.
        _ => PathBuf::from("."),
    };

    Ok((parent, PathBuf::from(name)))
}

/// A raw descriptor that closes itself. Only used for the directory handle.
#[cfg(unix)]
struct OwnedFd {
    fd: RawFd,
}

#[cfg(unix)]
impl OwnedFd {
    fn open_dir(dir: &Path) -> io::Result<Self> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_dir = CString::new(dir.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;

        // The parent directory is chosen by the user, not by the network, so
        // resolving it normally (following symlinks) is intended. O_DIRECTORY
        // still guarantees we ended up at a directory.
        // SAFETY: c_dir is a valid NUL-terminated string.
        let fd = unsafe {
            libc::open(
                c_dir.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };

        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { fd })
    }
}

#[cfg(unix)]
impl Drop for OwnedFd {
    fn drop(&mut self) {
        // SAFETY: we own this descriptor and this runs once.
        unsafe { libc::close(self.fd) };
    }
}

#[cfg(unix)]
fn fstat(fd: RawFd) -> Result<libc::stat> {
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };

    // SAFETY: fd is open and stat is a valid writable stat buffer.
    let rc = unsafe { libc::fstat(fd, &mut stat) };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("fstat failed");
    }

    Ok(stat)
}

/// True when the kernel does not implement the syscall, as opposed to
/// refusing this particular call. Only these justify a fallback; a real
/// permission or symlink refusal must propagate.
fn is_unsupported(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP) | Some(libc::EINVAL)
    )
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    /// `struct open_how`, from `include/uapi/linux/openat2.h`. Passing a size
    /// the kernel knows lets it reject a struct it does not understand rather
    /// than misread it.
    #[repr(C)]
    #[derive(Default)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }

    /// Refuse every symlink in the resolution, including the final component.
    const RESOLVE_NO_SYMLINKS: u64 = 0x04;
    /// Refuse anything escaping the directory the fd points at: `..`, an
    /// absolute path, or a magic link.
    const RESOLVE_BENEATH: u64 = 0x08;

    /// Syscall numbers from 424 up are the same on every architecture, so this
    /// does not need a per-arch table and does not depend on the libc crate
    /// exposing the constant.
    const SYS_OPENAT2: libc::c_long = 437;

    const RENAME_NOREPLACE: libc::c_uint = 1;

    pub(super) fn openat2(
        dir_fd: RawFd,
        name: &CString,
        flags: libc::c_int,
        mode: u32,
    ) -> io::Result<RawFd> {
        let how = OpenHow {
            flags: flags as u64,
            mode: if flags & libc::O_CREAT != 0 {
                mode as u64
            } else {
                // The kernel rejects a non-zero mode without O_CREAT.
                0
            },
            resolve: RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        };

        // SAFETY: dir_fd is open, name is NUL-terminated, and how/size
        // describe a correctly sized open_how.
        let rc = unsafe {
            libc::syscall(
                SYS_OPENAT2,
                dir_fd,
                name.as_ptr(),
                &how as *const OpenHow,
                std::mem::size_of::<OpenHow>(),
            )
        };

        if rc < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(rc as RawFd)
    }

    pub(super) fn renameat2_noreplace(from: &Path, to: &Path) -> io::Result<()> {
        let c_from = CString::new(from.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
        let c_to = CString::new(to.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;

        // SAFETY: both strings are NUL-terminated and AT_FDCWD is always
        // valid for absolute paths.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                c_from.as_ptr(),
                libc::AT_FDCWD,
                c_to.as_ptr(),
                RENAME_NOREPLACE,
            )
        };

        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Non-unix fallback
// ---------------------------------------------------------------------------

#[cfg(not(unix))]
fn open_impl(path: &Path, existing: Existing, access: Access, _mode: u32) -> io::Result<File> {
    let mut opts = std::fs::OpenOptions::new();

    match access {
        Access::ReadWrite => {
            opts.read(true).write(true);
        }
        Access::Append => {
            opts.append(true);
        }
    }

    match existing {
        Existing::Reject => {
            opts.create_new(true);
        }
        Existing::Open => {
            opts.create(true);
        }
        Existing::Truncate => {
            opts.truncate(true);
        }
    }

    opts.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmpdir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn a_fresh_file_can_be_created_and_written() {
        let dir = tmpdir();
        let path = dir.path().join("out.bin");

        let mut file =
            open_guarded(&path, Existing::Reject, Access::ReadWrite, DEFAULT_FILE_MODE).unwrap();
        file.write_all(b"hello").unwrap();
        drop(file);

        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
    }

    #[test]
    fn reject_means_reject() {
        let dir = tmpdir();
        let path = dir.path().join("out.bin");
        std::fs::write(&path, b"existing").unwrap();

        let err =
            open_guarded(&path, Existing::Reject, Access::ReadWrite, DEFAULT_FILE_MODE).unwrap_err();
        assert!(
            format!("{err:#}").contains("Failed to safely open"),
            "{err:#}"
        );
        // And the existing content is untouched.
        assert_eq!(std::fs::read(&path).unwrap(), b"existing");
    }

    #[test]
    fn append_does_not_rewrite_existing_bytes() {
        let dir = tmpdir();
        let path = dir.path().join("resume.part");
        std::fs::write(&path, b"abc").unwrap();

        let mut file =
            open_guarded(&path, Existing::Open, Access::Append, DEFAULT_FILE_MODE).unwrap();
        file.write_all(b"def").unwrap();
        drop(file);

        assert_eq!(std::fs::read(&path).unwrap(), b"abcdef");
    }

    /// The actual attack: a symlink planted at the predictable `.part` name,
    /// pointing at a file rdm should never touch.
    #[cfg(unix)]
    #[test]
    fn a_symlink_at_the_target_is_refused_and_the_victim_survives() {
        let dir = tmpdir();
        let victim = dir.path().join("precious.txt");
        std::fs::write(&victim, b"do not clobber").unwrap();

        let planted = dir.path().join("download.bin.part");
        std::os::unix::fs::symlink(&victim, &planted).unwrap();

        for (existing, access) in [
            (Existing::Reject, Access::ReadWrite),
            (Existing::Open, Access::ReadWrite),
            (Existing::Open, Access::Append),
            (Existing::Truncate, Access::ReadWrite),
        ] {
            let err = open_guarded(&planted, existing, access, DEFAULT_FILE_MODE)
                .expect_err("a symlink must never be followed");
            assert!(
                format!("{err:#}").contains("Failed to safely open"),
                "{err:#}"
            );
        }

        assert_eq!(
            std::fs::read(&victim).unwrap(),
            b"do not clobber",
            "the symlink target was modified"
        );
    }

    /// A dangling symlink is the sneakier version: `File::create` would
    /// happily create the target.
    #[cfg(unix)]
    #[test]
    fn a_dangling_symlink_does_not_create_its_target() {
        let dir = tmpdir();
        let target = dir.path().join("should-not-appear.txt");
        let planted = dir.path().join("download.bin.part");
        std::os::unix::fs::symlink(&target, &planted).unwrap();

        assert!(open_guarded(&planted, Existing::Open, Access::ReadWrite, DEFAULT_FILE_MODE).is_err());
        assert!(!target.exists(), "the symlink target was created");
    }

    #[cfg(unix)]
    #[test]
    fn a_fifo_is_refused_because_it_is_not_a_regular_file() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let dir = tmpdir();
        let fifo = dir.path().join("pipe.part");
        let c_fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();

        // SAFETY: valid NUL-terminated path.
        let rc = unsafe { libc::mkfifo(c_fifo.as_ptr(), 0o644) };
        if rc != 0 {
            return; // mkfifo unavailable in this environment
        }

        // O_NONBLOCK avoids blocking on open; the point is the fstat refusal.
        let err = open_guarded(&fifo, Existing::Open, Access::ReadWrite, DEFAULT_FILE_MODE)
            .expect_err("a FIFO must be refused");
        let msg = format!("{err:#}");
        assert!(msg.contains("Refusing") || msg.contains("Failed to safely open"), "{msg}");
    }

    #[cfg(unix)]
    #[test]
    fn a_hard_linked_destination_is_refused() {
        let dir = tmpdir();
        let real = dir.path().join("real.bin");
        std::fs::write(&real, b"x").unwrap();

        let alias = dir.path().join("alias.bin.part");
        std::fs::hard_link(&real, &alias).unwrap();

        let err = open_guarded(&alias, Existing::Open, Access::ReadWrite, DEFAULT_FILE_MODE)
            .expect_err("a multiply-linked inode must be refused");
        assert!(format!("{err:#}").contains("hard links"), "{err:#}");
    }

    #[test]
    fn temp_names_are_unpredictable_and_distinct() {
        let dir = tmpdir();

        let (_a, path_a) = create_temp_in(dir.path(), "movie.mkv", DEFAULT_FILE_MODE).unwrap();
        let (_b, path_b) = create_temp_in(dir.path(), "movie.mkv", DEFAULT_FILE_MODE).unwrap();

        assert_ne!(path_a, path_b, "temp names must not repeat");
        assert!(path_a.exists() && path_b.exists());
        assert_eq!(path_a.parent(), Some(dir.path()));

        // Guessable from the output name alone would defeat the purpose.
        assert_ne!(path_a, dir.path().join("movie.mkv.part"));
    }

    #[test]
    fn random_tokens_do_not_repeat() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..256 {
            assert!(seen.insert(random_token()), "random_token repeated");
        }
        assert_eq!(random_token().len(), 32);
    }

    #[test]
    fn no_replace_rename_publishes_when_the_destination_is_free() {
        let dir = tmpdir();
        let from = dir.path().join("a.part");
        let to = dir.path().join("a.bin");
        std::fs::write(&from, b"payload").unwrap();

        rename_no_replace(&from, &to).unwrap();

        assert_eq!(std::fs::read(&to).unwrap(), b"payload");
        assert!(!from.exists());
    }

    /// This is the race: a file appears between the existence check and the
    /// rename. A plain `fs::rename` would silently destroy it.
    #[test]
    fn no_replace_rename_refuses_to_clobber_a_file_that_appeared() {
        let dir = tmpdir();
        let from = dir.path().join("a.part");
        let to = dir.path().join("a.bin");
        std::fs::write(&from, b"new").unwrap();
        std::fs::write(&to, b"appeared after the check").unwrap();

        assert!(rename_no_replace(&from, &to).is_err());
        assert_eq!(std::fs::read(&to).unwrap(), b"appeared after the check");
    }

    #[test]
    fn replacing_rename_is_still_available_for_approved_overwrites() {
        let dir = tmpdir();
        let from = dir.path().join("a.part");
        let to = dir.path().join("a.bin");
        std::fs::write(&from, b"new").unwrap();
        std::fs::write(&to, b"old").unwrap();

        rename_replacing(&from, &to).unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"new");
    }

    #[test]
    fn available_bytes_reports_something_plausible() {
        let dir = tmpdir();
        if let Some(free) = available_bytes(dir.path()) {
            assert!(free > 0, "a writable tempdir should report free space");
        }
    }

    #[test]
    fn a_path_with_no_final_component_is_refused() {
        assert!(
            open_guarded(
                Path::new("/"),
                Existing::Open,
                Access::ReadWrite,
                DEFAULT_FILE_MODE
            )
            .is_err()
        );
    }
}
