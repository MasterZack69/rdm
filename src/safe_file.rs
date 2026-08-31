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
//! ## Two different kinds of path
//!
//! The distinction this module turns on is who chose the path:
//!
//! - A path the **user** gave us, via `-o` or `download_dir`. Every component
//!   is trusted. `~/Downloads` may well be a symlink, and following it is the
//!   whole point. [`open_guarded`] handles these.
//! - A path the **network** gave us: a relative path from a directory
//!   listing, joined onto the download root. No component is trusted, because
//!   a listing can name a directory that a local attacker has replaced with a
//!   symlink. [`open_beneath`] and friends handle these.
//!
//! Conflating the two was a real hole. Splitting a path into parent and final
//! component, opening the parent by full pathname, and applying the symlink
//! guard only to the last part means that given a download root containing
//! `album -> ~/.ssh` and a listing offering `album/authorized_keys`, the
//! directory descriptor is already inside `~/.ssh` before any guard runs. The
//! final component is then guarded perfectly, in the wrong directory.
//!
//! ## The approach for untrusted paths
//!
//! Open the download root normally — it is trusted — and keep that
//! descriptor. Then walk the untrusted relative path one component at a time,
//! opening each with `openat(O_DIRECTORY | O_NOFOLLOW)` against the previous
//! descriptor. A symlinked component fails with `ELOOP` rather than being
//! traversed, and no pathname is ever handed to the kernel for it to resolve
//! on its own. The final component is opened with `openat2` and
//! `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`, which additionally refuses `..`,
//! an absolute path and a magic link, atomically.
//!
//! The walk is not just belt-and-braces over `openat2`. `openat2` cannot
//! create directories, so `RESOLVE_BENEATH` could never have covered the
//! `create_dir_all` that has to happen before a nested download is written.
//! `mkdirat` per component against a held descriptor is the only way to make
//! that half safe, and once the walk exists the open may as well use it.
//!
//! Walking from `/` instead of from the root would be stricter and wrong: it
//! would reject the perfectly ordinary case of `/home` or `~/Downloads` being
//! a symlink. The root is the trust boundary, so the root is the anchor.
//!
//! Where `openat2` is unavailable — pre-5.6 kernels, or a seccomp filter that
//! rejects it — we fall back to `openat` with `O_NOFOLLOW`. Because the walk
//! has already reduced the open to a single component in a directory we hold
//! a descriptor for, that fallback is very nearly the same guarantee: it
//! cannot express `RESOLVE_BENEATH`, but there is no longer a multi-component
//! path for `..` to appear in.
//!
//! Every descriptor is then validated with `fstat` — on the descriptor, never
//! on the path, so there is nothing left to race.
//!
//! ## Why the root is registered rather than passed
//!
//! The download writers are handed a single absolute `output_path` and nothing
//! else, by the queue. Threading a root argument down to them would mean
//! storing it in `queue.json` as well, so that a resumed queue item still knew
//! it — a persisted schema change, and four changed signatures, to express
//! what is really one process-wide trust boundary that never varies within a
//! run.
//!
//! So [`download_root`] is resolved once, and the pathname-taking entry points
//! consult it: a path beneath the root is split and walked, a path outside it
//! keeps the old behaviour. The effect is that protection is the default for
//! every present and future call site, rather than something each one has to
//! remember to opt into.

use anyhow::{Context, Result, bail};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

#[cfg(unix)]
use std::ffi::{CString, OsStr};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

/// Default permissions for a downloaded file: owner read/write, group and
/// other read. Mirrors what `File::create` produces under a normal umask.
pub const DEFAULT_FILE_MODE: u32 = 0o644;

/// Permissions for anything that might contain a credential.
pub const PRIVATE_FILE_MODE: u32 = 0o600;

/// Permissions for directories created along an untrusted relative path.
/// The process umask applies on top, as with `mkdir`.
const DEFAULT_DIR_MODE: u32 = 0o755;

static DOWNLOAD_ROOT: OnceLock<Option<PathBuf>> = OnceLock::new();

/// Pins the trusted download root explicitly.
///
/// Optional: [`download_root`] reads it from the config file on first use.
/// This exists for a caller that already holds a [`crate::config::Config`] and
/// would rather set it than have it re-read. Only the first call takes effect,
/// because a trust boundary that can be moved mid-run is not one.
pub fn set_download_root(root: Option<PathBuf>) {
    let _ = DOWNLOAD_ROOT.set(root);
}

/// The directory beneath which paths are treated as untrusted in shape.
///
/// Read from `config.toml` directly rather than through
/// [`crate::config::Config::load`], which writes a default config file when
/// none exists. Deciding where the trust boundary is must not have side
/// effects, and certainly must not be the thing that creates a file. The value
/// is the same; only the write is skipped.
fn download_root() -> Option<&'static Path> {
    DOWNLOAD_ROOT
        .get_or_init(|| {
            let configured = std::fs::read_to_string(crate::config::config_path())
                .ok()
                .and_then(|text| toml::from_str::<crate::config::Config>(&text).ok())
                .map(|cfg| cfg.download_dir);

            let dir = PathBuf::from(
                configured.unwrap_or_else(|| crate::config::Config::default().download_dir),
            );

            (!dir.as_os_str().is_empty()).then_some(dir)
        })
        .as_deref()
}

/// The part of `path` that lies inside `root`, if it does.
///
/// Lexical, and deliberately so: the paths compared here were built by joining
/// onto the configured `download_dir` string, so they share it verbatim.
/// Canonicalising first would resolve the very symlinks the caller is about to
/// refuse to follow.
///
/// Split out as a plain function because the registered root is a set-once
/// cell, which cannot be rebound per test when the suite shares a process.
fn split_beneath<'a>(root: &Path, path: &'a Path) -> Option<&'a Path> {
    let relative = path.strip_prefix(root).ok()?;

    // The root itself is not a file within the root.
    (!relative.as_os_str().is_empty()).then_some(relative)
}

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

/// Opens `path` for writing, without following a symlink to get there.
///
/// If `path` lies beneath the download root, every component below the root is
/// walked with `O_NOFOLLOW` — the same treatment [`open_beneath`] gives, since
/// a path under the root may have been shaped by a listing.
///
/// Otherwise only the final component is guarded and the parent pathname is
/// resolved normally. That is correct for a path the user named in full, such
/// as an explicit `-o`: they chose every directory in it, and `~/Downloads`
/// being a symlink is legitimate.
pub fn open_guarded(path: &Path, existing: Existing, access: Access, mode: u32) -> Result<File> {
    let file = open_anywhere(path, existing, access, mode)
        .with_context(|| format!("Failed to safely open {}", path.display()))?;

    validate_regular_owned(&file)
        .with_context(|| format!("Refusing to write to {}", path.display()))?;

    Ok(file)
}

/// Opens `relative` beneath `root`, trusting no component of `relative`.
///
/// `root` is resolved normally: it comes from the config or `-o` and is the
/// trust boundary. Every component of `relative` is then opened against the
/// previous directory descriptor with `O_NOFOLLOW`, so a symlinked
/// intermediate directory fails rather than being traversed, and the opened
/// file is guaranteed to be the one at that path *inside* the root.
///
/// Parent directories must already exist; call [`create_dirs_beneath`] first.
pub fn open_beneath(
    root: &Path,
    relative: &Path,
    existing: Existing,
    access: Access,
    mode: u32,
) -> Result<File> {
    let file = open_beneath_impl(root, relative, existing, access, mode).with_context(|| {
        format!(
            "Failed to safely open '{}' beneath {}",
            relative.display(),
            root.display()
        )
    })?;

    validate_regular_owned(&file).with_context(|| {
        format!(
            "Refusing to write to '{}' beneath {}",
            relative.display(),
            root.display()
        )
    })?;

    Ok(file)
}

/// Creates every directory in `relative` beneath `root`, one component at a
/// time against a held descriptor.
///
/// This is the half `openat2` cannot do: it has no directory-creating mode, so
/// `RESOLVE_BENEATH` was never able to protect the `create_dir_all` that
/// precedes a nested download. Each component is `mkdirat`-ed against its
/// parent's descriptor, `EEXIST` is tolerated, and the component is then
/// reopened with `O_NOFOLLOW` — so a directory replaced by a symlink between
/// the create and the open is caught rather than followed.
///
/// `relative` is treated as a directory path in full. Pass the parent of a
/// file, not the file itself.
pub fn create_dirs_beneath(root: &Path, relative: &Path) -> Result<()> {
    create_dirs_beneath_impl(root, relative).with_context(|| {
        format!(
            "Failed to create '{}' beneath {}",
            relative.display(),
            root.display()
        )
    })
}

/// Removes `relative` beneath `root`, resolving the parent by descriptor walk.
///
/// Deletion through a full pathname has the same parent-swap exposure as
/// opening one: sync removes files it has selected for redownload, and a
/// symlinked intermediate directory would redirect that removal.
pub fn unlink_beneath(root: &Path, relative: &Path) -> Result<()> {
    unlink_beneath_impl(root, relative).with_context(|| {
        format!(
            "Failed to remove '{}' beneath {}",
            relative.display(),
            root.display()
        )
    })
}

/// Renames `from` to `to`, both relative to `root`, via verified descriptors.
///
/// When `replace` is false this is `RENAME_NOREPLACE`: the existence check and
/// the rename are one operation, so a file that appears in between is not
/// destroyed. Pass true only where an overwrite was actually approved.
pub fn rename_beneath(root: &Path, from: &Path, to: &Path, replace: bool) -> Result<()> {
    rename_beneath_impl(root, from, to, replace).with_context(|| {
        format!(
            "Failed to move '{}' to '{}' beneath {}",
            from.display(),
            to.display(),
            root.display()
        )
    })
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

        match open_anywhere(&candidate, Existing::Reject, Access::ReadWrite, mode) {
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
/// When both sides are beneath the download root this goes through
/// [`rename_beneath`], so the directories are reached by descriptor walk
/// rather than re-resolved from a pathname.
///
/// Use [`rename_replacing`] only where the user has actually approved an
/// overwrite (`--force`, or an explicit redownload).
pub fn rename_no_replace(from: &Path, to: &Path) -> Result<()> {
    if let Some(root) = download_root()
        && let Some(from_rel) = split_beneath(root, from)
        && let Some(to_rel) = split_beneath(root, to)
    {
        return rename_beneath(root, from_rel, to_rel, false);
    }

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
    if let Some(root) = download_root()
        && let Some(from_rel) = split_beneath(root, from)
        && let Some(to_rel) = split_beneath(root, to)
    {
        return rename_beneath(root, from_rel, to_rel, true);
    }

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

/// Opens a path, using the descriptor walk when it is beneath the root.
///
/// The single place the two path kinds are told apart. Returns `io::Result` so
/// that [`create_temp_in`] can still recognise `AlreadyExists` and retry.
fn open_anywhere(
    path: &Path,
    existing: Existing,
    access: Access,
    mode: u32,
) -> io::Result<File> {
    if let Some(root) = download_root()
        && let Some(relative) = split_beneath(root, path)
    {
        return open_beneath_impl(root, relative, existing, access, mode);
    }

    open_impl(path, existing, access, mode)
}

// ---------------------------------------------------------------------------
// Unix implementation
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn cstr(name: &OsStr) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt;

    CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
}

/// Splits an untrusted relative path into its ordinary components.
///
/// Anything that is not a plain name is refused outright: `..` because it
/// escapes, a leading `/` or a Windows prefix because it is not relative at
/// all. `.` is dropped as a no-op. This is a lexical check, and it is not the
/// security boundary — `O_NOFOLLOW` per component and `RESOLVE_BENEATH` on the
/// final open are. It exists so that a bad path fails with a clear message
/// rather than an `ELOOP` five components later.
#[cfg(unix)]
fn untrusted_components(relative: &Path) -> io::Result<Vec<&OsStr>> {
    use std::path::Component;

    let mut out = Vec::new();

    for component in relative.components() {
        match component {
            Component::Normal(name) => out.push(name),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "path must be relative and free of '..'",
                ));
            }
        }
    }

    Ok(out)
}

/// Walks `components` from `root`, returning a descriptor for the last one.
///
/// Each step is an `openat` against the descriptor from the previous step,
/// with `O_DIRECTORY | O_NOFOLLOW`. A symlinked component therefore fails with
/// `ELOOP` instead of being followed — which is the whole finding. With
/// `create`, each component is `mkdirat`-ed first and `EEXIST` ignored.
#[cfg(unix)]
fn walk_dirs(root: &Path, components: &[&OsStr], create: bool) -> io::Result<OwnedFd> {
    // The root, and only the root, is resolved by pathname. It is the trust
    // boundary: the user named it, and it is allowed to be a symlink.
    let mut fd = OwnedFd::open_dir(root)?;

    for name in components {
        fd = fd.open_child_dir(name, create)?;
    }

    Ok(fd)
}

/// Splits a relative path into (directory components, final name).
#[cfg(unix)]
fn split_untrusted<'a>(relative: &'a Path) -> io::Result<(Vec<&'a OsStr>, &'a OsStr)> {
    let mut components = untrusted_components(relative)?;

    let name = components.pop().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "path has no final component",
        )
    })?;

    Ok((components, name))
}

#[cfg(unix)]
fn open_beneath_impl(
    root: &Path,
    relative: &Path,
    existing: Existing,
    access: Access,
    mode: u32,
) -> io::Result<File> {
    let (dirs, name) = split_untrusted(relative)?;
    let dir_fd = walk_dirs(root, &dirs, false)?;

    open_at(dir_fd.fd, name, existing, access, mode)
}

#[cfg(unix)]
fn create_dirs_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    let components = untrusted_components(relative)?;

    // An empty relative path means "the root itself", which already exists.
    // Still open it, so that a missing or non-directory root is reported here
    // rather than at the first write.
    walk_dirs(root, &components, true)?;

    Ok(())
}

#[cfg(unix)]
fn unlink_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    let (dirs, name) = split_untrusted(relative)?;
    let dir_fd = walk_dirs(root, &dirs, false)?;
    let c_name = cstr(name)?;

    // SAFETY: dir_fd is open and c_name is NUL-terminated.
    let rc = unsafe { libc::unlinkat(dir_fd.fd, c_name.as_ptr(), 0) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(unix)]
fn rename_beneath_impl(
    root: &Path,
    from: &Path,
    to: &Path,
    replace: bool,
) -> io::Result<()> {
    let (from_dirs, from_name) = split_untrusted(from)?;
    let (to_dirs, to_name) = split_untrusted(to)?;

    let from_fd = walk_dirs(root, &from_dirs, false)?;
    let to_fd = walk_dirs(root, &to_dirs, false)?;

    let c_from = cstr(from_name)?;
    let c_to = cstr(to_name)?;

    if !replace {
        #[cfg(target_os = "linux")]
        {
            match linux::renameat2_at(from_fd.fd, &c_from, to_fd.fd, &c_to) {
                Ok(()) => return Ok(()),
                Err(e) if is_unsupported(&e) => {
                    // Older kernel or an exotic filesystem: the link/unlink
                    // emulation below gives the same no-clobber guarantee.
                }
                Err(e) => return Err(e),
            }
        }

        // `linkat` fails with EEXIST when the destination exists, atomically.
        // SAFETY: both descriptors are open and both names are NUL-terminated.
        let rc = unsafe {
            libc::linkat(from_fd.fd, c_from.as_ptr(), to_fd.fd, c_to.as_ptr(), 0)
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: as above.
        let rc = unsafe { libc::unlinkat(from_fd.fd, c_from.as_ptr(), 0) };
        if rc != 0 {
            // The file is published under its final name; a leftover temp
            // name is cosmetic.
            debug_assert!(false, "failed to unlink temp after publish");
        }

        return Ok(());
    }

    // SAFETY: both descriptors are open and both names are NUL-terminated.
    let rc = unsafe { libc::renameat(from_fd.fd, c_from.as_ptr(), to_fd.fd, c_to.as_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

/// Opens a single component relative to a directory descriptor.
///
/// Shared by [`open_guarded`]'s split and [`open_beneath`]'s walk. By the time
/// this runs the name is one component in a directory we hold open, so
/// `openat2`'s `RESOLVE_BENEATH` and the `openat` fallback's `O_NOFOLLOW`
/// differ only in how much they refuse beyond a final-component symlink.
#[cfg(unix)]
fn open_at(
    dir_fd: RawFd,
    name: &OsStr,
    existing: Existing,
    access: Access,
    mode: u32,
) -> io::Result<File> {
    let c_name = cstr(name)?;

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
        match linux::openat2(dir_fd, &c_name, flags, mode) {
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
    let fd = unsafe { libc::openat(dir_fd, c_name.as_ptr(), flags, mode as libc::c_uint) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: openat returned a fresh owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn open_impl(path: &Path, existing: Existing, access: Access, mode: u32) -> io::Result<File> {
    let (dir, name) = split_parent(path)?;
    let dir_fd = OwnedFd::open_dir(&dir)?;

    open_at(dir_fd.fd, name.as_os_str(), existing, access, mode)
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

/// A raw descriptor that closes itself. Only used for directory handles.
#[cfg(unix)]
struct OwnedFd {
    fd: RawFd,
}

#[cfg(unix)]
impl OwnedFd {
    /// Opens a trusted directory by pathname, following symlinks.
    ///
    /// Only for a directory the user named: the download root, or the parent
    /// of an explicit `-o`. `O_DIRECTORY` still guarantees we ended up at a
    /// directory.
    fn open_dir(dir: &Path) -> io::Result<Self> {
        let c_dir = cstr(dir.as_os_str())?;

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

    /// Opens one untrusted child directory, never following a symlink.
    ///
    /// `O_NOFOLLOW` with `O_DIRECTORY` is what closes the finding: a symlink
    /// at this component fails with `ELOOP` rather than moving the descriptor
    /// outside the root. With `create`, the directory is made first; `EEXIST`
    /// is the ordinary case and the reopen below is what verifies that what
    /// exists is a real directory and not a link put there in between.
    fn open_child_dir(&self, name: &OsStr, create: bool) -> io::Result<Self> {
        let c_name = cstr(name)?;

        if create {
            // SAFETY: self.fd is open and c_name is NUL-terminated.
            let rc = unsafe {
                libc::mkdirat(self.fd, c_name.as_ptr(), DEFAULT_DIR_MODE as libc::mode_t)
            };
            if rc != 0 {
                let e = io::Error::last_os_error();
                if e.raw_os_error() != Some(libc::EEXIST) {
                    return Err(e);
                }
            }
        }

        // SAFETY: self.fd is open and c_name is NUL-terminated.
        let fd = unsafe {
            libc::openat(
                self.fd,
                c_name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
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

    /// `renameat2(RENAME_NOREPLACE)` between two directory descriptors.
    ///
    /// The descriptor form matters as much as the no-replace flag: a rename
    /// given two full pathnames re-resolves both parents, which is the same
    /// parent-swap exposure the walk exists to remove.
    pub(super) fn renameat2_at(
        from_fd: RawFd,
        from_name: &CString,
        to_fd: RawFd,
        to_name: &CString,
    ) -> io::Result<()> {
        // SAFETY: both descriptors are open and both names are NUL-terminated.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                from_fd,
                from_name.as_ptr(),
                to_fd,
                to_name.as_ptr(),
                RENAME_NOREPLACE,
            )
        };

        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
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

/// Lexical containment only. There is no portable equivalent of the
/// descriptor walk, so on these platforms the relative path is checked for
/// traversal and then joined.
#[cfg(not(unix))]
fn joined_beneath(root: &Path, relative: &Path) -> io::Result<PathBuf> {
    use std::path::Component;

    let mut out = root.to_path_buf();

    for component in relative.components() {
        match component {
            Component::Normal(name) => out.push(name),
            Component::CurDir => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "path must be relative and free of '..'",
                ));
            }
        }
    }

    Ok(out)
}

#[cfg(not(unix))]
fn open_beneath_impl(
    root: &Path,
    relative: &Path,
    existing: Existing,
    access: Access,
    mode: u32,
) -> io::Result<File> {
    open_impl(&joined_beneath(root, relative)?, existing, access, mode)
}

#[cfg(not(unix))]
fn create_dirs_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    std::fs::create_dir_all(joined_beneath(root, relative)?)
}

#[cfg(not(unix))]
fn unlink_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    std::fs::remove_file(joined_beneath(root, relative)?)
}

#[cfg(not(unix))]
fn rename_beneath_impl(
    root: &Path,
    from: &Path,
    to: &Path,
    replace: bool,
) -> io::Result<()> {
    let from = joined_beneath(root, from)?;
    let to = joined_beneath(root, to)?;

    if !replace && to.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "destination already exists",
        ));
    }

    std::fs::rename(from, to)
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

    // ---------- Root-anchored operations ----------

    #[test]
    fn an_ordinary_nested_path_still_works() {
        let root = tmpdir();
        let relative = Path::new("album/disc 1/track.flac");

        create_dirs_beneath(root.path(), relative.parent().unwrap()).unwrap();

        let mut file = open_beneath(
            root.path(),
            relative,
            Existing::Reject,
            Access::ReadWrite,
            DEFAULT_FILE_MODE,
        )
        .unwrap();
        file.write_all(b"audio").unwrap();
        drop(file);

        assert_eq!(std::fs::read(root.path().join(relative)).unwrap(), b"audio");
    }

    /// The finding. `album` is a symlink out of the download root, and the
    /// listing offers `album/authorized_keys`. Splitting the path and opening
    /// the parent by pathname put the descriptor inside the target directory
    /// before any guard applied; the walk refuses at `album` instead.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_intermediate_directory_is_refused() {
        let root = tmpdir();
        let outside = tmpdir();

        // Stand-in for ~/.ssh, with something worth protecting in it.
        let victim = outside.path().join("authorized_keys");
        std::fs::write(&victim, b"ssh-ed25519 the-real-key").unwrap();

        std::os::unix::fs::symlink(outside.path(), root.path().join("album")).unwrap();

        let relative = Path::new("album/authorized_keys");

        for (existing, access) in [
            (Existing::Reject, Access::ReadWrite),
            (Existing::Open, Access::ReadWrite),
            (Existing::Open, Access::Append),
            (Existing::Truncate, Access::ReadWrite),
        ] {
            let err = open_beneath(root.path(), relative, existing, access, DEFAULT_FILE_MODE)
                .expect_err("a symlinked intermediate directory must not be traversed");
            assert!(
                format!("{err:#}").contains("Failed to safely open"),
                "{err:#}"
            );
        }

        assert_eq!(
            std::fs::read(&victim).unwrap(),
            b"ssh-ed25519 the-real-key",
            "the file outside the root was modified"
        );

        // Creating directories through the same link is refused too, so the
        // deeper case `album/nested/file` cannot get a foothold either.
        assert!(create_dirs_beneath(root.path(), Path::new("album/nested")).is_err());
        assert!(!outside.path().join("nested").exists());
    }

    /// Belt to the walk's braces: even without a symlink, `..` cannot be used
    /// to climb out, and an absolute path is not a relative path.
    #[test]
    fn traversal_and_absolute_paths_are_refused() {
        let root = tmpdir();

        for bad in [
            "../escaped.bin",
            "album/../../escaped.bin",
            "/etc/cron.d/rdm",
        ] {
            assert!(
                open_beneath(
                    root.path(),
                    Path::new(bad),
                    Existing::Open,
                    Access::ReadWrite,
                    DEFAULT_FILE_MODE,
                )
                .is_err(),
                "{bad} was accepted"
            );
        }

        // And an empty relative path names no file at all.
        assert!(
            open_beneath(
                root.path(),
                Path::new(""),
                Existing::Open,
                Access::ReadWrite,
                DEFAULT_FILE_MODE
            )
            .is_err()
        );
    }

    /// Re-running a sync must not fail on directories that already exist.
    #[test]
    fn creating_directories_is_idempotent() {
        let root = tmpdir();
        let relative = Path::new("a/b/c");

        create_dirs_beneath(root.path(), relative).unwrap();
        create_dirs_beneath(root.path(), relative).unwrap();

        assert!(root.path().join(relative).is_dir());

        // The root itself is a valid no-op.
        create_dirs_beneath(root.path(), Path::new("")).unwrap();
    }

    /// A file already occupying a directory's name must not be silently
    /// treated as one.
    #[cfg(unix)]
    #[test]
    fn a_file_in_the_way_of_a_directory_is_an_error() {
        let root = tmpdir();
        std::fs::write(root.path().join("album"), b"not a directory").unwrap();

        assert!(create_dirs_beneath(root.path(), Path::new("album/disc1")).is_err());
    }

    #[test]
    fn renaming_beneath_the_root_publishes_and_refuses_to_clobber() {
        let root = tmpdir();
        create_dirs_beneath(root.path(), Path::new("album")).unwrap();
        std::fs::write(root.path().join("album/track.part"), b"payload").unwrap();

        rename_beneath(
            root.path(),
            Path::new("album/track.part"),
            Path::new("album/track.flac"),
            false,
        )
        .unwrap();

        assert_eq!(
            std::fs::read(root.path().join("album/track.flac")).unwrap(),
            b"payload"
        );
        assert!(!root.path().join("album/track.part").exists());

        // A file that appeared at the destination is not destroyed.
        std::fs::write(root.path().join("album/other.part"), b"new").unwrap();
        assert!(
            rename_beneath(
                root.path(),
                Path::new("album/other.part"),
                Path::new("album/track.flac"),
                false,
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read(root.path().join("album/track.flac")).unwrap(),
            b"payload"
        );

        // Unless replacing was asked for.
        rename_beneath(
            root.path(),
            Path::new("album/other.part"),
            Path::new("album/track.flac"),
            true,
        )
        .unwrap();
        assert_eq!(
            std::fs::read(root.path().join("album/track.flac")).unwrap(),
            b"new"
        );
    }

    #[test]
    fn unlinking_beneath_the_root_removes_only_that_file() {
        let root = tmpdir();
        create_dirs_beneath(root.path(), Path::new("album")).unwrap();
        std::fs::write(root.path().join("album/a.flac"), b"a").unwrap();
        std::fs::write(root.path().join("album/b.flac"), b"b").unwrap();

        unlink_beneath(root.path(), Path::new("album/a.flac")).unwrap();

        assert!(!root.path().join("album/a.flac").exists());
        assert!(root.path().join("album/b.flac").exists());
    }

    /// Sync deletes files it has selected for redownload, so deletion has the
    /// same parent-swap exposure as opening.
    #[cfg(unix)]
    #[test]
    fn unlinking_through_a_symlinked_directory_is_refused() {
        let root = tmpdir();
        let outside = tmpdir();

        let victim = outside.path().join("keep-me.txt");
        std::fs::write(&victim, b"important").unwrap();

        std::os::unix::fs::symlink(outside.path(), root.path().join("album")).unwrap();

        assert!(unlink_beneath(root.path(), Path::new("album/keep-me.txt")).is_err());
        assert!(victim.exists(), "a file outside the root was deleted");

        assert!(
            rename_beneath(
                root.path(),
                Path::new("album/keep-me.txt"),
                Path::new("album/moved.txt"),
                true,
            )
            .is_err()
        );
        assert!(victim.exists(), "a file outside the root was renamed");
    }

    // ---------- Which paths count as being inside the root ----------

    /// The containment test the pathname-taking entry points use to decide
    /// whether a path needs the walk. Tested directly because the registered
    /// root is set once per process and cannot be rebound per test.
    #[test]
    fn paths_inside_the_root_are_split_and_others_are_left_alone() {
        let root = Path::new("/home/user/Downloads");

        assert_eq!(
            split_beneath(root, Path::new("/home/user/Downloads/file.mkv")),
            Some(Path::new("file.mkv"))
        );
        assert_eq!(
            split_beneath(root, Path::new("/home/user/Downloads/show/s01/e01.mkv")),
            Some(Path::new("show/s01/e01.mkv"))
        );
        // The `.part` and `.rdm` siblings have to be recognised too, or the
        // temp files would keep the old treatment.
        assert_eq!(
            split_beneath(root, Path::new("/home/user/Downloads/show/e01.mkv.part")),
            Some(Path::new("show/e01.mkv.part"))
        );

        // An explicit -o elsewhere: the user named every component, so it
        // keeps the trusted-parent treatment rather than being refused.
        assert_eq!(split_beneath(root, Path::new("/tmp/out.zip")), None);

        // The root itself is not a file in the root.
        assert_eq!(split_beneath(root, Path::new("/home/user/Downloads")), None);

        // A sibling directory that merely starts with the same characters is
        // not inside it. This is why the check is component-wise rather than
        // a string prefix.
        assert_eq!(
            split_beneath(root, Path::new("/home/user/Downloads-old/file.mkv")),
            None
        );
    }
}
