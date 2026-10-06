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
//! consult it: a path beneath the root is resolved against the root
//! descriptor, a path outside it keeps the trusted-parent behaviour. The
//! effect is that protection is the default for every present and future call
//! site, rather than something each one has to remember to opt into.
//!
//! The root descriptor itself is also opened once and kept (`PINNED_ROOT`).
//! A trust boundary that is re-resolved from a pathname on every operation is
//! one that can be moved between operations, and `RESOLVE_BENEATH` needs
//! something stable to be beneath.
//!
//! ## One resolution, not a chain of handles
//!
//! Opening each component in turn proves that each component was inside the
//! root *when it was opened*, and nothing about where it is by the time the
//! next one is opened: a directory moved out of the root mid-walk is still
//! open, and still writable through the descriptor that was taken of it. So
//! where the kernel can do it — `openat2`, Linux 5.6 and up — the whole
//! relative path is resolved in a single call against the pinned root
//! descriptor with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`. There is then no
//! intermediate handle at all, and containment is the kernel's answer rather
//! than an inference from several of them.
//!
//! Directory creation cannot work that way, because `openat2` has no
//! directory-creating mode: each component is `mkdirat`-ed against its
//! parent's descriptor and then the path so far is re-resolved from the root,
//! so the handle the next step builds on is one the kernel has just confirmed.
//! The pre-`openat2` fallback walk closes the same gap after the fact, by
//! climbing `..` from the directory it ended on until it reaches the root's
//! own inode ([`verify_contained`]).
//!
//! ## Publication
//!
//! Finishing a download means giving the final name to the bytes that were
//! written. Renaming the temporary *name* does not do that: a name is not a
//! file. Anyone able to create entries in the destination directory could
//! unlink the `.part` entry after rdm opened it — leaving rdm writing to an
//! inode with no name at all — put a file or a symlink of their own at that
//! name, and have the publication step hand the output name to *that*.
//! `RENAME_NOREPLACE` protected the destination from being clobbered and said
//! nothing about which file was arriving.
//!
//! Publication therefore goes through the descriptor rdm wrote: `linkat` gives
//! the destination name to that exact inode, either from the writer the caller
//! still holds ([`publish_open_file`]) or from a freshly opened and re-
//! validated descriptor ([`publish_no_replace`]). The staged name stops being
//! an input, which is why there is no stat-then-rename comparison here: such a
//! comparison is a race of its own, and there is nothing left to compare.
//! `linkat` also cannot overwrite, so the no-clobber guarantee comes with it;
//! an approved overwrite unlinks the destination and then links, so the
//! failure mode is "nothing published", never "somebody else's file
//! published".
//!
//! The staged name itself stays predictable (`<output>.part`), because resume
//! has to find it again between runs. That is now a question of availability
//! rather than of integrity: someone who can unlink that entry can stop a
//! download from being published — the inode has lost its last name, and the
//! kernel will not let an unprivileged process give an unlinked inode a new
//! one — but they cannot make rdm publish anything else under the output name.
//!
//! ## Validate before destroying
//!
//! `O_TRUNC` empties a file inside the open, before anything has been checked.
//! A destination rdm is about to refuse — a hard-linked file, a file belonging
//! to another user — would be left at zero bytes by the very call that decided
//! not to write to it. So [`Existing::Truncate`] carries no `O_TRUNC`: the
//! descriptor is validated first and emptied afterwards, through that same
//! descriptor.
//!
//! Opens also carry `O_NONBLOCK`. Opening a FIFO for writing blocks until a
//! reader appears, so an `fstat` that would refuse the FIFO never runs — the
//! transfer hangs instead of failing, and the resume path, which opens for
//! append, is exactly that case. The flag is cleared once the descriptor has
//! been validated as a regular file.
//!
//! ## Reading directories
//!
//! "Verify the directory, then enumerate the pathname" is two resolutions of
//! one name with a gap in between, and for `--delete` the enumeration is a
//! list of files to remove. [`read_dir_beneath`] resolves the directory once,
//! anchored at the root, and reads it — and stats each entry — through that
//! descriptor. A recursive sweep re-resolves each level from the root the same
//! way.
//!
//! ## What this does not promise
//!
//! A directory descriptor is a handle on an inode, not on a position in the
//! tree. Operations that need one — `mkdirat`, `unlinkat`, `linkat`,
//! `renameat` — are issued immediately after an anchored resolution, with no
//! I/O in between, but someone who can write the parent of a directory rdm has
//! just resolved can still move that directory out of the root afterwards, and
//! the pending operation will act inside it. Nothing short of a kernel
//! primitive that takes the whole path atomically can close that, and no
//! amount of pinning the root closes it either. Within one run every operation
//! is anchored as tightly as the platform allows; across operations, rdm
//! assumes the download root and the directories it created beneath it are not
//! being reorganised by a hostile process with write access to them.
//!
//! Directories below the root are deliberately *not* required to be owned by
//! the current user. A download tree on a NAS or a seedbox routinely contains
//! directories that belong to another account or to a shared group, and
//! refusing those would break the ordinary case to narrow an attack that
//! `RESOLVE_BENEATH`, `RESOLVE_NO_SYMLINKS` and the per-file
//! [`validate_regular_owned`] already cover: a directory planted below the
//! root can neither redirect resolution outside it nor hold a file rdm will
//! write through.

use anyhow::{Context, Result, bail};
use std::ffi::OsString;
use std::fs::{File, Metadata};
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
    /// Open the existing file and empty it. Fails if absent.
    ///
    /// The emptying happens through the validated descriptor, after the
    /// ownership, file-type and link-count checks, never as an `O_TRUNC` on
    /// the open itself. `O_TRUNC` destroys the file's contents before anything
    /// has been checked, so a destination rdm goes on to refuse — a
    /// hard-linked file, a file belonging to another user — would be left at
    /// zero bytes by the very call that decided not to write to it.
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

    prepare(file, existing).with_context(|| format!("Refusing to write to {}", path.display()))
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

    prepare(file, existing).with_context(|| {
        format!(
            "Refusing to write to '{}' beneath {}",
            relative.display(),
            root.display()
        )
    })
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

/// What one entry in a directory is.
///
/// A symlink is [`EntryKind::Other`], never the thing it points at: callers
/// neither descend into it nor delete it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Dir,
    File,
    Other,
}

/// One entry of a directory beneath the root, as read through a descriptor.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: OsString,
    pub kind: EntryKind,
}

/// Lists the directory `relative` beneath `root`, reading it by descriptor.
///
/// This replaces a "verify the directory, then enumerate the pathname" pair,
/// which was two resolutions of the same name with a gap in between: the
/// verification could succeed and the directory then be replaced with a
/// symlink before the enumeration, so the listing described somewhere else
/// entirely — and for `--delete`, a listing is a list of files to remove.
///
/// Here the directory is resolved once, anchored at the root, and both the
/// `readdir` and the per-entry `fstatat` go through that descriptor. Each
/// level of a recursive walk is re-resolved from the root the same way, so
/// every entry reported is one that was inside the root when it was read.
///
/// Entries are returned rather than a live handle because a recursive caller
/// would otherwise hold a descriptor per level for the whole sweep; the
/// anchored resolution is what makes each level safe, not the handle's
/// lifetime.
pub fn read_dir_beneath(root: &Path, relative: &Path) -> Result<Vec<DirEntry>> {
    read_dir_beneath_impl(root, relative).with_context(|| {
        format!(
            "Failed to safely list '{}' beneath {}",
            relative.display(),
            root.display()
        )
    })
}

/// Removes the empty directory `relative` beneath `root`.
///
/// Same descriptor resolution as every other removal here, so a directory
/// swapped for a symlink cannot redirect it. Non-empty is an ordinary error.
pub fn remove_dir_beneath(root: &Path, relative: &Path) -> Result<()> {
    remove_dir_beneath_impl(root, relative).with_context(|| {
        format!(
            "Failed to remove directory '{}' beneath {}",
            relative.display(),
            root.display()
        )
    })
}

/// Metadata for `relative` beneath `root`, resolved entirely by descriptor.
///
/// `Ok(None)` when nothing is there. Symlinks are never followed and a
/// non-regular file is an error rather than a stat of something rdm would
/// refuse to write to anyway.
///
/// The point is that the parent walk and the stat are one resolution: a
/// pathname `symlink_metadata` performed *after* a separate parent check can
/// be redirected by an intermediate directory swapped in between the two, so
/// the answer would describe a file outside the root. Here the final
/// component is `fstatat(AT_SYMLINK_NOFOLLOW)` against the descriptor the
/// walk ended on, and the returned [`Metadata`] comes from a descriptor we
/// hold open, so it cannot describe anything but the inode that was there.
pub fn metadata_beneath(root: &Path, relative: &Path) -> Result<Option<Metadata>> {
    metadata_beneath_impl(root, relative).with_context(|| {
        format!(
            "Failed to safely inspect '{}' beneath {}",
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

/// Publishes the staged file `from` as `to`, both relative to `root`.
///
/// The root-relative counterpart of [`publish_no_replace`], for a caller that
/// holds its own root rather than relying on the registered one. Same
/// guarantee: what arrives at `to` is the inode that was opened and validated
/// at `from`, not whatever entry that name happens to hold at the time.
pub fn publish_beneath(root: &Path, from: &Path, to: &Path, replace: bool) -> Result<()> {
    publish_beneath_impl(root, from, to, replace).with_context(|| {
        format!(
            "Failed to publish '{}' as '{}' beneath {}",
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
                validate_regular_owned(&file)
                    .with_context(|| format!("Refusing to write to {}", candidate.display()))?;
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

/// Publishes the finished file staged at `from` as `to`, refusing to replace
/// anything already there.
///
/// Publication used to be a rename of the staged *name*. A name is not a file:
/// anyone able to create entries in that directory could unlink the entry
/// after rdm opened it — leaving rdm writing to an inode with no name — put a
/// file or a symlink of their own at that name, and have rdm publish that.
/// `RENAME_NOREPLACE` kept the destination from being clobbered and had
/// nothing to say about which file was arriving.
///
/// So this publishes the inode rdm wrote and validated: the staged file is
/// opened, checked as a regular single-linked file this process owns, and the
/// destination name is then given to *that descriptor* with `linkat`. The
/// staged name is no longer an input to the step, which is why no
/// stat-then-rename comparison is needed — such a comparison would be another
/// race, and there is nothing left to compare. `linkat` cannot overwrite, so
/// the no-clobber guarantee comes with it.
///
/// Use [`publish_replacing`] only where the user has actually approved an
/// overwrite (`--force`, or an explicit redownload).
pub fn publish_no_replace(from: &Path, to: &Path) -> Result<()> {
    publish_path_impl(from, to, false)
        .with_context(|| format!("Failed to publish {} as {}", from.display(), to.display()))
}

/// Publishes `from` as `to`, replacing what is there. Only for an approved
/// overwrite.
///
/// The destination is unlinked and the validated descriptor is then linked
/// into its place, so what lands there is still the file rdm wrote. If
/// something else takes the name in between, the link fails and publication is
/// refused: the failure mode is "nothing published", never "somebody else's
/// file published".
pub fn publish_replacing(from: &Path, to: &Path) -> Result<()> {
    publish_path_impl(from, to, true)
        .with_context(|| format!("Failed to publish {} as {}", from.display(), to.display()))
}

/// Publishes the file behind an open descriptor as `to`.
///
/// The strongest form, for a caller that still holds the writer it filled:
/// nothing is reopened, so file identity runs unbroken from the open that
/// created the file to the name it is published under. `from` is the staged
/// name, needed only so that it can be tidied up afterwards — and it is only
/// removed while it still names this very inode.
///
/// Flush before calling. Buffered bytes are not part of the file yet, and
/// writing through the descriptor afterwards writes to the published file.
pub fn publish_open_file(file: &File, from: &Path, to: &Path, replace: bool) -> Result<()> {
    publish_open_impl(file, from, to, replace)
        .with_context(|| format!("Failed to publish {} as {}", from.display(), to.display()))
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

    let b = std::collections::hash_map::RandomState::new()
        .hash_one(a)
        .to_le_bytes();

    buf[..8].copy_from_slice(&a);
    buf[8..].copy_from_slice(&b);
    buf
}

/// Opens a path, using the descriptor walk when it is beneath the root.
///
/// The single place the two path kinds are told apart. Returns `io::Result` so
/// that [`create_temp_in`] can still recognise `AlreadyExists` and retry.
fn open_anywhere(path: &Path, existing: Existing, access: Access, mode: u32) -> io::Result<File> {
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
/// security boundary — `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS` on the open is.
/// It exists so that a bad path fails with a clear message rather than an
/// `ELOOP` five components later.
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

/// Splits a relative path into (directory components, final name).
#[cfg(unix)]
fn split_untrusted(relative: &Path) -> io::Result<(Vec<&OsStr>, &OsStr)> {
    let mut components = untrusted_components(relative)?;

    let name = components.pop().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "path has no final component")
    })?;

    Ok((components, name))
}

/// Joins already-verified components into one relative path.
///
/// Every component came out of [`untrusted_components`], so each is a plain
/// name: non-empty, and with no separator of its own. The result is handed to
/// a single `openat2` so that the kernel resolves the whole path in one
/// operation.
#[cfg(unix)]
fn join_components(components: &[&OsStr]) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt;

    let mut bytes: Vec<u8> = Vec::new();

    for (index, name) in components.iter().enumerate() {
        if index > 0 {
            bytes.push(b'/');
        }
        bytes.extend_from_slice(name.as_bytes());
    }

    CString::new(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
}

/// The descriptor for the registered download root, resolved once.
///
/// The root is the trust boundary, and a boundary that is re-resolved from a
/// pathname on every operation is one that can be moved between operations.
/// Pinning it means every resolution in a run is anchored at the same
/// directory inode the run started with, and that `RESOLVE_BENEATH` has
/// something stable to be beneath.
///
/// Only the registered root is pinned. A root a caller passes explicitly —
/// the SFTP destination, a test's temporary directory — belongs to that
/// caller and is opened per operation, which is no weaker than it was.
#[cfg(unix)]
static PINNED_ROOT: std::sync::Mutex<Option<(PathBuf, std::sync::Arc<OwnedFd>)>> =
    std::sync::Mutex::new(None);

#[cfg(unix)]
fn root_dir(root: &Path) -> io::Result<std::sync::Arc<OwnedFd>> {
    if download_root() == Some(root) {
        return pinned_dir(&PINNED_ROOT, root);
    }

    Ok(std::sync::Arc::new(OwnedFd::open_dir(root)?))
}

/// Resolves `root` through `cell`, remembering the descriptor for next time.
///
/// Only a successful open is remembered, so a root that does not exist yet is
/// retried rather than cached as a failure. A different path replaces the
/// entry: the registered root is set once per process, so in practice this
/// happens only in tests.
#[cfg(unix)]
fn pinned_dir(
    cell: &std::sync::Mutex<Option<(PathBuf, std::sync::Arc<OwnedFd>)>>,
    root: &Path,
) -> io::Result<std::sync::Arc<OwnedFd>> {
    let Ok(mut pinned) = cell.lock() else {
        // A poisoned lock costs the pin, not the operation.
        return Ok(std::sync::Arc::new(OwnedFd::open_dir(root)?));
    };

    if let Some((path, fd)) = pinned.as_ref()
        && path == root
    {
        return Ok(std::sync::Arc::clone(fd));
    }

    let fd = std::sync::Arc::new(OwnedFd::open_dir(root)?);
    *pinned = Some((root.to_path_buf(), std::sync::Arc::clone(&fd)));

    Ok(fd)
}

/// Resolves the directory named by `components`, beneath the pinned `root_fd`.
///
/// On Linux this is a single `openat2` of the whole relative path with
/// `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`: the kernel resolves every
/// component against the root descriptor in one operation, so there is no
/// intermediate handle, and therefore no step between two handles for a
/// directory to be moved out of the root in. That is the difference from a
/// component-at-a-time walk, which proves each component was inside the root
/// when it was opened and nothing about where it is by the time the next one
/// is.
#[cfg(unix)]
fn resolve_dir(root_fd: &OwnedFd, components: &[&OsStr]) -> io::Result<OwnedFd> {
    // The root itself. Duplicated rather than returned by reference so the
    // caller owns a descriptor either way.
    if components.is_empty() {
        return root_fd.try_clone();
    }

    #[cfg(target_os = "linux")]
    {
        let joined = join_components(components)?;
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;

        match linux::openat2(root_fd.fd, &joined, flags, 0) {
            Ok(fd) => return Ok(OwnedFd { fd }),
            Err(e) if is_unsupported(&e) => {
                // Pre-5.6 kernel, or seccomp. Fall through to the walk.
            }
            Err(e) => return Err(e),
        }
    }

    walk_dirs(root_fd, components)
}

/// The pre-`openat2` fallback: one `openat(O_DIRECTORY | O_NOFOLLOW)` per
/// component, then a check that the directory it ended on is still inside the
/// root.
///
/// `O_NOFOLLOW` refuses a symlinked component, which is most of it, but a
/// chain of descriptors cannot by itself say that the last one is still where
/// the first one found it: a directory moved out of the root mid-walk is still
/// open, and still writable through the descriptor. [`verify_contained`]
/// climbs back up to answer that. It is a check after the fact rather than an
/// atomic resolution — the honest guarantee on a kernel without `openat2`.
#[cfg(unix)]
fn walk_dirs(root_fd: &OwnedFd, components: &[&OsStr]) -> io::Result<OwnedFd> {
    let mut fd = root_fd.try_clone()?;

    for name in components {
        fd = fd.open_child_dir(name)?;
    }

    verify_contained(root_fd, &fd, components.len())?;

    Ok(fd)
}

/// Confirms `dir` is reachable from `root_fd` by climbing `..` at most `depth`
/// times.
///
/// `openat(fd, "..")` on a directory descriptor gives that directory's real
/// parent, whatever name it was opened under, so arriving at the root's own
/// inode proves containment rather than assuming it.
#[cfg(unix)]
fn verify_contained(root_fd: &OwnedFd, dir: &OwnedFd, depth: usize) -> io::Result<()> {
    let root = root_fd.stat()?;
    let mut current = dir.try_clone()?;

    for _ in 0..=depth {
        let here = current.stat()?;
        if here.st_dev == root.st_dev && here.st_ino == root.st_ino {
            return Ok(());
        }

        let parent = current.open_parent()?;
        let above = parent.stat()?;

        // The filesystem root is its own parent, so this is where a path that
        // never meets the download root stops.
        if above.st_dev == here.st_dev && above.st_ino == here.st_ino {
            break;
        }

        current = parent;
    }

    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "directory is no longer inside the download root",
    ))
}

/// Creates every component of `components` beneath the pinned root.
///
/// This is the half `openat2` cannot do: it has no directory-creating mode, so
/// each component is `mkdirat`-ed against its parent's descriptor. The
/// directory is then re-resolved *from the root* rather than opened against
/// the descriptor we happen to be holding, so every handle in the chain is one
/// the kernel has just confirmed is still beneath the root, and a directory
/// replaced by a symlink between the create and the open is caught rather
/// than followed.
#[cfg(unix)]
fn create_dirs_from(root_fd: &OwnedFd, components: &[&OsStr]) -> io::Result<OwnedFd> {
    let mut dir = root_fd.try_clone()?;

    for (index, name) in components.iter().enumerate() {
        dir.mkdir_child(name)?;
        dir = resolve_dir(root_fd, &components[..=index])?;
    }

    Ok(dir)
}

/// Resolves a directory beneath `root`, creating it if asked.
#[cfg(unix)]
fn dir_beneath(root: &Path, components: &[&OsStr], create: bool) -> io::Result<OwnedFd> {
    let root_fd = root_dir(root)?;

    if create {
        return create_dirs_from(&root_fd, components);
    }

    resolve_dir(&root_fd, components)
}

/// The flags for one guarded open.
///
/// Two of these matter beyond the obvious:
///
/// - `O_NONBLOCK`, because opening a FIFO for writing blocks until a reader
///   arrives, and an open that never returns is an `fstat` refusal that never
///   runs. A planted FIFO could therefore hang the transfer instead of being
///   rejected — including on the resume path, which opens for append. The flag
///   is cleared again once the descriptor has been validated as a regular
///   file, so ordinary writes keep ordinary blocking semantics.
/// - no `O_TRUNC`, ever. Truncation inside the open happens before anything
///   has been validated, so a destination rdm is about to refuse — a
///   hard-linked file, a file owned by someone else — would be emptied first
///   and refused afterwards. [`Existing::Truncate`] is applied through the
///   validated descriptor instead.
#[cfg(unix)]
fn open_flags(existing: Existing, access: Access) -> libc::c_int {
    let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;

    flags |= match access {
        Access::ReadWrite => libc::O_RDWR,
        Access::Append => libc::O_WRONLY | libc::O_APPEND,
    };

    flags |= match existing {
        Existing::Reject => libc::O_CREAT | libc::O_EXCL,
        Existing::Open => libc::O_CREAT,
        Existing::Truncate => 0,
    };

    flags
}

/// Opens a single component relative to a directory descriptor.
///
/// Shared by [`open_guarded`]'s trusted-parent split and the pre-`openat2`
/// fallback. By the time this runs the name is one component in a directory we
/// hold a descriptor for, so `openat2`'s `RESOLVE_BENEATH` and the `openat`
/// fallback's `O_NOFOLLOW` differ only in how much they refuse beyond a
/// final-component symlink.
#[cfg(unix)]
fn open_at(
    dir_fd: RawFd,
    name: &OsStr,
    existing: Existing,
    access: Access,
    mode: u32,
) -> io::Result<File> {
    let c_name = cstr(name)?;
    let flags = open_flags(existing, access);

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
fn open_beneath_impl(
    root: &Path,
    relative: &Path,
    existing: Existing,
    access: Access,
    mode: u32,
) -> io::Result<File> {
    let components = untrusted_components(relative)?;
    if components.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path has no final component",
        ));
    }

    let root_fd = root_dir(root)?;

    // One resolution for the whole path, anchored at the root descriptor.
    #[cfg(target_os = "linux")]
    {
        let joined = join_components(&components)?;

        match linux::openat2(root_fd.fd, &joined, open_flags(existing, access), mode) {
            Ok(fd) => {
                // SAFETY: openat2 returned a fresh owned descriptor.
                return Ok(unsafe { File::from_raw_fd(fd) });
            }
            Err(e) if is_unsupported(&e) => {
                // Pre-5.6 kernel, or seccomp. Fall through to the walk.
            }
            Err(e) => return Err(e),
        }
    }

    let (dirs, name) = components.split_at(components.len() - 1);
    let dir_fd = resolve_dir(&root_fd, dirs)?;

    open_at(dir_fd.fd, name[0], existing, access, mode)
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
        io::Error::new(io::ErrorKind::InvalidInput, "path has no final component")
    })?;

    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        // A bare filename is relative to the process's cwd.
        _ => PathBuf::from("."),
    };

    Ok((parent, PathBuf::from(name)))
}

/// Clears `O_NONBLOCK` from a descriptor that has been validated.
///
/// The flag is only there to stop the open itself blocking on something that
/// is not a regular file. On a regular file it means nothing, but leaving it
/// set would be a surprise to every later write, so it goes away as soon as
/// the `fstat` has had its say.
#[cfg(unix)]
fn clear_nonblock(file: &File) -> io::Result<()> {
    let fd = file.as_raw_fd();

    // SAFETY: fd is an open descriptor owned by the caller.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }

    if flags & libc::O_NONBLOCK == 0 {
        return Ok(());
    }

    // SAFETY: as above.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags & !libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(not(unix))]
fn clear_nonblock(_file: &File) -> io::Result<()> {
    Ok(())
}

/// Turns a freshly opened descriptor into one the caller may write through.
///
/// The order is the point: validate first, and only then do anything
/// destructive. `O_TRUNC` inside the open would empty the file before
/// [`validate_regular_owned`] had seen it, so a destination that is about to
/// be refused — a hard-linked file, a file belonging to someone else — would
/// already be zero bytes by the time it was refused. Truncating through the
/// validated descriptor cannot damage anything rdm would not have written to
/// anyway, and needs no second resolution of the name.
fn prepare(file: File, existing: Existing) -> Result<File> {
    validate_regular_owned(&file)?;

    clear_nonblock(&file).context("Failed to restore blocking mode")?;

    if existing == Existing::Truncate {
        file.set_len(0).context("Failed to truncate destination")?;
    }

    Ok(file)
}

#[cfg(unix)]
fn create_dirs_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    let components = untrusted_components(relative)?;

    // An empty relative path means "the root itself", which already exists.
    // Still resolved, so that a missing or non-directory root is reported here
    // rather than at the first write.
    dir_beneath(root, &components, true)?;

    Ok(())
}

#[cfg(unix)]
fn metadata_beneath_impl(root: &Path, relative: &Path) -> io::Result<Option<Metadata>> {
    let (dirs, name) = split_untrusted(relative)?;
    let dir_fd = dir_beneath(root, &dirs, false)?;
    let c_name = cstr(name)?;

    let mut stat: libc::stat = unsafe { std::mem::zeroed() };

    // SAFETY: dir_fd is open, c_name is NUL-terminated and stat is a valid
    // writable stat buffer.
    let rc = unsafe {
        libc::fstatat(
            dir_fd.fd,
            c_name.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };

    if rc != 0 {
        let e = io::Error::last_os_error();
        return if e.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(e)
        };
    }

    if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }

    // Reopen against the same descriptor so the value handed back belongs to
    // a file we are holding rather than to a name. `O_PATH` where it exists:
    // it needs no read permission and cannot block on a FIFO planted between
    // the stat above and this open. Elsewhere `O_NONBLOCK` is what stops the
    // open blocking, and `File::metadata` below is still the authoritative
    // check.
    #[cfg(target_os = "linux")]
    let kind = libc::O_PATH;
    #[cfg(not(target_os = "linux"))]
    let kind = libc::O_RDONLY | libc::O_NONBLOCK;

    // SAFETY: dir_fd is open and c_name is NUL-terminated.
    let fd = unsafe {
        libc::openat(
            dir_fd.fd,
            c_name.as_ptr(),
            kind | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };

    if fd < 0 {
        let e = io::Error::last_os_error();
        return if e.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(e)
        };
    }

    // SAFETY: openat returned a fresh owned descriptor.
    let file = unsafe { File::from_raw_fd(fd) };
    let meta = file.metadata()?;

    if !meta.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }

    Ok(Some(meta))
}

#[cfg(unix)]
fn unlink_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    let (dirs, name) = split_untrusted(relative)?;
    let dir_fd = dir_beneath(root, &dirs, false)?;
    let c_name = cstr(name)?;

    // SAFETY: dir_fd is open and c_name is NUL-terminated.
    let rc = unsafe { libc::unlinkat(dir_fd.fd, c_name.as_ptr(), 0) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    // A removal is a directory change like any other: without this the entry
    // can come back after a power loss.
    fsync_dir(dir_fd.fd)
}

#[cfg(unix)]
fn remove_dir_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    let (dirs, name) = split_untrusted(relative)?;
    let dir_fd = dir_beneath(root, &dirs, false)?;
    let c_name = cstr(name)?;

    // SAFETY: dir_fd is open and c_name is NUL-terminated.
    let rc = unsafe { libc::unlinkat(dir_fd.fd, c_name.as_ptr(), libc::AT_REMOVEDIR) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    fsync_dir(dir_fd.fd)
}

#[cfg(unix)]
fn read_dir_beneath_impl(root: &Path, relative: &Path) -> io::Result<Vec<DirEntry>> {
    use std::os::unix::ffi::OsStrExt;

    let components = untrusted_components(relative)?;
    let dir_fd = dir_beneath(root, &components, false)?;

    // `fdopendir` takes ownership of the descriptor it is given, and
    // `closedir` closes it, so it gets a duplicate and `dir_fd` keeps closing
    // its own. The duplicate is what the entry stats below are relative to, so
    // every entry is resolved against the directory that was opened rather
    // than against a pathname that could now mean something else.
    let stream_fd = dir_fd.try_clone()?.into_raw();

    // SAFETY: stream_fd is an open directory descriptor we have just
    // duplicated and are handing over.
    let stream = unsafe { libc::fdopendir(stream_fd) };
    if stream.is_null() {
        let e = io::Error::last_os_error();
        // SAFETY: fdopendir did not take ownership, so the duplicate is ours.
        unsafe { libc::close(stream_fd) };
        return Err(e);
    }

    let stream = DirStream(stream);
    let mut out = Vec::new();

    loop {
        // SAFETY: stream is a live DIR from fdopendir. readdir returns a
        // pointer owned by the stream, valid until the next readdir call.
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            // End of the directory. A read error here is indistinguishable
            // from the end without clearing errno first, and the callers of
            // this function delete what it does *not* return, so stopping
            // early is the safe direction to be wrong in.
            break;
        }

        // SAFETY: entry points at a live dirent for the duration of this
        // block, and d_name is NUL-terminated.
        let (name, d_type) = unsafe {
            (
                std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()),
                (*entry).d_type,
            )
        };

        let bytes = name.to_bytes();
        if bytes == b"." || bytes == b".." {
            continue;
        }

        let name = OsStr::from_bytes(bytes).to_os_string();

        // An entry that has gone since the readdir is not an entry.
        if let Some(kind) = entry_kind(&dir_fd, &name, d_type)? {
            out.push(DirEntry { name, kind });
        }
    }

    Ok(out)
}

/// What one directory entry is, without following anything.
///
/// `d_type` where the filesystem fills it in, `fstatat(AT_SYMLINK_NOFOLLOW)`
/// against the directory descriptor where it does not. A symlink reports as
/// [`EntryKind::Other`]: callers neither descend into it nor delete it.
#[cfg(unix)]
fn entry_kind(dir_fd: &OwnedFd, name: &OsStr, d_type: u8) -> io::Result<Option<EntryKind>> {
    match d_type {
        libc::DT_DIR => return Ok(Some(EntryKind::Dir)),
        libc::DT_REG => return Ok(Some(EntryKind::File)),
        libc::DT_UNKNOWN => {}
        // Symlinks, FIFOs, sockets, devices.
        _ => return Ok(Some(EntryKind::Other)),
    }

    let c_name = cstr(name)?;
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };

    // SAFETY: dir_fd is open, c_name is NUL-terminated and stat is a valid
    // writable stat buffer.
    let rc = unsafe {
        libc::fstatat(
            dir_fd.fd,
            c_name.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };

    if rc != 0 {
        let e = io::Error::last_os_error();
        return if e.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(e)
        };
    }

    Ok(Some(match stat.st_mode & libc::S_IFMT {
        libc::S_IFDIR => EntryKind::Dir,
        libc::S_IFREG => EntryKind::File,
        _ => EntryKind::Other,
    }))
}

/// A `DIR` stream that closes itself.
#[cfg(unix)]
struct DirStream(*mut libc::DIR);

#[cfg(unix)]
impl Drop for DirStream {
    fn drop(&mut self) {
        // SAFETY: we own this stream and this runs once. closedir also closes
        // the descriptor fdopendir took ownership of.
        unsafe { libc::closedir(self.0) };
    }
}

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

/// The staged file and the directory entry it currently occupies.
#[cfg(unix)]
struct StagedEntry {
    dir: OwnedFd,
    name: OsString,
}

/// Resolves the directory holding `path`'s final component, plus that name.
///
/// Beneath the registered root the directory is resolved from the pinned root
/// descriptor; outside it the parent is a path the user named in full and is
/// opened as such.
#[cfg(unix)]
fn resolve_entry(path: &Path) -> io::Result<StagedEntry> {
    if let Some(root) = download_root()
        && let Some(relative) = split_beneath(root, path)
    {
        let (dirs, name) = split_untrusted(relative)?;
        let root_fd = root_dir(root)?;

        return Ok(StagedEntry {
            dir: resolve_dir(&root_fd, &dirs)?,
            name: name.to_os_string(),
        });
    }

    let (parent, name) = split_parent(path)?;

    Ok(StagedEntry {
        dir: OwnedFd::open_dir(&parent)?,
        name: name.into_os_string(),
    })
}

/// Resolves `relative` beneath an explicitly supplied `root`.
#[cfg(unix)]
fn resolve_entry_beneath(root: &Path, relative: &Path) -> io::Result<StagedEntry> {
    let (dirs, name) = split_untrusted(relative)?;

    Ok(StagedEntry {
        dir: dir_beneath(root, &dirs, false)?,
        name: name.to_os_string(),
    })
}

/// Opens a staged file for publication: read-only, and validated.
///
/// Read-only because publication does not write, and validated because this
/// descriptor is the one that decides what gets published. Anything a
/// substituted entry could be — a symlink, a FIFO, a file belonging to
/// somebody else, a second link to a file rdm did not write — is refused here
/// instead of being handed to the destination name.
#[cfg(unix)]
fn open_staged(staged: &StagedEntry) -> Result<File> {
    let c_name = cstr(&staged.name)?;
    let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;

    // SAFETY: the directory descriptor is open and c_name is NUL-terminated.
    let fd = unsafe { libc::openat(staged.dir.fd, c_name.as_ptr(), flags) };
    if fd < 0 {
        return Err(io::Error::last_os_error()).context("Failed to open the staged file");
    }

    // SAFETY: openat returned a fresh owned descriptor.
    let file = unsafe { File::from_raw_fd(fd) };

    validate_regular_owned(&file).context("Refusing to publish the staged file")?;
    clear_nonblock(&file).context("Failed to restore blocking mode")?;

    Ok(file)
}

/// Publishes the inode behind `file` as `dest_name` in `dest`.
///
/// The old publication step renamed the staged *name*, which is not the same
/// thing as the file that was opened, written and validated. An attacker able
/// to create entries in the staging directory could unlink the entry after rdm
/// opened it — leaving rdm writing to a now-unnamed inode — put something else
/// at that name, and have rdm publish that instead. `RENAME_NOREPLACE`
/// protected the destination from being clobbered and said nothing about which
/// file was arriving.
///
/// So publication goes through the descriptor rdm actually wrote: `linkat`
/// gives the destination name to that exact inode. The staged name is no
/// longer an input to it, and a stat comparison before a rename — which would
/// be a race of its own — is not needed, because there is nothing left to
/// compare.
///
/// `linkat` also cannot replace an existing entry, which is the no-clobber
/// guarantee for free. An approved overwrite removes the destination first and
/// then links: if something takes the name in between, the link fails and
/// publication is refused, rather than succeeding with somebody else's file.
#[cfg(unix)]
fn publish_impl(
    staged: &StagedEntry,
    file: &File,
    dest: &StagedEntry,
    replace: bool,
) -> Result<()> {
    // Nothing is touched until the descriptor being published has been
    // checked, so a refusal leaves both names exactly as they were.
    validate_regular_owned(file).context("Refusing to publish this file")?;

    // A descriptor whose last name has been removed cannot be given a new one:
    // the kernel refuses to link an unlinked inode back into the filesystem
    // without CAP_DAC_READ_SEARCH, which rdm does not want to require. That is
    // what being substituted looks like from this side — somebody unlinked the
    // staged entry while the download was running — so it is reported as the
    // refusal it is, rather than papered over by publishing whatever entry now
    // holds the staged name.
    if fstat(file.as_raw_fd())?.st_nlink == 0 {
        bail!(
            "The staged file was unlinked while it was being written, so it cannot \
             be published; nothing was written to the destination"
        );
    }

    let c_dest = cstr(&dest.name)?;

    match link_into_place(file, dest.dir.fd, &c_dest) {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::EEXIST) && replace => {
            // SAFETY: the directory descriptor is open and c_dest is
            // NUL-terminated.
            let rc = unsafe { libc::unlinkat(dest.dir.fd, c_dest.as_ptr(), 0) };
            if rc != 0 {
                let e = io::Error::last_os_error();
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e).context("Failed to remove the file being replaced");
                }
            }

            link_into_place(file, dest.dir.fd, &c_dest)
                .context("The destination was taken by another file during publication")?;
        }
        Err(e) if is_unsupported_link(&e) => {
            // No way to link a descriptor on this platform. See
            // `rename_into_place` for what is and is not guaranteed there.
            return rename_into_place(staged, file, dest, replace);
        }
        Err(e) => {
            return Err(e).context("Failed to publish the download");
        }
    }

    // The staged entry has served its purpose. It is only removed while it
    // still names the inode that was just published: anything else at that
    // name belongs to whoever put it there, and is not rdm's to unlink.
    remove_staged(staged, file);

    sync_published_dirs(&staged.dir, &dest.dir)
}

/// Gives `dest_name` in `dest_fd` to the inode behind `file`.
#[cfg(unix)]
fn link_into_place(file: &File, dest_fd: RawFd, dest_name: &CString) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // `AT_EMPTY_PATH` is the direct form, and needs no /proc, but it is
        // privileged (CAP_DAC_READ_SEARCH). Its refusal is not an answer about
        // the destination, so it falls through rather than propagating.
        match linux::linkat_empty_path(file.as_raw_fd(), dest_fd, dest_name) {
            Ok(()) => return Ok(()),
            Err(e) if is_unsupported_link(&e) => {}
            Err(e) => return Err(e),
        }

        // The unprivileged form: the kernel resolves /proc/self/fd/<n> to the
        // inode the descriptor holds, so the link names that file and not
        // whatever the staged name now points at.
        linux::linkat_via_proc(file.as_raw_fd(), dest_fd, dest_name)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, dest_fd, dest_name);
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}

/// Whether an error means "this way of linking is not available here", as
/// opposed to an answer about the destination.
///
/// `EEXIST`, `ENOSPC`, `EDQUOT` and `EXDEV` are answers and must propagate.
#[cfg(unix)]
fn is_unsupported_link(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ENOSYS)
            | Some(libc::EOPNOTSUPP)
            | Some(libc::EPERM)
            | Some(libc::EACCES)
            | Some(libc::EINVAL)
            | Some(libc::ENOENT)
            | Some(libc::EMLINK)
    )
}

/// Publication where no descriptor can be linked: rename the staged name.
///
/// Only reachable where `linkat` from a descriptor does not exist — in
/// practice a Unix that is not Linux, since Linux has /proc. The identity
/// check immediately before the rename closes the window to a few
/// instructions; it cannot close it entirely, because a rename names an entry
/// rather than an inode. That is a platform limit, stated rather than hidden:
/// on Linux, publication is by descriptor and this function does not run.
#[cfg(unix)]
fn rename_into_place(
    staged: &StagedEntry,
    file: &File,
    dest: &StagedEntry,
    replace: bool,
) -> Result<()> {
    let c_staged = cstr(&staged.name)?;
    let c_dest = cstr(&dest.name)?;

    let want = fstat(file.as_raw_fd())?;
    let mut have: libc::stat = unsafe { std::mem::zeroed() };

    // SAFETY: the directory descriptor is open, c_staged is NUL-terminated and
    // have is a valid writable stat buffer.
    let rc = unsafe {
        libc::fstatat(
            staged.dir.fd,
            c_staged.as_ptr(),
            &mut have,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("Failed to inspect the staged file");
    }

    if have.st_dev != want.st_dev || have.st_ino != want.st_ino {
        bail!("The staged file was replaced before it could be published");
    }

    if !replace {
        #[cfg(target_os = "linux")]
        {
            match linux::renameat2_at(staged.dir.fd, &c_staged, dest.dir.fd, &c_dest) {
                Ok(()) => return sync_published_dirs(&staged.dir, &dest.dir),
                Err(e) if is_unsupported(&e) => {
                    // Older kernel or an exotic filesystem: the link/unlink
                    // emulation below gives the same no-clobber guarantee.
                }
                Err(e) => return Err(e).context("Failed to publish the download"),
            }
        }

        // `linkat` fails with EEXIST when the destination exists, atomically.
        // SAFETY: both descriptors are open and both names are NUL-terminated.
        let rc = unsafe {
            libc::linkat(
                staged.dir.fd,
                c_staged.as_ptr(),
                dest.dir.fd,
                c_dest.as_ptr(),
                0,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error()).context("Failed to publish the download");
        }

        remove_staged(staged, file);

        return sync_published_dirs(&staged.dir, &dest.dir);
    }

    // SAFETY: both descriptors are open and both names are NUL-terminated.
    let rc = unsafe {
        libc::renameat(
            staged.dir.fd,
            c_staged.as_ptr(),
            dest.dir.fd,
            c_dest.as_ptr(),
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error()).context("Failed to publish the download");
    }

    sync_published_dirs(&staged.dir, &dest.dir)
}

/// Unlinks the staged entry, but only while it still names `file`'s inode.
///
/// A failure is deliberately not an error: the download is published by now,
/// and a leftover staged name is cosmetic.
#[cfg(unix)]
fn remove_staged(staged: &StagedEntry, file: &File) {
    let Ok(c_name) = cstr(&staged.name) else {
        return;
    };

    let Ok(want) = fstat(file.as_raw_fd()) else {
        return;
    };

    let mut have: libc::stat = unsafe { std::mem::zeroed() };

    // SAFETY: the directory descriptor is open, c_name is NUL-terminated and
    // have is a valid writable stat buffer.
    let rc = unsafe {
        libc::fstatat(
            staged.dir.fd,
            c_name.as_ptr(),
            &mut have,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };

    if rc != 0 || have.st_dev != want.st_dev || have.st_ino != want.st_ino {
        return;
    }

    // SAFETY: as above.
    unsafe { libc::unlinkat(staged.dir.fd, c_name.as_ptr(), 0) };
}

/// Persists the directory entries a publication just changed.
///
/// Flushing the payload only guarantees its *contents* survive a power loss;
/// the entry that publishes it lives in the parent directory, and that is a
/// separate write. Both parents are synced, deduplicated by inode for the
/// common case of publishing within one directory.
#[cfg(unix)]
fn sync_published_dirs(from: &OwnedFd, to: &OwnedFd) -> Result<()> {
    fsync_dir(from.fd).context("Failed to flush the staging directory")?;

    let same = match (from.stat(), to.stat()) {
        (Ok(a), Ok(b)) => a.st_dev == b.st_dev && a.st_ino == b.st_ino,
        _ => false,
    };

    if !same {
        fsync_dir(to.fd).context("Failed to flush the destination directory")?;
    }

    Ok(())
}

/// `fsync` on a directory descriptor, which is how a link or unlink is made
/// durable.
///
/// A filesystem that does not implement it at all is tolerated: there is
/// nothing the caller could do instead, and failing the transfer over it
/// would be worse than the weaker guarantee.
#[cfg(unix)]
fn fsync_dir(fd: RawFd) -> io::Result<()> {
    // SAFETY: fd is an open directory descriptor owned by the caller.
    let rc = unsafe { libc::fsync(fd) };
    if rc != 0 {
        let e = io::Error::last_os_error();
        if is_unsupported(&e) {
            return Ok(());
        }
        return Err(e);
    }

    Ok(())
}

#[cfg(unix)]
fn publish_path_impl(staged: &Path, dest: &Path, replace: bool) -> Result<()> {
    let staged_entry = resolve_entry(staged)?;
    let file = open_staged(&staged_entry)?;
    let dest_entry = resolve_entry(dest)?;

    publish_impl(&staged_entry, &file, &dest_entry, replace)
}

#[cfg(unix)]
fn publish_open_impl(file: &File, staged: &Path, dest: &Path, replace: bool) -> Result<()> {
    let staged_entry = resolve_entry(staged)?;
    let dest_entry = resolve_entry(dest)?;

    publish_impl(&staged_entry, file, &dest_entry, replace)
}

#[cfg(unix)]
fn publish_beneath_impl(root: &Path, staged: &Path, dest: &Path, replace: bool) -> Result<()> {
    let staged_entry = resolve_entry_beneath(root, staged)?;
    let file = open_staged(&staged_entry)?;
    let dest_entry = resolve_entry_beneath(root, dest)?;

    publish_impl(&staged_entry, &file, &dest_entry, replace)
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
    /// Only used by the pre-`openat2` fallback walk; `resolve_dir` prefers a
    /// single anchored resolution of the whole path.
    fn open_child_dir(&self, name: &OsStr) -> io::Result<Self> {
        let c_name = cstr(name)?;

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

    /// Creates one child directory, tolerating one that is already there.
    ///
    /// Whether what is there is a directory, and not a symlink put there in
    /// between, is settled by the anchored reopen that follows — never by this
    /// call's success.
    fn mkdir_child(&self, name: &OsStr) -> io::Result<()> {
        let c_name = cstr(name)?;

        // SAFETY: self.fd is open and c_name is NUL-terminated.
        let rc =
            unsafe { libc::mkdirat(self.fd, c_name.as_ptr(), DEFAULT_DIR_MODE as libc::mode_t) };

        if rc != 0 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::EEXIST) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// Opens this directory's real parent.
    fn open_parent(&self) -> io::Result<Self> {
        // SAFETY: self.fd is an open directory descriptor and the path is a
        // NUL-terminated literal.
        let fd = unsafe {
            libc::openat(
                self.fd,
                c"..".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };

        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { fd })
    }

    fn try_clone(&self) -> io::Result<Self> {
        // SAFETY: self.fd is open and F_DUPFD_CLOEXEC takes an int argument.
        let fd = unsafe { libc::fcntl(self.fd, libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { fd })
    }

    /// Hands the descriptor to a caller that will close it.
    fn into_raw(self) -> RawFd {
        let fd = self.fd;
        std::mem::forget(self);
        fd
    }

    fn stat(&self) -> io::Result<libc::stat> {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };

        // SAFETY: self.fd is open and stat is a valid writable stat buffer.
        let rc = unsafe { libc::fstat(self.fd, &mut stat) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(stat)
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
#[cfg(unix)]
fn is_unsupported(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP) | Some(libc::EINVAL)
    )
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;

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

    /// Opens `name` relative to `dir_fd`, refusing to leave it.
    ///
    /// `name` may be several components: `RESOLVE_BENEATH` anchors the whole
    /// resolution at `dir_fd`, and the kernel performs it as one operation, so
    /// a multi-component path is resolved without ever exposing an
    /// intermediate directory handle.
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

    /// `linkat(fd, "", dir, name, AT_EMPTY_PATH)`: names the inode `fd` holds.
    ///
    /// The privileged form, needing `CAP_DAC_READ_SEARCH`. Tried first because
    /// it depends on nothing outside the kernel.
    pub(super) fn linkat_empty_path(fd: RawFd, dir_fd: RawFd, name: &CString) -> io::Result<()> {
        // SAFETY: fd and dir_fd are open, the source path is an empty
        // NUL-terminated literal as AT_EMPTY_PATH requires, and name is
        // NUL-terminated.
        let rc =
            unsafe { libc::linkat(fd, c"".as_ptr(), dir_fd, name.as_ptr(), libc::AT_EMPTY_PATH) };

        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// The unprivileged form of the same thing, through /proc.
    ///
    /// `AT_SYMLINK_FOLLOW` makes the kernel resolve the magic link
    /// `/proc/self/fd/<n>` to the inode that descriptor holds, so the new name
    /// is given to the file rdm wrote — not to whatever the staged name points
    /// at by now.
    pub(super) fn linkat_via_proc(fd: RawFd, dir_fd: RawFd, name: &CString) -> io::Result<()> {
        let source = CString::new(format!("/proc/self/fd/{}", fd))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;

        // SAFETY: both descriptors are open and both paths are
        // NUL-terminated.
        let rc = unsafe {
            libc::linkat(
                libc::AT_FDCWD,
                source.as_ptr(),
                dir_fd,
                name.as_ptr(),
                libc::AT_SYMLINK_FOLLOW,
            )
        };

        if rc != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// `renameat2(RENAME_NOREPLACE)` between two directory descriptors.
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
        // No `truncate(true)`: the file is emptied through the validated
        // handle by `prepare`, so a destination that is about to be refused is
        // not destroyed first.
        Existing::Truncate => {}
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
fn metadata_beneath_impl(root: &Path, relative: &Path) -> io::Result<Option<Metadata>> {
    match std::fs::symlink_metadata(joined_beneath(root, relative)?) {
        Ok(meta) if meta.is_file() => Ok(Some(meta)),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(not(unix))]
fn unlink_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    std::fs::remove_file(joined_beneath(root, relative)?)
}

/// Publication on a platform with no way to link a descriptor into place.
///
/// `hard_link` fails if the destination exists, which is the no-clobber half,
/// and the staged name is only dropped once the link has succeeded. Identity
/// through publication is *not* guaranteed here: there is no portable
/// equivalent of `linkat` from a descriptor, so this names an entry rather
/// than an inode. Stated plainly rather than implied, since the Unix path does
/// not work this way.
#[cfg(not(unix))]
fn publish_entries(from: &Path, to: &Path, replace: bool) -> Result<()> {
    if replace {
        return std::fs::rename(from, to)
            .with_context(|| format!("Failed to publish {}", from.display()));
    }

    std::fs::hard_link(from, to).with_context(|| {
        format!(
            "Failed to publish {} (the destination may already exist)",
            from.display()
        )
    })?;

    let _ = std::fs::remove_file(from);

    Ok(())
}

#[cfg(not(unix))]
fn publish_path_impl(from: &Path, to: &Path, replace: bool) -> Result<()> {
    publish_entries(from, to, replace)
}

#[cfg(not(unix))]
fn publish_open_impl(_file: &File, from: &Path, to: &Path, replace: bool) -> Result<()> {
    publish_entries(from, to, replace)
}

#[cfg(not(unix))]
fn publish_beneath_impl(root: &Path, from: &Path, to: &Path, replace: bool) -> Result<()> {
    publish_entries(
        &joined_beneath(root, from)?,
        &joined_beneath(root, to)?,
        replace,
    )
}

#[cfg(not(unix))]
fn read_dir_beneath_impl(root: &Path, relative: &Path) -> io::Result<Vec<DirEntry>> {
    let mut out = Vec::new();

    for entry in std::fs::read_dir(joined_beneath(root, relative)?)? {
        let entry = entry?;
        let kind = match std::fs::symlink_metadata(entry.path()) {
            // Never the thing a link points at: a reparse point reports as
            // `Other` and callers leave it alone.
            Ok(meta) if meta.is_dir() => EntryKind::Dir,
            Ok(meta) if meta.is_file() => EntryKind::File,
            Ok(_) => EntryKind::Other,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };

        out.push(DirEntry {
            name: entry.file_name(),
            kind,
        });
    }

    Ok(out)
}

#[cfg(not(unix))]
fn remove_dir_beneath_impl(root: &Path, relative: &Path) -> io::Result<()> {
    std::fs::remove_dir(joined_beneath(root, relative)?)
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

        let mut file = open_guarded(
            &path,
            Existing::Reject,
            Access::ReadWrite,
            DEFAULT_FILE_MODE,
        )
        .unwrap();
        file.write_all(b"hello").unwrap();
        drop(file);

        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
    }

    #[test]
    fn reject_means_reject() {
        let dir = tmpdir();
        let path = dir.path().join("out.bin");
        std::fs::write(&path, b"existing").unwrap();

        let err = open_guarded(
            &path,
            Existing::Reject,
            Access::ReadWrite,
            DEFAULT_FILE_MODE,
        )
        .unwrap_err();
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

        assert!(
            open_guarded(
                &planted,
                Existing::Open,
                Access::ReadWrite,
                DEFAULT_FILE_MODE
            )
            .is_err()
        );
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
        assert!(
            msg.contains("Refusing") || msg.contains("Failed to safely open"),
            "{msg}"
        );
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
    fn publication_puts_the_file_in_place_when_the_destination_is_free() {
        let dir = tmpdir();
        let from = dir.path().join("a.part");
        let to = dir.path().join("a.bin");
        std::fs::write(&from, b"payload").unwrap();

        publish_no_replace(&from, &to).unwrap();

        assert_eq!(std::fs::read(&to).unwrap(), b"payload");
        assert!(!from.exists());
    }

    /// This is the race: a file appears between the existence check and the
    /// rename. A plain `fs::rename` would silently destroy it.
    #[test]
    fn publication_refuses_to_clobber_a_file_that_appeared() {
        let dir = tmpdir();
        let from = dir.path().join("a.part");
        let to = dir.path().join("a.bin");
        std::fs::write(&from, b"new").unwrap();
        std::fs::write(&to, b"appeared after the check").unwrap();

        assert!(publish_no_replace(&from, &to).is_err());
        assert_eq!(std::fs::read(&to).unwrap(), b"appeared after the check");
    }

    #[test]
    fn replacing_publication_is_still_available_for_approved_overwrites() {
        let dir = tmpdir();
        let from = dir.path().join("a.part");
        let to = dir.path().join("a.bin");
        std::fs::write(&from, b"new").unwrap();
        std::fs::write(&to, b"old").unwrap();

        publish_replacing(&from, &to).unwrap();
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
    fn publishing_beneath_the_root_works_and_refuses_to_clobber() {
        let root = tmpdir();
        create_dirs_beneath(root.path(), Path::new("album")).unwrap();
        std::fs::write(root.path().join("album/track.part"), b"payload").unwrap();

        publish_beneath(
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
            publish_beneath(
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
        publish_beneath(
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
            publish_beneath(
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

    /// The `--delete` case: the sweep's own root is a listing-chosen folder
    /// name, so it is a component to be resolved rather than a root to be
    /// trusted — and the listing it produces is a list of files to delete.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_sweep_root_is_refused_rather_than_enumerated() {
        let root = tmpdir();
        let outside = tmpdir();
        std::fs::write(outside.path().join("precious.jpg"), b"x").unwrap();

        std::fs::create_dir(root.path().join("real-album")).unwrap();
        std::fs::write(root.path().join("real-album/track.flac"), b"a").unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("album")).unwrap();

        let listed = read_dir_beneath(root.path(), Path::new("real-album")).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "track.flac");
        assert_eq!(listed[0].kind, EntryKind::File);

        // Nothing behind the link is reachable, and the link itself reports as
        // neither a file nor a directory.
        assert!(read_dir_beneath(root.path(), Path::new("album")).is_err());
        assert!(read_dir_beneath(root.path(), Path::new("missing")).is_err());
        assert!(read_dir_beneath(root.path(), Path::new("../elsewhere")).is_err());

        let top = read_dir_beneath(root.path(), Path::new("")).unwrap();
        let link = top
            .iter()
            .find(|entry| entry.name == "album")
            .expect("the link is listed");
        assert_eq!(link.kind, EntryKind::Other);

        assert!(outside.path().join("precious.jpg").exists());
    }

    /// Empty directories left behind by a sweep are removed through the same
    /// anchored resolution, so a link cannot redirect the removal.
    #[cfg(unix)]
    #[test]
    fn removing_a_directory_goes_through_the_root() {
        let root = tmpdir();
        let outside = tmpdir();
        std::fs::create_dir(outside.path().join("empty-but-not-ours")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("elsewhere")).unwrap();

        create_dirs_beneath(root.path(), Path::new("album/disc1")).unwrap();

        assert!(remove_dir_beneath(root.path(), Path::new("album")).is_err());
        remove_dir_beneath(root.path(), Path::new("album/disc1")).unwrap();
        remove_dir_beneath(root.path(), Path::new("album")).unwrap();
        assert!(!root.path().join("album").exists());

        assert!(
            remove_dir_beneath(root.path(), Path::new("elsewhere/empty-but-not-ours")).is_err()
        );
        assert!(outside.path().join("empty-but-not-ours").exists());
    }

    /// The TOCTOU a parent check followed by a pathname stat leaves open: the
    /// stat itself must be unable to land outside the root.
    #[cfg(unix)]
    #[test]
    fn metadata_is_read_by_descriptor_and_never_through_a_link() {
        let root = tmpdir();
        let outside = tmpdir();
        std::fs::write(outside.path().join("secret.txt"), b"not ours").unwrap();

        // Absent is an answer, not a failure.
        assert!(
            metadata_beneath(root.path(), Path::new("absent.bin"))
                .unwrap()
                .is_none()
        );

        std::fs::write(root.path().join("real.bin"), b"abcd").unwrap();
        let meta = metadata_beneath(root.path(), Path::new("real.bin"))
            .unwrap()
            .expect("a real file");
        assert_eq!(meta.len(), 4);

        // A symlinked intermediate directory: exactly what a pathname stat
        // after a separate parent check would follow.
        std::os::unix::fs::symlink(outside.path(), root.path().join("album")).unwrap();
        assert!(metadata_beneath(root.path(), Path::new("album/secret.txt")).is_err());

        // A symlink at the final component is not the file it points at.
        std::os::unix::fs::symlink(
            outside.path().join("secret.txt"),
            root.path().join("link.bin"),
        )
        .unwrap();
        assert!(metadata_beneath(root.path(), Path::new("link.bin")).is_err());

        // Nor is a directory, or anything reached by climbing out.
        std::fs::create_dir(root.path().join("dir")).unwrap();
        assert!(metadata_beneath(root.path(), Path::new("dir")).is_err());
        assert!(metadata_beneath(root.path(), Path::new("../elsewhere")).is_err());
    }

    // ---------- Validate before destroying ----------

    /// The finding: `O_TRUNC` is applied by the open itself, so a destination
    /// that is then refused has already been emptied. The victim here is
    /// reachable under a second name, which is the case `validate_regular_owned`
    /// exists to refuse.
    #[cfg(unix)]
    #[test]
    fn truncate_does_not_empty_a_file_it_goes_on_to_refuse() {
        let dir = tmpdir();
        let victim = dir.path().join("precious.txt");
        std::fs::write(&victim, b"do not clobber").unwrap();

        let alias = dir.path().join("download.bin.part");
        std::fs::hard_link(&victim, &alias).unwrap();

        let err = open_guarded(
            &alias,
            Existing::Truncate,
            Access::ReadWrite,
            DEFAULT_FILE_MODE,
        )
        .expect_err("a multiply-linked inode must be refused");
        assert!(format!("{err:#}").contains("hard links"), "{err:#}");

        assert_eq!(
            std::fs::read(&victim).unwrap(),
            b"do not clobber",
            "the file was emptied by the open that refused it"
        );
    }

    /// And when the destination *is* acceptable, `Truncate` still truncates.
    #[test]
    fn truncate_still_empties_a_file_it_accepts() {
        let dir = tmpdir();
        let path = dir.path().join("out.bin");
        std::fs::write(&path, b"old and longer").unwrap();

        let file = open_guarded(
            &path,
            Existing::Truncate,
            Access::ReadWrite,
            DEFAULT_FILE_MODE,
        )
        .unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0);
        drop(file);

        assert_eq!(std::fs::read(&path).unwrap(), b"");

        // Absent is an error rather than a creation: `Truncate` means "open
        // the existing file".
        assert!(
            open_guarded(
                &dir.path().join("absent.bin"),
                Existing::Truncate,
                Access::ReadWrite,
                DEFAULT_FILE_MODE
            )
            .is_err()
        );
    }

    /// The finding: opening a FIFO for writing blocks until a reader arrives,
    /// so the `fstat` that refuses it never runs. Resume opens for append,
    /// which is exactly this mode — the existing FIFO test used `O_RDWR`,
    /// which returns immediately and therefore never exercised it.
    ///
    /// Run on a thread with a deadline, so a regression fails the test instead
    /// of hanging the suite.
    #[cfg(unix)]
    #[test]
    fn an_append_open_on_a_fifo_fails_instead_of_blocking() {
        use std::os::unix::ffi::OsStrExt;
        use std::sync::mpsc;
        use std::time::Duration;

        let dir = tmpdir();
        let fifo = dir.path().join("pipe.part");
        let c_fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();

        // SAFETY: valid NUL-terminated path.
        if unsafe { libc::mkfifo(c_fifo.as_ptr(), 0o644) } != 0 {
            return; // mkfifo unavailable in this environment
        }

        for access in [Access::Append, Access::ReadWrite] {
            let (tx, rx) = mpsc::channel();
            let path = fifo.clone();

            std::thread::spawn(move || {
                let result = open_guarded(&path, Existing::Open, access, DEFAULT_FILE_MODE);
                let _ = tx.send(result.is_err());
            });

            match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(refused) => assert!(refused, "a FIFO must be refused, not opened"),
                Err(_) => panic!("the open blocked instead of refusing the FIFO"),
            }
        }
    }

    // ---------- Publication ----------

    /// The finding, reproduced: the staged entry is substituted after rdm has
    /// opened and written it. Publication by name hands the output name to the
    /// substitute. Publication by descriptor cannot: the only inode it can
    /// publish is the one rdm wrote, and when that inode has lost its last
    /// name — which is what being substituted means — publication is refused
    /// outright.
    #[cfg(unix)]
    #[test]
    fn a_substituted_staged_entry_is_never_published() {
        let dir = tmpdir();
        let outside = tmpdir();

        let victim = outside.path().join("attacker-payload.bin");
        std::fs::write(&victim, b"attacker").unwrap();

        let staged = dir.path().join("download.bin.part");
        let dest = dir.path().join("download.bin");

        let mut file = open_guarded(
            &staged,
            Existing::Reject,
            Access::ReadWrite,
            DEFAULT_FILE_MODE,
        )
        .unwrap();
        file.write_all(b"the real download").unwrap();
        file.sync_all().unwrap();

        // The substitution: the entry rdm opened is unlinked and a symlink to
        // somewhere else is put at that name. rdm's descriptor still holds the
        // real payload.
        std::fs::remove_file(&staged).unwrap();
        std::os::unix::fs::symlink(&victim, &staged).unwrap();

        let err = publish_open_file(&file, &staged, &dest, false)
            .expect_err("a substituted entry must not be published");
        assert!(format!("{err:#}").contains("unlinked"), "{err:#}");

        assert!(!dest.exists(), "something was published");
        // The planted link is not rdm's to remove, and what it points at is
        // untouched.
        assert!(std::fs::symlink_metadata(&staged).unwrap().is_symlink());
        assert_eq!(std::fs::read(&victim).unwrap(), b"attacker");
    }

    /// The ordinary path, by way of contrast: the name goes to the inode the
    /// descriptor holds, and identity survives publication.
    #[cfg(unix)]
    #[test]
    fn publication_gives_the_final_name_to_the_written_inode() {
        use std::os::unix::fs::MetadataExt;

        let dir = tmpdir();
        let staged = dir.path().join("download.bin.part");
        let dest = dir.path().join("download.bin");

        let mut file = open_guarded(
            &staged,
            Existing::Reject,
            Access::ReadWrite,
            DEFAULT_FILE_MODE,
        )
        .unwrap();
        file.write_all(b"the real download").unwrap();
        file.sync_all().unwrap();

        let written_ino = file.metadata().unwrap().ino();

        publish_open_file(&file, &staged, &dest, false).unwrap();

        let published = std::fs::symlink_metadata(&dest).unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"the real download");
        assert_eq!(
            published.ino(),
            written_ino,
            "the published name points at a different file"
        );
        assert_eq!(published.nlink(), 1);
        assert!(!staged.exists(), "the staged name outlived publication");
    }

    /// The same substitution against the path-taking form, which has to reopen
    /// the staged file. It cannot publish the planted entry either: the reopen
    /// is validated, and a symlink is refused there.
    #[cfg(unix)]
    #[test]
    fn publishing_a_substituted_staged_name_is_refused() {
        let dir = tmpdir();
        let outside = tmpdir();

        let victim = outside.path().join("precious.bin");
        std::fs::write(&victim, b"not ours").unwrap();

        let staged = dir.path().join("download.bin.part");
        let dest = dir.path().join("download.bin");
        std::os::unix::fs::symlink(&victim, &staged).unwrap();

        assert!(publish_no_replace(&staged, &dest).is_err());
        assert!(!dest.exists(), "something was published");
        assert_eq!(std::fs::read(&victim).unwrap(), b"not ours");
    }

    /// A publication that cannot take the destination name must fail having
    /// changed nothing — and must not have removed the file it was asked to
    /// publish.
    #[test]
    fn a_refused_publication_leaves_the_staged_file_alone() {
        let dir = tmpdir();
        let staged = dir.path().join("a.part");
        let dest = dir.path().join("a.bin");
        std::fs::write(&staged, b"mine").unwrap();
        std::fs::write(&dest, b"appeared after the check").unwrap();

        assert!(publish_no_replace(&staged, &dest).is_err());

        assert_eq!(std::fs::read(&dest).unwrap(), b"appeared after the check");
        assert_eq!(
            std::fs::read(&staged).unwrap(),
            b"mine",
            "the staged file was lost to a failed publication"
        );
    }

    /// An approved overwrite replaces the destination with the validated
    /// descriptor's contents, and the published file is a single-linked file of
    /// its own rather than a second name for the staged one.
    #[cfg(unix)]
    #[test]
    fn an_approved_overwrite_publishes_one_file_under_one_name() {
        use std::os::unix::fs::MetadataExt;

        let dir = tmpdir();
        let staged = dir.path().join("a.part");
        let dest = dir.path().join("a.bin");
        std::fs::write(&staged, b"new").unwrap();
        std::fs::write(&dest, b"old").unwrap();

        publish_replacing(&staged, &dest).unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"new");
        assert!(!staged.exists(), "the staged name outlived publication");
        assert_eq!(
            std::fs::metadata(&dest).unwrap().nlink(),
            1,
            "the published file is still reachable under another name"
        );
    }

    // ---------- Containment ----------

    /// The finding behind the fallback walk: a directory descriptor is a handle
    /// on an inode, not on a place in the tree, so a directory moved out of the
    /// root is still open. The pre-`openat2` walk therefore checks where the
    /// directory it ended on actually is, rather than trusting the chain of
    /// opens that reached it.
    #[cfg(unix)]
    #[test]
    fn a_directory_moved_out_of_the_root_fails_the_containment_check() {
        let root = tmpdir();
        let outside = tmpdir();

        std::fs::create_dir(root.path().join("album")).unwrap();

        let root_fd = root_dir(root.path()).unwrap();
        let album = walk_dirs(&root_fd, &[OsStr::new("album")]).unwrap();

        // Still inside: the same check passes.
        verify_contained(&root_fd, &album, 1).unwrap();

        // Moved out from under the open descriptor.
        std::fs::rename(root.path().join("album"), outside.path().join("album")).unwrap();

        assert!(
            verify_contained(&root_fd, &album, 1).is_err(),
            "a directory outside the root passed the containment check"
        );
    }

    /// `openat2` resolves the whole relative path against the root descriptor
    /// in one call, which is what removes the intermediate handle the walk
    /// above has to check after the fact. Both paths must agree about what is
    /// inside the root.
    #[cfg(unix)]
    #[test]
    fn the_walk_and_the_single_resolution_agree() {
        let root = tmpdir();
        let outside = tmpdir();

        create_dirs_beneath(root.path(), Path::new("album/disc 1")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("linked")).unwrap();

        let root_fd = root_dir(root.path()).unwrap();
        let components = [OsStr::new("album"), OsStr::new("disc 1")];

        assert!(resolve_dir(&root_fd, &components).is_ok());
        assert!(walk_dirs(&root_fd, &components).is_ok());

        let through_link = [OsStr::new("linked")];
        assert!(resolve_dir(&root_fd, &through_link).is_err());
        assert!(walk_dirs(&root_fd, &through_link).is_err());
    }

    /// The root is resolved once and reused, so every operation in a run is
    /// anchored at the same directory — including one whose pathname has since
    /// been made to mean somewhere else.
    #[cfg(unix)]
    #[test]
    fn a_pinned_root_is_resolved_once_and_kept() {
        use std::os::unix::fs::MetadataExt;

        let parent = tmpdir();
        let root = parent.path().join("Downloads");
        std::fs::create_dir(&root).unwrap();
        let original_ino = std::fs::metadata(&root).unwrap().ino();

        // The process-wide cell is set once by the first sync in a run, so the
        // mechanism is exercised through its own cell rather than that one.
        let cell = std::sync::Mutex::new(None);

        let first = pinned_dir(&cell, &root).unwrap();
        let second = pinned_dir(&cell, &root).unwrap();

        assert!(
            std::sync::Arc::ptr_eq(&first, &second),
            "the root was resolved again instead of being reused"
        );

        // The pathname now names a different directory; the pin does not.
        std::fs::rename(&root, parent.path().join("moved")).unwrap();
        std::fs::create_dir(&root).unwrap();

        let third = pinned_dir(&cell, &root).unwrap();
        assert_eq!(
            third.stat().unwrap().st_ino,
            original_ino,
            "the trust boundary moved mid-run"
        );
    }
}
