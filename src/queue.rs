//! The download queue: persistent state, cross-process locking, and the
//! runner that works through it.
//!
//! Presentation lives in [`crate::ui`]. The runner owns a [`ui::Board`] and
//! hands each worker a lane, so every in-flight file gets its own progress
//! line with a real per-file ETA, and finished items scroll above the live
//! block instead of fighting with it.
//!
//! ## Two downloaders, one runner
//!
//! An item is either an ordinary HTTP download or a MEGA link, and the two
//! need completely different machinery — MEGA has its own API handshake, key
//! schedule and quota rules. [`run_item`] picks between them and flattens both
//! into [`ItemOutcome`], so everything downstream (status writing, skip
//! detection, board logging) stays written once.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::engine::{self, DownloadRequest, ExistingPolicy, Outcome};
use crate::hoster::{gdrive, onedrive};
use crate::mega;
use crate::ui;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Status {
    Pending,
    Downloading,
    Complete,
    Failed { reason: String, attempts: u32 },
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: u64,
    pub url: String,
    pub output: Option<String>,
    pub connections: Option<usize>,
    pub status: Status,
    /// Bytes written once the item finished. `serde(default)` keeps queue.json
    /// files written by older versions loadable.
    #[serde(default)]
    pub size: Option<u64>,
}

impl Item {
    /// Does this item need the MEGA downloader rather than the engine?
    pub fn is_mega(&self) -> bool {
        mega::is_mega_url(&self.url)
    }

    /// Does this item need the OneDrive API before anything can be fetched?
    pub fn is_onedrive(&self) -> bool {
        onedrive::is_onedrive_url(&self.url)
    }

    /// Does this item need the Drive API, or the warning page, before
    /// anything can be fetched?
    pub fn is_gdrive(&self) -> bool {
        gdrive::is_gdrive_url(&self.url)
    }

    /// Human-friendly name for progress lines: the output path if we have one,
    /// otherwise the last URL segment.
    pub fn display_name(&self) -> String {
        if let Some(o) = &self.output {
            let raw = o.rsplit('/').next().unwrap_or(o);
            return engine::percent_decode(raw);
        }

        // A MEGA link's last segment is `<handle>#<key>`, which is both
        // meaningless to the user and a secret we should not be printing. The
        // real filename only arrives once the API decrypts the attributes, so
        // until then the handle alone is the honest label.
        if self.is_mega() {
            return mega::parse_link(&self.url)
                .map(|link| format!("MEGA {}", link.handle))
                .unwrap_or_else(|_| "MEGA link".to_owned());
        }

        // A share link's last segment is an opaque token, and the real name
        // exists only once the API has been asked. Naming the host is the
        // honest label until then.
        if self.is_onedrive() {
            return "OneDrive link".to_owned();
        }

        // A Drive link's last segment is `view` or an id, and neither names
        // anything.
        if self.is_gdrive() {
            return "Google Drive link".to_owned();
        }

        let raw = self
            .url
            .split('?')
            .next()
            .unwrap_or(&self.url)
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.url);
        engine::percent_decode(raw)
    }

    /// Where this item should be written, resolved against the config.
    pub fn resolve_output(&self, cfg: &Config) -> String {
        let raw_path = match &self.output {
            Some(o) => engine::percent_decode(o),
            None => {
                let raw = self
                    .url
                    .split('?')
                    .next()
                    .and_then(|p| p.rsplit('/').next())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("download.bin");
                engine::percent_decode(raw)
            }
        };
        cfg.resolve_output_path(&raw_path)
    }

    /// The `(explicit destination, fallback directory)` pair a share-link
    /// downloader takes.
    ///
    /// Unlike every other source, MEGA and OneDrive know the filename and we
    /// do not: MEGA has it encrypted in the file attributes, OneDrive has it
    /// behind an API call. So an item with no explicit output deliberately
    /// passes `None` and lets them name the file, instead of
    /// [`Self::resolve_output`] inventing one out of the link.
    pub fn share_destination(&self, cfg: &Config) -> (Option<String>, String) {
        match &self.output {
            Some(o) => (
                Some(cfg.resolve_output_path(&engine::percent_decode(o))),
                cfg.download_dir.clone(),
            ),
            None => (None, cfg.download_dir.clone()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Queue {
    next_id: u64,
    items: Vec<Item>,
}

impl Default for Queue {
    fn default() -> Self {
        Self {
            next_id: 1,
            items: Vec::new(),
        }
    }
}

// ── Paths ───────────────────────────────────────────────────────────────

fn dir() -> PathBuf {
    crate::config::config_path()
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn queue_file() -> PathBuf {
    dir().join("queue.json")
}
fn queue_lock_file() -> PathBuf {
    dir().join("queue.lock")
}
fn processor_lock_file() -> PathBuf {
    dir().join("processor.lock")
}
fn signal_file() -> PathBuf {
    dir().join("queue.signal")
}

// ── PID helpers ─────────────────────────────────────────────────────────

fn pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let ret = unsafe { libc::kill(pid as i32, 0) };
        if ret == 0 {
            return true;
        }
        let err = std::io::Error::last_os_error();
        err.raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

fn read_lock_pid(path: &PathBuf) -> Option<u32> {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

// ── File lock ──────────────────────────────────────────────────────────

pub struct FileLock {
    path: PathBuf,
}

impl FileLock {
    fn acquire(path: PathBuf, timeout_ms: u64) -> Result<Self> {
        fs::create_dir_all(dir())?;

        let max_attempts = (timeout_ms / 100).max(1);
        let mut stale_removals = 0u32;
        const MAX_STALE_REMOVALS: u32 = 3;

        for attempt in 0..max_attempts {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut f) => {
                    let _ = write!(f, "{}", std::process::id());
                    return Ok(Self { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if stale_removals < MAX_STALE_REMOVALS {
                        let is_stale = match read_lock_pid(&path) {
                            Some(pid) => !pid_alive(pid),
                            None => fs::metadata(&path)
                                .ok()
                                .and_then(|m| m.modified().ok())
                                .and_then(|t| t.elapsed().ok())
                                .map(|age| age > Duration::from_secs(86400))
                                .unwrap_or(false),
                        };

                        if is_stale {
                            let _ = fs::remove_file(&path);
                            stale_removals += 1;
                            continue;
                        }
                    }

                    if attempt < max_attempts - 1 {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
                Err(e) => {
                    return Err(e).context(format!("Failed to acquire lock: {}", path.display()));
                }
            }
        }

        anyhow::bail!(
            "Could not acquire lock {} after {}ms — another rdm instance is running (PID: {})",
            path.display(),
            timeout_ms,
            read_lock_pid(&path)
                .map(|p| p.to_string())
                .unwrap_or_else(|| "unknown".into()),
        )
    }

    fn transaction() -> Result<Self> {
        Self::acquire(queue_lock_file(), 5000)
    }

    fn processor() -> Result<Self> {
        Self::acquire(processor_lock_file(), 2000)
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

// ── Atomic write ────────────────────────────────────────────────────────

fn atomic_write(path: &PathBuf, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");

    let mut f = fs::File::create(&tmp)
        .with_context(|| format!("Failed to create temp file: {}", tmp.display()))?;

    f.write_all(data).context("Failed to write temp file")?;

    f.sync_all().context("Failed to sync temp file")?;

    fs::rename(&tmp, path)
        .with_context(|| format!("Failed to rename {} → {}", tmp.display(), path.display()))?;

    if let Some(parent) = path.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }

    Ok(())
}

// ── Queue state ─────────────────────────────────────────────────────────

/// Aggregate item counts, used by the runner summary and `queue list`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub total: usize,
    pub pending: usize,
    pub downloading: usize,
    pub complete: usize,
    pub failed: usize,
    pub skipped: usize,
    pub bytes: u64,
}

impl Queue {
    fn load_inner() -> Self {
        fs::read_to_string(queue_file())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save_inner(&self) -> Result<()> {
        fs::create_dir_all(dir()).context("Failed to create config directory")?;
        let json = serde_json::to_string_pretty(self).context("Failed to serialize queue")?;
        atomic_write(&queue_file(), json.as_bytes())
    }

    pub fn locked<F, T>(f: F) -> Result<T>
    where
        F: FnOnce(&mut Queue) -> Result<T>,
    {
        let _lock = FileLock::transaction()?;
        let mut queue = Self::load_inner();
        let result = f(&mut queue)?;
        queue.save_inner()?;
        Ok(result)
    }

    pub fn load_readonly() -> Self {
        Self::load_inner()
    }

    pub fn add(&mut self, url: String, output: Option<String>, connections: Option<usize>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.items.push(Item {
            id,
            url,
            output,
            connections,
            status: Status::Pending,
            size: None,
        });
        id
    }

    pub fn remove(&mut self, id: u64) -> bool {
        let len = self.items.len();
        self.items.retain(|i| i.id != id);
        self.items.len() < len
    }

    pub fn clear_all(&mut self) -> usize {
        let len = self.items.len();
        self.items.clear();
        self.next_id = 1;
        len
    }

    pub fn clear_finished(&mut self) -> usize {
        let len = self.items.len();
        self.items
            .retain(|i| matches!(i.status, Status::Pending | Status::Downloading));
        len - self.items.len()
    }

    pub fn clear_pending(&mut self) -> usize {
        let len = self.items.len();
        self.items.retain(|i| i.status != Status::Pending);
        len - self.items.len()
    }

    pub fn retry_failed(&mut self) -> usize {
        let mut count = 0;
        for item in &mut self.items {
            if matches!(item.status, Status::Failed { .. }) {
                item.status = Status::Pending;
                count += 1;
            }
        }
        count
    }

    pub fn retry_skipped(&mut self) -> usize {
        let mut count = 0;
        for item in &mut self.items {
            if item.status == Status::Skipped {
                item.status = Status::Pending;
                count += 1;
            }
        }
        count
    }

    pub fn retry_item(&mut self, id: u64) -> bool {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            match item.status {
                Status::Failed { .. } | Status::Skipped => {
                    item.status = Status::Pending;
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    fn next_pending(&self) -> Option<&Item> {
        self.items.iter().find(|i| i.status == Status::Pending)
    }

    fn set_status(&mut self, id: u64, status: Status) {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            item.status = status;
        }
    }

    /// Records the final status plus the byte count, so `queue list` can show
    /// what was actually downloaded.
    fn finish_item(&mut self, id: u64, status: Status, size: Option<u64>) {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            item.status = status;
            if size.is_some() {
                item.size = size;
            }
        }
    }

    fn attempts_so_far(&self, id: u64) -> u32 {
        match self.items.iter().find(|i| i.id == id) {
            Some(Item {
                status: Status::Failed { attempts, .. },
                ..
            }) => *attempts,
            _ => 0,
        }
    }

    /// Moves every `Downloading` item back to `Pending`. Used on startup (to
    /// recover from a crash) and after a Ctrl+C.
    fn requeue_in_flight(&mut self) -> usize {
        let mut count = 0;
        for item in &mut self.items {
            if item.status == Status::Downloading {
                item.status = Status::Pending;
                count += 1;
            }
        }
        count
    }

    pub fn pending_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| i.status == Status::Pending)
            .count()
    }

    /// How many pending items need the MEGA path. Used only to warn about the
    /// serialisation up front, so a stalled-looking board makes sense.
    pub fn pending_mega_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| i.status == Status::Pending && i.is_mega())
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| matches!(i.status, Status::Failed { .. }))
            .count()
    }

    pub fn stats(&self) -> Stats {
        let mut s = Stats {
            total: self.items.len(),
            ..Stats::default()
        };
        for item in &self.items {
            match item.status {
                Status::Pending => s.pending += 1,
                Status::Downloading => s.downloading += 1,
                Status::Complete => s.complete += 1,
                Status::Failed { .. } => s.failed += 1,
                Status::Skipped => s.skipped += 1,
            }
            s.bytes += item.size.unwrap_or(0);
        }
        s
    }

    pub fn print_list(&self) {
        if self.items.is_empty() {
            eprintln!("  Queue is empty.");
            return;
        }

        let width = ui::term_width();
        // Name and size columns are fixed; the URL gets whatever is left.
        let name_col = 34usize;
        let size_col = 10usize;
        let url_col = width.saturating_sub(name_col + size_col + 22).max(20);

        eprintln!();
        eprintln!(
            "  {:>4}  {:<14}  {:<name_col$}  {:>size_col$}  URL",
            "ID",
            "Status",
            "File",
            "Size",
            name_col = name_col,
            size_col = size_col,
        );
        eprintln!("  {}", "\u{2500}".repeat(width.saturating_sub(4).min(160)));

        for item in &self.items {
            let status = match &item.status {
                Status::Pending => "\u{23f3} pending",
                Status::Downloading => "\u{2b07} downloading",
                Status::Complete => "\u{2705} complete",
                Status::Failed { .. } => "\u{274c} failed",
                Status::Skipped => "\u{23ed} skipped",
            };

            let size = match item.size {
                Some(b) if b > 0 => ui::format_size(b),
                _ => "\u{2014}".to_owned(),
            };

            eprintln!(
                "  {:>4}  {:<14}  {:<name_col$}  {:>size_col$}  {}",
                item.id,
                status,
                pad_display(&ui::ellipsize(&item.display_name(), name_col), name_col),
                size,
                ui::ellipsize(&item.url, url_col),
                name_col = name_col,
                size_col = size_col,
            );

            if let Status::Failed {
                ref reason,
                attempts,
            } = item.status
            {
                eprintln!(
                    "        \u{21b3} error after {} attempt{}: {}",
                    attempts,
                    if attempts == 1 { "" } else { "s" },
                    ui::ellipsize(reason, width.saturating_sub(34)),
                );
            }
        }

        let s = self.stats();
        eprintln!();
        eprintln!(
            "  {} total \u{b7} {} pending \u{b7} {} complete \u{b7} {} failed \u{b7} {} skipped",
            s.total, s.pending, s.complete, s.failed, s.skipped
        );
        if s.bytes > 0 {
            eprintln!("  {} downloaded", ui::format_size(s.bytes));
        }
        if s.failed > 0 {
            eprintln!("  Run `rdm queue retry failed` to requeue the failures.");
        }
    }
}

/// `{:<width$}` pads by bytes; names are UTF-8, so pad by characters instead.
fn pad_display(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len >= width {
        s.to_owned()
    } else {
        format!("{}{}", s, " ".repeat(width - len))
    }
}

// ── Signals ─────────────────────────────────────────────────────────────

pub fn send_signal(sig: &str) -> Result<()> {
    fs::create_dir_all(dir())?;
    atomic_write(&signal_file(), sig.as_bytes())
}

fn read_signal() -> Option<String> {
    fs::read_to_string(signal_file())
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn clear_signal() {
    let _ = fs::remove_file(signal_file());
}

// ── Per-item dispatch ───────────────────────────────────────────────────

/// What happened to one item, with the differences between the two
/// downloaders already flattened out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemOutcome {
    Completed { bytes: u64 },
    AlreadyPresent,
    Cancelled,
}

/// Runs one queue item on whichever downloader it needs.
///
/// `mega_gate` serialises MEGA items. MEGA's bandwidth limit is enforced per
/// IP, not per file, so running three of them at once does not go three times
/// faster — all three hit the same 509 wall and each sits out its own backoff,
/// while the file already has `mega_workers` parallel connections inside it.
async fn run_item(
    cfg: &Config,
    item: &Item,
    cancel: CancellationToken,
    sink: Arc<dyn ui::ProgressSink>,
    mega_client: reqwest::Client,
    mega_gate: Arc<tokio::sync::Semaphore>,
) -> Result<ItemOutcome> {
    if item.is_mega() {
        let _permit = mega_gate.acquire_owned().await;

        if cancel.is_cancelled() {
            return Ok(ItemOutcome::Cancelled);
        }

        let (output, download_dir) = item.share_destination(cfg);
        let options = mega::MegaOptions {
            workers: item.connections.unwrap_or(cfg.mega_workers),
            verify_mac: cfg.mega_verify_mac,
            resume_on_ip_change: cfg.mega_resume_on_ip_change,
            max_retries: cfg.max_retries,
            // The queue never stops to ask, and an existing file of the right
            // size is treated as already downloaded.
            overwrite: false,
        };

        let outcome = mega::download(
            mega_client,
            &item.url,
            output,
            &download_dir,
            options,
            cancel,
            sink,
        )
        .await?;

        return Ok(match outcome {
            mega::MegaOutcome::Completed { bytes, .. } => ItemOutcome::Completed { bytes },
            mega::MegaOutcome::AlreadyPresent { .. } => ItemOutcome::AlreadyPresent,
            mega::MegaOutcome::Cancelled { .. } => ItemOutcome::Cancelled,
        });
    }

    if item.is_onedrive() {
        return run_onedrive_item(cfg, item, cancel, sink).await;
    }

    if item.is_gdrive() {
        return run_gdrive_item(cfg, item, cancel, sink).await;
    }

    let request = DownloadRequest::new(
        item.url.clone(),
        Some(item.resolve_output(cfg)),
        item.connections.unwrap_or(cfg.connections),
    )
    // Never stop a batch run to ask about an existing file.
    .with_policy(ExistingPolicy::Reuse);

    Ok(match engine::download(request, cancel, sink).await? {
        Outcome::Completed { bytes, .. } => ItemOutcome::Completed { bytes },
        Outcome::AlreadyPresent { .. } => ItemOutcome::AlreadyPresent,
        Outcome::Cancelled => ItemOutcome::Cancelled,
    })
}

/// Runs one OneDrive item.
///
/// A share link is not a fetchable address, so the API is asked what it points
/// at first, and the answer decides the shape of the work. A file goes to the
/// engine like any other download. A folder is a whole tree behind one item,
/// which the queue cannot represent as separate rows, so it is fetched under
/// this item and reported into this item's one progress line.
///
/// The link is what gets stored, never the resolved URL: that comes back signed
/// and expires within the hour, so an item that sat in the queue overnight has
/// to ask again.
async fn run_onedrive_item(
    cfg: &Config,
    item: &Item,
    cancel: CancellationToken,
    sink: Arc<dyn ui::ProgressSink>,
) -> Result<ItemOutcome> {
    let options = onedrive::OneDriveOptions {
        // On a folder `-c` means files at once, the way it means workers for
        // MEGA. On a single file it stays chunks — see below.
        workers: item.connections.unwrap_or(cfg.onedrive_workers),
        max_retries: cfg.max_retries,
        // The queue never re-downloads what is already there.
        overwrite: false,
    };

    let (output, download_dir) = item.share_destination(cfg);

    let folder = match onedrive::resolve(reqwest::Client::new(), &item.url, &options).await? {
        onedrive::Resolved::File(link) => {
            let destination = output.unwrap_or_else(|| cfg.resolve_output_path(&link.name));
            if let Some(parent) = std::path::Path::new(&destination).parent() {
                std::fs::create_dir_all(parent).ok();
            }

            let request = DownloadRequest::new(
                link.url,
                Some(destination),
                item.connections.unwrap_or(cfg.connections),
            )
            .with_policy(ExistingPolicy::Reuse)
            // A fresh signature every run, so the URL cannot be the thing
            // resume recognises the file by.
            .with_resume_identity(format!("onedrive:{}", link.id));

            return Ok(match engine::download(request, cancel, sink).await? {
                Outcome::Completed { bytes, .. } => ItemOutcome::Completed { bytes },
                Outcome::AlreadyPresent { .. } => ItemOutcome::AlreadyPresent,
                Outcome::Cancelled => ItemOutcome::Cancelled,
            });
        }
        onedrive::Resolved::Folder(folder) => folder,
    };

    let summary = onedrive::download_folder(
        folder,
        output,
        &download_dir,
        options,
        cancel,
        onedrive::Progress::Lane(sink),
    )
    .await?;

    if summary.cancelled {
        return Ok(ItemOutcome::Cancelled);
    }

    // One failed file must not read as a finished folder: leaving the item
    // failed is what lets `queue retry failed` finish the job, and the files
    // already on disk are skipped when it does.
    if !summary.failed.is_empty() {
        let (path, reason) = &summary.failed[0];
        anyhow::bail!(
            "{} of {} file(s) failed, starting with {}: {}",
            summary.failed.len(),
            summary.total,
            path,
            reason
        );
    }

    if summary.completed == 0 && summary.skipped > 0 {
        return Ok(ItemOutcome::AlreadyPresent);
    }

    Ok(ItemOutcome::Completed {
        bytes: summary.bytes,
    })
}

/// Runs one Google Drive item.
///
/// Same shape as the OneDrive path and for the same reason: a link is not a
/// fetchable address, so what it points at decides the shape of the work. A
/// file goes to the engine; a folder is a whole tree behind one item, fetched
/// under this item and reported into this item's one progress line.
///
/// The link is what gets stored, never the resolved URL: a confirmed download
/// URL carries a short-lived `at` token, so an item that sat in the queue
/// overnight has to ask again.
async fn run_gdrive_item(
    cfg: &Config,
    item: &Item,
    cancel: CancellationToken,
    sink: Arc<dyn ui::ProgressSink>,
) -> Result<ItemOutcome> {
    let options = gdrive::GdriveOptions {
        // On a folder `-c` means files at once. On a single file it stays
        // chunks — see below.
        workers: item.connections.unwrap_or(cfg.gdrive_workers),
        max_retries: cfg.max_retries,
        api_key: cfg.gdrive_key(),
        doc_format: cfg.gdrive_doc_format.clone(),
        // The queue never re-downloads what is already there.
        overwrite: false,
    };

    let (output, download_dir) = item.share_destination(cfg);

    let folder = match gdrive::resolve(reqwest::Client::new(), &item.url, &options).await? {
        gdrive::Resolved::File(link) => {
            let destination = output.unwrap_or_else(|| cfg.resolve_output_path(&link.name));
            if let Some(parent) = std::path::Path::new(&destination).parent() {
                std::fs::create_dir_all(parent).ok();
            }

            let request = DownloadRequest::new(
                link.url,
                Some(destination),
                item.connections.unwrap_or(cfg.connections),
            )
            .with_policy(ExistingPolicy::Reuse)
            // A fresh token every run, so the URL cannot be the thing resume
            // recognises the file by.
            .with_resume_identity(format!("gdrive:{}", link.id));

            return Ok(match engine::download(request, cancel, sink).await? {
                Outcome::Completed { bytes, .. } => ItemOutcome::Completed { bytes },
                Outcome::AlreadyPresent { .. } => ItemOutcome::AlreadyPresent,
                Outcome::Cancelled => ItemOutcome::Cancelled,
            });
        }
        gdrive::Resolved::Folder(folder) => folder,
    };

    let listing = gdrive::list_folder(&folder, &options).await?;
    let root = gdrive::destination_root(output, &download_dir, folder.name());

    gdrive::create_tree(&root, &listing.dirs).await?;
    let summary = gdrive::download_files(
        &listing.files,
        &root,
        &options,
        cancel,
        gdrive::Progress::Lane(sink),
    )
    .await?;

    if summary.cancelled {
        return Ok(ItemOutcome::Cancelled);
    }

    // One failed file must not read as a finished folder: leaving the item
    // failed is what lets `queue retry failed` finish the job, and the files
    // already on disk are skipped when it does.
    if !summary.failed.is_empty() {
        let (path, reason) = &summary.failed[0];
        anyhow::bail!(
            "{} of {} file(s) failed, starting with {}: {}",
            summary.failed.len(),
            summary.total,
            path,
            reason
        );
    }

    if summary.completed == 0 && summary.skipped > 0 {
        return Ok(ItemOutcome::AlreadyPresent);
    }

    Ok(ItemOutcome::Completed {
        bytes: summary.bytes,
    })
}

// ── Queue processor ─────────────────────────────────────────────────────

/// Works through every pending item, `parallel` files at a time.
///
/// This is the single download path for `rdm queue start`, `rdm sync`, and
/// `rdm <directory-url>`, so all three get the same progress display.
pub async fn start(cfg: &Config, cancel: CancellationToken, parallel: usize) -> Result<()> {
    let _processor_lock =
        FileLock::processor().context("Another `rdm queue start` is already running")?;

    clear_signal();

    // Reset stale Downloading items from a previous crash.
    Queue::locked(|q| {
        q.requeue_in_flight();
        Ok(())
    })?;

    let snapshot = Queue::load_readonly();
    let pending = snapshot.pending_count();
    let pending_mega = snapshot.pending_mega_count();
    if pending == 0 {
        eprintln!("  Queue is empty — nothing to do.");
        return Ok(());
    }

    let parallel = parallel.max(1).min(pending);

    let board = ui::Board::new("Queue", pending, parallel);
    board.log(&format!(
        "  \u{1f680} {} file(s) queued \u{b7} {} at a time",
        pending, parallel
    ));
    if pending_mega > 1 {
        board.log(&format!(
            "  \u{2139} {} MEGA link(s) \u{b7} run one at a time \u{2014} the quota is per-IP, not per-file",
            pending_mega
        ));
    }
    let renderer = board.spawn_renderer();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallel));
    // Separate from `semaphore`: this one is about MEGA's quota, not about how
    // many files the user wants in flight.
    let mega_gate = Arc::new(tokio::sync::Semaphore::new(1));
    let mega_client = reqwest::Client::new();
    let completed = Arc::new(AtomicU32::new(0));
    let failed = Arc::new(AtomicU32::new(0));
    let skipped = Arc::new(AtomicU32::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let active_children: Arc<Mutex<Vec<(u64, CancellationToken)>>> =
        Arc::new(Mutex::new(Vec::new()));

    // Signal watcher — polls for skip/stop sent from another terminal.
    let watcher_cancel = cancel.clone();
    let watcher_children = Arc::clone(&active_children);
    let watcher_stop = Arc::clone(&stop_flag);
    let watcher_board = board.clone();
    let watcher = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if watcher_cancel.is_cancelled() {
                break;
            }

            match read_signal() {
                Some(ref s) if s == "skip" => {
                    clear_signal();
                    let children = watcher_children.lock().await;
                    for (id, token) in children.iter() {
                        watcher_board.log(&format!("  \u{23ed} Skipping #{}", id));
                        token.cancel();
                    }
                }
                Some(ref s) if s == "stop" => {
                    clear_signal();
                    watcher_stop.store(true, Ordering::SeqCst);
                    watcher_board
                        .log("  \u{23f9} Stop signal — finishing active downloads\u{2026}");
                }
                _ => {}
            }
        }
    });

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        if cancel.is_cancelled() || stop_flag.load(Ordering::SeqCst) {
            break;
        }

        handles.retain(|h| !h.is_finished());

        let permit = tokio::select! {
            p = semaphore.clone().acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => break,
            },
            _ = cancel.cancelled() => break,
        };

        if cancel.is_cancelled() || stop_flag.load(Ordering::SeqCst) {
            drop(permit);
            break;
        }

        let next = Queue::locked(|q| match q.next_pending().cloned() {
            Some(item) => {
                q.set_status(item.id, Status::Downloading);
                Ok(Some(item))
            }
            None => Ok(None),
        })?;

        let next = match next {
            Some(item) => item,
            None => {
                drop(permit);
                if handles.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        };

        let child = cancel.child_token();
        {
            let mut children = active_children.lock().await;
            children.push((next.id, child.clone()));
        }

        let cfg = cfg.clone();
        let board = board.clone();
        let completed = Arc::clone(&completed);
        let failed = Arc::clone(&failed);
        let skipped = Arc::clone(&skipped);
        let bytes_total = Arc::clone(&bytes_total);
        let active_children = Arc::clone(&active_children);
        let cancel_main = cancel.clone();
        let mega_gate = Arc::clone(&mega_gate);
        let mega_client = mega_client.clone();

        let handle = tokio::spawn(async move {
            let _permit = permit; // held until the task completes
            let item_id = next.id;
            let name = next.display_name();

            // MEGA creates its own parent directories once it knows the real
            // filename; for engine downloads the path is known up front.
            if !next.is_mega() {
                let output = next.resolve_output(&cfg);
                if let Some(parent) = std::path::Path::new(&output).parent()
                    && !parent.exists()
                {
                    std::fs::create_dir_all(parent).ok();
                }
            }

            // A lane is a live progress line on the board. If the board is
            // somehow full we still download, just without a line.
            let lane = board.claim(item_id, &name);
            let sink = match &lane {
                Some(l) => l.sink(),
                None => ui::silent(),
            };

            let elapsed_before = lane.as_ref().map(|l| l.elapsed());
            let result = run_item(
                &cfg,
                &next,
                child.clone(),
                sink,
                mega_client,
                mega_gate,
            )
            .await;
            let elapsed = elapsed_before
                .map(|_| lane.as_ref().map(|l| l.elapsed()).unwrap_or_default())
                .unwrap_or_default();

            // Free the progress line before we print the outcome.
            drop(lane);

            {
                let mut children = active_children.lock().await;
                children.retain(|(id, _)| *id != item_id);
            }

            // A cancelled child token with the main token still alive means
            // the user asked to skip this one file.
            let was_skipped = child.is_cancelled() && !cancel_main.is_cancelled();

            let downloaded = match &result {
                Ok(ItemOutcome::Completed { bytes }) => Some(*bytes),
                _ => None,
            };

            // Always write the final status — even during Ctrl+C.
            let _ = Queue::locked(|q| {
                if cancel_main.is_cancelled() {
                    match &result {
                        Ok(ItemOutcome::Completed { .. }) | Ok(ItemOutcome::AlreadyPresent) => {
                            q.finish_item(item_id, Status::Complete, downloaded)
                        }
                        _ => q.set_status(item_id, Status::Pending),
                    }
                } else if was_skipped {
                    q.set_status(item_id, Status::Skipped);
                } else {
                    match &result {
                        Ok(ItemOutcome::Completed { .. }) | Ok(ItemOutcome::AlreadyPresent) => {
                            q.finish_item(item_id, Status::Complete, downloaded);
                        }
                        Ok(ItemOutcome::Cancelled) => q.set_status(item_id, Status::Skipped),
                        Err(e) => {
                            let attempts = q.attempts_so_far(item_id) + 1;
                            q.set_status(
                                item_id,
                                Status::Failed {
                                    reason: format!("{:#}", e),
                                    attempts,
                                },
                            );
                        }
                    }
                }
                Ok(())
            });

            if cancel_main.is_cancelled() {
                return;
            }

            if was_skipped {
                skipped.fetch_add(1, Ordering::Relaxed);
                board.file_skipped();
                board.log(&format!("  \u{23ed} #{}  {} — skipped", item_id, name));
                return;
            }

            match result {
                Ok(ItemOutcome::Completed { bytes }) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                    bytes_total.fetch_add(bytes, Ordering::Relaxed);
                    board.file_completed(bytes);

                    let secs = elapsed.as_secs_f64();
                    let avg = if secs > 0.1 {
                        Some((bytes as f64 / secs) as u64)
                    } else {
                        None
                    };
                    board.log(&format!(
                        "  \u{2705} #{}  {}  {} in {} ({})",
                        item_id,
                        name,
                        ui::format_size(bytes),
                        ui::format_duration(elapsed.as_secs()),
                        ui::format_speed(avg),
                    ));
                }
                Ok(ItemOutcome::AlreadyPresent) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                    board.file_completed(0);
                    board.log(&format!(
                        "  \u{2713} #{}  {} — already downloaded",
                        item_id, name
                    ));
                }
                Ok(ItemOutcome::Cancelled) => {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    board.file_skipped();
                    board.log(&format!("  \u{23ed} #{}  {} — skipped", item_id, name));
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    board.file_failed();
                    board.log(&format!("  \u{274c} #{}  {} — {:#}", item_id, name, e));
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    watcher.abort();
    board.finish();
    let _ = renderer.await;

    // Ctrl+C — catch any truly orphaned tasks.
    if cancel.is_cancelled() {
        let _ = Queue::locked(|q| {
            q.requeue_in_flight();
            Ok(())
        });
        eprintln!();
        eprintln!("  \u{26a0} Queue interrupted — progress saved. Run `rdm queue start` to resume.");
        return Ok(());
    }

    print_summary(
        completed.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed),
        bytes_total.load(Ordering::Relaxed),
        board.elapsed(),
    );

    Ok(())
}

fn print_summary(completed: u32, failed: u32, skipped: u32, bytes: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let avg = if secs > 0.1 && bytes > 0 {
        Some((bytes as f64 / secs) as u64)
    } else {
        None
    };

    let mut parts = vec![format!("{} completed", completed)];
    if failed > 0 {
        parts.push(format!("{} failed", failed));
    }
    if skipped > 0 {
        parts.push(format!("{} skipped", skipped));
    }

    eprintln!();
    eprintln!(
        "  Done in {} \u{b7} {}",
        ui::format_duration(elapsed.as_secs()),
        parts.join(" \u{b7} "),
    );
    if bytes > 0 {
        eprintln!(
            "  {} downloaded at {}",
            ui::format_size(bytes),
            ui::format_speed(avg)
        );
    }
    if failed > 0 {
        eprintln!("  Run `rdm queue retry failed` to requeue the failures.");
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const MEGA_LINK: &str = "https://mega.nz/file/AbCdEfGh#thekey";

    fn queue_with(urls: &[&str]) -> Queue {
        let mut q = Queue::default();
        for url in urls {
            q.add((*url).to_owned(), None, None);
        }
        q
    }

    #[test]
    fn ids_are_stable_and_increasing() {
        let mut q = Queue::default();
        let a = q.add("https://x.com/a.bin".into(), None, None);
        let b = q.add("https://x.com/b.bin".into(), None, None);
        assert_eq!((a, b), (1, 2));
        assert!(q.remove(a));
        assert!(!q.remove(a));
        assert_eq!(q.add("https://x.com/c.bin".into(), None, None), 3);
    }

    #[test]
    fn display_name_prefers_output_and_decodes() {
        let mut q = Queue::default();
        q.add(
            "https://x.com/dir/my%20song.flac".into(),
            Some("album/my%20song.flac".into()),
            None,
        );
        assert_eq!(q.items[0].display_name(), "my song.flac");

        let mut q = queue_with(&["https://x.com/dir/track%2001.flac?token=1"]);
        assert_eq!(q.items[0].display_name(), "track 01.flac");
        assert!(q.remove(1));
    }

    // ── MEGA items ──

    #[test]
    fn mega_items_are_recognised() {
        let q = queue_with(&[MEGA_LINK, "https://x.com/a.bin"]);
        assert!(q.items[0].is_mega());
        assert!(!q.items[1].is_mega());
        assert_eq!(q.pending_mega_count(), 1);
    }

    /// The link's last segment is `<handle>#<key>` — printing that on the
    /// progress line would leak the decryption key into logs and scrollback.
    #[test]
    fn mega_display_name_never_shows_the_key() {
        let q = queue_with(&[MEGA_LINK]);
        let name = q.items[0].display_name();
        assert_eq!(name, "MEGA AbCdEfGh");
        assert!(!name.contains("thekey"));

        // An explicit output still wins — that is a real filename.
        let mut q = Queue::default();
        q.add(MEGA_LINK.into(), Some("movies/holiday.mkv".into()), None);
        assert_eq!(q.items[0].display_name(), "holiday.mkv");
    }

    /// With no explicit output, MEGA must be left to name the file: only it
    /// can decrypt the real filename out of the attributes.
    #[test]
    fn mega_destination_defers_naming_when_it_can() {
        let cfg = Config::default();

        let q = queue_with(&[MEGA_LINK]);
        let (output, dir) = q.items[0].share_destination(&cfg);
        assert_eq!(output, None);
        assert_eq!(dir, cfg.download_dir);

        // resolve_output would have invented this nonsense from the link.
        assert!(q.items[0].resolve_output(&cfg).contains("AbCdEfGh"));

        let mut q = Queue::default();
        q.add(MEGA_LINK.into(), Some("movies/my%20film.mkv".into()), None);
        let (output, _) = q.items[0].share_destination(&cfg);
        let output = output.expect("an explicit output must be honoured");
        assert!(output.ends_with("movies/my film.mkv"), "{output}");
    }

    const ONEDRIVE_LINK: &str = "https://1drv.ms/f/c/abc123/AbCdEfGh";

    #[test]
    fn onedrive_items_are_recognised() {
        let q = queue_with(&[ONEDRIVE_LINK, "https://x.com/a.bin"]);
        assert!(q.items[0].is_onedrive());
        assert!(!q.items[1].is_onedrive());
        assert!(!q.items[0].is_mega(), "the two dispatch paths must not overlap");
    }

    /// The last segment of a share link is an opaque token, so it names
    /// nothing. Printing it would put a meaningless string on the board and
    /// leave it in `queue list` afterwards.
    #[test]
    fn onedrive_display_name_never_shows_the_share_token() {
        let q = queue_with(&[ONEDRIVE_LINK]);
        assert_eq!(q.items[0].display_name(), "OneDrive link");
        assert!(!q.items[0].display_name().contains("AbCdEfGh"));

        // An explicit output still wins — that is a real filename.
        let mut q = Queue::default();
        q.add(ONEDRIVE_LINK.into(), Some("share/holiday.mkv".into()), None);
        assert_eq!(q.items[0].display_name(), "holiday.mkv");
    }

    /// Same hazard as MEGA: resolve_output would carve a filename out of the
    /// share token, so naming has to wait for the API.
    #[test]
    fn onedrive_naming_waits_for_the_api_too() {
        let cfg = Config::default();
        let q = queue_with(&[ONEDRIVE_LINK]);

        let (output, dir) = q.items[0].share_destination(&cfg);
        assert_eq!(output, None);
        assert_eq!(dir, cfg.download_dir);
        assert!(q.items[0].resolve_output(&cfg).contains("AbCdEfGh"));
    }

    #[test]
    fn retry_only_touches_finished_failures() {
        let mut q = queue_with(&["https://x.com/a", "https://x.com/b", "https://x.com/c"]);
        q.set_status(1, Status::Failed { reason: "404".into(), attempts: 2 });
        q.set_status(2, Status::Skipped);
        q.set_status(3, Status::Complete);

        assert!(!q.retry_item(3), "completed items are not retryable");
        assert_eq!(q.retry_failed(), 1);
        assert_eq!(q.retry_skipped(), 1);
        assert_eq!(q.pending_count(), 2);
    }

    #[test]
    fn failure_attempts_accumulate() {
        let mut q = queue_with(&["https://x.com/a"]);
        assert_eq!(q.attempts_so_far(1), 0);
        q.set_status(1, Status::Failed { reason: "boom".into(), attempts: 1 });
        assert_eq!(q.attempts_so_far(1), 1);
        let attempts = q.attempts_so_far(1) + 1;
        q.set_status(1, Status::Failed { reason: "boom".into(), attempts });
        assert_eq!(q.attempts_so_far(1), 2);
    }

    #[test]
    fn finish_item_records_size() {
        let mut q = queue_with(&["https://x.com/a"]);
        q.finish_item(1, Status::Complete, Some(4096));
        assert_eq!(q.items[0].size, Some(4096));
        assert_eq!(q.stats().bytes, 4096);

        // A later status change must not wipe a known size.
        q.finish_item(1, Status::Complete, None);
        assert_eq!(q.items[0].size, Some(4096));
    }

    #[test]
    fn interrupted_downloads_are_requeued() {
        let mut q = queue_with(&["https://x.com/a", "https://x.com/b"]);
        q.set_status(1, Status::Downloading);
        q.set_status(2, Status::Complete);
        assert_eq!(q.requeue_in_flight(), 1);
        assert_eq!(q.pending_count(), 1);
    }

    #[test]
    fn clear_variants_target_the_right_items() {
        let mut q = queue_with(&["https://x.com/a", "https://x.com/b", "https://x.com/c"]);
        q.set_status(2, Status::Complete);
        q.set_status(3, Status::Failed { reason: "x".into(), attempts: 1 });

        assert_eq!(q.clear_finished(), 2);
        assert_eq!(q.stats().total, 1);
        assert_eq!(q.clear_pending(), 1);
        assert_eq!(q.stats().total, 0);
    }

    #[test]
    fn stats_count_every_state() {
        let mut q = queue_with(&["a", "b", "c", "d", "e"]);
        q.set_status(1, Status::Downloading);
        q.set_status(2, Status::Complete);
        q.set_status(3, Status::Failed { reason: "x".into(), attempts: 1 });
        q.set_status(4, Status::Skipped);

        let s = q.stats();
        assert_eq!(
            (s.total, s.pending, s.downloading, s.complete, s.failed, s.skipped),
            (5, 1, 1, 1, 1, 1)
        );
    }

    #[test]
    fn old_queue_files_without_size_still_load() {
        let json = r#"{"next_id":2,"items":[{"id":1,"url":"https://x.com/a.bin","output":null,"connections":null,"status":"Pending"}]}"#;
        let q: Queue = serde_json::from_str(json).expect("legacy queue.json must still parse");
        assert_eq!(q.pending_count(), 1);
        assert_eq!(q.items[0].size, None);
    }
}
