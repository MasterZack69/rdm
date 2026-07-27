//! Terminal presentation layer.
//!
//! Everything rdm draws while work is in flight goes through this module so
//! that concurrent downloads can't scribble over each other:
//!
//!   - [`Board`]  — a live, multi-line display: one line per in-flight file
//!                  plus an aggregate header. Used by the queue (and therefore
//!                  by `sync` and directory downloads).
//!   - [`SoloBar`] — a single refreshing line for one-file downloads.
//!   - [`CountProgress`] — a counter + ETA line for non-byte work (hashing,
//!                  verification, deletes).
//!   - [`ScanSpinner`] — a live counter for directory scraping.
//!
//! All of them write to **stderr**, degrade to plain, scroll-safe output when
//! stderr is not a TTY, and share the formatters at the bottom of the file.

use std::collections::VecDeque;
use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

/// Speed is averaged over this window so the number stays readable instead of
/// jittering with every TCP burst.
const RATE_WINDOW: Duration = Duration::from_secs(3);
/// Redraw interval when we own a terminal.
const TTY_TICK: Duration = Duration::from_millis(120);
/// How often a non-TTY run prints a plain status line.
const PLAIN_TICK: Duration = Duration::from_secs(5);
/// Minimum gap between [`SoloBar`] repaints.
const SOLO_TICK: Duration = Duration::from_millis(100);

// ── Terminal basics ─────────────────────────────────────────────────────────

/// Is stderr an interactive terminal? Decides ANSI vs. plain output.
pub fn is_tty() -> bool {
    std::io::stderr().is_terminal()
}

/// Usable terminal width, clamped to something sane for layout math.
pub fn term_width() -> usize {
    #[cfg(unix)]
    unsafe {
        let mut ws: libc::winsize = std::mem::zeroed();
        if libc::ioctl(libc::STDERR_FILENO, libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 20 {
            return (ws.ws_col as usize).min(200);
        }
    }

    std::env::var("COLUMNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|w| *w > 20)
        .map(|w| w.min(200))
        .unwrap_or(100)
}

fn emit(s: &str) {
    let mut err = std::io::stderr();
    let _ = err.write_all(s.as_bytes());
    let _ = err.flush();
}

/// A poisoned progress mutex should never take the download down with it.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Erases the current line (single-line widgets only).
pub fn clear_line() {
    if is_tty() {
        emit("\r\x1b[2K");
    }
}

// ── Rate estimation ─────────────────────────────────────────────────────────

/// Sliding-window byte-rate estimator.
///
/// Feeding it a monotonically increasing total (rather than deltas) means a
/// caller can sample whenever it likes without losing accuracy.
#[derive(Default)]
pub struct Rate {
    samples: VecDeque<(Instant, u64)>,
}

impl Rate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, total: u64) {
        self.record_at(Instant::now(), total);
    }

    fn record_at(&mut self, now: Instant, total: u64) {
        self.samples.push_back((now, total));
        while self.samples.len() > 2
            && now.duration_since(self.samples[0].0) > RATE_WINDOW
        {
            self.samples.pop_front();
        }
    }

    /// Bytes per second across the window, or `None` until there is enough
    /// history to say anything honest.
    pub fn per_second(&self) -> Option<u64> {
        let first = self.samples.front()?;
        let last = self.samples.back()?;
        let secs = last.0.duration_since(first.0).as_secs_f64();
        if secs < 0.2 {
            return None;
        }
        let bytes = last.1.saturating_sub(first.1) as f64;
        Some((bytes / secs) as u64)
    }
}

// ── Progress sinks ──────────────────────────────────────────────────────────

/// What a worker is currently doing. Drives the glyph on its progress line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Waiting,
    Inspecting,
    Downloading,
    Finishing,
}

impl SlotState {
    pub fn glyph(self) -> &'static str {
        match self {
            SlotState::Waiting => "\u{25cb}",
            SlotState::Inspecting => "\u{1f50e}",
            SlotState::Downloading => "\u{2b07}",
            SlotState::Finishing => "\u{1f4be}",
        }
    }
}

/// The download engine reports progress through this trait instead of printing
/// directly, so the same engine drives a solo bar, a queue lane, or nothing.
pub trait ProgressSink: Send + Sync {
    /// Total size, once known. `None` means the server never told us.
    fn total(&self, _bytes: Option<u64>) {}
    /// Absolute number of bytes on disk so far.
    fn progress(&self, _downloaded: u64) {}
    /// Phase change.
    fn state(&self, _state: SlotState) {}
    /// Chatty per-download detail ("File size: …"). Hidden in multi-file runs.
    fn detail(&self, _msg: &str) {}
    /// Something the user must see even mid-queue (retries, server hiccups).
    fn note(&self, _msg: &str) {}
    /// Tear down any live rendering owned by this sink.
    fn finish(&self) {}
}

/// Prints nothing. Used by `--quiet`.
pub struct Silent;

impl ProgressSink for Silent {}

pub fn silent() -> Arc<dyn ProgressSink> {
    Arc::new(Silent)
}

// ── Solo download bar ───────────────────────────────────────────────────────

/// One refreshing line for a single download.
pub struct SoloBar {
    tty: bool,
    name: String,
    started: Instant,
    total: AtomicU64,
    done: AtomicU64,
    dirty: AtomicBool,
    state: Mutex<SlotState>,
    rate: Mutex<Rate>,
    last_draw: Mutex<Instant>,
}

impl SoloBar {
    pub fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            tty: is_tty(),
            name: name.to_string(),
            started: Instant::now(),
            total: AtomicU64::new(0),
            done: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            state: Mutex::new(SlotState::Waiting),
            rate: Mutex::new(Rate::new()),
            last_draw: Mutex::new(Instant::now() - SOLO_TICK),
        })
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn wipe(&self) {
        if self.tty && self.dirty.swap(false, Ordering::Relaxed) {
            emit("\r\x1b[2K");
        }
    }

    fn draw(&self, force: bool) {
        if !self.tty {
            return;
        }

        {
            let mut last = lock(&self.last_draw);
            if !force && last.elapsed() < SOLO_TICK {
                return;
            }
            *last = Instant::now();
        }

        let done = self.done.load(Ordering::Relaxed);
        let total = self.total.load(Ordering::Relaxed);
        let speed = lock(&self.rate).per_second();
        let state = *lock(&self.state);

        let width = term_width();
        let right = progress_tail(done, total, speed, self.started);
        let name_room = width
            .saturating_sub(display_width(&right) + 6)
            .clamp(8, 60);

        let line = format!(
            "  {} {}  {}",
            state.glyph(),
            pad(&ellipsize(&self.name, name_room), name_room),
            right
        );

        self.dirty.store(true, Ordering::Relaxed);
        emit(&format!("\r\x1b[2K{}", ellipsize(&line, width)));
    }
}

impl ProgressSink for SoloBar {
    fn total(&self, bytes: Option<u64>) {
        self.total.store(bytes.unwrap_or(0), Ordering::Relaxed);
    }

    fn progress(&self, downloaded: u64) {
        self.done.store(downloaded, Ordering::Relaxed);
        lock(&self.rate).record(downloaded);
        self.draw(false);
    }

    fn state(&self, state: SlotState) {
        *lock(&self.state) = state;
        self.draw(true);
    }

    fn detail(&self, msg: &str) {
        self.wipe();
        eprintln!("  {}", msg);
    }

    fn note(&self, msg: &str) {
        self.wipe();
        eprintln!("  {}", msg);
    }

    fn finish(&self) {
        self.wipe();
    }
}

// ── Multi-file board ────────────────────────────────────────────────────────

struct Slot {
    id: u64,
    name: String,
    board: Weak<BoardInner>,
    started: Instant,
    total: AtomicU64,
    done: AtomicU64,
    state: Mutex<SlotState>,
    rate: Mutex<Rate>,
}

impl ProgressSink for Slot {
    fn total(&self, bytes: Option<u64>) {
        self.total.store(bytes.unwrap_or(0), Ordering::Relaxed);
    }

    fn progress(&self, downloaded: u64) {
        self.done.store(downloaded, Ordering::Relaxed);
        lock(&self.rate).record(downloaded);
    }

    fn state(&self, state: SlotState) {
        *lock(&self.state) = state;
    }

    // Per-file chatter is dropped: with N files in flight it is pure noise.
    fn detail(&self, _msg: &str) {}

    fn note(&self, msg: &str) {
        if let Some(board) = self.board.upgrade() {
            board.log(&format!("     #{} {}", self.id, msg));
        }
    }
}

struct BoardInner {
    title: String,
    tty: bool,
    started: Instant,
    lanes: Mutex<Vec<Option<Arc<Slot>>>>,
    drawn: Mutex<usize>,
    total_files: AtomicUsize,
    completed: AtomicUsize,
    failed: AtomicUsize,
    skipped: AtomicUsize,
    finished_bytes: AtomicU64,
    sized_files: AtomicUsize,
    rate: Mutex<Rate>,
    stopped: AtomicBool,
}

impl BoardInner {
    fn active(&self) -> Vec<Arc<Slot>> {
        lock(&self.lanes).iter().flatten().cloned().collect()
    }

    /// Bytes pulled down since the board started, including in-flight files.
    fn downloaded(&self) -> u64 {
        let live: u64 = self
            .active()
            .iter()
            .map(|s| s.done.load(Ordering::Relaxed))
            .sum();
        self.finished_bytes.load(Ordering::Relaxed) + live
    }

    fn finished_count(&self) -> usize {
        self.completed.load(Ordering::Relaxed)
            + self.failed.load(Ordering::Relaxed)
            + self.skipped.load(Ordering::Relaxed)
    }

    /// Overall ETA.
    ///
    /// Bytes are the honest unit, but we only know the size of files that have
    /// started, so queued files are projected using the average size of what
    /// has already been measured. With no size information at all we fall back
    /// to "seconds per finished file".
    fn eta(&self) -> Option<u64> {
        let total_files = self.total_files.load(Ordering::Relaxed);
        let finished = self.finished_count();
        let remaining_files = total_files.saturating_sub(finished);
        if remaining_files == 0 {
            return Some(0);
        }

        let active = self.active();
        let speed = lock(&self.rate).per_second().unwrap_or(0);

        let live_remaining: u64 = active
            .iter()
            .filter_map(|s| {
                let total = s.total.load(Ordering::Relaxed);
                (total > 0).then(|| total.saturating_sub(s.done.load(Ordering::Relaxed)))
            })
            .sum();

        let measured = self.sized_files.load(Ordering::Relaxed);
        let queued = remaining_files.saturating_sub(active.len());

        if speed > 0 && measured > 0 {
            let avg = self.finished_bytes.load(Ordering::Relaxed) / measured as u64;
            let projected = live_remaining + avg * queued as u64;
            return Some(projected / speed.max(1));
        }

        if speed > 0 && queued == 0 && live_remaining > 0 {
            return Some(live_remaining / speed.max(1));
        }

        if finished > 0 {
            let per_file = self.started.elapsed().as_secs_f64() / finished as f64;
            return Some((per_file * remaining_files as f64) as u64);
        }

        None
    }

    fn header(&self, width: usize) -> String {
        let total_files = self.total_files.load(Ordering::Relaxed);
        let completed = self.completed.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let skipped = self.skipped.load(Ordering::Relaxed);
        let active = self.active().len();
        let bytes = self.downloaded();
        let speed = lock(&self.rate).per_second();

        let mut parts = vec![
            format!("{}/{} files", self.finished_count(), total_files),
            format!("{} active", active),
            format_size(bytes),
            format_speed(speed),
            format_eta(self.eta()),
        ];
        if failed > 0 {
            parts.push(format!("{} failed", failed));
        }
        if skipped > 0 {
            parts.push(format!("{} skipped", skipped));
        }
        let _ = completed;

        ellipsize(
            &format!("  {}  {}", self.title, parts.join(" \u{b7} ")),
            width,
        )
    }

    fn lane_line(&self, slot: &Slot, width: usize) -> String {
        let done = slot.done.load(Ordering::Relaxed);
        let total = slot.total.load(Ordering::Relaxed);
        let speed = lock(&slot.rate).per_second();
        let state = *lock(&slot.state);

        let right = progress_tail(done, total, speed, slot.started);
        let label = format!("#{} {}", slot.id, slot.name);
        let name_room = width
            .saturating_sub(display_width(&right) + 8)
            .clamp(8, 64);

        ellipsize(
            &format!(
                "    {} {}  {}",
                state.glyph(),
                pad(&ellipsize(&label, name_room), name_room),
                right
            ),
            width,
        )
    }

    /// Erases the live block so ordinary output can scroll past it.
    fn wipe(&self) {
        if !self.tty {
            return;
        }
        let mut drawn = lock(&self.drawn);
        if *drawn == 0 {
            return;
        }
        let mut buf = format!("\x1b[{}A", *drawn);
        for _ in 0..*drawn {
            buf.push_str("\x1b[2K\n");
        }
        buf.push_str(&format!("\x1b[{}A", *drawn));
        *drawn = 0;
        emit(&buf);
    }

    fn render(&self) {
        if !self.tty || self.stopped.load(Ordering::Relaxed) {
            return;
        }

        let width = term_width();
        let mut lines = vec![self.header(width)];
        for slot in self.active() {
            lines.push(self.lane_line(&slot, width));
        }

        let mut drawn = lock(&self.drawn);
        let mut buf = String::new();
        if *drawn > 0 {
            buf.push_str(&format!("\x1b[{}A", *drawn));
        }
        for line in &lines {
            buf.push_str("\x1b[2K");
            buf.push_str(line);
            buf.push('\n');
        }
        // Blank out lanes that finished since the last frame.
        for _ in lines.len()..*drawn {
            buf.push_str("\x1b[2K\n");
        }
        *drawn = lines.len().max(*drawn);
        emit(&buf);
    }

    fn log(&self, msg: &str) {
        self.wipe();
        emit(&format!("{}\n", msg));
        self.render();
    }

    fn plain_status(&self) {
        emit(&format!("{}\n", self.header(100).trim_end()));
    }
}

/// A live, multi-line progress display: one lane per in-flight file plus an
/// aggregate header. Cheap to clone; every clone points at the same display.
#[derive(Clone)]
pub struct Board(Arc<BoardInner>);

impl Board {
    pub fn new(title: &str, total_files: usize, lanes: usize) -> Self {
        Board(Arc::new(BoardInner {
            title: title.to_string(),
            tty: is_tty(),
            started: Instant::now(),
            lanes: Mutex::new((0..lanes.max(1)).map(|_| None).collect()),
            drawn: Mutex::new(0),
            total_files: AtomicUsize::new(total_files),
            completed: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            skipped: AtomicUsize::new(0),
            finished_bytes: AtomicU64::new(0),
            sized_files: AtomicUsize::new(0),
            rate: Mutex::new(Rate::new()),
            stopped: AtomicBool::new(false),
        }))
    }

    /// Starts the repaint loop. On a TTY it redraws in place; otherwise it
    /// prints a plain status line every few seconds so logs stay useful.
    pub fn spawn_renderer(&self) -> tokio::task::JoinHandle<()> {
        let inner = Arc::clone(&self.0);
        tokio::spawn(async move {
            let mut last_plain = Instant::now();
            loop {
                if inner.stopped.load(Ordering::Relaxed) {
                    break;
                }
                lock(&inner.rate).record(inner.downloaded());

                if inner.tty {
                    inner.render();
                    tokio::time::sleep(TTY_TICK).await;
                } else {
                    if last_plain.elapsed() >= PLAIN_TICK {
                        last_plain = Instant::now();
                        inner.plain_status();
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        })
    }

    /// Takes a lane for `id`. Returns `None` only if every lane is busy, which
    /// the caller prevents with its own concurrency limit.
    pub fn claim(&self, id: u64, name: &str) -> Option<Lane> {
        let slot = Arc::new(Slot {
            id,
            name: name.to_string(),
            board: Arc::downgrade(&self.0),
            started: Instant::now(),
            total: AtomicU64::new(0),
            done: AtomicU64::new(0),
            state: Mutex::new(SlotState::Waiting),
            rate: Mutex::new(Rate::new()),
        });

        let mut lanes = lock(&self.0.lanes);
        let idx = lanes.iter().position(|l| l.is_none())?;
        lanes[idx] = Some(Arc::clone(&slot));
        drop(lanes);

        Some(Lane {
            board: Arc::clone(&self.0),
            slot,
            idx,
        })
    }

    /// Prints a line above the live block without corrupting it.
    pub fn log(&self, msg: &str) {
        self.0.log(msg);
    }

    pub fn add_files(&self, n: usize) {
        self.0.total_files.fetch_add(n, Ordering::Relaxed);
    }

    pub fn file_completed(&self, bytes: u64) {
        self.0.completed.fetch_add(1, Ordering::Relaxed);
        if bytes > 0 {
            self.0.finished_bytes.fetch_add(bytes, Ordering::Relaxed);
            self.0.sized_files.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn file_failed(&self) {
        self.0.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn file_skipped(&self) {
        self.0.skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn downloaded_bytes(&self) -> u64 {
        self.0.finished_bytes.load(Ordering::Relaxed)
    }

    pub fn elapsed(&self) -> Duration {
        self.0.started.elapsed()
    }

    /// Stops repainting and clears the live block. Safe to call twice.
    pub fn finish(&self) {
        self.0.stopped.store(true, Ordering::Relaxed);
        self.0.wipe();
    }
}

/// A claimed lane on the [`Board`]. Dropping it frees the lane.
pub struct Lane {
    board: Arc<BoardInner>,
    slot: Arc<Slot>,
    idx: usize,
}

impl Lane {
    /// The sink to hand to the download engine.
    pub fn sink(&self) -> Arc<dyn ProgressSink> {
        Arc::clone(&self.slot) as Arc<dyn ProgressSink>
    }

    pub fn downloaded(&self) -> u64 {
        self.slot.done.load(Ordering::Relaxed)
    }

    pub fn elapsed(&self) -> Duration {
        self.slot.started.elapsed()
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        let mut lanes = lock(&self.board.lanes);
        if let Some(entry) = lanes.get_mut(self.idx) {
            *entry = None;
        }
    }
}

// ── Counter progress (non-byte work) ────────────────────────────────────────

/// `120/430 \u{2593}\u{2591} 28% ETA 12s` for work measured in items, not bytes.
pub struct CountProgress {
    label: String,
    total: usize,
    done: AtomicUsize,
    started: Instant,
    tty: bool,
    dirty: AtomicBool,
    last_draw: Mutex<Instant>,
}

impl CountProgress {
    pub fn new(label: &str, total: usize) -> Self {
        Self {
            label: label.to_string(),
            total,
            done: AtomicUsize::new(0),
            started: Instant::now(),
            tty: is_tty(),
            dirty: AtomicBool::new(false),
            last_draw: Mutex::new(Instant::now() - SOLO_TICK),
        }
    }

    pub fn tick(&self) -> usize {
        let done = self.done.fetch_add(1, Ordering::Relaxed) + 1;
        self.draw(done == self.total);
        done
    }

    fn wipe(&self) {
        if self.tty && self.dirty.swap(false, Ordering::Relaxed) {
            emit("\r\x1b[2K");
        }
    }

    fn draw(&self, force: bool) {
        if !self.tty || self.total == 0 {
            return;
        }
        {
            let mut last = lock(&self.last_draw);
            if !force && last.elapsed() < SOLO_TICK {
                return;
            }
            *last = Instant::now();
        }

        let done = self.done.load(Ordering::Relaxed);
        let fraction = done as f64 / self.total as f64;
        let eta = if done == 0 {
            None
        } else {
            let per = self.started.elapsed().as_secs_f64() / done as f64;
            Some((per * (self.total - done) as f64) as u64)
        };

        self.dirty.store(true, Ordering::Relaxed);
        emit(&format!(
            "\r\x1b[2K  {}  {}/{}  {} {:>4}  {}",
            self.label,
            done,
            self.total,
            progress_bar(fraction, 18),
            format!("{}%", (fraction * 100.0) as u64),
            format_eta(eta),
        ));
    }

    /// Clears the live line and prints a closing summary in its place.
    pub fn finish(&self, summary: &str) {
        self.wipe();
        if !summary.is_empty() {
            eprintln!("  {}", summary);
        }
    }

    /// Prints a line without leaving the progress line behind.
    pub fn note(&self, msg: &str) {
        self.wipe();
        eprintln!("  {}", msg);
    }
}

// ── Directory scan spinner ──────────────────────────────────────────────────

/// Live counter for the scraper: one refreshing line instead of one printed
/// line per directory (which used to bury the rest of the output).
pub struct ScanSpinner {
    tty: bool,
    started: Instant,
    dirs: AtomicUsize,
    files: AtomicUsize,
    frame: AtomicUsize,
    dirty: AtomicBool,
    last_draw: Mutex<Instant>,
}

const SPINNER_FRAMES: [&str; 10] = [
    "\u{280b}", "\u{2819}", "\u{2839}", "\u{2838}", "\u{283c}", "\u{2834}", "\u{2826}",
    "\u{2827}", "\u{2807}", "\u{280f}",
];

impl Default for ScanSpinner {
    fn default() -> Self {
        Self::new()
    }
}

impl ScanSpinner {
    pub fn new() -> Self {
        Self {
            tty: is_tty(),
            started: Instant::now(),
            dirs: AtomicUsize::new(0),
            files: AtomicUsize::new(0),
            frame: AtomicUsize::new(0),
            dirty: AtomicBool::new(false),
            last_draw: Mutex::new(Instant::now() - SOLO_TICK),
        }
    }

    /// Called as each directory starts being scanned.
    pub fn dir(&self, label: &str) {
        self.dirs.fetch_add(1, Ordering::Relaxed);
        self.draw(label);
    }

    pub fn add_files(&self, n: usize) {
        self.files.fetch_add(n, Ordering::Relaxed);
    }

    pub fn dirs(&self) -> usize {
        self.dirs.load(Ordering::Relaxed)
    }

    fn wipe(&self) {
        if self.tty && self.dirty.swap(false, Ordering::Relaxed) {
            emit("\r\x1b[2K");
        }
    }

    fn draw(&self, label: &str) {
        if !self.tty {
            return;
        }
        {
            let mut last = lock(&self.last_draw);
            if last.elapsed() < SOLO_TICK {
                return;
            }
            *last = Instant::now();
        }

        let frame = self.frame.fetch_add(1, Ordering::Relaxed) % SPINNER_FRAMES.len();
        let head = format!(
            "  {} Scanning  {} dir(s) \u{b7} {} file(s) \u{b7} {}  ",
            SPINNER_FRAMES[frame],
            self.dirs.load(Ordering::Relaxed),
            self.files.load(Ordering::Relaxed),
            format_duration(self.started.elapsed().as_secs()),
        );
        let room = term_width().saturating_sub(display_width(&head)).max(8);

        self.dirty.store(true, Ordering::Relaxed);
        emit(&format!("\r\x1b[2K{}{}", head, ellipsize(label, room)));
    }

    /// Prints a warning without leaving the spinner line behind.
    pub fn note(&self, msg: &str) {
        self.wipe();
        eprintln!("{}", msg);
    }

    pub fn finish(&self) {
        self.wipe();
    }
}

// ── Formatting ──────────────────────────────────────────────────────────────

/// Shared right-hand side of every progress line: bar, percent, sizes, speed,
/// ETA. Kept in one place so the solo bar and the board stay in sync.
fn progress_tail(done: u64, total: u64, speed: Option<u64>, started: Instant) -> String {
    if total > 0 {
        let fraction = (done as f64 / total as f64).clamp(0.0, 1.0);
        let eta = match speed {
            Some(s) if s > 0 => Some(total.saturating_sub(done) / s),
            _ => None,
        };
        format!(
            "{} {:>4}  {:>9} / {:<9} {:>11}  {}",
            progress_bar(fraction, 20),
            format!("{}%", (fraction * 100.0) as u64),
            format_size(done),
            format_size(total),
            format_speed(speed),
            format_eta(eta),
        )
    } else {
        // Unknown length: no bar, no fake ETA — just honest numbers.
        format!(
            "{:>22}  {:>9} {:>11}  {}",
            "size unknown",
            format_size(done),
            format_speed(speed),
            format_duration(started.elapsed().as_secs()),
        )
    }
}

pub fn progress_bar(fraction: f64, width: usize) -> String {
    let fraction = fraction.clamp(0.0, 1.0);
    let filled = (fraction * width as f64).round() as usize;
    let filled = filled.min(width);
    format!(
        "\u{2595}{}{}\u{258f}",
        "\u{2588}".repeat(filled),
        "\u{2591}".repeat(width - filled)
    )
}

/// Compact size: `1.4 GiB`, `937 KiB`, `512 B`.
pub fn format_size(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    let b = bytes as f64;
    if b < KIB {
        format!("{} B", bytes)
    } else if b < KIB * KIB {
        format!("{:.1} KiB", b / KIB)
    } else if b < KIB * KIB * KIB {
        format!("{:.1} MiB", b / (KIB * KIB))
    } else {
        format!("{:.2} GiB", b / (KIB * KIB * KIB))
    }
}

pub fn format_speed(bps: Option<u64>) -> String {
    match bps {
        Some(b) if b > 0 => format!("{}/s", format_size(b)),
        _ => "--".to_string(),
    }
}

/// `1h 04m`, `4m 12s`, `37s`.
pub fn format_duration(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

/// `ETA 4m 12s`, or `ETA --` while we still have nothing to base it on.
pub fn format_eta(secs: Option<u64>) -> String {
    match secs {
        Some(s) => format!("ETA {}", format_duration(s)),
        None => "ETA --".to_string(),
    }
}

/// Character count, used for layout. Wide glyphs are close enough here.
fn display_width(s: &str) -> usize {
    s.chars().count()
}

/// Truncates on a character boundary, with an ellipsis when it had to cut.
pub fn ellipsize(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if display_width(s) <= max {
        return s.to_string();
    }
    if max <= 1 {
        return "\u{2026}".to_string();
    }
    let keep: String = s.chars().take(max - 1).collect();
    format!("{}\u{2026}", keep)
}

/// Right-pads to `width` characters (`{:<width$}` counts bytes, not chars).
fn pad(s: &str, width: usize) -> String {
    let len = display_width(s);
    if len >= width {
        s.to_string()
    } else {
        format!("{}{}", s, " ".repeat(width - len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_size_scales() {
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(2048), "2.0 KiB");
        assert_eq!(format_size(5 * 1024 * 1024), "5.0 MiB");
        assert_eq!(format_size(3 * 1024 * 1024 * 1024), "3.00 GiB");
    }

    #[test]
    fn format_duration_buckets() {
        assert_eq!(format_duration(37), "37s");
        assert_eq!(format_duration(252), "4m 12s");
        assert_eq!(format_duration(3900), "1h 05m");
    }

    #[test]
    fn eta_is_honest_when_unknown() {
        assert_eq!(format_eta(None), "ETA --");
        assert_eq!(format_eta(Some(90)), "ETA 1m 30s");
    }

    #[test]
    fn speed_is_honest_when_unknown() {
        assert_eq!(format_speed(None), "--");
        assert_eq!(format_speed(Some(0)), "--");
        assert_eq!(format_speed(Some(1024 * 1024)), "1.0 MiB/s");
    }

    #[test]
    fn bar_endpoints() {
        assert_eq!(progress_bar(0.0, 4), "\u{2595}\u{2591}\u{2591}\u{2591}\u{2591}\u{258f}");
        assert_eq!(progress_bar(1.0, 4), "\u{2595}\u{2588}\u{2588}\u{2588}\u{2588}\u{258f}");
        // Out-of-range input must not panic or overflow the width.
        assert_eq!(progress_bar(9.0, 4).chars().count(), 6);
        assert_eq!(progress_bar(-1.0, 4).chars().count(), 6);
    }

    #[test]
    fn ellipsize_respects_char_boundaries() {
        assert_eq!(ellipsize("hello", 10), "hello");
        assert_eq!(ellipsize("hello world", 8), "hello w\u{2026}");
        // Multi-byte characters must not be sliced in half.
        let s = "\u{e5}\u{e4}\u{f6}\u{e5}\u{e4}\u{f6}";
        assert_eq!(ellipsize(s, 3).chars().count(), 3);
    }

    #[test]
    fn pad_counts_characters() {
        assert_eq!(pad("ab", 5), "ab   ");
        assert_eq!(pad("\u{e5}\u{e4}", 4).chars().count(), 4);
        assert_eq!(pad("toolong", 3), "toolong");
    }

    #[test]
    fn rate_needs_history_then_averages() {
        let mut rate = Rate::new();
        let t0 = Instant::now();
        rate.record_at(t0, 0);
        // Too little history to claim a speed.
        assert_eq!(rate.per_second(), None);

        rate.record_at(t0 + Duration::from_secs(2), 2 * 1024 * 1024);
        let bps = rate.per_second().expect("rate after two samples");
        assert!((bps as i64 - 1024 * 1024).abs() < 32 * 1024, "got {bps}");
    }

    #[test]
    fn rate_window_drops_stale_samples() {
        let mut rate = Rate::new();
        let t0 = Instant::now();
        for i in 0..10 {
            rate.record_at(t0 + Duration::from_secs(i), i * 1024);
        }
        assert!(rate.samples.len() <= 5, "window should stay small");
    }

    #[test]
    fn progress_tail_without_total_has_no_fake_eta() {
        let tail = progress_tail(1024, 0, Some(1024), Instant::now());
        assert!(tail.contains("size unknown"));
        assert!(!tail.contains("ETA"));
    }

    #[test]
    fn board_counts_and_frees_lanes() {
        let board = Board::new("Queue", 3, 2);
        let a = board.claim(1, "a.bin").expect("lane a");
        let b = board.claim(2, "b.bin").expect("lane b");
        assert!(board.claim(3, "c.bin").is_none(), "only two lanes exist");

        a.sink().progress(1024);
        assert_eq!(a.downloaded(), 1024);

        drop(a);
        board.file_completed(1024);
        assert!(board.claim(3, "c.bin").is_some(), "lane freed on drop");
        drop(b);

        assert_eq!(board.downloaded_bytes(), 1024);
    }
}
