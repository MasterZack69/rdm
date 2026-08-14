//! Terminal presentation layer.
//!
//! Everything rdm draws while work is in flight goes through this module so
//! that concurrent downloads can't scribble over each other:
//!
//!   - [`Board`] — a live block: one short line per in-flight file plus a
//!     summary line. Used by the queue (and therefore by `sync` and directory
//!     downloads).
//!   - [`SoloBar`] — a single refreshing line for one-file downloads.
//!   - [`CountProgress`] — a counter + ETA line for non-byte work.
//!   - [`ScanSpinner`] — a live counter for directory scraping.
//!
//! ## Rules this module must never break
//!
//! 1. **Every emitted line is clipped to `term_width() - 1`.** A line that is
//!    even one column too long wraps onto a second physical row, and then the
//!    `ESC[nA` cursor math (which counts logical lines) is wrong forever after
//!    — the block scrolls instead of redrawing. This is what caused the
//!    repeated-line spam.
//! 2. **Width is measured in columns, not chars.** Emoji occupy two columns;
//!    counting them as one re-introduces rule 1.
//! 3. **A live block never ends with a newline.** The cursor stays parked on
//!    the last drawn line so the next frame can move up exactly `lines - 1`.
//! 4. **Never hold one of these mutexes across a call that might take it
//!    again.** They are plain `std` mutexes, so they are not reentrant and a
//!    second acquisition on the same thread deadlocks the whole download.
//!    Beware of guards created inside a larger expression: they live until the
//!    end of the *statement*, not the end of the sub-expression.
//!
//! Everything writes to stderr and degrades to plain, scroll-safe output when
//! stderr is not a TTY.

use std::collections::VecDeque;
use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

/// Speed is averaged over this window so the number stays readable.
const RATE_WINDOW: Duration = Duration::from_secs(3);
/// Redraw interval when we own a terminal.
const TTY_TICK: Duration = Duration::from_millis(150);
/// How often a non-TTY run prints a plain status line.
const PLAIN_TICK: Duration = Duration::from_secs(5);
/// Minimum gap between single-line widget repaints.
const SOLO_TICK: Duration = Duration::from_millis(150);
/// Below this width there is no room for a bar, so we drop it.
const BAR_MIN_WIDTH: usize = 76;

// ── Terminal basics ────────────────────────────────────────────────

/// Is stderr an interactive terminal? Decides ANSI vs. plain output.
pub fn is_tty() -> bool {
    std::io::stderr().is_terminal()
}

/// Total terminal columns.
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
        .unwrap_or(80)
}

/// Columns we are allowed to fill. One short of the real width: writing the
/// final column makes some terminals wrap immediately.
fn draw_width() -> usize {
    term_width().saturating_sub(1).max(20)
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

// ── Width accounting ──────────────────────────────────────────────

/// Approximate column width of a character. Only needs to be right about the
/// two cases that matter: zero-width joiners/selectors and double-width
/// glyphs (CJK and emoji), which is what our own status lines contain.
fn char_width(c: char) -> usize {
    let c = c as u32;
    if c == 0x200d || c == 0xfe0f || c == 0xfe0e || (0x0300..=0x036f).contains(&c) {
        return 0;
    }
    let wide = (0x1100..=0x115f).contains(&c)
        || (0x2e80..=0x303e).contains(&c)
        || (0x3041..=0x33ff).contains(&c)
        || (0x3400..=0x4dbf).contains(&c)
        || (0x4e00..=0x9fff).contains(&c)
        || (0xa000..=0xa4cf).contains(&c)
        || (0xac00..=0xd7a3).contains(&c)
        || (0xf900..=0xfaff).contains(&c)
        || (0xfe30..=0xfe6f).contains(&c)
        || (0xff00..=0xff60).contains(&c)
        || (0xffe0..=0xffe6).contains(&c)
        || (0x1f300..=0x1f64f).contains(&c)
        || (0x1f680..=0x1f6ff).contains(&c)
        || (0x1f900..=0x1f9ff).contains(&c)
        || (0x1fa70..=0x1faff).contains(&c)
        || matches!(c, 0x231a..=0x231b | 0x23e9..=0x23ec | 0x23f0 | 0x23f3)
        || matches!(c, 0x25fd..=0x25fe | 0x2614..=0x2615 | 0x2648..=0x2653)
        || matches!(c, 0x267f | 0x2693 | 0x26a1 | 0x26aa..=0x26ab | 0x26bd..=0x26be)
        || matches!(c, 0x26c4..=0x26c5 | 0x26ce | 0x26d4 | 0x26ea | 0x26f2..=0x26f3)
        || matches!(c, 0x26f5 | 0x26fa | 0x26fd | 0x2705 | 0x270a..=0x270b | 0x2728)
        || matches!(c, 0x274c | 0x274e | 0x2753..=0x2755 | 0x2757 | 0x2795..=0x2797)
        || matches!(c, 0x27b0 | 0x27bf | 0x2b1b..=0x2b1c | 0x2b50 | 0x2b55);
    if wide { 2 } else { 1 }
}

/// Column width of a string.
fn display_width(s: &str) -> usize {
    s.chars().map(char_width).sum()
}

/// Truncates to `max` **columns**, adding an ellipsis when it had to cut.
/// This is the single choke point that keeps rule 1 above true.
pub fn clip(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if display_width(s) <= max {
        return s.to_owned();
    }
    let budget = max - 1; // room for the ellipsis
    let mut out = String::new();
    let mut used = 0;
    for ch in s.chars() {
        let w = char_width(ch);
        if used + w > budget {
            break;
        }
        out.push(ch);
        used += w;
    }
    out.push('\u{2026}');
    out
}

/// Kept under its old name; callers outside this module use it for names.
pub fn ellipsize(s: &str, max: usize) -> String {
    clip(s, max)
}

/// Right-pads to `width` columns (`{:<width$}` counts bytes, not columns).
fn pad(s: &str, width: usize) -> String {
    let len = display_width(s);
    if len >= width {
        s.to_owned()
    } else {
        format!("{}{}", s, " ".repeat(width - len))
    }
}

// ── Rate estimation ───────────────────────────────────────────────

/// Sliding-window byte-rate estimator. Fed absolute totals, not deltas.
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
        while self.samples.len() > 2 && now.duration_since(self.samples[0].0) > RATE_WINDOW {
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

// ── Progress sinks ────────────────────────────────────────────────

/// What a worker is currently doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Waiting,
    Inspecting,
    Downloading,
    Finishing,
}

impl SlotState {
    /// Short word shown instead of numbers while there are no bytes yet.
    pub fn label(self) -> &'static str {
        match self {
            SlotState::Waiting => "queued",
            SlotState::Inspecting => "connecting",
            SlotState::Downloading => "starting",
            SlotState::Finishing => "saving",
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
    /// Chatty per-download detail. Hidden in multi-file runs.
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

// ── Solo download bar ─────────────────────────────────────────────

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
            name: name.to_owned(),
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

        // Each guard is released before the next line (rule 4).
        let state = *lock(&self.state);
        let speed = lock(&self.rate).per_second();
        let done = self.done.load(Ordering::Relaxed);
        let total = self.total.load(Ordering::Relaxed);

        let line = compose(
            &self.name,
            state,
            done,
            total,
            speed,
            self.started,
            draw_width(),
        );

        self.dirty.store(true, Ordering::Relaxed);
        emit(&format!("\r\x1b[2K{}", line));
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
        eprintln!("{}", clip(&format!("  {}", msg), draw_width()));
    }

    fn note(&self, msg: &str) {
        self.detail(msg);
    }

    fn finish(&self) {
        self.wipe();
    }
}

// ── Multi-file board ──────────────────────────────────────────────

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
            board.log(&format!("  #{} {}", self.id, msg));
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

    /// Overall ETA. Bytes are the honest unit, but we only know the size of
    /// files that have started, so queued files are projected from the average
    /// size of what has already finished. With no size information at all we
    /// fall back to seconds-per-finished-file.
    ///
    /// Takes the rate lock, so callers must not already hold it (rule 4).
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
            return Some((live_remaining + avg * queued as u64) / speed.max(1));
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

    /// `  3/12  1.4G  38M/s  ETA 3m12s` — no title, no word "files".
    fn summary(&self, width: usize) -> String {
        let finished = self.finished_count();
        let total_files = self.total_files.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let skipped = self.skipped.load(Ordering::Relaxed);
        let bytes = self.downloaded();

        // Both of these must happen before the vec! below: a guard created
        // inside the vec! expression would still be alive when eta() takes the
        // same lock, which deadlocks the thread (rule 4).
        let speed = lock(&self.rate).per_second();
        let eta = self.eta();

        let mut parts = vec![
            format!("{}/{}", finished, total_files),
            short_size(bytes),
            short_speed(speed),
            format_eta(eta),
        ];
        if failed > 0 {
            parts.push(format!("{} failed", failed));
        }
        if skipped > 0 {
            parts.push(format!("{} skipped", skipped));
        }

        clip(&format!("  {}", parts.join("  ")), width)
    }

    fn lane_line(&self, slot: &Slot, width: usize) -> String {
        let state = *lock(&slot.state);
        let speed = lock(&slot.rate).per_second();
        compose(
            &slot.name,
            state,
            slot.done.load(Ordering::Relaxed),
            slot.total.load(Ordering::Relaxed),
            speed,
            slot.started,
            width,
        )
    }

    fn frame(&self) -> Vec<String> {
        let width = draw_width();
        let mut lines: Vec<String> = self
            .active()
            .iter()
            .map(|s| self.lane_line(s, width))
            .collect();
        lines.push(self.summary(width));
        lines
    }

    /// Erases the live block so ordinary output can scroll past it. Leaves the
    /// cursor at column 0 of the block's first line.
    fn wipe(&self) {
        if !self.tty {
            return;
        }
        let mut drawn = lock(&self.drawn);
        if *drawn == 0 {
            return;
        }
        let mut buf = String::from("\r\x1b[2K");
        for _ in 1..*drawn {
            buf.push_str("\x1b[1A\r\x1b[2K");
        }
        *drawn = 0;
        emit(&buf);
    }

    /// Redraws the block in place. Never emits a trailing newline, so the
    /// cursor stays on the last line and the next frame can move up exactly
    /// `drawn - 1` rows.
    fn render(&self) {
        if !self.tty || self.stopped.load(Ordering::Relaxed) {
            return;
        }

        // Built before taking the `drawn` lock so the two are never nested.
        let lines = self.frame();
        let mut drawn = lock(&self.drawn);

        let mut buf = String::new();
        if *drawn > 1 {
            buf.push_str(&format!("\x1b[{}A", *drawn - 1));
        }
        for (i, line) in lines.iter().enumerate() {
            if i > 0 {
                buf.push('\n');
            }
            buf.push_str("\r\x1b[2K");
            buf.push_str(line);
        }
        // Blank out rows left over from a taller previous frame, then come back.
        let leftover = drawn.saturating_sub(lines.len());
        for _ in 0..leftover {
            buf.push_str("\n\r\x1b[2K");
        }
        if leftover > 0 {
            buf.push_str(&format!("\x1b[{}A", leftover));
        }

        *drawn = lines.len();
        emit(&buf);
    }

    fn log(&self, msg: &str) {
        let line = clip(msg, draw_width());
        self.wipe();
        emit(&format!("{}\n", line));
        self.render();
    }

    fn plain_status(&self) {
        emit(&format!(
            "{}{}\n",
            self.title,
            self.summary(usize::MAX).trim_end()
        ));
    }
}

/// A live progress block: one short line per in-flight file plus a summary.
/// Cheap to clone; every clone points at the same display.
#[derive(Clone)]
pub struct Board(Arc<BoardInner>);

impl Board {
    pub fn new(title: &str, total_files: usize, lanes: usize) -> Self {
        Board(Arc::new(BoardInner {
            title: title.to_owned(),
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
                let bytes = inner.downloaded();
                lock(&inner.rate).record(bytes);

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
            name: name.to_owned(),
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

// ── Counter progress (non-byte work) ───────────────────────────────────

/// `Verifying 120/430  28%  ETA 12s` for work measured in items, not bytes.
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
            label: label.to_owned(),
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

        let width = draw_width();
        let done = self.done.load(Ordering::Relaxed);
        let fraction = done as f64 / self.total as f64;
        let eta = if done == 0 {
            None
        } else {
            let per = self.started.elapsed().as_secs_f64() / done as f64;
            Some((per * (self.total - done) as f64) as u64)
        };

        let mut line = format!("  {} {}/{}", self.label, done, self.total);
        if width >= BAR_MIN_WIDTH {
            line.push_str(&format!("  {}", progress_bar(fraction, 12)));
        }
        line.push_str(&format!(
            "  {:>3}%  {}",
            (fraction * 100.0) as u64,
            format_eta(eta)
        ));

        self.dirty.store(true, Ordering::Relaxed);
        emit(&format!("\r\x1b[2K{}", clip(&line, width)));
    }

    /// Clears the live line and prints a closing summary in its place.
    pub fn finish(&self, summary: &str) {
        self.wipe();
        if !summary.is_empty() {
            eprintln!("{}", clip(&format!("  {}", summary), draw_width()));
        }
    }

    /// Prints a line without leaving the progress line behind.
    pub fn note(&self, msg: &str) {
        self.wipe();
        eprintln!("{}", clip(&format!("  {}", msg), draw_width()));
    }
}

// ── Directory scan spinner ───────────────────────────────────────────

/// One refreshing line for the scraper instead of a line per directory.
pub struct ScanSpinner {
    tty: bool,
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

        let width = draw_width();
        let frame = self.frame.fetch_add(1, Ordering::Relaxed) % SPINNER_FRAMES.len();
        let head = format!(
            "  {} {} dirs  {} files  ",
            SPINNER_FRAMES[frame],
            self.dirs.load(Ordering::Relaxed),
            self.files.load(Ordering::Relaxed),
        );
        let room = width.saturating_sub(display_width(&head));

        self.dirty.store(true, Ordering::Relaxed);
        emit(&format!(
            "\r\x1b[2K{}",
            clip(&format!("{}{}", head, clip(label, room)), width)
        ));
    }

    /// Prints a warning without leaving the spinner line behind.
    pub fn note(&self, msg: &str) {
        self.wipe();
        eprintln!("{}", clip(msg, draw_width()));
    }

    pub fn finish(&self) {
        self.wipe();
    }
}

// ── Line composition ──────────────────────────────────────────────

/// The one place a per-file line is built, shared by the solo bar and the
/// board so they can't drift apart. Always returns at most `width` columns.
///
/// Wide:   `name                 ▏███░░░░░░░▕  38%  1.2M/3.1M  4.2M/s  27s`
/// Narrow: `name       38%  1.2M/3.1M  4.2M/s  27s`
/// No size yet: `name       1.2M  4.2M/s  27s`
/// Not started: `name       connecting`
fn compose(
    name: &str,
    state: SlotState,
    done: u64,
    total: u64,
    speed: Option<u64>,
    started: Instant,
    width: usize,
) -> String {
    let right = if done == 0 && state != SlotState::Downloading {
        state.label().to_owned()
    } else if total > 0 {
        let fraction = (done as f64 / total as f64).clamp(0.0, 1.0);
        let eta = match speed {
            Some(s) if s > 0 => Some(total.saturating_sub(done) / s),
            _ => None,
        };
        let bar = if width >= BAR_MIN_WIDTH {
            format!("{}  ", progress_bar(fraction, 10))
        } else {
            String::new()
        };
        format!(
            "{}{:>3}%  {:>6}/{:<6} {:>8}  {:>6}",
            bar,
            (fraction * 100.0) as u64,
            short_size(done),
            short_size(total),
            short_speed(speed),
            eta.map(short_duration).unwrap_or_else(|| "--".into()),
        )
    } else {
        // Unknown length: no bar, no fake percentage, no fake ETA.
        format!(
            "{:>6} {:>8}  {:>6}",
            short_size(done),
            short_speed(speed),
            short_duration(started.elapsed().as_secs()),
        )
    };

    let name_room = width
        .saturating_sub(display_width(&right) + 4)
        .clamp(6, 38);
    clip(
        &format!("  {}  {}", pad(&clip(name, name_room), name_room), right),
        width,
    )
}

// ── Formatting ─────────────────────────────────────────────────────

pub fn progress_bar(fraction: f64, width: usize) -> String {
    let fraction = fraction.clamp(0.0, 1.0);
    let filled = ((fraction * width as f64).round() as usize).min(width);
    format!(
        "\u{2595}{}{}\u{258f}",
        "\u{2588}".repeat(filled),
        "\u{2591}".repeat(width - filled)
    )
}

/// Tight size for progress lines: `512B`, `19.5K`, `142K`, `3.1G`.
///
/// Walks the whole unit table — stopping at `G` meant anything past a
/// terabyte grew without bound and blew the fixed layout budget.
pub fn short_size(bytes: u64) -> String {
    const UNITS: [char; 7] = ['B', 'K', 'M', 'G', 'T', 'P', 'E'];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}B", bytes)
    } else if value < 10.0 {
        format!("{:.1}{}", value, UNITS[unit])
    } else {
        format!("{:.0}{}", value, UNITS[unit])
    }
}

/// Tight rate: `4.2M/s`, or `--` when we don't know yet.
pub fn short_speed(bps: Option<u64>) -> String {
    match bps {
        Some(b) if b > 0 => format!("{}/s", short_size(b)),
        _ => "--".to_owned(),
    }
}

/// Tight duration for progress lines: `47s`, `3m12s`, `1h04m`.
pub fn short_duration(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

/// Roomier size for summaries and listings: `1.4 GiB`, `937 KiB`, `512 B`.
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
        _ => "--".to_owned(),
    }
}

/// `1h 04m`, `4m 12s`, `37s`. Used in end-of-run summaries.
pub fn format_duration(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

/// `ETA 4m12s`, or `ETA --` while we still have nothing to base it on.
pub fn format_eta(secs: Option<u64>) -> String {
    match secs {
        Some(s) => format!("ETA {}", short_duration(s)),
        None => "ETA --".to_owned(),
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
    fn short_size_never_exceeds_six_columns() {
        for bytes in [
            0u64,
            1,
            999,
            1024,
            20_000,
            999_999,
            1 << 20,
            1 << 30,
            1 << 40,
            1 << 50,
            u64::MAX,
        ] {
            let s = short_size(bytes);
            assert!(display_width(&s) <= 6, "{bytes} -> {s}");
        }
        assert_eq!(short_size(512), "512B");
        assert_eq!(short_size(20_000), "20K");
        assert_eq!(short_size(2 * 1024 * 1024), "2.0M");
        assert_eq!(short_size(1 << 40), "1.0T");
        // The case that used to render as twelve columns of digits.
        assert_eq!(short_size(u64::MAX), "16E");
    }

    #[test]
    fn duration_buckets() {
        assert_eq!(format_duration(252), "4m 12s");
        assert_eq!(short_duration(37), "37s");
        assert_eq!(short_duration(252), "4m12s");
        assert_eq!(short_duration(3900), "1h05m");
    }

    #[test]
    fn eta_and_speed_are_honest_when_unknown() {
        assert_eq!(format_eta(None), "ETA --");
        assert_eq!(format_eta(Some(90)), "ETA 1m30s");
        assert_eq!(short_speed(None), "--");
        assert_eq!(short_speed(Some(0)), "--");
        assert_eq!(short_speed(Some(1024 * 1024)), "1.0M/s");
    }

    #[test]
    fn bar_endpoints() {
        assert_eq!(progress_bar(0.0, 4).chars().count(), 6);
        assert_eq!(progress_bar(1.0, 4).chars().count(), 6);
        // Out-of-range input must not panic or overflow the width.
        assert_eq!(progress_bar(9.0, 4).chars().count(), 6);
        assert_eq!(progress_bar(-1.0, 4).chars().count(), 6);
    }

    #[test]
    fn emoji_count_as_two_columns() {
        // The bug that caused the redraw spam: these were counted as one.
        assert_eq!(display_width("\u{1f50e}"), 2);
        assert_eq!(display_width("\u{2705}"), 2);
        assert_eq!(display_width("abc"), 3);
    }

    #[test]
    fn clip_respects_columns_and_boundaries() {
        assert_eq!(clip("hello", 10), "hello");
        assert_eq!(clip("hello world", 8), "hello w\u{2026}");
        // Multi-byte characters must not be sliced in half.
        let s = "\u{e5}\u{e4}\u{f6}\u{e5}\u{e4}\u{f6}";
        assert_eq!(display_width(&clip(s, 3)), 3);
        // A wide glyph that doesn't fit is dropped rather than half-drawn.
        assert!(display_width(&clip("\u{1f50e}\u{1f50e}\u{1f50e}", 5)) <= 5);
    }

    #[test]
    fn pad_counts_columns() {
        assert_eq!(pad("ab", 5), "ab   ");
        assert_eq!(display_width(&pad("\u{e5}\u{e4}", 4)), 4);
        assert_eq!(pad("toolong", 3), "toolong");
    }

    #[test]
    fn composed_lines_always_fit() {
        let long = "Screenshot_20250812-114142_some_absurdly_long_name.webp";
        for width in [20usize, 40, 60, 80, 120, 199] {
            for (done, total) in [
                (0, 0),
                (0, 5_000_000),
                (1_234_567, 5_000_000),
                (999, 0),
                (u64::MAX / 2, u64::MAX),
            ] {
                for state in [
                    SlotState::Waiting,
                    SlotState::Inspecting,
                    SlotState::Downloading,
                    SlotState::Finishing,
                ] {
                    let line = compose(
                        long,
                        state,
                        done,
                        total,
                        Some(4_200_000),
                        Instant::now(),
                        width,
                    );
                    assert!(
                        display_width(&line) <= width,
                        "width {width}: {} cols in {line:?}",
                        display_width(&line)
                    );
                }
            }
        }
    }

    #[test]
    fn idle_lane_shows_a_word_not_fake_numbers() {
        let line = compose(
            "a.webp",
            SlotState::Inspecting,
            0,
            0,
            None,
            Instant::now(),
            80,
        );
        assert!(line.contains("connecting"), "{line}");
        assert!(!line.contains("ETA"), "{line}");
        assert!(!line.contains('%'), "{line}");
    }

    /// Also a deadlock regression test: `summary()` used to hold the rate lock
    /// while calling `eta()`, which takes it again. If that comes back, this
    /// test hangs instead of failing.
    #[test]
    fn summary_and_frame_fit_the_width() {
        let board = Board::new("Queue", 12, 3);
        let _a = board.claim(1, "first-file-with-a-long-name.iso").unwrap();
        let _b = board.claim(2, "second.iso").unwrap();
        board.file_completed(4096);
        board.file_failed();

        for width in [24usize, 40, 80, 160] {
            let s = board.0.summary(width);
            assert!(display_width(&s) <= width, "{width}: {s:?}");
        }
        // One line per active lane, plus the summary. Nothing else.
        assert_eq!(board.0.frame().len(), 3);
    }

    #[test]
    fn rate_needs_history_then_averages() {
        let mut rate = Rate::new();
        let t0 = Instant::now();
        rate.record_at(t0, 0);
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
