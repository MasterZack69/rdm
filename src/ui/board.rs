//! The multi-file live block: one lane per in-flight file plus a summary line.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use super::compose::compose;
use super::format::{format_eta, short_size, short_speed};
use super::rate::Rate;
use super::sink::{ProgressSink, SlotState};
use super::term::{draw_width, emit, is_tty, lock};
use super::width::clip;

/// Redraw interval when we own a terminal.
const TTY_TICK: Duration = Duration::from_millis(150);
/// How often a non-TTY run prints a plain status line.
const PLAIN_TICK: Duration = Duration::from_secs(5);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::width::display_width;

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
