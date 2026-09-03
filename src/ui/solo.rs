//! One refreshing line for a single download.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::compose::compose;
use super::rate::Rate;
use super::sanitize::terminal_safe;
use super::sink::{ProgressSink, SlotState};
use super::term::{SOLO_TICK, draw_width, emit, is_tty, lock};
use super::width::clip;

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
        // This is the line the finding follows to stderr: details quote
        // server-supplied filenames, response snippets and error text, and
        // stderr is redirected into logs. Sanitised here so that every caller
        // of `detail` and `note` is covered by one place.
        eprintln!(
            "{}",
            clip(&format!("  {}", terminal_safe(msg)), draw_width())
        );
    }

    fn note(&self, msg: &str) {
        self.detail(msg);
    }

    fn finish(&self) {
        self.wipe();
    }
}
