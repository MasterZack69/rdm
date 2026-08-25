//! One refreshing line for the scraper instead of a line per directory.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use super::term::{SOLO_TICK, draw_width, emit, is_tty, lock};
use super::width::{clip, display_width};

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
    "\u{280b}", "\u{2819}", "\u{2839}", "\u{2838}", "\u{283c}", "\u{2834}", "\u{2826}", "\u{2827}",
    "\u{2807}", "\u{280f}",
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
