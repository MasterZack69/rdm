//! A counter + ETA line for work measured in items rather than bytes.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use super::format::{BAR_MIN_WIDTH, format_eta, progress_bar};
use super::term::{SOLO_TICK, draw_width, emit, is_tty, lock};
use super::width::clip;

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
