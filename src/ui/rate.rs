//! Sliding-window byte-rate estimation.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Speed is averaged over this window so the number stays readable.
const RATE_WINDOW: Duration = Duration::from_secs(3);

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
