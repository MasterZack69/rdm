//! The shared view of how a server is coping, across every chunk worker.
//!
//! The reduction this replaces could not fire. A worker only reported
//! anything once it had exhausted all of its own retries and was on its way
//! out with an error — and the main loop returns that error immediately, so
//! the count was read at most once, after the run was already over. The
//! threshold made it doubly unreachable: `active_workers * 2`, when each
//! worker could contribute at most one before being joined.
//!
//! So the report has to happen at the other end: every retryable *attempt*,
//! while the worker is still alive and about to try again. That turns the
//! counter into something with a rate, which is what "the server is
//! struggling" actually means, and it is read by a controller that all the
//! workers and the assignment loop share:
//!
//! - failures are kept in a rolling window, so a slow trickle over an hour
//!   is not the same thing as eight in ten seconds;
//! - `Retry-After` pauses everyone, not just the worker that was told;
//! - the limit it hands back is the number of ranges the loop may have in
//!   flight, so pressure reduces new assignments rather than killing work
//!   already in progress;
//! - and it recovers, slowly, once the window is quiet.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How far back a failure still counts.
const DEFAULT_WINDOW: Duration = Duration::from_secs(30);

/// How long the window must stay clean before a connection is given back.
const RECOVERY_QUIET: Duration = Duration::from_secs(60);

/// What the controller did about a reported failure, for the caller to print.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Reaction {
    /// The concurrency limit was lowered from this, to `limit`.
    pub reduced_from: Option<usize>,
    /// The limit in force now.
    pub limit: usize,
    /// Everyone is to stop starting work for this long.
    pub pause: Option<Duration>,
}

#[derive(Debug)]
struct State {
    limit: usize,
    failures: VecDeque<Instant>,
    paused_until: Option<Instant>,
    last_change: Instant,
}

#[derive(Debug)]
pub struct PressureController {
    max_workers: usize,
    window: Duration,
    recovery_quiet: Duration,
    state: Mutex<State>,
}

impl PressureController {
    pub fn new(max_workers: usize) -> Self {
        Self::with_timings(max_workers, DEFAULT_WINDOW, RECOVERY_QUIET)
    }

    pub fn with_timings(max_workers: usize, window: Duration, recovery_quiet: Duration) -> Self {
        let max_workers = max_workers.max(1);
        Self {
            max_workers,
            window,
            recovery_quiet,
            state: Mutex::new(State {
                limit: max_workers,
                failures: VecDeque::new(),
                paused_until: None,
                last_change: Instant::now(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // A panicking worker must not take the whole download's pacing with
        // it; the state behind this lock is advisory.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// How many ranges may be in flight.
    pub fn limit(&self) -> usize {
        self.lock().limit
    }

    /// How long is left of a server-requested pause, if any.
    pub fn pause_remaining(&self) -> Option<Duration> {
        self.pause_remaining_at(Instant::now())
    }

    pub fn pause_remaining_at(&self, now: Instant) -> Option<Duration> {
        let until = self.lock().paused_until?;
        (until > now).then(|| until - now)
    }

    pub fn is_paused(&self) -> bool {
        self.pause_remaining().is_some()
    }

    /// Waits out a pause, or returns at once. Cancellation wins.
    pub async fn wait_while_paused(&self, cancel: &tokio_util::sync::CancellationToken) {
        while let Some(wait) = self.pause_remaining() {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// Reports one retryable attempt — not one abandoned chunk.
    pub fn record_retryable(&self, retry_after: Option<Duration>) -> Reaction {
        self.record_retryable_at(retry_after, Instant::now())
    }

    pub fn record_retryable_at(&self, retry_after: Option<Duration>, now: Instant) -> Reaction {
        let mut state = self.lock();
        prune(&mut state, self.window, now);
        state.failures.push_back(now);

        let mut reaction = Reaction {
            limit: state.limit,
            ..Reaction::default()
        };

        // A server that states a pause has answered the question for us, and
        // it applies to every connection rather than the one that asked.
        if let Some(wait) = retry_after.filter(|w| !w.is_zero()) {
            let until = now + wait;
            if state.paused_until.is_none_or(|current| current < until) {
                state.paused_until = Some(until);
                reaction.pause = Some(wait);
            }
        }

        // Enough recent failures that the connections in flight are, on
        // average, failing: back off rather than keep pushing.
        if state.failures.len() >= state.limit && state.limit > 1 {
            let reduced_from = state.limit;
            state.limit = (state.limit / 2).max(1);
            state.failures.clear();
            state.last_change = now;
            reaction.reduced_from = Some(reduced_from);
            reaction.limit = state.limit;
        }

        reaction
    }

    /// Reports a chunk that finished. Recovery is deliberately slower than
    /// reduction: one connection at a time, and only after a quiet spell.
    pub fn record_success(&self) -> Option<usize> {
        self.record_success_at(Instant::now())
    }

    pub fn record_success_at(&self, now: Instant) -> Option<usize> {
        let mut state = self.lock();
        prune(&mut state, self.window, now);

        if state.limit >= self.max_workers
            || !state.failures.is_empty()
            || now.duration_since(state.last_change) < self.recovery_quiet
            || state.paused_until.is_some_and(|until| until > now)
        {
            return None;
        }

        state.limit += 1;
        state.last_change = now;
        Some(state.limit)
    }
}

fn prune(state: &mut State, window: Duration, now: Instant) {
    while let Some(oldest) = state.failures.front() {
        if now.duration_since(*oldest) > window {
            state.failures.pop_front();
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn controller(max: usize) -> PressureController {
        PressureController::with_timings(max, Duration::from_secs(30), Duration::from_secs(60))
    }

    /// The whole point of the rewrite: retryable attempts, reported while the
    /// workers are still running, actually reduce concurrency. Under the old
    /// scheme this sequence could never have reached the threshold.
    #[test]
    fn repeated_retryable_attempts_reduce_the_limit() {
        let c = controller(8);
        let t0 = Instant::now();

        assert_eq!(c.limit(), 8);
        for i in 0..7 {
            let reaction = c.record_retryable_at(None, t0 + Duration::from_millis(i * 100));
            assert_eq!(reaction.reduced_from, None, "too early at {i}");
        }
        let reaction = c.record_retryable_at(None, t0 + Duration::from_millis(700));
        assert_eq!(reaction.reduced_from, Some(8));
        assert_eq!(reaction.limit, 4);
        assert_eq!(c.limit(), 4);

        // And again, from the lower limit.
        for i in 0..4 {
            c.record_retryable_at(None, t0 + Duration::from_millis(800 + i * 100));
        }
        assert_eq!(c.limit(), 2);
    }

    /// A failure an hour ago says nothing about the server now.
    #[test]
    fn failures_outside_the_window_are_forgotten() {
        let c = controller(4);
        let t0 = Instant::now();

        for i in 0..3 {
            c.record_retryable_at(None, t0 + Duration::from_secs(i));
        }
        // Far enough ahead that all three have aged out.
        let reaction = c.record_retryable_at(None, t0 + Duration::from_secs(600));
        assert_eq!(reaction.reduced_from, None);
        assert_eq!(c.limit(), 4);
    }

    /// One connection never reduces to zero.
    #[test]
    fn the_limit_never_falls_below_one() {
        let c = controller(1);
        let t0 = Instant::now();
        for i in 0..20 {
            c.record_retryable_at(None, t0 + Duration::from_millis(i * 10));
        }
        assert_eq!(c.limit(), 1);
    }

    #[test]
    fn a_stated_retry_after_pauses_every_worker() {
        let c = controller(8);
        let t0 = Instant::now();

        assert!(c.pause_remaining_at(t0).is_none());
        let reaction = c.record_retryable_at(Some(Duration::from_secs(10)), t0);
        assert_eq!(reaction.pause, Some(Duration::from_secs(10)));

        assert_eq!(
            c.pause_remaining_at(t0 + Duration::from_secs(4)),
            Some(Duration::from_secs(6))
        );
        assert!(c.pause_remaining_at(t0 + Duration::from_secs(11)).is_none());
    }

    /// A shorter pause arriving during a longer one does not shorten it.
    #[test]
    fn the_longest_stated_pause_wins() {
        let c = controller(8);
        let t0 = Instant::now();

        c.record_retryable_at(Some(Duration::from_secs(30)), t0);
        let reaction = c.record_retryable_at(Some(Duration::from_secs(5)), t0);
        assert_eq!(reaction.pause, None, "a shorter pause is not news");
        assert_eq!(
            c.pause_remaining_at(t0),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn a_quiet_window_gives_a_connection_back_at_a_time() {
        let c = controller(8);
        let t0 = Instant::now();

        for i in 0..8 {
            c.record_retryable_at(None, t0 + Duration::from_millis(i * 10));
        }
        assert_eq!(c.limit(), 4);

        // Still inside the quiet period.
        assert_eq!(c.record_success_at(t0 + Duration::from_secs(30)), None);
        // Quiet for long enough.
        assert_eq!(c.record_success_at(t0 + Duration::from_secs(61)), Some(5));
        // ...and not twice in the same instant.
        assert_eq!(c.record_success_at(t0 + Duration::from_secs(61)), None);
        assert_eq!(c.record_success_at(t0 + Duration::from_secs(122)), Some(6));
    }

    #[test]
    fn recovery_never_exceeds_the_configured_connections() {
        let c = controller(2);
        let t0 = Instant::now();
        assert_eq!(c.record_success_at(t0 + Duration::from_secs(600)), None);
        assert_eq!(c.limit(), 2);
    }
}
