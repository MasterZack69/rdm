//! The shared per-file line builder.

use std::time::Instant;

use super::format::{BAR_MIN_WIDTH, progress_bar, short_duration, short_size, short_speed};
use super::sink::SlotState;
use super::width::{clip, display_width, pad};

/// The one place a per-file line is built, shared by the solo bar and the
/// board so they can't drift apart. Always returns at most `width` columns.
///
/// Wide:   `name                 ▏███░░░░░░░▕  38%  1.2M/3.1M  4.2M/s  27s`
/// Narrow: `name       38%  1.2M/3.1M  4.2M/s  27s`
/// No size yet: `name       1.2M  4.2M/s  27s`
/// Not started: `name       connecting`
pub(super) fn compose(
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

    let name_room = width.saturating_sub(display_width(&right) + 4).clamp(6, 38);
    clip(
        &format!("  {}  {}", pad(&clip(name, name_room), name_room), right),
        width,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
