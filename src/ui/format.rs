//! Sizes, speeds, durations and bars.

/// Below this width there is no room for a bar, so we drop it.
pub(super) const BAR_MIN_WIDTH: usize = 76;

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
/// Walks the whole unit table, so that anything past a terabyte still fits the
/// fixed layout budget instead of growing without bound.
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
    use crate::ui::width::display_width;

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
}
