//! Column accounting: measuring, clipping and padding text the way a terminal
//! actually renders it.
//!
//! Rule 2 of the module contract lives here, and [`clip`] is the single choke
//! point that keeps rule 1 true.

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
        || matches!(
            c,
            0x26f5 | 0x26fa | 0x26fd | 0x2705 | 0x270a..=0x270b | 0x2728
        )
        || matches!(c, 0x274c | 0x274e | 0x2753..=0x2755 | 0x2757 | 0x2795..=0x2797)
        || matches!(c, 0x27b0 | 0x27bf | 0x2b1b..=0x2b1c | 0x2b50 | 0x2b55);
    if wide { 2 } else { 1 }
}

/// Column width of a string.
pub(super) fn display_width(s: &str) -> usize {
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

/// The name callers outside this module use when trimming file names.
pub fn ellipsize(s: &str, max: usize) -> String {
    clip(s, max)
}

/// Right-pads to `width` columns (`{:<width$}` counts bytes, not columns).
pub(super) fn pad(s: &str, width: usize) -> String {
    let len = display_width(s);
    if len >= width {
        s.to_owned()
    } else {
        format!("{}{}", s, " ".repeat(width - len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
