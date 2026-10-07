use unicode_width::UnicodeWidthChar;

//! Column accounting: measuring, clipping and padding text the way a terminal
//! actually renders it.
//!
//! Rule 2 of the module contract lives here, and [`clip`] is the single choke
//! point that keeps rule 1 true.

/// Approximate column width of a character. Only needs to be right about the
/// two cases that matter: zero-width joiners/selectors and double-width
/// glyphs (CJK and emoji), which is what our own status lines contain.
fn char_width(c: char) -> usize {
    c.width().unwrap_or(0)
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
