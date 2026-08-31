//! Making a network-derived string safe to hand to a terminal.
//!
//! ## Why this is centralised
//!
//! `engine::safe_filename` already stripped control characters, but only for
//! *filenames*. Several other strings reach the terminal straight from the
//! network without passing through it:
//!
//! - Directory labels, which the scraper percent-decodes and hands to
//!   `ScanSpinner::dir`, which emits them directly.
//! - Scan failure notes, which interpolate a URL and an error message.
//! - API body snippets in error text.
//! - Hoster-supplied filenames on paths that predate `safe_filename`.
//!
//! An ESC in any of those is enough to erase the line rdm just drew and forge
//! output over it. An OSC sequence goes further: `ESC ] 0 ; title BEL` retitles
//! the window, and `ESC ] 52 ; c ; base64 BEL` writes to the clipboard on
//! terminals that implement it. So every such string goes through
//! [`terminal_safe`] instead of each call site remembering.
//!
//! ## What is removed
//!
//! Anything that can move the cursor, start a control sequence, begin a new
//! physical row, or reorder what the reader sees:
//!
//! - C0 controls and DEL, which includes ESC, CR, LF and NUL.
//! - C1 controls U+0080..=U+009F. U+009B is a single-byte CSI and U+009D a
//!   single-byte OSC on terminals that still decode them, so stripping ESC
//!   alone is not enough.
//! - Bidi marks, embeddings, overrides and isolates. U+202E turns
//!   `holiday<RLO>gpj.exe` into something drawn as `holidayexe.jpg`.
//! - U+2028 and U+2029, which some terminals treat as line breaks.
//! - The zero-width characters used to pad a name into looking like another
//!   one, and U+FEFF, which is invisible.
//!
//! Printable text is kept rather than the whole string being dropped, so the
//! user still sees that something odd arrived instead of the name silently
//! vanishing.

/// Strips everything a terminal would act on rather than draw.
pub fn terminal_safe(s: &str) -> String {
    s.chars().filter(|c| !is_terminal_unsafe(*c)).collect()
}

/// [`terminal_safe`], but a string that sanitises away entirely becomes
/// `placeholder` rather than an empty label.
pub fn terminal_safe_or(s: &str, placeholder: &str) -> String {
    let cleaned = terminal_safe(s);
    if cleaned.trim().is_empty() {
        placeholder.to_owned()
    } else {
        cleaned
    }
}

fn is_terminal_unsafe(c: char) -> bool {
    let code = c as u32;

    // C0 (which contains ESC, CR, LF and NUL) and DEL.
    if code < 0x20 || code == 0x7f {
        return true;
    }

    // C1. U+009B is CSI and U+009D is OSC as single bytes.
    if (0x80..=0x9f).contains(&code) {
        return true;
    }

    matches!(
        c,
        // Bidi marks, embeddings, overrides, isolates.
        '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
        // Line and paragraph separators.
        | '\u{2028}' | '\u{2029}'
        // Zero-width and invisible.
        | '\u{200b}'..='\u{200d}' | '\u{feff}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_text_is_untouched() {
        assert_eq!(terminal_safe("Season 1/Episode 02.mkv"), "Season 1/Episode 02.mkv");
        assert_eq!(terminal_safe("h\u{e5}llo \u{4e16}\u{754c}"), "h\u{e5}llo \u{4e16}\u{754c}");
        assert_eq!(terminal_safe("100% done"), "100% done");
    }

    /// A CSI sequence, which is what erases the line rdm just drew. The
    /// directory name arrives percent-encoded in a listing and is decoded
    /// before it reaches the spinner, so this is the decoded form.
    #[test]
    fn a_csi_sequence_cannot_survive() {
        // ESC [ 2 K — erase line.
        assert_eq!(terminal_safe("\u{1b}[2Kfake progress"), "[2Kfake progress");
        // ESC [ 1 A — cursor up, which lets a name scribble over the line above.
        assert_eq!(terminal_safe("dir\u{1b}[1A"), "dir[1A");
        // Single-byte C1 CSI.
        assert_eq!(terminal_safe("\u{9b}31mred"), "31mred");
    }

    /// An OSC sequence retitles the window, and OSC 52 reaches the clipboard
    /// on terminals that implement it.
    #[test]
    fn an_osc_sequence_cannot_survive() {
        // ESC ] 0 ; pwned BEL — set window title.
        assert_eq!(terminal_safe("\u{1b}]0;pwned\u{7}dir"), "]0;pwneddir");
        // ESC ] 52 ; c ; <base64> BEL — clipboard write.
        let clipboard = "\u{1b}]52;c;cm0gLXJmIH4K\u{7}holiday";
        let cleaned = terminal_safe(clipboard);
        assert!(!cleaned.contains('\u{1b}'), "{cleaned}");
        assert!(!cleaned.contains('\u{7}'), "{cleaned}");
        // Single-byte C1 OSC.
        assert_eq!(terminal_safe("\u{9d}0;pwned"), "0;pwned");
    }

    #[test]
    fn carriage_returns_and_newlines_cannot_forge_lines() {
        assert_eq!(terminal_safe("safe\r\rSUCCESS"), "safeSUCCESS");
        assert_eq!(terminal_safe("line one\nline two"), "line oneline two");
        assert_eq!(terminal_safe("a\u{2028}b\u{2029}c"), "abc");
    }

    #[test]
    fn a_name_cannot_lie_about_its_direction() {
        assert_eq!(terminal_safe("holiday\u{202e}gpj.exe"), "holidaygpj.exe");
        assert_eq!(terminal_safe("a\u{2066}b\u{2069}c"), "abc");
    }

    #[test]
    fn nul_cannot_survive() {
        assert_eq!(terminal_safe("evil.sh\u{0}.txt"), "evil.sh.txt");
    }

    #[test]
    fn zero_width_padding_is_removed() {
        assert_eq!(terminal_safe("in\u{200b}voice\u{feff}.pdf"), "invoice.pdf");
    }

    #[test]
    fn a_string_that_sanitises_away_can_fall_back_to_a_placeholder() {
        assert_eq!(terminal_safe("\u{1b}\u{1b}"), "");
        assert_eq!(terminal_safe_or("\u{1b}\u{1b}", "(unnamed)"), "(unnamed)");
        assert_eq!(terminal_safe_or("   ", "(unnamed)"), "(unnamed)");
        assert_eq!(terminal_safe_or("real.mkv", "(unnamed)"), "real.mkv");
    }

    /// Nothing that survives may still be actionable: no ESC, no C0, no C1.
    #[test]
    fn the_output_is_always_inert() {
        let nasty = "\u{1b}]0;t\u{7}\u{1b}[2K\u{9b}A\u{202e}x\u{0}\r\n\u{2028}\u{9d}q";
        let cleaned = terminal_safe(nasty);

        for c in cleaned.chars() {
            let code = c as u32;
            assert!(code >= 0x20, "C0 survived: {:?}", c);
            assert_ne!(code, 0x7f, "DEL survived");
            assert!(!(0x80..=0x9f).contains(&code), "C1 survived: {:?}", c);
        }
    }
}
