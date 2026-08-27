//! Reducing a name the network chose into a name we are willing to write.
//!
//! Two things on the generic HTTP path are picked by the server rather than by
//! the user: the `filename` in a `Content-Disposition` header, and the last
//! path segment of the URL. Both are percent-decoded before use, so a
//! separator can arrive encoded \u{2014} `..%2f..%2f.ssh%2fauthorized_keys` is one
//! harmless-looking segment until it is decoded, and a traversal immediately
//! afterwards.
//!
//! The folder-listing hosters each grew their own `safe_component` for exactly
//! this (see `hoster::gdrive`, `hoster::onedrive`, `hoster::pixeldrain`). This
//! is the same idea for the paths those modules do not cover, plus one thing
//! they do not need: a name reaching here is also printed to a terminal, so
//! the control sequences that let it repaint the screen are stripped in one
//! place rather than at every call site that prints one.

/// The longest single component ext4, APFS and NTFS all accept. Bytes, not
/// characters, because that is what the filesystem counts.
const MAX_COMPONENT_BYTES: usize = 255;

/// Turns a server-supplied name into exactly one safe path component.
///
/// Everything up to and including the last separator is dropped, so the result
/// can never contain a directory and can never be absolute. Callers must
/// percent-decode *before* calling: `%2f` only becomes a separator once
/// decoded, and splitting first lets it straight through.
///
/// Returns `None` when nothing usable survives. That is deliberate \u{2014} the
/// caller then keeps the name it already trusted rather than this function
/// inventing one.
pub fn safe_filename(name: &str) -> Option<String> {
    // Both separators, always. A Windows-style name arriving on a Unix host
    // still has to lose its directories, or `..\..\..\etc\passwd` counts as a
    // single component to `rsplit('/')`.
    let leaf = name.rsplit(['/', '\\']).next().unwrap_or_default();
    let leaf = strip_drive_prefix(leaf);

    let cleaned: String = leaf.chars().filter(|c| !is_unsafe(*c)).collect();

    // Windows silently drops trailing dots and spaces from real names, so a
    // file saved as `report. ` lands at `report` \u{2014} a path we did not choose.
    // Trimming also reduces `.` and `..` to nothing, which the emptiness check
    // below then rejects.
    let trimmed = trim_tail(cleaned.trim());

    // Truncating can expose a fresh trailing dot, so trim once more after it.
    let capped = trim_tail(truncate_on_char_boundary(trimmed, MAX_COMPONENT_BYTES));

    (!capped.is_empty()).then(|| capped.to_owned())
}

/// Characters that must never reach a path or a terminal.
fn is_unsafe(c: char) -> bool {
    // Unicode's Cc category: C0, DEL and C1. Everything that matters is in
    // there \u{2014} NUL truncates the path at the syscall boundary, ESC introduces
    // an ANSI sequence, and U+009B is a single-byte CSI on terminals that
    // still decode it.
    c.is_control()
        // Less terminal control than lying to the reader: U+202E reverses
        // everything after it, so a name ending `\u{202e}gpj.exe` is drawn
        // ending `exe.jpg`. That is the entire trick.
        || matches!(
            c,
            '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
        )
        // A new physical row on some terminals, which is enough to forge a
        // prompt underneath a progress line.
        || matches!(c, '\u{2028}' | '\u{2029}')
}

/// Trims the trailing dots and spaces Windows would have dropped anyway.
fn trim_tail(s: &str) -> &str {
    s.trim_end_matches(['.', ' '])
}

/// Drops a `C:` prefix on Windows, where a drive-relative name resolves
/// against that drive's current directory rather than the one we chose.
///
/// Gated, because on Unix a colon is an ordinary filename character and
/// stripping it would corrupt legitimate names like `S1:E02.mkv`.
fn strip_drive_prefix(leaf: &str) -> &str {
    if !cfg!(windows) {
        return leaf;
    }
    match leaf.as_bytes() {
        [drive, b':', ..] if drive.is_ascii_alphabetic() => &leaf[2..],
        _ => leaf,
    }
}

/// Truncates to at most `max` bytes without splitting a character in half.
fn truncate_on_char_boundary(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_cannot_carry_a_directory() {
        assert_eq!(
            safe_filename("../../.ssh/authorized_keys").as_deref(),
            Some("authorized_keys")
        );
        assert_eq!(safe_filename("/etc/cron.d/rdm").as_deref(), Some("rdm"));
        assert_eq!(
            safe_filename("..\\..\\Windows\\System32\\drivers\\etc\\hosts").as_deref(),
            Some("hosts")
        );
        assert_eq!(
            safe_filename("C:\\Windows\\evil.dll").as_deref(),
            Some("evil.dll")
        );
    }

    /// On Windows only: `C:evil.dll` is a single component, but resolved
    /// against that drive's current directory instead of ours.
    #[cfg(windows)]
    #[test]
    fn a_drive_relative_name_loses_its_drive() {
        assert_eq!(safe_filename("C:evil.dll").as_deref(), Some("evil.dll"));
    }

    /// A colon is legal in a Unix filename, so it must survive there.
    #[cfg(not(windows))]
    #[test]
    fn a_colon_is_an_ordinary_character_on_unix() {
        assert_eq!(safe_filename("S1:E02.mkv").as_deref(), Some("S1:E02.mkv"));
    }

    #[test]
    fn a_name_with_nothing_left_is_rejected_rather_than_invented() {
        // The caller has a name it already trusts to fall back on; one made up
        // here would be worse than admitting there isn't one.
        assert_eq!(safe_filename(""), None);
        assert_eq!(safe_filename("   "), None);
        assert_eq!(safe_filename("."), None);
        assert_eq!(safe_filename(".."), None);
        assert_eq!(safe_filename("../.."), None);
        assert_eq!(safe_filename("/"), None);
        assert_eq!(safe_filename("/etc/"), None);
        assert_eq!(safe_filename("\u{1b}"), None);
    }

    #[test]
    fn terminal_control_sequences_do_not_survive() {
        // ESC[2K erases the line the progress bar just drew, so a filename
        // carrying it can forge whatever it likes over rdm's own output. The
        // printable remainder is kept, so the user still sees something odd
        // happened rather than the name silently vanishing.
        assert_eq!(
            safe_filename("\u{1b}[2Kinvoice.pdf").as_deref(),
            Some("[2Kinvoice.pdf")
        );
        // A single-byte C1 CSI, which some terminals still decode.
        assert_eq!(
            safe_filename("\u{9b}31mred.bin").as_deref(),
            Some("31mred.bin")
        );
        // A bare carriage return is enough to overwrite the current line.
        assert_eq!(
            safe_filename("safe.txt\r\rSUCCESS").as_deref(),
            Some("safe.txtSUCCESS")
        );
        // NUL truncates the path at the syscall boundary, so the name the
        // kernel sees is not the name that was checked.
        assert_eq!(
            safe_filename("evil.sh\u{0}.txt").as_deref(),
            Some("evil.sh.txt")
        );
    }

    #[test]
    fn a_name_cannot_lie_about_its_own_extension() {
        // Displayed as `holidayexe.jpg`; actually an executable.
        assert_eq!(
            safe_filename("holiday\u{202e}gpj.exe").as_deref(),
            Some("holidaygpj.exe")
        );
    }

    #[test]
    fn trailing_dots_and_spaces_go_because_windows_drops_them() {
        assert_eq!(safe_filename("report. ").as_deref(), Some("report"));
        assert_eq!(safe_filename(" spaced.txt ").as_deref(), Some("spaced.txt"));
        // Leading and interior dots stay: hidden files and double extensions
        // are both perfectly ordinary names.
        assert_eq!(safe_filename(".gitignore").as_deref(), Some(".gitignore"));
        assert_eq!(
            safe_filename("archive.tar.gz").as_deref(),
            Some("archive.tar.gz")
        );
    }

    #[test]
    fn long_names_are_capped_without_splitting_a_character() {
        let long = format!("{}.bin", "\u{e5}".repeat(400));
        let capped = safe_filename(&long).expect("there is still a name here");
        assert!(capped.len() <= MAX_COMPONENT_BYTES);
        // Every character intact: a half-written one would not have survived
        // being collected back into a String.
        assert!(capped.chars().all(|c| c == '\u{e5}'));
    }
}
