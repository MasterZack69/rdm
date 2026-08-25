//! Terminal detection, the width we are allowed to fill, and the raw write
//! path every widget draws through.

use std::io::{IsTerminal, Write};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

/// Minimum gap between single-line widget repaints.
pub(super) const SOLO_TICK: Duration = Duration::from_millis(150);

/// Is stderr an interactive terminal? Decides ANSI vs. plain output.
pub fn is_tty() -> bool {
    std::io::stderr().is_terminal()
}

/// Total terminal columns.
pub fn term_width() -> usize {
    #[cfg(unix)]
    unsafe {
        let mut ws: libc::winsize = std::mem::zeroed();
        if libc::ioctl(libc::STDERR_FILENO, libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 20 {
            return (ws.ws_col as usize).min(200);
        }
    }

    std::env::var("COLUMNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|w| *w > 20)
        .map(|w| w.min(200))
        .unwrap_or(80)
}

/// Columns we are allowed to fill. One short of the real width: writing the
/// final column makes some terminals wrap immediately.
pub(super) fn draw_width() -> usize {
    term_width().saturating_sub(1).max(20)
}

pub(super) fn emit(s: &str) {
    let mut err = std::io::stderr();
    let _ = err.write_all(s.as_bytes());
    let _ = err.flush();
}

/// A poisoned progress mutex should never take the download down with it.
pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Erases the current line (single-line widgets only).
pub fn clear_line() {
    if is_tty() {
        emit("\r\x1b[2K");
    }
}
