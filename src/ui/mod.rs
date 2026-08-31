//! Terminal presentation layer.
//!
//! Everything rdm draws while work is in flight goes through this module so
//! that concurrent downloads can't scribble over each other:
//!
//!   - [`Board`] — a live block: one short line per in-flight file plus a
//!     summary line. Used by the queue (and therefore by `sync` and directory
//!     downloads).
//!   - [`SoloBar`] — a single refreshing line for one-file downloads.
//!   - [`CountProgress`] — a counter + ETA line for non-byte work.
//!   - [`ScanSpinner`] — a live counter for directory scraping.
//!
//! ## Submodules
//!
//! - `term`: terminal detection, the drawable width, and the raw write path.
//! - `width`: column-accurate measurement, clipping and padding.
//! - `rate`: the sliding-window byte-rate estimator.
//! - `sink`: the reporting contract the download engine drives.
//! - `solo`, `board`, `count`, `spinner`: the four widgets above.
//! - `compose`: the shared per-file line builder.
//! - `format`: sizes, speeds, durations and bars.
//! - `sanitize`: strips terminal control sequences from network-derived text.
//!
//! ## Rules this module must never break
//!
//! 1. **Every emitted line is clipped to `term_width() - 1`.** A line that is
//!    even one column too long wraps onto a second physical row, and then the
//!    `ESC[nA` cursor math (which counts logical lines) is wrong forever after
//!    — the block scrolls instead of redrawing. This is what caused the
//!    repeated-line spam.
//! 2. **Width is measured in columns, not chars.** Emoji occupy two columns;
//!    counting them as one re-introduces rule 1.
//! 3. **A live block never ends with a newline.** The cursor stays parked on
//!    the last drawn line so the next frame can move up exactly `lines - 1`.
//! 4. **Never hold one of these mutexes across a call that might take it
//!    again.** They are plain `std` mutexes, so they are not reentrant and a
//!    second acquisition on the same thread deadlocks the whole download.
//!    Beware of guards created inside a larger expression: they live until the
//!    end of the *statement*, not the end of the sub-expression.
//! 5. **Anything derived from the network goes through [`terminal_safe`]
//!    before it is drawn.** A directory name, a hoster filename or an error
//!    body can carry ESC or OSC sequences, and rules 1-3 only hold for text
//!    the terminal draws rather than acts on. See `sanitize`.
//!
//! Everything writes to stderr and degrades to plain, scroll-safe output when
//! stderr is not a TTY.

mod board;
mod compose;
mod count;
mod format;
mod rate;
mod sanitize;
mod sink;
mod solo;
mod spinner;
mod term;
mod width;

pub use board::{Board, Lane};
pub use count::CountProgress;
pub use format::{
    format_duration, format_eta, format_size, format_speed, progress_bar, short_duration,
    short_size, short_speed,
};
pub use rate::Rate;
pub use sanitize::{terminal_safe, terminal_safe_or};
pub use sink::{ProgressSink, Silent, SlotState, silent};
pub use solo::SoloBar;
pub use spinner::ScanSpinner;
pub use term::{clear_line, is_tty, term_width};
pub use width::{clip, ellipsize};
