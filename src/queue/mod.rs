//! The download queue: persistent state, cross-process locking, and the
//! runner that works through it.
//!
//! Presentation lives in [`crate::ui`]. The runner owns a
//! [`Board`](crate::ui::Board) and hands each worker a lane, so every
//! in-flight file gets its own progress line with a real per-file ETA, and
//! finished items scroll above the live block instead of fighting with it.
//!
//! ## One runner, several downloaders
//!
//! An item is an ordinary HTTP download, a MEGA link, or a share link for
//! OneDrive, Google Drive or pixeldrain — and each needs different machinery
//! to become a fetchable address. `dispatch` picks between them and flattens
//! every result into one outcome type, so everything downstream (status
//! writing, skip detection, board logging) stays written once.
//!
//! The module is laid out as:
//!
//! - `item`: one queued item, and what to call it before anything is fetched.
//! - `state`: the item list, its statuses, and the locked mutations.
//! - `runner`: works through the pending items, `parallel` at a time.
//! - `dispatch`: picks the downloader for an item and flattens its result.
//! - `share`: resolving OneDrive, Google Drive and pixeldrain links.
//! - `list`: the `queue list` table.
//! - `lock`: the queue-file and processor locks.
//! - `store`: where the state lives on disk, and how it is written.
//! - `signals`: skip/stop sent from another terminal.

mod dispatch;
mod item;
mod list;
mod lock;
mod runner;
mod share;
mod signals;
mod state;
mod store;

#[cfg(test)]
mod test_support;

pub use item::{Item, Status};
pub use lock::FileLock;
pub use runner::start;
pub use signals::send_signal;
pub use state::{Queue, Stats};
