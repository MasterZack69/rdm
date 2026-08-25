//! Skip and stop, sent from another terminal.
//!
//! A second `rdm` writes one word to a file and exits; the runner's watcher
//! polls it. That is the whole mechanism behind `queue skip` and `queue stop`.

use anyhow::Result;
use std::fs;

use super::store::{atomic_write, dir, signal_file};

pub fn send_signal(sig: &str) -> Result<()> {
    fs::create_dir_all(dir())?;
    atomic_write(&signal_file(), sig.as_bytes())
}

pub(super) fn read_signal() -> Option<String> {
    fs::read_to_string(signal_file())
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

pub(super) fn clear_signal() {
    let _ = fs::remove_file(signal_file());
}
