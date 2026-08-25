//! Telling the user what a sync is about to do.

use std::io::Write;

/// How many paths to show before collapsing the rest into a count.
const SAMPLE: usize = 20;

/// Prints at most [`SAMPLE`] entries, then says how many were left out.
pub(super) fn print_sample<I: Iterator<Item = String>>(marker: &str, items: I, total: usize) {
    for item in items.take(SAMPLE) {
        eprintln!("     {} {}", marker, item);
    }
    if total > SAMPLE {
        eprintln!("     \u{2026} and {} more", total - SAMPLE);
    }
}

/// Asks before a delete large enough to suggest the listing was incomplete.
///
/// Returns whether to go ahead.
pub(super) fn confirm_bulk_delete(to_delete: usize, total_local: usize) -> bool {
    if total_local == 0 {
        return true;
    }

    let pct = (to_delete as f64 / total_local as f64) * 100.0;
    if to_delete <= 10 || pct <= 50.0 {
        return true;
    }

    eprintln!(
        "  \u{26a0} Warning: about to delete {} of {} local files ({:.0}%)",
        to_delete, total_local, pct,
    );
    eprintln!("    This usually means the remote listing is incomplete.");
    eprint!("    Continue? [y/N]: ");
    let _ = std::io::stderr().flush();

    let mut input = String::new();
    std::io::stdin().read_line(&mut input).ok();

    if matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
        true
    } else {
        eprintln!("  \u{26d4} Aborted.");
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_delete_only_prompts_when_it_is_drastic() {
        // Small deletions and small proportions never prompt, so these return
        // true without touching stdin.
        assert!(confirm_bulk_delete(0, 0));
        assert!(confirm_bulk_delete(5, 6));
        assert!(confirm_bulk_delete(20, 100));
    }
}
