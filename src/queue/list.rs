//! The `queue list` table.

use super::item::Status;
use super::state::Queue;
use crate::ui;

impl Queue {
    pub fn print_list(&self) {
        if self.items.is_empty() {
            eprintln!("  Queue is empty.");
            return;
        }

        let width = ui::term_width();
        // Name and size columns are fixed; the URL gets whatever is left.
        let name_col = 34usize;
        let size_col = 10usize;
        let url_col = width.saturating_sub(name_col + size_col + 22).max(20);

        eprintln!();
        eprintln!(
            "  {:>4}  {:<14}  {:<name_col$}  {:>size_col$}  URL",
            "ID",
            "Status",
            "File",
            "Size",
            name_col = name_col,
            size_col = size_col,
        );
        eprintln!("  {}", "\u{2500}".repeat(width.saturating_sub(4).min(160)));

        for item in &self.items {
            let status = match &item.status {
                Status::Pending => "\u{23f3} pending",
                Status::Downloading => "\u{2b07} downloading",
                Status::Complete => "\u{2705} complete",
                Status::Failed { .. } => "\u{274c} failed",
                Status::Skipped => "\u{23ed} skipped",
            };

            let size = match item.size {
                Some(b) if b > 0 => ui::format_size(b),
                _ => "\u{2014}".to_owned(),
            };

            eprintln!(
                "  {:>4}  {:<14}  {:<name_col$}  {:>size_col$}  {}",
                item.id,
                status,
                pad_display(&ui::ellipsize(&item.display_name(), name_col), name_col),
                size,
                ui::ellipsize(&item.url, url_col),
                name_col = name_col,
                size_col = size_col,
            );

            if let Status::Failed {
                ref reason,
                attempts,
            } = item.status
            {
                eprintln!(
                    "        \u{21b3} error after {} attempt{}: {}",
                    attempts,
                    if attempts == 1 { "" } else { "s" },
                    ui::ellipsize(reason, width.saturating_sub(34)),
                );
            }
        }

        let s = self.stats();
        eprintln!();
        eprintln!(
            "  {} total \u{b7} {} pending \u{b7} {} complete \u{b7} {} failed \u{b7} {} skipped",
            s.total, s.pending, s.complete, s.failed, s.skipped
        );
        if s.bytes > 0 {
            eprintln!("  {} downloaded", ui::format_size(s.bytes));
        }
        if s.failed > 0 {
            eprintln!("  Run `rdm queue retry failed` to requeue the failures.");
        }
    }
}

/// `{:<width$}` pads by bytes; names are UTF-8, so pad by characters instead.
fn pad_display(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len >= width {
        s.to_owned()
    } else {
        format!("{}{}", s, " ".repeat(width - len))
    }
}
