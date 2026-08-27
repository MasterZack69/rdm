//! The queue itself: the item list, its statuses, and the locked
//! read-modify-write that every mutation goes through.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;

use super::item::{Item, Status};
use super::lock::FileLock;
use super::store::{atomic_write, dir, queue_file};

#[derive(Debug, Serialize, Deserialize)]
pub struct Queue {
    next_id: u64,
    pub(super) items: Vec<Item>,
}

impl Default for Queue {
    fn default() -> Self {
        Self {
            next_id: 1,
            items: Vec::new(),
        }
    }
}

/// Aggregate item counts, used by the runner summary and `queue list`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub total: usize,
    pub pending: usize,
    pub downloading: usize,
    pub complete: usize,
    pub failed: usize,
    pub skipped: usize,
    pub bytes: u64,
}

impl Queue {
    fn load_inner() -> Self {
        fs::read_to_string(queue_file())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save_inner(&self) -> Result<()> {
        fs::create_dir_all(dir()).context("Failed to create config directory")?;
        let json = serde_json::to_string_pretty(self).context("Failed to serialize queue")?;
        atomic_write(&queue_file(), json.as_bytes())
    }

    pub fn locked<F, T>(f: F) -> Result<T>
    where
        F: FnOnce(&mut Queue) -> Result<T>,
    {
        let _lock = FileLock::transaction()?;
        let mut queue = Self::load_inner();
        let result = f(&mut queue)?;
        queue.save_inner()?;
        Ok(result)
    }

    pub fn load_readonly() -> Self {
        Self::load_inner()
    }

    /// Queues an item that may only be fetched from a public address.
    pub fn add(&mut self, url: String, output: Option<String>, connections: Option<usize>) -> u64 {
        self.add_with_scope(url, output, connections, false)
    }

    /// Queues an item, remembering whether the user waived the address check.
    pub fn add_with_scope(
        &mut self,
        url: String,
        output: Option<String>,
        connections: Option<usize>,
        allow_private: bool,
    ) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.items.push(Item {
            id,
            url,
            output,
            connections,
            status: Status::Pending,
            size: None,
            allow_private,
        });
        id
    }

    pub fn remove(&mut self, id: u64) -> bool {
        let len = self.items.len();
        self.items.retain(|i| i.id != id);
        self.items.len() < len
    }

    pub fn clear_all(&mut self) -> usize {
        let len = self.items.len();
        self.items.clear();
        self.next_id = 1;
        len
    }

    pub fn clear_finished(&mut self) -> usize {
        let len = self.items.len();
        self.items
            .retain(|i| matches!(i.status, Status::Pending | Status::Downloading));
        len - self.items.len()
    }

    pub fn clear_pending(&mut self) -> usize {
        let len = self.items.len();
        self.items.retain(|i| i.status != Status::Pending);
        len - self.items.len()
    }

    pub fn retry_failed(&mut self) -> usize {
        let mut count = 0;
        for item in &mut self.items {
            if matches!(item.status, Status::Failed { .. }) {
                item.status = Status::Pending;
                count += 1;
            }
        }
        count
    }

    pub fn retry_skipped(&mut self) -> usize {
        let mut count = 0;
        for item in &mut self.items {
            if item.status == Status::Skipped {
                item.status = Status::Pending;
                count += 1;
            }
        }
        count
    }

    pub fn retry_item(&mut self, id: u64) -> bool {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            match item.status {
                Status::Failed { .. } | Status::Skipped => {
                    item.status = Status::Pending;
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    pub(super) fn next_pending(&self) -> Option<&Item> {
        self.items.iter().find(|i| i.status == Status::Pending)
    }

    pub(super) fn set_status(&mut self, id: u64, status: Status) {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            item.status = status;
        }
    }

    /// Records the final status plus the byte count, so `queue list` can show
    /// what was actually downloaded.
    ///
    /// `name` is the filename the downloader discovered, for the links that
    /// carry none. Recording it is what stops `queue list` showing an id in the
    /// File column for the rest of the item's life. An output the user chose is
    /// never overwritten.
    pub(super) fn finish_item(
        &mut self,
        id: u64,
        status: Status,
        size: Option<u64>,
        name: Option<&str>,
    ) {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            item.status = status;
            if size.is_some() {
                item.size = size;
            }
            if item.output.is_none()
                && let Some(name) = name
            {
                item.output = Some(name.to_owned());
            }
        }
    }

    pub(super) fn attempts_so_far(&self, id: u64) -> u32 {
        match self.items.iter().find(|i| i.id == id) {
            Some(Item {
                status: Status::Failed { attempts, .. },
                ..
            }) => *attempts,
            _ => 0,
        }
    }

    /// Moves every `Downloading` item back to `Pending`. Used on startup (to
    /// recover from a crash) and after a Ctrl+C.
    pub(super) fn requeue_in_flight(&mut self) -> usize {
        let mut count = 0;
        for item in &mut self.items {
            if item.status == Status::Downloading {
                item.status = Status::Pending;
                count += 1;
            }
        }
        count
    }

    pub fn pending_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| i.status == Status::Pending)
            .count()
    }

    /// How many pending items need the MEGA path. Used only to warn about the
    /// serialisation up front, so a stalled-looking board makes sense.
    pub fn pending_mega_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| i.status == Status::Pending && i.is_mega())
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| matches!(i.status, Status::Failed { .. }))
            .count()
    }

    pub fn stats(&self) -> Stats {
        let mut s = Stats {
            total: self.items.len(),
            ..Stats::default()
        };
        for item in &self.items {
            match item.status {
                Status::Pending => s.pending += 1,
                Status::Downloading => s.downloading += 1,
                Status::Complete => s.complete += 1,
                Status::Failed { .. } => s.failed += 1,
                Status::Skipped => s.skipped += 1,
            }
            s.bytes += item.size.unwrap_or(0);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::test_support::{PIXELDRAIN_LINK, queue_with};

    #[test]
    fn ids_are_stable_and_increasing() {
        let mut q = Queue::default();
        let a = q.add("https://x.com/a.bin".into(), None, None);
        let b = q.add("https://x.com/b.bin".into(), None, None);
        assert_eq!((a, b), (1, 2));
        assert!(q.remove(a));
        assert!(!q.remove(a));
        assert_eq!(q.add("https://x.com/c.bin".into(), None, None), 3);
    }

    #[test]
    fn retry_only_touches_finished_failures() {
        let mut q = queue_with(&["https://x.com/a", "https://x.com/b", "https://x.com/c"]);
        q.set_status(
            1,
            Status::Failed {
                reason: "404".into(),
                attempts: 2,
            },
        );
        q.set_status(2, Status::Skipped);
        q.set_status(3, Status::Complete);

        assert!(!q.retry_item(3), "completed items are not retryable");
        assert_eq!(q.retry_failed(), 1);
        assert_eq!(q.retry_skipped(), 1);
        assert_eq!(q.pending_count(), 2);
    }

    #[test]
    fn failure_attempts_accumulate() {
        let mut q = queue_with(&["https://x.com/a"]);
        assert_eq!(q.attempts_so_far(1), 0);
        q.set_status(
            1,
            Status::Failed {
                reason: "boom".into(),
                attempts: 1,
            },
        );
        assert_eq!(q.attempts_so_far(1), 1);
        let attempts = q.attempts_so_far(1) + 1;
        q.set_status(
            1,
            Status::Failed {
                reason: "boom".into(),
                attempts,
            },
        );
        assert_eq!(q.attempts_so_far(1), 2);
    }

    #[test]
    fn finish_item_records_size() {
        let mut q = queue_with(&["https://x.com/a"]);
        q.finish_item(1, Status::Complete, Some(4096), None);
        assert_eq!(q.items[0].size, Some(4096));
        assert_eq!(q.stats().bytes, 4096);

        // A later status change must not wipe a known size.
        q.finish_item(1, Status::Complete, None, None);
        assert_eq!(q.items[0].size, Some(4096));
    }

    /// A link that names nothing gets a real filename only once a downloader
    /// has been there, so the item has to be told afterwards — otherwise
    /// `queue list` shows an id in the File column forever.
    #[test]
    fn a_finished_item_takes_the_name_the_downloader_found() {
        let mut q = queue_with(&[PIXELDRAIN_LINK]);
        assert_eq!(q.items[0].display_name(), "pixeldrain AbCdEf12");

        q.finish_item(1, Status::Complete, Some(4096), Some("holiday.mkv"));
        assert_eq!(q.items[0].display_name(), "holiday.mkv");

        // An output the user chose is theirs, not ours to correct.
        let mut q = Queue::default();
        q.add(PIXELDRAIN_LINK.into(), Some("clips/mine.mkv".into()), None);
        q.finish_item(1, Status::Complete, Some(1), Some("theirs.mkv"));
        assert_eq!(q.items[0].display_name(), "mine.mkv");
    }

    #[test]
    fn interrupted_downloads_are_requeued() {
        let mut q = queue_with(&["https://x.com/a", "https://x.com/b"]);
        q.set_status(1, Status::Downloading);
        q.set_status(2, Status::Complete);
        assert_eq!(q.requeue_in_flight(), 1);
        assert_eq!(q.pending_count(), 1);
    }

    #[test]
    fn clear_variants_target_the_right_items() {
        let mut q = queue_with(&["https://x.com/a", "https://x.com/b", "https://x.com/c"]);
        q.set_status(2, Status::Complete);
        q.set_status(
            3,
            Status::Failed {
                reason: "x".into(),
                attempts: 1,
            },
        );

        assert_eq!(q.clear_finished(), 2);
        assert_eq!(q.stats().total, 1);
        assert_eq!(q.clear_pending(), 1);
        assert_eq!(q.stats().total, 0);
    }

    #[test]
    fn stats_count_every_state() {
        let mut q = queue_with(&["a", "b", "c", "d", "e"]);
        q.set_status(1, Status::Downloading);
        q.set_status(2, Status::Complete);
        q.set_status(
            3,
            Status::Failed {
                reason: "x".into(),
                attempts: 1,
            },
        );
        q.set_status(4, Status::Skipped);

        let s = q.stats();
        assert_eq!(
            (
                s.total,
                s.pending,
                s.downloading,
                s.complete,
                s.failed,
                s.skipped
            ),
            (5, 1, 1, 1, 1, 1)
        );
    }

    #[test]
    fn old_queue_files_without_size_still_load() {
        let json = r#"{"next_id":2,"items":[{"id":1,"url":"https://x.com/a.bin","output":null,"connections":null,"status":"Pending"}]}"#;
        let q: Queue = serde_json::from_str(json).expect("legacy queue.json must still parse");
        assert_eq!(q.pending_count(), 1);
        assert_eq!(q.items[0].size, None);
    }

    #[test]
    fn old_queue_files_without_allow_private_still_load() {
        let json = r#"{"next_id":2,"items":[{"id":1,"url":"https://x.com/a.bin","output":null,"connections":null,"size":null,"status":"Pending"}]}"#;
        let q: Queue = serde_json::from_str(json).expect("legacy queue.json must still parse");
        assert_eq!(q.pending_count(), 1);
        assert!(!q.items[0].allow_private);
    }
}
