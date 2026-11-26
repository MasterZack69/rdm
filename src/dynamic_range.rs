//! Range ownership for dynamic tail splitting.
//!
//! A receiver reserves bytes before it writes them. A split can only take
//! the unreserved tail, so buffered or in-flight writes never overlap it.
use std::sync::Mutex;

pub(crate) struct RangeLimit(Mutex<Bounds>);
struct Bounds {
    end: u64,      // inclusive
    reserved: u64, // next unreserved byte
}
impl RangeLimit {
    pub(crate) fn new(end: u64, next: u64) -> Self {
        Self(Mutex::new(Bounds {
            end,
            reserved: next,
        }))
    }
    pub(crate) fn end(&self) -> u64 {
        self.0.lock().unwrap().end
    }
    pub(crate) fn reset(&self, next: u64) {
        let mut b = self.0.lock().unwrap();
        b.reserved = next;
    }
    pub(crate) fn claim(&self, at: u64, requested: usize) -> usize {
        let mut b = self.0.lock().unwrap();
        assert_eq!(at, b.reserved, "out-of-order range claim");
        let available = b.end.saturating_add(1).saturating_sub(at);
        let n = available.min(requested as u64) as usize;
        b.reserved += n as u64;
        n
    }
    pub(crate) fn remaining(&self) -> u64 {
        let b = self.0.lock().unwrap();
        b.end.saturating_add(1).saturating_sub(b.reserved)
    }
    /// Returns the new tail [start, old_end] and shortens the owner.
    pub(crate) fn split(&self, min_piece: u64) -> Option<(u64, u64)> {
        let mut b = self.0.lock().unwrap();
        let remaining = b.end.saturating_add(1).saturating_sub(b.reserved);
        if remaining / 2 < min_piece {
            return None;
        }
        let start = b.reserved + remaining / 2;
        let old_end = b.end;
        b.end = start - 1;
        Some((start, old_end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_excludes_reserved_bytes_and_claims_are_clipped() {
        let range = RangeLimit::new(99, 0);
        assert_eq!(range.claim(0, 30), 30);
        assert_eq!(range.split(10), Some((65, 99)));
        assert_eq!(range.claim(30, 50), 35);
        assert_eq!(range.claim(65, 5), 0);
        assert_eq!(range.remaining(), 0);
    }
    #[test]
    fn retry_can_release_uncommitted_claims_without_crossing_split() {
        let range = RangeLimit::new(99, 0);
        assert_eq!(range.claim(0, 30), 30);
        assert_eq!(range.split(10), Some((65, 99)));
        range.reset(10);
        assert_eq!(range.claim(10, 90), 55);
    }
    #[test]
    fn no_split_below_minimum() {
        let range = RangeLimit::new(18, 0);
        assert_eq!(range.split(10), None);
    }
}
