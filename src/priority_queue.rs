use std::collections::VecDeque;

use crate::queue_state::Priority;

pub struct PriorityQueue<T> {
    high: VecDeque<T>,
    normal: VecDeque<T>,
    low: VecDeque<T>,
}

impl<T> PriorityQueue<T> {
    pub fn new() -> Self {
        Self {
            high: VecDeque::new(),
            normal: VecDeque::new(),
            low: VecDeque::new(),
        }
    }

    fn lane_mut(&mut self, priority: Priority) -> &mut VecDeque<T> {
        match priority {
            Priority::High => &mut self.high,
            Priority::Normal => &mut self.normal,
            Priority::Low => &mut self.low,
        }
    }

    fn lane(&self, priority: Priority) -> &VecDeque<T> {
        match priority {
            Priority::High => &self.high,
            Priority::Normal => &self.normal,
            Priority::Low => &self.low,
        }
    }

    pub fn push_back(&mut self, priority: Priority, item: T) {
        self.lane_mut(priority).push_back(item);
    }

    pub fn pop_front(&mut self) -> Option<T> {
        if let Some(item) = self.high.pop_front() {
            return Some(item);
        }
        if let Some(item) = self.normal.pop_front() {
            return Some(item);
        }
        self.low.pop_front()
    }

    pub fn len(&self) -> usize {
        self.high.len() + self.normal.len() + self.low.len()
    }

    pub fn is_empty(&self) -> bool {
        self.high.is_empty() && self.normal.is_empty() && self.low.is_empty()
    }

    pub fn remove_by<F>(&mut self, predicate: F) -> Option<T>
    where
        F: Fn(&T) -> bool,
    {
        for lane in [&mut self.high, &mut self.normal, &mut self.low] {
            if let Some(pos) = lane.iter().position(&predicate) {
                return lane.remove(pos);
            }
        }
        None
    }

    pub fn retain<F>(&mut self, predicate: F)
    where
        F: Fn(&T) -> bool,
    {
        self.high.retain(&predicate);
        self.normal.retain(&predicate);
        self.low.retain(&predicate);
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.high.iter().chain(self.normal.iter()).chain(self.low.iter())
    }

    pub fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.high.drain(..).chain(self.normal.drain(..)).chain(self.low.drain(..))
    }

    pub fn find_lane(&self, predicate: impl Fn(&T) -> bool) -> Option<Priority> {
        if self.high.iter().any(&predicate) {
            return Some(Priority::High);
        }
        if self.normal.iter().any(&predicate) {
            return Some(Priority::Normal);
        }
        if self.low.iter().any(&predicate) {
            return Some(Priority::Low);
        }
        None
    }

    pub fn move_to(&mut self, from: Priority, to: Priority, predicate: impl Fn(&T) -> bool) -> bool {
        if from == to {
            return false;
        }
        let lane = self.lane_mut(from);
        if let Some(pos) = lane.iter().position(predicate) {
            if let Some(item) = lane.remove(pos) {
                self.lane_mut(to).push_back(item);
                return true;
            }
        }
        false
    }

    pub fn len_for(&self, priority: Priority) -> usize {
        self.lane(priority).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let q: PriorityQueue<u32> = PriorityQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn test_push_pop_single_lane() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Normal, 1);
        q.push_back(Priority::Normal, 2);
        q.push_back(Priority::Normal, 3);

        assert_eq!(q.pop_front(), Some(1));
        assert_eq!(q.pop_front(), Some(2));
        assert_eq!(q.pop_front(), Some(3));
        assert_eq!(q.pop_front(), None);
    }

    #[test]
    fn test_priority_order() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Low, "low1");
        q.push_back(Priority::Normal, "norm1");
        q.push_back(Priority::High, "high1");
        q.push_back(Priority::Low, "low2");
        q.push_back(Priority::High, "high2");
        q.push_back(Priority::Normal, "norm2");

        assert_eq!(q.pop_front(), Some("high1"));
        assert_eq!(q.pop_front(), Some("high2"));
        assert_eq!(q.pop_front(), Some("norm1"));
        assert_eq!(q.pop_front(), Some("norm2"));
        assert_eq!(q.pop_front(), Some("low1"));
        assert_eq!(q.pop_front(), Some("low2"));
        assert_eq!(q.pop_front(), None);
    }

    #[test]
    fn test_fifo_within_priority() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::High, 10);
        q.push_back(Priority::High, 20);
        q.push_back(Priority::High, 30);

        assert_eq!(q.pop_front(), Some(10));
        assert_eq!(q.pop_front(), Some(20));
        assert_eq!(q.pop_front(), Some(30));
    }

    #[test]
    fn test_len() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::High, 1);
        q.push_back(Priority::Normal, 2);
        q.push_back(Priority::Low, 3);
        assert_eq!(q.len(), 3);
        assert_eq!(q.len_for(Priority::High), 1);
        assert_eq!(q.len_for(Priority::Normal), 1);
        assert_eq!(q.len_for(Priority::Low), 1);
    }

    #[test]
    fn test_remove_by() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Normal, 10);
        q.push_back(Priority::Normal, 20);
        q.push_back(Priority::Normal, 30);

        let removed = q.remove_by(|&x| x == 20);
        assert_eq!(removed, Some(20));
        assert_eq!(q.len(), 2);
        assert_eq!(q.pop_front(), Some(10));
        assert_eq!(q.pop_front(), Some(30));
    }

    #[test]
    fn test_remove_by_across_lanes() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::High, 1);
        q.push_back(Priority::Normal, 2);
        q.push_back(Priority::Low, 3);

        let removed = q.remove_by(|&x| x == 2);
        assert_eq!(removed, Some(2));
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn test_remove_by_not_found() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Normal, 1);
        assert_eq!(q.remove_by(|&x| x == 99), None);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn test_retain() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::High, 1);
        q.push_back(Priority::High, 2);
        q.push_back(Priority::Normal, 3);
        q.push_back(Priority::Normal, 4);
        q.push_back(Priority::Low, 5);

        q.retain(|&x| x % 2 != 0);
        assert_eq!(q.len(), 3);
        assert_eq!(q.pop_front(), Some(1));
        assert_eq!(q.pop_front(), Some(3));
        assert_eq!(q.pop_front(), Some(5));
    }

    #[test]
    fn test_iter_order() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Low, 3);
        q.push_back(Priority::High, 1);
        q.push_back(Priority::Normal, 2);

        let items: Vec<&i32> = q.iter().collect();
        assert_eq!(items, vec![&1, &2, &3]);
    }

    #[test]
    fn test_drain() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Low, 30);
        q.push_back(Priority::High, 10);
        q.push_back(Priority::Normal, 20);

        let items: Vec<i32> = q.drain().collect();
        assert_eq!(items, vec![10, 20, 30]);
        assert!(q.is_empty());
    }

    #[test]
    fn test_find_lane() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Low, 3);
        q.push_back(Priority::High, 1);

        assert_eq!(q.find_lane(|&x| x == 1), Some(Priority::High));
        assert_eq!(q.find_lane(|&x| x == 3), Some(Priority::Low));
        assert_eq!(q.find_lane(|&x| x == 99), None);
    }

    #[test]
    fn test_move_to() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Normal, 1);
        q.push_back(Priority::Normal, 2);

        assert!(q.move_to(Priority::Normal, Priority::High, |&x| x == 2));
        assert_eq!(q.len_for(Priority::High), 1);
        assert_eq!(q.len_for(Priority::Normal), 1);
        assert_eq!(q.pop_front(), Some(2)); // high first
        assert_eq!(q.pop_front(), Some(1));
    }

    #[test]
    fn test_move_to_same_lane() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Normal, 1);
        assert!(!q.move_to(Priority::Normal, Priority::Normal, |&x| x == 1));
    }

    #[test]
    fn test_move_to_not_found() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Normal, 1);
        assert!(!q.move_to(Priority::Normal, Priority::High, |&x| x == 99));
    }

    #[test]
    fn test_interleaved_push_pop() {
        let mut q = PriorityQueue::new();
        q.push_back(Priority::Normal, "a");
        q.push_back(Priority::Low, "b");
        assert_eq!(q.pop_front(), Some("a"));

        q.push_back(Priority::High, "c");
        assert_eq!(q.pop_front(), Some("c"));
        assert_eq!(q.pop_front(), Some("b"));
    }

    #[test]
    fn test_high_always_before_normal() {
        let mut q = PriorityQueue::new();
        for i in 0..5 {
            q.push_back(Priority::Normal, i);
        }
        q.push_back(Priority::High, 100);

        assert_eq!(q.pop_front(), Some(100));
    }
}
