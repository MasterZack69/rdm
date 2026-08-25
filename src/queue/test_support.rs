//! Fixtures shared by the queue's unit tests.

use super::state::Queue;

pub(super) const MEGA_LINK: &str = "https://mega.nz/file/AbCdEfGh#thekey";
pub(super) const ONEDRIVE_LINK: &str = "https://1drv.ms/f/c/abc123/AbCdEfGh";
pub(super) const PIXELDRAIN_LINK: &str = "https://pixeldrain.com/u/AbCdEf12";
pub(super) const PIXELDRAIN_LIST: &str = "https://pixeldrain.com/l/Zz9900";

/// A queue holding one pending item per URL, with ids starting at 1.
pub(super) fn queue_with(urls: &[&str]) -> Queue {
    let mut q = Queue::default();
    for url in urls {
        q.add((*url).to_owned(), None, None);
    }
    q
}
