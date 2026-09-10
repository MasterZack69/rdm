use std::fs::{File, FileTimes, Metadata};
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use crate::sftp::discard_state;
use crate::sftp::local::{Destination, state_paths};
use crate::sftp::stamp::{Compare, Difference, FileStamp, Verdict};

const REMOTE_MTIME: u64 = 1_700_000_000;

/// A local file whose timestamp records when it was written here, which is
/// what every library fetched by something other than this backend looks like.
fn local_file(root: &Path, name: &str, bytes: &[u8], modified: u64) -> Metadata {
    let path = root.join(name);
    std::fs::write(&path, bytes).unwrap();
    let file = File::options().write(true).open(&path).unwrap();
    file.set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(modified))).unwrap();
    std::fs::metadata(&path).unwrap()
}

fn remote(size: u64) -> FileStamp {
    FileStamp { size, modified: Some(REMOTE_MTIME) }
}

#[test]
fn a_library_fetched_elsewhere_is_not_downloaded_again() {
    let root = tempfile::tempdir().unwrap();
    let local = local_file(root.path(), "track.flac", b"audio", 1_000_000);
    assert_eq!(
        remote(5).compare_local(&local, Compare::Size, 2),
        Verdict::Retime { seconds: REMOTE_MTIME },
    );
    assert!(matches!(
        remote(5).compare_local(&local, Compare::SizeAndTime, 2),
        Verdict::Stale(Difference::Modified { .. }),
    ));
}

#[test]
fn a_repaired_timestamp_makes_every_later_run_a_no_op() {
    let root = tempfile::tempdir().unwrap();
    let local = local_file(root.path(), "track.flac", b"audio", 1_000_000);
    let destination = Destination::beneath(root.path(), "track.flac").unwrap();
    assert_eq!(
        destination.align_modified(&local, REMOTE_MTIME).unwrap(),
        Verdict::Retime { seconds: REMOTE_MTIME },
    );
    let repaired = std::fs::metadata(destination.path()).unwrap();
    assert_eq!(remote(5).compare_local(&repaired, Compare::Size, 0), Verdict::Current);
    assert_eq!(remote(5).compare_local(&repaired, Compare::SizeAndTime, 0), Verdict::Current);
    assert_eq!(std::fs::read(destination.path()).unwrap(), b"audio");
}

#[test]
fn a_changed_size_is_always_downloaded() {
    let root = tempfile::tempdir().unwrap();
    let local = local_file(root.path(), "track.flac", b"cut", REMOTE_MTIME);
    let stale = Verdict::Stale(Difference::Size { local: 3 });
    assert_eq!(remote(5).compare_local(&local, Compare::Size, 2), stale);
    assert_eq!(remote(5).compare_local(&local, Compare::SizeAndTime, 2), stale);
}

#[test]
fn rounded_filesystem_timestamps_stay_inside_the_window() {
    let root = tempfile::tempdir().unwrap();
    let local = local_file(root.path(), "track.flac", b"audio", REMOTE_MTIME + 1);
    assert_eq!(remote(5).compare_local(&local, Compare::SizeAndTime, 2), Verdict::Current);
    assert!(matches!(
        remote(5).compare_local(&local, Compare::SizeAndTime, 0),
        Verdict::Stale(Difference::Modified { .. }),
    ));
}

#[test]
fn a_repair_refuses_a_file_that_changed_underneath_it() {
    let root = tempfile::tempdir().unwrap();
    let local = local_file(root.path(), "track.flac", b"audio", 1_000_000);
    std::fs::write(root.path().join("track.flac"), b"longer audio").unwrap();
    let destination = Destination::beneath(root.path(), "track.flac").unwrap();
    assert_eq!(
        destination.align_modified(&local, REMOTE_MTIME).unwrap(),
        Verdict::Stale(Difference::Replaced),
    );
    assert_eq!(std::fs::read(destination.path()).unwrap(), b"longer audio");
}

#[test]
fn the_partial_data_of_an_aborted_run_is_reclaimed_once_the_file_is_current() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("track.flac"), b"audio").unwrap();
    let destination = Destination::beneath(root.path(), "track.flac").unwrap();
    let (directory, key) = state_paths(&destination.relative).unwrap();
    let directory = root.path().join(directory);
    std::fs::create_dir_all(&directory).unwrap();
    let part = directory.join(format!("{key}.part"));
    std::fs::write(&part, vec![0_u8; 4096]).unwrap();
    std::fs::write(directory.join(format!("{key}.json")), b"{}").unwrap();
    assert_eq!(discard_state(&destination).unwrap(), 4096);
    assert!(!part.exists());
    // Nothing is left to reclaim, and the published file is never touched.
    assert_eq!(discard_state(&destination).unwrap(), 0);
    assert_eq!(std::fs::read(destination.path()).unwrap(), b"audio");
}
