use std::io::{Cursor, Write};
use std::path::Path;
use tokio_util::sync::CancellationToken;

use crate::sftp::checkpoint::Partial;
use crate::sftp::local::{Destination, STATE_DIR};
use crate::sftp::stamp::{FileStamp, Identity};
use crate::sftp::transfer::copy_data;

fn identity(size: u64) -> Identity {
    Identity {
        url: String::from("sftp://alice@example.test:22/file"),
        host_key: String::from("test-key"),
        stamp: FileStamp { size, modified: Some(1_700_000_000) },
    }
}

#[test]
fn resume_requires_the_same_host_key_url_size_and_mtime() {
    let original = identity(10);
    assert!(original.can_resume(&original));
    let mut changed = original.clone();
    changed.stamp.size += 1;
    assert!(!original.can_resume(&changed));
    changed = original.clone();
    changed.stamp.modified = Some(2);
    assert!(!original.can_resume(&changed));
    changed = original.clone();
    changed.url.push('x');
    assert!(!original.can_resume(&changed));
    changed = original.clone();
    changed.host_key.push('x');
    assert!(!original.can_resume(&changed));
    changed.stamp.modified = None;
    assert!(!changed.can_resume(&changed));
}

#[test]
fn checkpoints_discard_uncommitted_tail_bytes_after_a_crash() {
    let root = tempfile::tempdir().unwrap();
    let destination = Destination::beneath(root.path(), "file").unwrap();
    let mut partial = Partial::open(destination.clone(), identity(8)).unwrap();
    partial.file.write_all(b"abc").unwrap();
    partial.checkpoint(3).unwrap();
    partial.file.write_all(b"unsynced").unwrap();
    drop(partial);
    let resumed = Partial::open(destination, identity(8)).unwrap();
    assert_eq!(resumed.offset, 3);
    assert_eq!(resumed.file.metadata().unwrap().len(), 3);
}

#[test]
fn a_different_remote_cannot_reuse_old_partial_bytes() {
    let root = tempfile::tempdir().unwrap();
    let destination = Destination::beneath(root.path(), "file").unwrap();
    let mut partial = Partial::open(destination.clone(), identity(8)).unwrap();
    partial.file.write_all(b"old").unwrap();
    partial.checkpoint(3).unwrap();
    drop(partial);
    let mut changed = identity(8);
    changed.stamp.modified = Some(1_700_000_001);
    let reset = Partial::open(destination, changed).unwrap();
    assert_eq!(reset.offset, 0);
    assert_eq!(reset.file.metadata().unwrap().len(), 0);
}

#[test]
fn transfers_to_the_same_output_are_exclusive() {
    let root = tempfile::tempdir().unwrap();
    let destination = Destination::beneath(root.path(), "file").unwrap();
    let partial = Partial::open(destination.clone(), identity(8)).unwrap();
    assert!(Partial::open(destination.clone(), identity(8)).is_err());
    drop(partial);
    assert!(Partial::open(destination, identity(8)).is_ok());
}

#[test]
fn publication_never_clobbers_without_explicit_overwrite() {
    let root = tempfile::tempdir().unwrap();
    let destination = Destination::beneath(root.path(), "file").unwrap();
    std::fs::write(destination.path(), b"original").unwrap();
    let mut partial = Partial::open(destination.clone(), identity(3)).unwrap();
    partial.file.write_all(b"new").unwrap();
    assert!(partial.publish(false).is_err());
    assert_eq!(std::fs::read(destination.path()).unwrap(), b"original");
    let mut partial = Partial::open(destination.clone(), identity(3)).unwrap();
    partial.file.write_all(b"new").unwrap();
    partial.publish(true).unwrap();
    assert_eq!(std::fs::read(destination.path()).unwrap(), b"new");
}

#[test]
fn empty_files_are_published_and_short_reads_are_not() {
    let root = tempfile::tempdir().unwrap();
    let empty = Destination::beneath(root.path(), "empty").unwrap();
    let mut partial = Partial::open(empty.clone(), identity(0)).unwrap();
    copy_data(&mut Cursor::new(b""), &mut partial, 0, &CancellationToken::new(), &crate::ui::Silent).unwrap();
    partial.publish(false).unwrap();
    assert_eq!(std::fs::metadata(empty.path()).unwrap().len(), 0);
    let short = Destination::beneath(root.path(), "short").unwrap();
    let mut partial = Partial::open(short.clone(), identity(10)).unwrap();
    assert!(copy_data(&mut Cursor::new(b"abc"), &mut partial, 10, &CancellationToken::new(), &crate::ui::Silent).is_err());
    assert!(!short.path().exists());
}

#[test]
fn cancellation_stops_before_writing_or_publishing() {
    let root = tempfile::tempdir().unwrap();
    let destination = Destination::beneath(root.path(), "file").unwrap();
    let mut partial = Partial::open(destination.clone(), identity(3)).unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(copy_data(&mut Cursor::new(b"abc"), &mut partial, 3, &cancel, &crate::ui::Silent).is_err());
    assert_eq!(partial.file.metadata().unwrap().len(), 0);
    assert!(!destination.path().exists());
}

#[test]
fn directory_and_state_symlinks_cannot_redirect_writes() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("linked")).unwrap();
    let linked = Destination::beneath(root.path(), "linked/file").unwrap();
    assert!(Partial::open(linked, identity(1)).is_err());
    assert!(!outside.path().join("file").exists());
    std::os::unix::fs::symlink(outside.path(), root.path().join(STATE_DIR)).unwrap();
    let file = Destination::beneath(root.path(), "file").unwrap();
    assert!(Partial::open(file, identity(1)).is_err());
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
}

#[test]
fn outside_outputs_do_not_turn_untrusted_parents_into_roots() {
    let destination = Destination::from_output(Path::new("/tmp/elsewhere/a/file"), Path::new("/tmp/downloads")).unwrap();
    assert_eq!(destination.root, Path::new("/"));
    assert_eq!(destination.relative, Path::new("tmp/elsewhere/a/file"));
    assert!(Destination::from_output(
        Path::new("/tmp/downloads/.rdm-sftp/payload"),
        Path::new("/tmp/downloads"),
    ).is_err());
}
