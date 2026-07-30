//! Turning remote names into local paths.
//!
//! Two problems, both of which end in lost data if they are ignored:
//!
//!   1. **Names come from whoever made the upload.** `../../.bashrc` is a name
//!      we have to assume we will eventually be handed.
//!   2. **GoFile allows duplicates, filesystems do not.** One folder can hold
//!      two files called `video.mp4`, and the second one quietly overwriting
//!      the first is the worst available outcome.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Strips anything that would let a remote name escape the download root or
/// confuse the filesystem.
///
/// Leading dots are kept on purpose: `.bashrc` is a legitimate filename, and
/// renaming every hidden file to `bashrc` would be its own small disaster.
/// Trailing dots and spaces are dropped because Windows cannot represent them.
pub fn sanitize(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();

    let cleaned = cleaned.trim().trim_end_matches(['.', ' ']).to_string();

    // `.` and `..` survive the pass above unscathed and both mean "a directory
    // that is not this one".
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        return "download.bin".to_string();
    }

    cleaned
}

/// Gives every discovered name a path of its own.
///
/// Repeats become `video(1).mp4`, mirroring the reference downloader. `taken`
/// carries the collision counts across the whole tree walk, so it must be the
/// same map for every call within one content id.
pub fn unique_path(
    taken: &mut HashMap<PathBuf, usize>,
    parent: &Path,
    name: &str,
    is_dir: bool,
) -> PathBuf {
    let candidate = parent.join(sanitize(name));

    let seen = taken.entry(candidate.clone()).or_insert(0);
    let index = *seen;
    *seen += 1;

    if index == 0 {
        return candidate;
    }

    let renamed = if is_dir {
        // A directory called `season.1` keeps its whole name; splitting an
        // "extension" off a folder produces `season(1).1`, which is nonsense.
        format!(
            "{}({index})",
            candidate.file_name().unwrap_or_default().to_string_lossy()
        )
    } else {
        let stem = candidate
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        match candidate.extension() {
            Some(extension) => format!("{stem}({index}).{}", extension.to_string_lossy()),
            None => format!("{stem}({index})"),
        }
    };

    parent.join(renamed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_names_cannot_escape_the_root() {
        assert_eq!(sanitize("../../.bashrc"), "_.._.bashrc");
        assert_eq!(sanitize("a/b"), "a_b");
        assert_eq!(sanitize(".."), "download.bin");
        assert_eq!(sanitize("."), "download.bin");
        assert_eq!(sanitize("   "), "download.bin");

        // Every sanitised name is a single path component.
        for name in ["../../.bashrc", "a/b", "..", "x\\y"] {
            assert_eq!(Path::new(&sanitize(name)).components().count(), 1);
        }
    }

    /// Hidden files stay hidden; ordinary names are left completely alone.
    #[test]
    fn legitimate_names_survive_untouched() {
        assert_eq!(sanitize(".bashrc"), ".bashrc");
        assert_eq!(sanitize("normal name.mkv"), "normal name.mkv");
        assert_eq!(sanitize("S01E01 [1080p].mkv"), "S01E01 [1080p].mkv");
        assert_eq!(sanitize("trailing."), "trailing");
    }

    #[test]
    fn duplicate_names_get_a_suffix_instead_of_overwriting() {
        let mut taken = HashMap::new();
        let root = Path::new("");

        assert_eq!(
            unique_path(&mut taken, root, "video.mp4", false),
            PathBuf::from("video.mp4")
        );
        assert_eq!(
            unique_path(&mut taken, root, "video.mp4", false),
            PathBuf::from("video(1).mp4")
        );
        assert_eq!(
            unique_path(&mut taken, root, "video.mp4", false),
            PathBuf::from("video(2).mp4")
        );

        // The same name in a different folder is not a collision.
        assert_eq!(
            unique_path(&mut taken, Path::new("sub"), "video.mp4", false),
            PathBuf::from("sub/video.mp4")
        );
    }

    #[test]
    fn directories_keep_their_whole_name() {
        let mut taken = HashMap::new();
        let root = Path::new("");

        assert_eq!(
            unique_path(&mut taken, root, "season.1", true),
            PathBuf::from("season.1")
        );
        assert_eq!(
            unique_path(&mut taken, root, "season.1", true),
            PathBuf::from("season.1(1)")
        );
    }

    #[test]
    fn extensionless_files_still_get_a_suffix() {
        let mut taken = HashMap::new();
        let root = Path::new("");

        assert_eq!(
            unique_path(&mut taken, root, "README", false),
            PathBuf::from("README")
        );
        assert_eq!(
            unique_path(&mut taken, root, "README", false),
            PathBuf::from("README(1)")
        );
    }
}
