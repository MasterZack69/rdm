//! Unit tests for the OneDrive hoster.
//!
//! In their own file rather than at the foot of `mod.rs`, which is long enough
//! already. Nothing here touches the network: every test is a pure function or
//! a JSON shape.

use super::*;

// ── Link recognition ──

#[test]
fn onedrive_links_are_recognised() {
    assert!(is_onedrive_url("https://1drv.ms/u/s!AbCdEf"));
    assert!(is_onedrive_url("https://1drv.ms/f/s!AbCdEf"));
    assert!(is_onedrive_url("  https://1drv.ms/u/s!AbCdEf  "));
    assert!(is_onedrive_url("http://1drv.ms/u/s!AbCdEf"));
    assert!(is_onedrive_url("https://onedrive.live.com/?cid=ABC&id=ABC%21123"));
    assert!(is_onedrive_url("https://WWW.OneDrive.Live.Com/redir?resid=ABC"));
}

/// Lookalikes fall through to the generic engine, and business shares are left
/// alone on purpose: an anonymous token cannot open a tenant's share, so
/// claiming one would turn a download that might work into one that never can.
#[test]
fn lookalikes_and_business_shares_are_not_claimed() {
    assert!(!is_onedrive_url("https://not1drv.ms/u/s!AbCdEf"));
    assert!(!is_onedrive_url("https://1drv.ms.evil.com/u/s!AbCdEf"));
    assert!(!is_onedrive_url("https://evil.com/1drv.ms/u/s!AbCdEf"));
    // Userinfo, which is why this parses the URL instead of matching on text.
    assert!(!is_onedrive_url("https://evil.com@1drv.ms.evil.net/u/s!AbCdEf"));
    assert!(!is_onedrive_url("https://contoso-my.sharepoint.com/:u:/g/personal/x"));
    assert!(!is_onedrive_url("ftp://1drv.ms/u/s!AbCdEf"));
    assert!(!is_onedrive_url("https://example.com/file.zip"));
    assert!(!is_onedrive_url("not a url at all"));
}

// ── Share ids ──

/// Microsoft's recipe, pinned against inputs whose base64 is known by heart:
/// standard base64 would spell the last two `fn5+` and `Pz8/`, and would pad
/// `YQ` out to `YQ==`.
#[test]
fn a_share_id_is_unpadded_base64url() {
    assert_eq!(share_id("abc"), "u!YWJj");
    assert_eq!(share_id("a"), "u!YQ");
    assert_eq!(share_id("~~~"), "u!fn5-");
    assert_eq!(share_id("???"), "u!Pz8_");
}

/// Surrounding whitespace is all that is stripped. A normalised URL is a
/// different string and therefore a different share.
#[test]
fn a_link_is_encoded_as_it_was_given() {
    assert_eq!(
        share_id("  https://1drv.ms/u/s!A?e=1  "),
        share_id("https://1drv.ms/u/s!A?e=1")
    );
    assert_ne!(
        share_id("https://1drv.ms/u/s!A?e=1"),
        share_id("https://1drv.ms/u/s!A?e=1&")
    );
}

// ── Listing URLs ──

/// The drive id is the part of the item id in front of the `!`, and the API
/// wants both halves.
#[test]
fn a_listing_url_carries_the_drive_and_the_item() {
    let url = children_url("A1B2C3!789");
    assert!(
        url.starts_with(&format!("{API_BASE}/drives/A1B2C3/items/A1B2C3!789?")),
        "{url}"
    );
    assert!(
        url.contains("expand=children(select=name,@content.downloadUrl,id)"),
        "{url}"
    );
}

/// An id without a `!` is its own drive, which is what the reference
/// downloader's `${folder_id%%!*}` amounts to as well.
#[test]
fn an_id_without_a_bang_is_its_own_drive() {
    assert!(children_url("ABC").starts_with(&format!("{API_BASE}/drives/ABC/items/ABC?")));
}

// ── Page shapes ──

/// The first page nests items under `children`; a continuation page returns
/// them under `value` and renames the next link. Getting either wrong means a
/// silently half-downloaded folder, so both shapes are pinned here.
#[test]
fn both_page_shapes_are_understood() {
    let first: ChildrenPage = serde_json::from_str(
        r#"{"children":[{"id":"D!1","name":"a.txt","@content.downloadUrl":"https://cdn/a"},
                        {"id":"D!2","name":"sub"}],
            "children@odata.nextLink":"https://api/next"}"#,
    )
    .unwrap();

    assert_eq!(first.items().count(), 2);
    assert_eq!(first.next_link(), Some("https://api/next"));

    let second: ChildrenPage = serde_json::from_str(
        r#"{"value":[{"id":"D!3","name":"b.txt","@content.downloadUrl":"https://cdn/b"}],
            "@odata.nextLink":"https://api/more"}"#,
    )
    .unwrap();

    assert_eq!(second.items().count(), 1);
    assert_eq!(second.next_link(), Some("https://api/more"));

    let last: ChildrenPage = serde_json::from_str(r#"{"children":[]}"#).unwrap();
    assert_eq!(last.items().count(), 0);
    assert!(last.next_link().is_none());
}

/// A download URL is what makes a child a file. Without one, an id means a
/// folder to walk into, and a child with neither is skipped rather than failing
/// the whole tree.
#[test]
fn children_are_read_as_files_folders_or_skipped() {
    let page: ChildrenPage = serde_json::from_str(
        r#"{"children":[{"id":"D!1","name":"a.txt","@content.downloadUrl":"https://cdn/a"},
                        {"id":"D!2","name":"sub"},
                        {"name":"still uploading"}]}"#,
    )
    .unwrap();

    let items: Vec<&DriveItem> = page.items().collect();

    assert!(matches!(classify(items[0]), Some(Child::File { id, url }) if id == "D!1" && url == "https://cdn/a"));
    assert!(matches!(classify(items[1]), Some(Child::Folder { id }) if id == "D!2"));
    assert!(classify(items[2]).is_none());
}

/// serde has already turned `\uXXXX` escapes into characters, so nothing in
/// this module needs the reference downloader's `decode_name`.
#[test]
fn escaped_names_arrive_decoded() {
    let page: ChildrenPage =
        serde_json::from_str(r#"{"children":[{"id":"D!1","name":"caf\u00e9.txt"}]}"#).unwrap();

    assert_eq!(
        page.items().next().and_then(|item| item.name.as_deref()),
        Some("caf\u{e9}.txt")
    );
}

// ── Names ──

/// Names come off the network, so a name that is really a path must not become
/// one on disk.
#[test]
fn remote_names_cannot_escape_the_download_directory() {
    for name in [
        "../../.ssh/authorized_keys",
        "..",
        ".",
        "/etc/passwd",
        "a/b/c",
        "..%2F..%2Fx",
        "sub\\evil.exe",
    ] {
        let component = safe_component(name);
        assert_eq!(
            Path::new(&component).components().count(),
            1,
            "{name} produced {component}"
        );
        assert!(!component.contains('/'), "{name} produced {component}");
        assert!(!component.contains('\\'), "{name} produced {component}");
    }
}

#[test]
fn ordinary_names_are_left_alone() {
    assert_eq!(safe_component("Holiday 2026.zip"), "Holiday 2026.zip");
    assert_eq!(safe_component("na\u{ef}ve \u{2014} \u{444}\u{430}\u{439}\u{43b}.pdf"), "na\u{ef}ve \u{2014} \u{444}\u{430}\u{439}\u{43b}.pdf");
    assert_eq!(safe_component("report.pdf\u{0}"), "report.pdf");
    assert_eq!(safe_component("  spaced.txt  "), "spaced.txt");
    assert_eq!(safe_component(""), "download.bin");
}

// ── Collisions ──

/// Two remote names that sanitise to the same component must not become the
/// same file: one download would silently overwrite the other and both would
/// look finished.
#[test]
fn colliding_names_are_numbered() {
    let mut taken = HashSet::new();

    assert_eq!(
        unique(&mut taken, PathBuf::from("dir/report.pdf")),
        PathBuf::from("dir/report.pdf")
    );
    assert_eq!(
        unique(&mut taken, PathBuf::from("dir/report.pdf")),
        PathBuf::from("dir/report (2).pdf")
    );
    assert_eq!(
        unique(&mut taken, PathBuf::from("dir/report.pdf")),
        PathBuf::from("dir/report (3).pdf")
    );
}

#[test]
fn a_name_without_an_extension_still_gets_numbered() {
    let mut taken = HashSet::new();

    unique(&mut taken, PathBuf::from("notes"));
    assert_eq!(
        unique(&mut taken, PathBuf::from("notes")),
        PathBuf::from("notes (2)")
    );
}

/// The same name in two different folders is not a collision.
#[test]
fn identical_names_in_different_folders_are_left_alone() {
    let mut taken = HashSet::new();

    assert_eq!(
        unique(&mut taken, PathBuf::from("a/x.txt")),
        PathBuf::from("a/x.txt")
    );
    assert_eq!(
        unique(&mut taken, PathBuf::from("b/x.txt")),
        PathBuf::from("b/x.txt")
    );
}

// ── Where a folder lands ──

#[test]
fn a_folder_keeps_its_own_name() {
    assert_eq!(
        destination_root(None, "/dl", Some("Holiday photos")),
        PathBuf::from("/dl/Holiday photos")
    );
}

/// A folder share is a tree, so an explicit output is the directory it goes
/// into rather than a filename.
#[test]
fn an_explicit_output_is_the_directory() {
    assert_eq!(
        destination_root(Some("/here".to_owned()), "/dl", Some("Holiday photos")),
        PathBuf::from("/here")
    );
}

/// A folder name is a remote name like any other, and an unusable one falls
/// back to something neutral rather than to `download.bin`.
#[test]
fn a_hostile_folder_name_stays_inside_the_download_directory() {
    assert_eq!(
        destination_root(None, "/dl", Some("../../etc")),
        PathBuf::from("/dl/etc")
    );
    assert_eq!(
        destination_root(None, "/dl", Some("..")),
        PathBuf::from("/dl/onedrive")
    );
    assert_eq!(destination_root(None, "/dl", None), PathBuf::from("/dl/onedrive"));
}

// ── Failure reporting ──

/// A refusal is a decision: asking again with the same anonymous token gets the
/// same answer, while a rate limit or a bad gateway is worth another go.
#[test]
fn hopeless_statuses_are_not_retried() {
    assert!(is_final(StatusCode::BAD_REQUEST));
    assert!(is_final(StatusCode::UNAUTHORIZED));
    assert!(is_final(StatusCode::FORBIDDEN));
    assert!(is_final(StatusCode::NOT_FOUND));
    assert!(is_final(StatusCode::GONE));

    assert!(!is_final(StatusCode::TOO_MANY_REQUESTS));
    assert!(!is_final(StatusCode::INTERNAL_SERVER_ERROR));
    assert!(!is_final(StatusCode::BAD_GATEWAY));
}

/// The status code alone does not say whether a link is dead or private, which
/// is the first thing anyone wants to know.
#[test]
fn refusals_say_what_they_probably_mean() {
    let dead = explain(StatusCode::NOT_FOUND, br#"{"error":"itemNotFound"}"#);
    assert!(dead.contains("404"), "{dead}");
    assert!(dead.contains("expired"), "{dead}");
    assert!(dead.contains("itemNotFound"), "{dead}");

    let private = explain(StatusCode::UNAUTHORIZED, b"");
    assert!(private.contains("password"), "{private}");
}

#[test]
fn body_snippets_are_short_and_single_line() {
    assert_eq!(
        snippet(b"<html>\n  <body>no</body>\n</html>"),
        "<html> <body>no</body> </html>"
    );
    assert_eq!(snippet(b""), "");
    assert!(snippet(&vec![b'x'; 500]).ends_with('\u{2026}'));

    // Cutting a multi-byte character in half must not panic.
    let mut multibyte = "\u{e9}".repeat(300).into_bytes();
    multibyte.truncate(401);
    let _ = snippet(&multibyte);
}

// ── Options ──

#[test]
fn defaults_are_conservative() {
    let options = OneDriveOptions::default();

    assert_eq!(options.workers, WORKERS_DEFAULT);
    assert!(!options.overwrite);
    const { assert!(WORKERS_DEFAULT <= WORKERS_MAX) };
}
