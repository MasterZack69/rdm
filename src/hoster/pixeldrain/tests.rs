//! Everything worth testing in this module is reachable without a network.
//!
//! Link parsing, id validation and the `availability` check are pure, and the
//! two response shapes can be fed real payloads from pixeldrain's own
//! documentation. The transfer itself belongs to [`crate::engine`], which has
//! its own tests.

use super::*;

/// A [`FileInfo`] with nothing set but the availability fields, for the
/// refusal tests.
fn unavailable(code: &str, message: &str) -> FileInfo {
    FileInfo {
        id: None,
        name: None,
        size: None,
        availability: code.to_owned(),
        availability_message: message.to_owned(),
        download_speed_limit: 0,
    }
}

#[test]
fn pixeldrain_links_are_recognised() {
    assert!(is_pixeldrain_url("https://pixeldrain.com/u/AbCdEf12"));
    assert!(is_pixeldrain_url("https://www.pixeldrain.com/l/AbCdEf12"));
    assert!(is_pixeldrain_url("http://pixeldrain.com/u/AbCdEf12"));
    // Hosts are case-insensitive, and a fully-qualified name may end in a dot.
    assert!(is_pixeldrain_url("https://PixelDrain.COM/u/AbCdEf12"));
    assert!(is_pixeldrain_url("https://pixeldrain.com./u/AbCdEf12"));
}

/// The whole reason [`host_of`] parses the URL instead of matching on a
/// substring. Every one of these contains the text "pixeldrain.com" and none
/// of them is pixeldrain.
#[test]
fn lookalike_hosts_belong_to_somebody_else() {
    assert!(!is_pixeldrain_url("https://notpixeldrain.com/u/AbCdEf12"));
    assert!(!is_pixeldrain_url("https://pixeldrain.com.evil.com/u/AbCdEf12"));
    // Userinfo before the host is the sneakiest of these: the host here is
    // `pixeldrain.com.evil.net`, and the part that reads like pixeldrain is a
    // username.
    assert!(!is_pixeldrain_url(
        "https://evil.com@pixeldrain.com.evil.net/u/AbCdEf12"
    ));
    assert!(!is_pixeldrain_url("ftp://pixeldrain.com/u/AbCdEf12"));
    assert!(!is_pixeldrain_url("pixeldrain.com/u/AbCdEf12"));
    assert!(!is_pixeldrain_url(""));
}

#[test]
fn a_link_says_whether_it_is_a_file_or_a_list() {
    assert_eq!(
        parse_link("https://pixeldrain.com/u/AbCdEf12").unwrap(),
        Link::File("AbCdEf12".to_owned())
    );
    assert_eq!(
        parse_link("https://pixeldrain.com/l/AbCdEf12").unwrap(),
        Link::List("AbCdEf12".to_owned())
    );
    // Trailing slashes and surrounding whitespace come free with copy-paste.
    assert_eq!(
        parse_link("  https://www.pixeldrain.com/l/AbCdEf12/  ").unwrap(),
        Link::List("AbCdEf12".to_owned())
    );

    assert!(is_list_link("https://pixeldrain.com/l/AbCdEf12"));
    assert!(!is_list_link("https://pixeldrain.com/u/AbCdEf12"));
    // Infallible, so an unparseable link has to answer something, and "not a
    // list" is the answer that sends it to `resolve` for a real error.
    assert!(!is_list_link("https://pixeldrain.com/"));
}

/// Anyone who reads the API documentation will eventually paste one of these.
#[test]
fn the_api_forms_are_read_too() {
    assert_eq!(
        parse_link("https://pixeldrain.com/api/file/AbCdEf12").unwrap(),
        Link::File("AbCdEf12".to_owned())
    );
    // `/info` and `/thumbnail` name the same file, so the trailing segment is
    // ignored rather than treated as a malformed link.
    assert_eq!(
        parse_link("https://pixeldrain.com/api/file/AbCdEf12/info").unwrap(),
        Link::File("AbCdEf12".to_owned())
    );
    assert_eq!(
        parse_link("https://pixeldrain.com/api/list/AbCdEf12").unwrap(),
        Link::List("AbCdEf12".to_owned())
    );
}

#[test]
fn a_link_that_is_neither_says_what_it_should_have_been() {
    for url in [
        "https://pixeldrain.com/",
        "https://pixeldrain.com/u/",
        "https://pixeldrain.com/u/AbCdEf12/extra",
        "https://pixeldrain.com/d/AbCdEf12",
    ] {
        let error = format!("{:#}", parse_link(url).expect_err(url));
        assert!(error.contains("/u/<id>"), "{url}: {error}");
        assert!(error.contains("/l/<id>"), "{url}: {error}");
    }

    let error = format!(
        "{:#}",
        parse_link("https://example.com/u/AbCdEf12").unwrap_err()
    );
    assert!(error.contains("not a pixeldrain link"), "{error}");
}

#[test]
fn an_id_that_could_mean_something_else_is_refused() {
    assert_eq!(checked_id("AbCdEf12").unwrap(), "AbCdEf12");
    assert_eq!(checked_id("a-b_C9").unwrap(), "a-b_C9");

    // The comma is the one plausible mistake, so it gets its own message:
    // `/info` reads `a,b` as two files and answers with an array.
    let error = format!("{:#}", checked_id("aaa,bbb").unwrap_err());
    assert!(error.contains("several files at once"), "{error}");

    for id in ["", "a.b", "a b", "a/b", "a?b", "..", &"x".repeat(65)] {
        let error = format!("{:#}", checked_id(id).expect_err(id));
        assert!(error.contains("is not a pixeldrain id"), "{id}: {error}");
    }
}

#[test]
fn a_file_link_becomes_a_url_the_engine_can_fetch() {
    let url = direct_url("https://pixeldrain.com/u/AbCdEf12").unwrap();
    assert_eq!(url, "https://pixeldrain.com/api/file/AbCdEf12?download");
    // `?download` is what makes pixeldrain send a `Content-Disposition`, which
    // is what lets a queued link land under its real name with no extra
    // request. Losing the suffix would look like it still worked.
    assert!(url.ends_with("?download"));
}

#[test]
fn a_list_has_no_single_url() {
    let error = format!(
        "{:#}",
        direct_url("https://pixeldrain.com/l/AbCdEf12").unwrap_err()
    );
    assert!(error.contains("no single download URL"), "{error}");
}

/// The one failure pixeldrain reports with HTTP 200.
#[test]
fn a_blocked_file_is_refused_in_pixeldrains_own_words() {
    let info = unavailable("virus_detected_unpaid", "This file has been flagged as malware");
    let error = format!("{:#}", refuse_if_unavailable(&info).unwrap_err());
    assert!(error.contains("flagged as malware"), "{error}");
    assert!(error.contains("virus_detected_unpaid"), "{error}");
}

#[test]
fn a_refusal_with_only_a_code_still_names_it() {
    let info = unavailable("bandwidth_limit", "   ");
    let error = format!("{:#}", refuse_if_unavailable(&info).unwrap_err());
    assert!(error.contains("bandwidth_limit"), "{error}");
}

#[test]
fn an_available_file_is_not_refused() {
    assert!(refuse_if_unavailable(&unavailable("", "")).is_ok());
    assert!(refuse_if_unavailable(&unavailable("  ", "")).is_ok());
}

/// Field names are the one part of this module a typo would break silently, so
/// both shapes are parsed from a response as pixeldrain documents it —
/// including the fields this module has no use for, which must be ignored
/// rather than rejected.
#[test]
fn a_file_info_response_parses() {
    let body = r#"{
        "success": true,
        "id": "AbCdEf12",
        "name": "holiday.mp4",
        "size": 1234567,
        "views": 42,
        "bandwidth_used": 9876,
        "date_upload": "2024-01-02T03:04:05.000Z",
        "mime_type": "video/mp4",
        "hash_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "availability": "",
        "availability_message": "",
        "can_download": true,
        "download_speed_limit": 3145728
    }"#;

    let info: FileInfo = serde_json::from_str(body).expect("documented file info should parse");
    assert_eq!(info.id.as_deref(), Some("AbCdEf12"));
    assert_eq!(info.name.as_deref(), Some("holiday.mp4"));
    assert_eq!(info.size, Some(1234567));
    assert_eq!(info.download_speed_limit, 3145728);
    assert!(refuse_if_unavailable(&info).is_ok());
}

#[test]
fn a_list_response_parses_into_names_and_sizes() {
    let body = r#"{
        "success": true,
        "id": "ListId01",
        "title": "Holiday pictures",
        "date_created": "2024-01-02T03:04:05.000Z",
        "file_count": 2,
        "can_edit": false,
        "files": [
            {
                "detail_href": "/file/aaa11111",
                "description": "",
                "id": "aaa11111",
                "name": "one.jpg",
                "size": 111,
                "mime_type": "image/jpeg"
            },
            {
                "detail_href": "/file/bbb22222",
                "id": "bbb22222",
                "name": "two.jpg",
                "size": 222,
                "mime_type": "image/jpeg"
            }
        ]
    }"#;

    let list: ListInfo = serde_json::from_str(body).expect("documented list should parse");
    assert_eq!(list.title.as_deref(), Some("Holiday pictures"));
    assert_eq!(list.files.len(), 2);
    assert_eq!(list.files[0].name.as_deref(), Some("one.jpg"));
    assert_eq!(list.files[1].size, Some(222));
}

/// A list with no `files` key at all still parses. pixeldrain has no reason to
/// send one, but an empty list is a result and not a failure.
#[test]
fn a_list_with_nothing_in_it_is_not_an_error() {
    let list: ListInfo = serde_json::from_str(r#"{"id":"x","title":null}"#).unwrap();
    assert!(list.title.is_none());
    assert!(list.files.is_empty());
}

#[test]
fn a_cap_is_mentioned_only_when_there_is_something_to_do_about_it() {
    let note = speed_limit_note(3 * 1024 * 1024, false).expect("a capped file should say so");
    assert!(note.contains("capping this file"), "{note}");
    assert!(note.contains("RDM_PIXELDRAIN_API_KEY"), "{note}");

    // Uncapped: nothing to report.
    assert!(speed_limit_note(0, false).is_none());
    // Capped, but the user has already done the thing the note would ask for.
    assert!(speed_limit_note(3 * 1024 * 1024, true).is_none());
}

#[test]
fn a_list_keeps_its_own_title_unless_told_otherwise() {
    assert_eq!(
        destination_root(None, "/tmp/dl", Some("Holiday pictures")),
        PathBuf::from("/tmp/dl").join("Holiday pictures")
    );

    // An untitled list still needs somewhere to go.
    assert_eq!(
        destination_root(None, "/tmp/dl", None),
        PathBuf::from("/tmp/dl").join("pixeldrain")
    );

    // An explicit -o wins outright, and is a directory: a list is many files,
    // so there is no single file for a filename to name.
    assert_eq!(
        destination_root(Some("/srv/elsewhere".to_owned()), "/tmp/dl", Some("Holiday")),
        PathBuf::from("/srv/elsewhere")
    );
}

/// A list title is a remote string, so it must not be able to choose the
/// directory. Whatever it reduces to, it stays under the download directory.
#[test]
fn a_list_title_cannot_choose_where_the_files_go() {
    for title in ["../../etc", "/etc/passwd", "..", "C:\\Windows", "."] {
        let root = destination_root(None, "/tmp/dl", Some(title));
        assert!(root.starts_with("/tmp/dl"), "{title}: {}", root.display());
        assert_ne!(root, PathBuf::from("/tmp/dl"), "{title}");
    }
}

#[test]
fn the_defaults_agree_with_what_the_config_advertises() {
    let options = PixeldrainOptions::default();
    assert_eq!(options.workers, WORKERS_DEFAULT);
    assert!(options.api_key.is_none());
    assert!(!options.overwrite);
    // `download_files` clamps to WORKERS_MAX, so a default above it would be
    // silently ignored rather than honoured.
    assert!(WORKERS_DEFAULT >= 1 && WORKERS_DEFAULT <= WORKERS_MAX);
}
