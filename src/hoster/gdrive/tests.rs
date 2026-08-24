//! Everything here is offline: link shapes, export endpoints, the markup of
//! the warning page, and the naming rules a folder listing goes through.

use std::collections::HashSet;
use std::path::PathBuf;

use reqwest::Url;

use super::api::{confirm_url, docs_export_url, name_from_page};
use super::*;

/// A plausible id: long, and only the characters Drive uses.
const ID: &str = "1A2b3C4d5E6f7G8h9I";

/// Today's warning page, trimmed to the markup that decides the outcome: a
/// search form that must not be followed, and a download form whose hidden
/// inputs carry what the action leaves out.
const MODERN_PAGE: &str = r#"<html><body>
<form id="search-form" action="/drive/search" method="get"><input name="q" value=""></form>
<form id="download-form" action="https://drive.usercontent.google.com/download" method="post">
<input type="hidden" name="id" value="FILEID">
<input type="hidden" name="export" value="download">
<input type="hidden" name="confirm" value="t">
<input type="hidden" name="uuid" value="9f1c-4d">
<input name="at" value="AKxx-1700000000000">
</form>
<div class="uc-error-caption">Google Drive can't scan this file for viruses.</div>
</body></html>"#;

/// The older page: one form, everything already in its action.
const LEGACY_FORM_PAGE: &str = r#"<form action="https://drive.google.com/uc?export=download&amp;confirm=t&amp;id=FILEID">
<input type="hidden" name="id" value="FILEID">
<input type="hidden" name="confirm" value="t">
</form>"#;

/// The oldest page still in the wild: a plain link, and the filename printed
/// next to it.
const LEGACY_ANCHOR_PAGE: &str = r#"<html><body><a id="uc-download-link" class="goog-inline-block" href="/uc?export=download&amp;confirm=xyz&amp;id=FILEID">Download anyway</a><span class="uc-name-size"><a href="/open?id=FILEID">holiday video.mkv</a> (2.1G)</span></body></html>"#;

/// A URL's query, in a form that is pleasant to assert against.
fn query_of(url: &Url) -> Vec<(String, String)> {
    url.query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect()
}

// ── Links ────────────────────────────────────────────────

#[test]
fn claims_drive_links_and_nothing_that_merely_looks_like_one() {
    assert!(is_gdrive_url(&format!(
        "https://drive.google.com/file/d/{ID}/view"
    )));
    assert!(is_gdrive_url(&format!(
        "https://docs.google.com/document/d/{ID}/edit"
    )));
    assert!(is_gdrive_url(&format!(
        "https://colab.research.google.com/drive/{ID}"
    )));

    // A suffix and a subdomain, which is how a lookalike host is built.
    assert!(!is_gdrive_url(&format!(
        "https://drive.google.com.evil.net/file/d/{ID}/view"
    )));
    assert!(!is_gdrive_url(&format!(
        "https://notdrive.google.com/file/d/{ID}/view"
    )));

    // Left alone on purpose: this is where a confirmed download lands, and
    // claiming it would send an already-resolved URL back through resolution.
    assert!(!is_gdrive_url(&format!(
        "https://drive.usercontent.google.com/download?id={ID}&export=download"
    )));
}

#[test]
fn reads_the_id_out_of_every_shape_a_file_link_comes_in() {
    for url in [
        format!("https://drive.google.com/file/d/{ID}/view?usp=sharing"),
        format!("https://drive.google.com/uc?export=download&id={ID}"),
        format!("https://drive.google.com/open?id={ID}"),
        format!("https://colab.research.google.com/drive/{ID}"),
    ] {
        assert!(
            matches!(parse_link(&url).unwrap(), Link::File { id } if id == ID),
            "{url}"
        );
    }
}

#[test]
fn tells_the_document_kinds_apart() {
    let cases = [
        ("document", DocKind::Document),
        ("spreadsheets", DocKind::Spreadsheet),
        ("presentation", DocKind::Presentation),
        ("drawings", DocKind::Drawing),
    ];

    for (segment, expected) in cases {
        let url = format!("https://docs.google.com/{segment}/d/{ID}/edit#gid=0");
        let Link::Doc { kind, id } = parse_link(&url).unwrap() else {
            panic!("{url} is a document");
        };
        assert_eq!(kind.segment(), expected.segment(), "{url}");
        assert_eq!(id, ID);
    }
}

#[test]
fn recognises_folders_in_both_of_their_spellings() {
    let folder = format!("https://drive.google.com/drive/folders/{ID}?usp=sharing");
    let with_account = format!("https://drive.google.com/drive/u/0/folders/{ID}");
    let folderview = format!("https://drive.google.com/folderview?id={ID}");

    for url in [&folder, &with_account, &folderview] {
        assert!(
            matches!(parse_link(url).unwrap(), Link::Folder { id } if id == ID),
            "{url}"
        );
        assert!(is_folder_link(url), "{url}");
    }

    // A file link is not a folder, and routing depends on the difference.
    assert!(!is_folder_link(&format!(
        "https://drive.google.com/file/d/{ID}/view"
    )));
}

#[test]
fn refuses_links_with_nothing_to_download_behind_them() {
    // A form is a Google document by URL shape and not a file by any measure.
    assert!(parse_link(&format!("https://docs.google.com/forms/d/{ID}/viewform")).is_err());
    // No id anywhere.
    assert!(parse_link("https://drive.google.com/drive/my-drive").is_err());
    // Too short to be an id, which is what keeps a stray path segment from
    // being taken for one.
    assert!(parse_link("https://drive.google.com/file/d/abc/view").is_err());
}

// ── Export formats ───────────────────────────────────────

#[test]
fn exports_documents_as_asked_and_as_pdf_otherwise() {
    assert_eq!(
        DocKind::Document.export_as("pdf"),
        ("pdf", "application/pdf")
    );

    // `office` is the one alias: whatever Microsoft format the kind has.
    assert_eq!(DocKind::Document.export_as("office").0, "docx");
    assert_eq!(DocKind::Spreadsheet.export_as("office").0, "xlsx");
    assert_eq!(DocKind::Presentation.export_as("office").0, "pptx");

    assert_eq!(DocKind::Spreadsheet.export_as("csv").0, "csv");

    // A format the kind cannot be rendered as is not an error worth failing a
    // download over: PDF is what the Docs menu offers for everything.
    assert_eq!(DocKind::Spreadsheet.export_as("mp3").0, "pdf");
    assert_eq!(DocKind::Document.export_as("").0, "pdf");
}

#[test]
fn exports_through_the_endpoint_each_kind_actually_uses() {
    assert_eq!(
        docs_export_url(DocKind::Spreadsheet, "SHEETID", "csv")
            .unwrap()
            .as_str(),
        "https://docs.google.com/spreadsheets/d/SHEETID/export?format=csv"
    );
    assert_eq!(
        docs_export_url(DocKind::Presentation, "SLIDEID", "pptx")
            .unwrap()
            .as_str(),
        "https://docs.google.com/presentation/d/SLIDEID/export/pptx"
    );
}

// ── The warning page ──────────────────────────────────────

#[test]
fn follows_the_download_form_rather_than_the_first_form_on_the_page() {
    let url = confirm_url(MODERN_PAGE).expect("the page carries a download form");
    let query = query_of(&url);

    assert_eq!(url.host_str(), Some("drive.usercontent.google.com"));
    assert!(query.contains(&("confirm".to_owned(), "t".to_owned())));
    assert!(query.contains(&("uuid".to_owned(), "9f1c-4d".to_owned())));
    // Carried even though the input never says `type="hidden"`: `at` is the
    // parameter the download does not work without.
    assert!(query.contains(&("at".to_owned(), "AKxx-1700000000000".to_owned())));
    // The search form came first on the page and has nothing to do with this.
    assert!(!query.iter().any(|(name, _)| name == "q"));
}

#[test]
fn does_not_repeat_parameters_the_action_already_carries() {
    let url = confirm_url(LEGACY_FORM_PAGE).expect("the action carries a confirmation");
    let query = query_of(&url);

    for name in ["id", "confirm"] {
        assert_eq!(
            query.iter().filter(|(found, _)| found == name).count(),
            1,
            "{name} was sent twice"
        );
    }
}

#[test]
fn resolves_the_old_page_from_its_link_alone() {
    assert_eq!(
        confirm_url(LEGACY_ANCHOR_PAGE)
            .expect("the page carries a download link")
            .as_str(),
        "https://drive.google.com/uc?export=download&confirm=xyz&id=FILEID"
    );

    // The old page is also the only place that names the file.
    assert_eq!(
        name_from_page(LEGACY_ANCHOR_PAGE).as_deref(),
        Some("holiday video.mkv")
    );
}

#[test]
fn gives_up_on_a_page_that_offers_no_download() {
    // A sign-in wall or a quota notice: HTML, no form, no link.
    assert!(confirm_url("<html><body>Sign in to continue</body></html>").is_none());
    assert!(name_from_page("<html><body>Sign in to continue</body></html>").is_none());
}

// ── Naming ───────────────────────────────────────────────

#[test]
fn keeps_a_remote_name_to_a_single_local_component() {
    // Drive allows `/` in a filename, so a listing must not be able to write
    // outside the directory it was asked for.
    assert_eq!(safe_component("../../etc/passwd"), "passwd");
    assert_eq!(safe_component("C:\\Users\\me\\notes.txt"), "notes.txt");

    // Characters Windows will not take, and a trailing dot it silently drops.
    assert_eq!(safe_component("report:final?.pdf"), "reportfinal.pdf");
    assert_eq!(safe_component("backup."), "backup");

    assert_eq!(safe_component("holiday%20video.mkv"), "holiday video.mkv");

    // Nothing usable left, and a name is still needed.
    assert_eq!(safe_component("   "), "download.bin");
    assert_eq!(safe_component(".."), "download.bin");
}

#[test]
fn numbers_names_a_folder_uses_twice() {
    let mut taken = HashSet::new();
    let first = unique(&mut taken, PathBuf::from("trip/report.pdf"));
    let second = unique(&mut taken, PathBuf::from("trip/report.pdf"));
    let third = unique(&mut taken, PathBuf::from("trip/report.pdf"));
    let extensionless = unique(&mut taken, PathBuf::from("trip"));

    assert_eq!(first, PathBuf::from("trip/report.pdf"));
    assert_eq!(second, PathBuf::from("trip/report (2).pdf"));
    assert_eq!(third, PathBuf::from("trip/report (3).pdf"));
    // The same name in another directory is not a collision.
    assert_eq!(
        unique(&mut taken, PathBuf::from("notes/report.pdf")),
        PathBuf::from("notes/report.pdf")
    );
    assert_eq!(extensionless, PathBuf::from("trip"));
}

#[test]
fn adds_an_export_extension_only_when_it_is_missing() {
    assert_eq!(with_extension("Budget", "xlsx"), "Budget.xlsx");
    assert_eq!(with_extension("Budget.xlsx", "xlsx"), "Budget.xlsx");
    assert_eq!(with_extension("Budget.XLSX", "xlsx"), "Budget.XLSX");
    // A different extension is part of the title, not a duplicate.
    assert_eq!(with_extension("Budget.xlsx", "pdf"), "Budget.xlsx.pdf");
}

#[test]
fn puts_a_folder_where_its_name_says() {
    assert_eq!(
        destination_root(Some("/tmp/out".to_owned()), "/downloads", Some("Trip 2024")),
        PathBuf::from("/tmp/out")
    );
    assert_eq!(
        destination_root(None, "/downloads", Some("Trip 2024")),
        PathBuf::from("/downloads/Trip 2024")
    );
    // No name from the API, and no placeholder filename either.
    assert_eq!(
        destination_root(None, "/downloads", None),
        PathBuf::from("/downloads/gdrive")
    );
    assert_eq!(
        destination_root(None, "/downloads", Some("   ")),
        PathBuf::from("/downloads/gdrive")
    );
}
