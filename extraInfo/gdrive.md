# Google Drive

No Google Drive link is a fetchable address. Hand one to a plain downloader and
you get an HTML page saved as `view` or `edit`, which is why this host needs a
module rather than a URL rewrite.

There are three shapes, and they are unfetchable in three different ways.

## 1. A file

```
https://drive.google.com/file/d/<id>/view
https://drive.google.com/uc?export=download&id=<id>
https://drive.google.com/open?id=<id>
https://colab.research.google.com/drive/<id>          # a notebook is a file
```

Without a key, resolution starts at `uc?export=download&id=<id>`:

- **A small file** redirects to the bytes, and the response carries a
  `Content-Disposition`. That URL is handed to the engine as it stands.
- **Anything Drive will not virus-scan** (roughly, over 100 MB) answers with
  the "can't scan this file for viruses" warning page instead. HTTP 200, no
  redirect, and an HTML body. The real URL is the `action` of the download form
  on that page, plus the `id`, `export`, `confirm`, `uuid` and `at` parameters
  its hidden inputs carry. `at` is the one it will not work without.

Two older shapes of that page still turn up, and both are handled: a form whose
action already spells out `confirm=`, and an `<a id="uc-download-link">` link.
Parameters already present in the action are not appended a second time, since
sending `id` twice turns the download into a 400.

With a key, none of that happens: the API says what the item is and the bytes
come from `files/<id>?alt=media`.

## 2. A Google document

```
https://docs.google.com/document/d/<id>/edit
https://docs.google.com/spreadsheets/d/<id>/edit
https://docs.google.com/presentation/d/<id>/edit
https://docs.google.com/drawings/d/<id>/edit
```

There is no file to download. A Google document is rendered on request, so a
format has to be chosen; `gdrive_doc_format` in `config.toml` chooses it, and
the default is `pdf`.

| `gdrive_doc_format` | Doc    | Sheet  | Slides | Drawing |
| ------------------- | ------ | ------ | ------ | ------- |
| `pdf` *(default)*   | `pdf`  | `pdf`  | `pdf`  | `pdf`   |
| `office`            | `docx` | `xlsx` | `pptx` | `png`   |
| anything else       | that extension when the kind exports as it, `pdf` otherwise |

A format a kind cannot be rendered as is not an error: PDF is what the Docs
"File \u{2192} Download" menu offers for everything, so that is the fallback. The export
extension is appended unless the title already ends in it — somebody's
spreadsheet really is called `budget.xlsx`, and `budget.xlsx.xlsx` is nobody's
idea of a result.

Slides and Drawings take the format as a path segment (`/export/pptx`) while
Docs and Sheets take it as a query parameter (`/export?format=docx`). Nobody's
mistake, just two generations of one endpoint, and both are still live.

## 3. A folder

```
https://drive.google.com/drive/folders/<id>
https://drive.google.com/drive/u/0/folders/<id>
https://drive.google.com/folderview?id=<id>
```

**A folder needs an API key.** Anonymous access can fetch a file whose link you
already hold, but nothing anonymous can enumerate a folder, so this refuses up
front rather than failing halfway through.

The walk is breadth-unbounded and depth-unbounded: `q='<id>' in parents and
trashed = false`, 1000 children per page, following `nextPageToken`. Every
directory it sees is created, empty ones included, and files land under their
own names inside it. Google-native documents inside a folder are exported using
the same format rules as above.

Two children of one Drive folder are allowed to share a name, and a
case-insensitive filesystem makes near-misses collide too, so names are
numbered while walking: `report.pdf`, `report (2).pdf`. Numbering happens
before anything is fetched, which is what makes the name a file is announced
under the name it is written to.

Shortcuts and Apps Script projects are counted as unsupported and skipped: a
shortcut points at another item rather than holding one, and the API will not
export a script project. The count is reported at the end rather than dropped
silently, so a local copy that is short of the folder says so.

## The API key

Optional, and it buys exactly two things: folder listings, and real filenames
without a round trip through the warning page.

It is a *quota identity*, not a credential. A key does not grant access to
anything — a restricted share stays unreadable with or without one — it only
tells Google whose request quota to spend. No login, no OAuth, no consent
screen, and nothing to keep in sync.

Set it in one of two places:

```toml
# ~/.config/rdm/config.toml
gdrive_api_key = "AIza..."
```

```sh
RDM_GDRIVE_API_KEY=AIza... rdm download "https://drive.google.com/drive/folders/..."
```

The environment variable wins, which is the arrangement for a key you would
rather not leave on disk. Neither is ever printed: `rdm config` says `set` or
`none`, because config output ends up in bug reports.

To make one: Google Cloud console → any project → enable the Drive API →
Credentials → Create credentials → API key. Restricting it to the Drive API is
worth the extra click.

## What you get, and what you do not

Transfers go through the same engine as every other host, so chunking, ranged
resume, retries, `.part` files and the progress board are the shared ones.

Resume is keyed on the Drive **id** rather than the URL. A confirmed download
URL carries a short-lived `at` token, so the same bytes come back under a
different URL on the next run; keying on the URL would mean re-downloading from
zero every time.

No integrity check. The API publishes an `md5Checksum` for an uploaded file,
but nothing in this crate can compute one to compare it against, and a digest
that cannot be recomputed is decoration.

Inside a folder, each file takes one connection and the parallelism is files at
once — `gdrive_workers`, default 5, capped at 15. Drive's quota is counted per
key and per second rather than per connection, so a higher number buys 403s
rather than throughput. A single file link, on the other hand, is handed to the
engine whole and gets parallel chunks.

Things that will not work, and why:

- **A file over its share quota** ("Sorry, you can't view or download this file
  at this time"). Drive serves a page with no download form on it. Nothing to
  follow, so this reports what happened instead of saving the page.
- **A restricted share.** Anything that needs sign-in needs OAuth, which this
  does not do.
- **A Google Form**, and anything else Google-native that has no export.

## Layout

| File          | What it does                                                |
| ------------- | ----------------------------------------------------------- |
| `mod.rs`      | vocabulary, entry points, naming rules                      |
| `link.rs`     | link shapes and export formats, all offline                 |
| `api.rs`      | the only part that talks to Google, warning page included   |
| `transfer.rs` | writes a walked folder to disk                              |
| `tests.rs`    | link parsing, export endpoints, warning-page markup, naming |

## Credit

The warning-page handling and the folder-needs-a-key rule follow
[goodls](https://github.com/tanaikech/goodls) by tanaike, which has been
keeping up with Drive's changes to that page for years. Same endpoints and the
same fallbacks, re-implemented against this crate's engine.
