# Google Drive

Drive links work as they come out of the browser:

```
rdm 'https://drive.google.com/file/d/1A2b3C4d5E6f/view'
rdm 'https://docs.google.com/spreadsheets/d/1A2b3C4d5E6f/edit'
rdm 'https://drive.google.com/drive/folders/1A2b3C4d5E6f' -o ~/Music
```

Quote the link. Links often carry a `?usp=sharing` tail, and an unquoted `?` is a glob pattern your shell will either mangle or refuse outright.

## What rdm actually does

No Drive link is a fetchable address — hand one to a plain downloader and you get an HTML page saved as `view`. So rdm resolves first, and the three link shapes need three different fixes:

- **A file** (`/file/d/<id>/view`, `/uc?id=<id>`, `/open?id=<id>`) is fetched through the same endpoint your browser uses. Small files redirect straight to the bytes; anything Drive declines to virus-scan answers with a warning page instead, and rdm follows the download form on that page to get at the real URL.
- **A Google Doc** (`docs.google.com/document|spreadsheets|presentation|drawings/d/<id>`) has no file behind it at all — it is rendered on request. rdm swaps the link for an export endpoint in the format from `gdrive_doc_format` (default `pdf`; set it to `office` for docx/xlsx/pptx).
- **A folder** (`/drive/folders/<id>`) needs an **API key**. Anonymous access can fetch a file whose link you hold, but nothing anonymous can enumerate a folder, so this refuses up front rather than failing halfway through.

## The API key

Optional, and it buys exactly two things: folder listings, and real filenames without a round trip through the warning page. It is a *quota identity*, not a credential — a restricted share stays unreadable with or without one.

```toml
# ~/.config/rdm/config.toml
gdrive_api_key = "AIza..."
```

```
RDM_GDRIVE_API_KEY=AIza... rdm 'https://drive.google.com/drive/folders/...'
```

The environment variable wins, which is the arrangement for a key you would rather not leave on disk. To make one: Google Cloud console → enable the Drive API → Credentials → API key.

Notes:

- **`-o` follows the link** — a filename for a single file, a directory for a folder. Folders are walked recursively and mirrored under their own name, empty directories included.
- **`-c` means two different things**, following the link: chunks per file for a single file, files at once for a folder. Capped at 15, defaults to `gdrive_workers` — Drive's quota is per key per second, so more buys 403s rather than throughput.
- **Resume is keyed on the Drive id, not the URL.** A confirmed download URL carries a short-lived token, so the same bytes come back under a different URL next run.
- **Duplicate names are numbered while walking** (`report.pdf`, `report (2).pdf`), before anything is fetched — Drive allows two children of one folder to share a name.
- **Shortcuts and Apps Script projects are skipped**, counted as unsupported in the summary rather than dropped silently.
- **No integrity check.** The API publishes an `md5Checksum`, but nothing here can compute one to compare against, so a finished file is only verified against its advertised length.
- **Drive links cannot be queued or synced.** Run them directly with `rdm`.

Things that will not work: a file over its share quota ("Sorry, you can't view or download this file at this time"), anything needing sign-in (that means OAuth, which this does not do), and Google Forms.

Warning-page handling follows [goodls](https://github.com/tanaikech/goodls) by tanaike.
