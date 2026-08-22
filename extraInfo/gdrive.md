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
- **A folder** (`/drive/folders/<id>`) is listed one of two ways. With an API key, through the Drive API: paged to the end, with a size for every file. Without one, through the page Drive hands websites to embed, which lists a folder's children as links — enough to walk an ordinary share, but it renders one batch and never says what it left out. Subfolders are followed either way, and a keyless listing that comes back at the length the page stops at says so on stderr rather than passing itself off as complete.

## The API key

Optional, and it buys three things: a folder listing paged to the end rather than capped by a page, a size for every file, and real filenames without a round trip through the warning page. It is a *quota identity*, not a credential — a restricted share stays unreadable with or without one.

```toml
# ~/.config/rdm/config.toml
gdrive_api_key = "AIza..."
```

```
RDM_GDRIVE_API_KEY=AIza... rdm 'https://drive.google.com/drive/folders/...'
```

The environment variable wins, which is the arrangement for a key you would rather not leave on disk. To make one: Google Cloud console → enable the Drive API → Credentials → API key.

## Queue

A folder goes in as one item, not one item per file:

```
rdm q a 'https://drive.google.com/drive/folders/1A2b3C4d5E6f'
rdm q s
```

The whole tree is fetched under that item and reported into its single progress line — the queue has no way to turn one row into four hundred. `-c` on the item sets how many of its files download at once. A file that fails leaves the item failed, so `rdm queue retry failed` picks the folder back up, skipping everything already on disk.

## Sync

```
rdm sync 'https://drive.google.com/drive/folders/1A2b3C4d5E6f' -o ~/Music
```

Mirrored through Drive's own path rather than through the queue, so `-p` does not apply and `-c` is files at once.

What the mirror can promise depends on the key, because only the API states a size:

- **With a key**, a file whose local copy is a different size is stale: the copy is removed and fetched again. A Google Doc has no stored size to be compared against — an export is rendered on request — so an existing copy is left alone and counted under `Unverified`.
- **Without a key** nothing has a size, so every existing file is `Unverified`. A keyless mirror adds what is missing and never repairs what changed.

`-d`/`--delete` needs a key, and is refused without one: a keyless listing may be short, and a file the page never rendered is indistinguishable from one the folder dropped. It is skipped for the same reason when the folder holds anything unsupported.

## Notes

- **`-o` follows the link** — a filename for a single file, a directory for a folder. Folders are walked recursively and mirrored under their own name, empty directories included.
- **`-c` means two different things**, following the link: chunks per file for a single file, files at once for a folder. Capped at 15, defaults to `gdrive_workers` — Drive's quota is per key per second, so more buys 403s rather than throughput.
- **Resume is keyed on the Drive id, not the URL.** A confirmed download URL carries a short-lived token, so the same bytes come back under a different URL next run.
- **Duplicate names are numbered while walking** (`report.pdf`, `report (2).pdf`), before anything is fetched — Drive allows two children of one folder to share a name.
- **Shortcuts and Apps Script projects are skipped**, counted as unsupported in the summary rather than dropped silently.
- **No integrity check.** The API publishes an `md5Checksum`, but nothing here can compute one to compare against, so a finished file is only verified against its advertised length.

Things that will not work: a file over its share quota ("Sorry, you can't view or download this file at this time"), anything needing sign-in (that means OAuth, which this does not do), Google Forms, and a folder whose link carries a `resourcekey`.

Warning-page handling follows [goodls](https://github.com/tanaikech/goodls) by tanaike; the keyless folder listing follows [gdown](https://github.com/wkentaro/gdown) by wkentaro.
