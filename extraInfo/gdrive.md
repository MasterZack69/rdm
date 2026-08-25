# Google Drive

Drive links work as they come out of the browser:

```
rdm 'https://drive.google.com/file/d/1A2b3C4d5E6f/view'
rdm 'https://docs.google.com/spreadsheets/d/1A2b3C4d5E6f/edit'
rdm 'https://drive.google.com/drive/folders/1A2b3C4d5E6f' -o ~/Music
```

Quote the link. Links often carry a `?usp=sharing` tail, and an unquoted `?` is a glob pattern your shell will either mangle or refuse outright.

A Drive link is never a fetchable address — hand one to a plain downloader and you get an HTML page. rdm resolves it first: a file is fetched through the endpoint your browser uses (following the virus-scan warning page when Drive serves one), a Google Doc is swapped for an export endpoint, and a folder is listed and walked recursively.

Notes:

- **`-o` follows the link** — a filename for a single file, a directory for a folder. Folders are mirrored under their own name, empty directories included.
- **Google Docs** export to `gdrive_doc_format` (default `pdf`; set it to `office` for docx/xlsx/pptx).
- **`-c` means two different things**: chunks per file for a single file, files at once for a folder. Capped at 15, defaults to `gdrive_workers` — more just buys 403s from Drive's per-key quota.
- **Resume is keyed on the Drive id, not the URL.** A confirmed download URL carries a short-lived token, so the same bytes come back under a different URL next run.
- **Folders are one queue item.** `rdm queue add` stores the link and the whole tree downloads inside that single row; `queue retry failed` picks up where it stopped.
- **`rdm sync` mirrors a folder** through Drive's own path (`-p` does not apply, `-c` is files at once). With an API key, wrong-sized files are refetched; without one, every existing file is `Unverified`. `-d` needs a key.
- **Duplicate names are numbered** (`report.pdf`, `report (2).pdf`) before anything is fetched.
- **No integrity check.** A finished file is only verified against its advertised length.
- **Shortcuts and Apps Script projects are skipped.** Blocked files (over share quota, sign-in required, Forms, or a `resourcekey` link) will not work.

## The API key

Optional. It buys a folder listing paged to the end, a size for every file, and real filenames without a round trip through the warning page. It is a *quota identity*, not a credential — a restricted share stays unreadable with or without one.

```toml
# ~/.config/rdm/config.toml
gdrive_api_key = "AIza..."
```

```
RDM_GDRIVE_API_KEY=AIza... rdm 'https://drive.google.com/drive/folders/...'
```

The environment variable wins. To make one: Google Cloud console → enable the Drive API → Credentials → API key.
