# Dropbox

Share links work as they come out of the browser:

```
rdm "https://www.dropbox.com/scl/fi/abc123/album.zip?rlkey=xyz&dl=0"
rdm "https://www.dropbox.com/scl/fo/abc123/h?rlkey=xyz&dl=0"
rdm queue add "https://www.dropbox.com/s/abc123/report.pdf?dl=0"
```

Quote the link, or your shell will eat the `&`.

## What rdm actually does

A share link is not a file: with `?dl=0` Dropbox answers with an HTML preview
page. With `?dl=1` the same link redirects to Dropbox's CDN, which serves the
bytes, honours `Range`, and names the file in a `Content-Disposition` header.

So Dropbox needs no API, no token and no downloader of its own. rdm rewrites
the link and hands it to the normal engine, which means Dropbox downloads get
everything the engine already does: parallel connections (`-c`), resume after
an interrupted run, retries, and the usual progress bar.

Recognised link shapes:

| Shape | Meaning |
| --- | --- |
| `/scl/fi/<id>/<name>` | one file (current links) |
| `/scl/fo/<id>/<hash>` | one folder (current links) |
| `/s/<id>/<name>` | one file (legacy) |
| `/sh/<id>/<hash>` | one folder (legacy) |

Anything else on `dropbox.com` is left alone, and a `dl.dropboxusercontent.com`
link is already direct, so it goes straight down the generic path.

## A folder share is one zip

Dropbox packs a folder share on the fly and serves it as a single response.
There is no listing behind the link and no per-file URLs, so:

- `rdm <folder link>` downloads one `.zip`, not a directory tree.
- `rdm sync <folder link>` is refused up front: there is nothing to diff.
- Without `-o`, a folder share is saved as `dropbox-<id>.zip`, because the link
  itself does not say what the folder is called. Pass `-o` for a better name.

A file share keeps the name in its own path, so `album.zip` stays `album.zip`.

## Notes

- `dl=0`, `dl=1` or no `dl` at all: all three work, the flag is never
  duplicated, and every other parameter (`rlkey`, `st`) is preserved — the
  server 404s without them.
- `-p` does nothing here. It parallelises files within a listing, and a share
  link is a single download.
- No config keys and no environment variables were added: Dropbox needs
  neither. `connections`, `download_dir` and `max_retries` apply as usual.
- Password-protected shares are not supported yet.
