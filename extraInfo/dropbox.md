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
bytes and names the file in a `Content-Disposition` header.

So Dropbox needs no API, no token and no downloader of its own. rdm rewrites
the link and hands it to the normal engine, which means a **file** share gets
everything the engine already does: parallel connections (`-c`), resume after
an interrupted run, retries and the usual progress bar.

A **folder** share is the exception, and it is Dropbox's doing rather than
rdm's — see below.

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

**No resume and no parallel connections on a folder share.** The zip is built
while it is being sent, so Dropbox cannot offer a byte range into a file that
does not exist yet and the response arrives without `Accept-Ranges`. rdm
notices and drops to a single connection — `Range: not supported`,
`Chunks: 1` — which is correct rather than a failure, but it does mean an
interrupted folder download starts over. `-c` is not ignored on purpose; there
is simply nothing for it to split.

A file share has none of that problem: it is a real stored object, so ranges,
resume and `-c` all work normally.

## Password-protected shares

Put the password in the environment, not on the command line:

```
RDM_DROPBOX_PASSWORD=hunter2 rdm "https://www.dropbox.com/scl/fi/abc123/report.pdf?rlkey=xyz&dl=0"
```

Same reasoning as `RDM_GOFILE_PASSWORD`: an argument ends up in shell history
and in `ps` output for every other user on the machine.

This is the one case that cannot be handled by rewriting a URL, because the
authorisation is a session rather than part of the link. rdm fetches the share
page, and if it finds a password form it posts the password to Dropbox and
keeps the resulting cookies for the download. The cookie jar is scoped to
`dropbox.com`, so the session is not handed to `dropboxusercontent.com` when
the download redirects to the CDN.

The download itself is still done by the normal engine, so ranges, resume, `-c`
and retries behave exactly as they do on a public share.

Worth knowing:

- **Every Dropbox link costs one small HTML request**, public ones included,
  because that is the only way to know a password is wanted. Without it a
  protected share answers `dl=1` with its password page and rdm would save
  that HTML under the name of the file you asked for.
- **A missing password is reported before anything is downloaded**, naming the
  variable to set.
- **A wrong password is reported as a rejection**, not as a network error:
  Dropbox answers the attempt with a 200 either way, so it is the body that
  decides.
- **The session lasts for the one command.** Nothing is written to disk, so
  each run authenticates again.

**Not supported yet:** `rdm queue add` of a password-protected share. The queue
runner is deliberately hoster-agnostic and holds no session, so it would fetch
the password page instead of the file. Run protected links directly.

## Notes

- `dl=0`, `dl=1` or no `dl` at all: all three work, the flag is never
  duplicated, and every other parameter (`rlkey`, `st`, `e`) is preserved — the
  server 404s without them.
- `-p` does nothing here. It parallelises files within a listing, and a share
  link is a single download.
- No config keys were added: Dropbox needs none. `connections`, `download_dir`
  and `max_retries` apply as usual, and the password is environment-only.
