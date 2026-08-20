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
the link and hands it to the normal engine.
A **folder** share is the exception — Dropbox packs it on the fly and serves it as a single response.

- A folder link is downloaded as a single zip file.
- No resume on a folder link.
- No sync for a folder link.
- `-c` does not apply to a folder link.


Recognised link shapes:

| Shape | Meaning |
| --- | --- |
| `/scl/fi/<id>/<name>` | one file (current links) |
| `/scl/fo/<id>/<hash>` | one folder (current links) |
| `/s/<id>/<name>` | one file (legacy) |
| `/sh/<id>/<hash>` | one folder (legacy) |

Anything else on `dropbox.com` is left alone, and a `dl.dropboxusercontent.com`
link is already direct, so it goes straight down the generic path.


## Password-protected shares

Put the password in the environment, not on the command line:

```
RDM_DROPBOX_PASSWORD=hunter2 rdm "https://www.dropbox.com/scl/fi/abc123/report.pdf?rlkey=xyz&dl=0"
```

Same reasoning as `RDM_GOFILE_PASSWORD`: an argument ends up in shell history
and in `ps` output for every other user on the machine.

**Not supported yet:** `rdm queue add` of a password-protected share. The queue
runner is deliberately hoster-agnostic and holds no session, so it would fetch
the password page instead of the file. Run protected links directly.

## Developer's Note
The testing for password-protected dropbox download has not been done yet due to the lack of a dropbox premium plan, I'm merging this branch `dropbox` anyway as it has been days.


