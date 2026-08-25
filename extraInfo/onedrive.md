# OneDrive

Share links work as they come out of the browser:

```
rdm 'https://1drv.ms/f/c/abc123/AbCdEfGh'
rdm 'https://1drv.ms/u/s!AbCdEfGhIjKl' -o ~/Videos/lecture.mp4
rdm queue add 'https://1drv.ms/f/c/abc123/AbCdEfGh'
rdm sync 'https://1drv.ms/f/c/abc123/AbCdEfGh' -o ~/Music -e flac
```

Quote the link. Share links often carry a `?e=...` tail, and an unquoted `?` is a glob pattern your shell will either mangle or refuse outright.

A share link is a shortened invitation, not an address, and its path shape tells you nothing — the same `1drv.ms` link may be one file or a folder of four hundred. rdm asks with no login or API key: it fetches an anonymous token, encodes the link into a share id, and reads back either a download URL (one file) or an item id to walk (a folder). `1drv.ms` and `onedrive.live.com` are claimed; business shares (`*.sharepoint.com`) need a sign-in rdm does not have.

Notes:

- **`-o` follows the share** — a filename for a single file, a directory for a folder. Folders are walked recursively and mirrored, and every remote name is sanitised on the way in.
- **`-c` means two different things**: chunks per file for a single file, files at once for a folder. Capped at 15, defaults to `onedrive_workers`.
- **Resume is keyed on the item id, not the URL.** OneDrive hands out a signed, short-lived download URL, so asking twice gives two different URLs.
- **A folder link is one queue item.** `rdm queue add` stores the link and the whole folder downloads inside that single row; `queue retry failed` picks up where it stopped.
- **`rdm sync` mirrors a folder.** Sizes come with the listing, so a matching size is up to date, a wrong size is refetched, and a file with no reported size is counted `Unverified`. `--delete` refuses if any child could not be read.
- **No integrity check.** A finished file is only verified against its advertised length.
