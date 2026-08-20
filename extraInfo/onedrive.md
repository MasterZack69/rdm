# OneDrive

Share links work as they come out of the browser:

```
rdm 'https://1drv.ms/f/c/abc123/AbCdEfGh'
rdm 'https://1drv.ms/u/s!AbCdEfGhIjKl' -o ~/Videos/lecture.mp4
rdm queue add 'https://1drv.ms/f/c/abc123/AbCdEfGh'
rdm sync 'https://1drv.ms/f/c/abc123/AbCdEfGh' -o ~/Music -e flac
```

Quote the link. Share links often carry a `?e=...` tail, and an unquoted `?` is a glob pattern your shell will either mangle or refuse outright.

## What rdm actually does

A share link is not an address, it is a shortened invitation, and its path shape tells you nothing: the same `1drv.ms` link may be one file or a folder of four hundred. So rdm asks. No login and no API key are involved — it fetches an anonymous token from Microsoft's badger endpoint, encodes the link itself into a share id (base64url, padding stripped, exactly as the OneDrive REST docs describe), and reads back either a download URL, meaning one file, or an item id to walk, meaning a folder.

Personal OneDrive only. `1drv.ms` and `onedrive.live.com` are claimed; `*.sharepoint.com` and other business shares are left to the generic path, since they need a real sign-in rdm does not have.

A few things worth knowing:

- **`-o` follows the share**, a filename for a single file and a directory for a folder. Folders are walked recursively and mirrored, and every remote name is sanitised on the way in, so a file called `../../.bashrc` still lands inside your download directory.
- **`-c` means two different things**, again following the share: chunks per file for a single file, files at once for a folder. Capped at 15, defaulting to `onedrive_workers`.
- **Resume is keyed on the item id, not the URL.** OneDrive hands out a signed, short-lived download URL, so asking twice for the same file gives two different URLs. Comparing URLs would make every resume look like a different source and start the file over from zero.
- **A folder link is one queue item.** The listing lives behind an API call, so there are no per-file URLs to enqueue: `rdm queue add` stores the link and the whole folder downloads inside that single row. If some files fail the row is marked failed, and `queue retry failed` picks up where it stopped — finished files are skipped, half-finished ones resume.
- **`rdm sync` mirrors a folder share.** The listing carries sizes, so the diff costs no `HEAD` requests: a matching size is up to date, a wrong size is refetched, a missing file is downloaded, and a file the API reported no size for is counted as unverified rather than quietly pulled again. `--delete` refuses to run if any child of the share could not be read, because a listing with holes in it makes every unreadable file look like an orphan.
- **No integrity check.** A share listing publishes no checksum, so a finished file is only verified against its advertised length.
