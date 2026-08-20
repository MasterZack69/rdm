# GoFile

gofile.io links work the same way:

```
rdm 'https://gofile.io/d/AbCdEf'
rdm 'https://gofile.io/d/AbCdEf' -o ~/Videos/thatshow -c 3
```

A GoFile link is a content id, not an address. rdm creates a throwaway guest account (same as your browser does when opening the page), asks the API what is behind the id, mirrors the folder tree locally and downloads the files.

Notes:

- **`-o` is always a directory, never a filename** — one content id can hold a single file or a hundred in nested folders, and the link does not say which.
- **`-c` sets files downloading at once, not chunks per file.** GoFile throttles per connection, so several files side by side is faster. Capped at 10; the API gets unfriendly beyond that.
- **Password-protected links** need `RDM_GOFILE_PASSWORD`. It is hashed before leaving the process, and lives in the environment rather than a flag so it stays out of shell history and `ps`.

  ```
  RDM_GOFILE_PASSWORD='hunter2' rdm 'https://gofile.io/d/AbCdEf'
  ```

- **Got a GoFile account?** Put its token in `gofile_token`, or pass `RDM_GOFILE_TOKEN`, and your quota is used instead of a guest one.
- **GoFile links cannot be queued or synced.** One link is N files with no individual URLs to store, so `rdm queue add` refuses it. `rdm sync` refuses it too: there is no listing page to re-read and diff, only an API.
- **No integrity check.** GoFile publishes no checksum, so a finished file is only verified against its advertised length.
