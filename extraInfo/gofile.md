# GoFile

gofile.io links work the same way:

```
rdm 'https://gofile.io/d/AbCdEf'
rdm 'https://gofile.io/d/AbCdEf' -o ~/Videos/thatshow -c 3
```

A GoFile link is not an address, it is a content id. rdm creates a throwaway guest account (the same thing your browser does when you open the page), asks the API what is behind the id, mirrors the folder tree locally and downloads the files.

A few things worth knowing:

- **A single file lands straight in `download_dir`.** No wrapper folder: a one-file link gives you `~/Downloads/thatfile.zip`, not `~/Downloads/AbCdEf/thatfile.zip`.
- **Anything else goes in a folder of its own** — several files, or one file the uploader already put in a folder. The alternative is forty loose files strewn across your download directory with nothing tying them together.
- **That folder keeps the uploader's name for it**, so a link to a folder called `bakchodi` gives you `~/Downloads/bakchodi`. When nobody named it, GoFile fills the field in with the useless default `root` or with the content id, and in that case the content id is used: `~/Downloads/AbCdEf`.
- **`-o` is always a directory, never a filename**, whichever of those cases you land in. One content id can hold a single file or a hundred in nested folders and the link does not say which, so a flag that sometimes meant a filename would be decided by somebody else's upload.
- **`-c` sets how many files download at once**, not chunks per file. GoFile throttles per connection, so several files side by side is what actually goes faster. Capped at 10; the API gets unfriendly beyond that.
- **Files already on disk are skipped**, so rerunning a link is cheap. This is also the closest thing to `sync` for GoFile — see below.
- **Password-protected links** need `RDM_GOFILE_PASSWORD`. It is hashed before it leaves the process, and it lives in the environment rather than in a flag so it stays out of your shell history and out of `ps` for everyone else on the machine.

  ```
  RDM_GOFILE_PASSWORD='hunter2' rdm 'https://gofile.io/d/AbCdEf'
  ```

- **Got a GoFile account?** Put its token in `gofile_token`, or pass `RDM_GOFILE_TOKEN`, and your quota is used instead of a guest one. The environment wins over the config file.
- **GoFile links cannot be queued or synced.** One link is N files with no individual URLs to store, so `rdm queue add` refuses it. `rdm sync` refuses it too: there is no listing page to re-read and diff, only an API. Run `rdm <link>` directly — rerunning it skips whatever is already on disk, which is most of what you wanted `sync` for.
- **No integrity check.** GoFile publishes no checksum, so a finished file is only verified against its advertised length. That catches a truncated download, not a corrupt one.
