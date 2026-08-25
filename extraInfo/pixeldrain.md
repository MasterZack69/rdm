# pixeldrain

```
rdm https://pixeldrain.com/u/AbCdEf12       # one file
rdm https://pixeldrain.com/l/AbCdEf12       # a list, into a directory
rdm sync https://pixeldrain.com/l/AbCdEf12  # that list, kept in step
```

pixeldrain is the least troublesome host rdm supports. The API is documented, needs no credential, and `https://pixeldrain.com/api/file/<id>` is an ordinary ranged HTTPS URL that keeps working — no signature, no expiry. rdm asks the API for a name, hands the URL to the normal download engine, and gets resume, `.part` files and parallel chunks for free.

The one thing rdm cannot do is fetch the link as given: `/u/<id>` is a page for a human, so a plain downloader saves an HTML document and reports success.

## Files and lists

The link says which it is:

| Link | Meaning | What rdm does |
|---|---|---|
| `/u/<id>` | one file | one download, one progress bar |
| `/l/<id>` | a list (album) | a directory named after the list, several files at once |

The `/api/file/<id>` and `/api/list/<id>` forms work too. A `/u/` link queues as given and asks the API only when it runs; a `/l/` link is turned away from the queue and pointed at `rdm sync` instead, since a list is many files with no single URL to store.

## The API key

Optional. It buys **speed, not access**: pixeldrain caps anonymous transfers and lifts the cap for an account. When a file is capped and no key is configured, rdm says so before starting.

```toml
# ~/.config/rdm/config.toml
pixeldrain_api_key = "your-key"
pixeldrain_workers = 4          # files of a list at a time
```

```sh
export RDM_PIXELDRAIN_API_KEY=your-key
```

The environment wins over the config. There is deliberately **no command-line flag** for the key — an argument ends up in shell history and in `ps`.

## `-c` means two things

- on a **file** link, `-c` is parallel chunks within that file;
- on a **list** link, `-c` is how many files download at once, one connection each.

A file with a `download_speed_limit` is capped whatever the connection count, so chunking those buys latency rather than bandwidth.

## Mirroring a list

```
rdm sync https://pixeldrain.com/l/AbCdEf12 [-o dir] [-c N] [-e mkv,flac] [-d]
```

One request returns every file's name and size, so the diff costs a `stat` per file rather than a request. The mirror gets its own directory named after the list title unless `-o` says otherwise. A wrong-sized file is removed and refetched (keeping its `.part`/`.rdm` so an interrupted mirror resumes); an entry with no size is counted `Unverified`; `-d` deletes local files the list no longer has, and is skipped if anything could not be read.

Notes:

- **Blocked files fail early.** pixeldrain reports an abuse block or spent share in the `availability` field of a `200 OK`, so rdm quotes its own message instead of a later bare 403.
- **Duplicate names survive** as `cover (2).jpg` rather than overwriting; a mirror compares against those settled names.
- **No checksum verification.** `/info` publishes a SHA-256, but checking it would mean reading the finished file back off disk.
- Only `pixeldrain.com` and `www.pixeldrain.com` are recognised.
