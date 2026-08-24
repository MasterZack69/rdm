# pixeldrain

```
rdm https://pixeldrain.com/u/AbCdEf12      # one file
rdm https://pixeldrain.com/l/AbCdEf12      # a list, into a directory
```

pixeldrain is the least troublesome host rdm supports. The API is documented,
reading it needs no credential, and `https://pixeldrain.com/api/file/<id>` is
an ordinary ranged HTTPS URL that keeps working — no signature, no expiry, no
redeem step. So rdm asks the API for a name, hands the URL to the normal
download engine, and gets resume, `.part` files and parallel chunks for free.

The one thing rdm cannot do is fetch the link as given: `/u/<id>` is a page
for a human, so a plain downloader saves an HTML document under a plausible
filename and reports success.

## Files and lists

pixeldrain is the only host here whose link says which it is:

| Link | Meaning | What rdm does |
|---|---|---|
| `/u/<id>` | one file | one download, one progress bar |
| `/l/<id>` | a list (album) | a directory named after the list, several files at a time |

The `/api/file/<id>` and `/api/list/<id>` forms work too, so a URL copied out
of the API docs does the right thing.

Because the link is unambiguous, `rdm queue add` accepts a `/u/` link and
rewrites it to the direct URL on the spot — unsigned, so it is still valid
whenever the queue reaches it. A `/l/` link is refused there: a list is many
files with no single URL to store. Run it directly instead; rerunning skips
whatever is already on disk.

## The API key

Optional. It buys **speed, not access**: pixeldrain caps anonymous transfers
and lifts the cap for an account. When a file is capped and no key is
configured, rdm says so before starting rather than leaving you to guess why a
download is slow.

```toml
# ~/.config/rdm/config.toml
pixeldrain_api_key = "your-key"
pixeldrain_workers = 4          # files of a list at a time
```

Or, to keep the credential out of a file:

```sh
export RDM_PIXELDRAIN_API_KEY=your-key
```

The environment wins over the config. There is deliberately **no
command-line flag** for the key: an argument ends up in shell history and in
`ps` output for every other user on the machine. The key travels in an
`Authorization` header, never in the URL, so it stays out of resume state and
off the progress line.

## `-c` means two things

As with GoFile and OneDrive:

- on a **file** link, `-c` is parallel chunks within that file;
- on a **list** link, `-c` is how many files download at once, one connection
  each.

A file with a `download_speed_limit` is capped whatever the connection count,
so chunking those buys latency rather than bandwidth.

## Notes and limits

- **Blocked files fail early and honestly.** pixeldrain reports an abuse block
  or a spent bandwidth share in the `availability` field of a `200 OK`, so rdm
  checks it and quotes pixeldrain's own message instead of getting a bare 403
  later and guessing at it.
- **Duplicate names survive.** Lists are flat and names in them need not be
  unique, so a second `cover.jpg` becomes `cover (2).jpg` rather than
  overwriting the first.
- **No checksum verification.** `/info` publishes a SHA-256, but checking it
  would mean reading the finished file back off disk, which the engine has no
  hook for. rdm does not claim a check it does not perform.
- **`rdm sync` does not take pixeldrain links yet.** A list is re-readable and
  therefore diffable, so this is a missing feature rather than an impossible
  one. Rerunning the link is the workaround.
- Only `pixeldrain.com` and `www.pixeldrain.com` are recognised. Lookalike
  hosts such as `notpixeldrain.com` or `pixeldrain.com.example.net` are left
  to the generic engine on purpose.
