# Rust Download Manager
- A "usable" download manager. Emphasis on usable.
- Resume support on most connections like other download managers.
- Efficient as Fuck.
- Written in rust so it must be cool. Emphasis on must.
- Linux only.

# HoW To uSe
```
RDM — Rust Download Manager

Usage: rdm [OPTIONS] [URL]
       rdm <COMMAND>

Commands:
  download  Download a single URL [aliases: d]
  sync      Mirror a remote directory listing into a local directory
  queue     Manage the download queue [aliases: q]
  config    Show the effective configuration
  help      Print this message or the help of the given subcommand(s)

Arguments:
  [URL]  URL to download (shorthand for `rdm download <URL>`)

Options:
  -o, --output <PATH>     Output file or directory [default: download_dir from config]
  -c, --connections <N>   Connections per file [default: connections from config]
      --allow-private     Allow scanning private, loopback and link-local addresses [aliases: --ap]
  -q, --quiet             Suppress progress output
  -p, --parallel <N>      Files to download concurrently if <URL> is a directory listing [default: queue_parallel from config]
  -h, --help              Print help
  -V, --version           Print version

Defaults for -c/-p and the download directory come from config.toml.
Run `rdm config` to see the values currently in effect.

-p applies only when <URL> is a directory listing, which is expanded into
the queue and downloaded concurrently.

sync and queue have options of their own — see `rdm sync --help` and
`rdm queue --help`.
```

If you have set up the path variable like a normal person then you can reproduce the above wall of text by typing 'rdm', assuming the variable is rdm.

`-o`, `-c`, `--allow-private` and `-q` work on every download path: bare `rdm <URL>`, `rdm download`, `rdm sync` and `rdm queue add`.
`--ap` is a shorthand for `--allow-private`.

## Sync

```
rdm sync <URL> [-o dir] [-c N] [-p N] [-d] [-e flac,mkv]

  -p, --parallel <N>   Files to download concurrently [default: queue_parallel from config]
  -d, --delete         Delete local files that no longer exist on the remote
  -e, --ext <EXT>      Only sync these extensions (repeatable or comma separated)
```

`-o` sets the output path only for a sync, not a filename. `-e` takes a comma separated list or repeated flags, with or without leading dots, and is case insensitive: `-e flac,mkv` and `-e .flac -e .MKV` do the same thing.

A MEGA or OneDrive folder share is mirrored through that hoster's own path rather than through the queue, so `-p` does not apply to it. On a OneDrive share `-c` sets how many files download at once, and the sizes come from the API, so there is no round of `HEAD` requests before the diff.

## Queue

```
rdm queue add <URL> [-o name] [-c N]     Add to queue                [a]
rdm queue list                           Show queue                  [ls, l]
rdm queue start [-p N]                   Start processing            [run, s]
rdm queue stop                           Stop after current download
rdm queue skip                           Skip the download(s) in flight  [next, n]
rdm queue remove <ID>                    Remove one item             [rm]
rdm queue retry [ID|failed|skipped]      Requeue items               [r]
rdm queue clear [pending|done]            Clear queue (all by default)   [c]
```

`-p` on `queue start` defaults to `queue_parallel` from the config.

Directory-looking URLs are scraped: `rdm <URL>` on a listing enqueues everything it finds and starts downloading, and `rdm queue add <URL>` enqueues without starting.

## Hoster section

- [mega - click to view](extraInfo/mega.md)
- [gofile - click to view](extraInfo/gofile.md)
- [dropbox - click to view](extraInfo/dropbox.md)
- [onedrive - click to view](extraInfo/onedrive.md)

# Example Config File

```
# Parallel connections per file
connections = 12

# Default download directory
download_dir = "~/Downloads"

# Max Retries?
max_retries = 69

# multi-file download at once
queue_parallel = 5

# MEGA: chunk workers per file
mega_workers = 6

# MEGA: verify the MAC after downloading. Costs a full reread of the file.
# Turning this off means silent corruption stays silent.
mega_verify_mac = true

# MEGA: when quota-blocked, resume early if your public IP changes
mega_resume_on_ip_change = true

# GoFile: how many files to download at once (max 10)
gofile_workers = 5

# GoFile: your account token
gofile_token = ""

# OneDrive: how many files to download at once (max 15)
onedrive_workers = 5
```

Everything in the config is optional as they have their own defaults.

# Release
Zack encourages you to build from source. As some random internet person once said, "Always build from source"
NixOS users get a flake for easy installation :)

# Build from source
```
git clone https://github.com/MasterZack69/rdm
cd rdm
cargo build --release
```

# Tests
```
cargo test
```

# Credits
- MasterZack69 - Of course I am getting the Credits
- Claude Opus 4.6 - Wrote the code, fixed the bugs
- GPT 5.2 - Asked “what if it races?” one too many times
- Claude Opus 4.7 - Here to do everything better
- Claude Opus 5 - Clap Migration, Queue System and Hosters
- DeepSeek V4 Flash - Clippy error fixer

## Prior art

[MegaBasterd](https://github.com/tonikelope/megabasterd) by tonikelope (GPLv3) — the MEGA support here is a clean-room Rust implementation, but MegaBasterd is where the non-obvious parts came from: that the 509 quota is per-IP rather than per-connection, that backoff should end early when your IP changes, that chunk workers must not share a keep-alive socket, and that a 403 means an expired temp URL rather than a missing file. Years of bug reports, distilled. No MegaBasterd code was copied.

[gofile-downloader](https://github.com/ltsdw/gofile-downloader) by ltsdw (GPLv3) — the GoFile support here is a clean-room Rust implementation, but gofile-downloader is where the non-obvious parts came from: the `X-Website-Token` recipe and the four-hour slot it is built from, that a guest account has to exist before anything can be listed at all, that the token has to travel as both an `Authorization` header and an `accountToken` cookie, that a `200` answering a `Range` request means the resume was refused rather than granted, and that GoFile's own top-level folder is usually named the useless default `root`. All of that is invisible in the API and obvious only after someone else has been bitten by it. No gofile-downloader code was copied.
