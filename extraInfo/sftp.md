# SFTP

Native, read-only SFTP over SSH. No shell, no `ls` parsing, no remote writes.

```sh
rdm 'sftp://alice@files.example.com/home/alice/README'
rdm download 'sftp://alice@files.example.com/home/alice/archive.tar' -o archive.tar
rdm 'sftp://alice@files.example.com/home/alice/music/' -o music -p 3
rdm queue add 'sftp://alice@files.example.com/home/alice/music/'
rdm sync 'sftp://alice@files.example.com/home/alice/music/' -o /data/music -p 3
rdm sync 'sftp://alice@files.example.com/home/alice/music/' -o /data/music -e flac,mp3 --delete
rdm 'sftp://alice@192.168.1.10:2222/home/alice/file' --allow-private
```

Rules:

- Username required. Port defaults to 22. Bracket IPv6.
- Paths are absolute server paths. No `~` or `host:path` shorthand.
- `-p` is concurrent files. `-c` does not split one file; `-c > 1` warns instead.

## Auth and host keys

No URL passwords. Set credentials in the environment of the process doing the download — queued jobs do not keep the `queue add` environment.

| Variable | Meaning |
|---|---|
| `RDM_SFTP_IDENTITY_FILE` | Private key file; beats password and agent |
| `RDM_SFTP_KEY_PASSPHRASE` | Passphrase for that key |
| `RDM_SFTP_PASSWORD` | SSH password, only when no key is set |
| `RDM_SFTP_KNOWN_HOSTS` | Known-hosts file; defaults to `~/.ssh/known_hosts` |

With nothing configured, up to 16 SSH-agent identities are tried. An explicit credential that fails does not fall back. Prefer `ssh-add` over a passphrase in the environment.

Unknown or changed host keys fail before auth. Verify the fingerprint out of band and enrol it first — no accept-any-key switch. `@revoked` / `@cert-authority` entries are refused.

Not supported: ssh_config, aliases, ProxyJump/ProxyCommand, certificates, default-key discovery, keyboard-interactive MFA, password prompts.

## Resume and sync

Partial state lives in `.rdm-sftp` under each output parent. Resume needs same URL, host key, size, and mtime. A single-file download refuses an existing output; use another `-o` or `sync`.

Sync compares sizes by default, like the HTTP mirror. Use `size+mtime` to also require timestamps:

| Variable | Meaning |
|---|---|
| `RDM_SFTP_SYNC_COMPARE` | `size` (default) or `size+mtime` |
| `RDM_SFTP_MODIFY_WINDOW` | Slack in seconds; default `2`, `0` for exact |

Size-match but time-mismatch files are retimed in place (`Retimed`), no bytes moved. Interrupted syncs never truncate your existing file; stale partials are kept for resume, unreachable ones are removed (`Reclaimed`).

Limit: SFTP size/mtime is not a hash. Same-size rewrites go undetected with default settings. Verify checksums separately when it matters.

## Safety limits

- Symlinks and special files are skipped, never followed. Any skip blocks `sync --delete`.
- UTF-8 paths only. Traversal, oversized names, and `.rdm-sftp` components are rejected.
- `sync --delete` needs explicit `-o`, never runs on `/` or `~`, only touches the `-e` filter, and only after a clean transfer plus a second matching listing.
- Bounded scans (100k files, depth 64, 10-min budget), 15s SSH timeouts. Over budget is an error, not an empty dir.

