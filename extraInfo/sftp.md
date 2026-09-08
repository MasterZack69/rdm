# SFTP

Native, read-only SFTP over SSH, using `ssh2`/libssh2. RDM does not invoke a shell, parse `ls`, or require a remote command-execution permission.

**Branch integration:** the CLI and generic scrape hooks in the existing large files are supplied in `SFTP-INTEGRATION.md` for the maintainer to apply. Refresh `Cargo.lock` after adding the dependencies. Until those steps and the Rust checks are complete, this branch is not merge-ready.

## Usage

```sh
# One file, including an extensionless file
rdm 'sftp://alice@files.example.com/home/alice/README'
rdm download 'sftp://alice@files.example.com/home/alice/archive.tar' -o archive.tar

# Recursive directory download; -o names a directory for this case
rdm 'sftp://alice@files.example.com/home/alice/music/' -o music -p 3

# Persistent queue: one file or every regular file in a directory
rdm queue add 'sftp://alice@files.example.com/home/alice/music/'
rdm queue start -p 3

# Metadata-based remote-to-local mirror
rdm sync 'sftp://alice@files.example.com/home/alice/music/' -o /data/music -p 3
rdm sync 'sftp://alice@files.example.com/home/alice/music/' -o /data/music -e flac,mp3 --delete

# Nonstandard port / an explicitly permitted private server
rdm 'sftp://alice@192.168.1.10:2222/home/alice/file' --allow-private
```

- Include the username explicitly. Port defaults to 22. Bracket IPv6 addresses.
- Paths are absolute, canonical server paths, not paths relative to the login home. There is no `~` expansion or scp-style `host:path` shorthand.
- File/directory classification comes from SFTP metadata, regardless of extensions, trailing slashes or `-o`.
- Percent-encode literal `%`, `?` and `#` in URLs. Filenames are decoded exactly once; directory entries are not URL-decoded at all.
- One stream per file. `-p` controls concurrent files; `-c` does not split an SFTP file into chunks. Explicit `-c > 1` is reported rather than silently presented as parallel transfer support.
- Bare and explicit directory downloads use an isolated batch, not unrelated pending queue items. `queue add` persists items without starting them.
- `--allow-private` is retained in queued items. `RDM_ALLOW_PRIVATE` also applies. Every DNS result is checked and the vetted addresses are dialled directly.

## Authentication and trust

RDM never accepts URL passwords or stores SSH credentials in queue JSON or checkpoints. Configure credentials in the environment of the process that actually downloads; queued transfers do not retain the environment of `queue add`.

| Variable | Meaning |
|---|---|
| `RDM_SFTP_IDENTITY_FILE` | Private key file; takes precedence over password and agent |
| `RDM_SFTP_KEY_PASSPHRASE` | Optional passphrase for that key; whitespace is preserved |
| `RDM_SFTP_PASSWORD` | Non-interactive SSH password authentication when no explicit key is set |
| `RDM_SFTP_KNOWN_HOSTS` | Dedicated known-hosts file; defaults to `~/.ssh/known_hosts` |

With neither key nor password configured, RDM tries up to 16 SSH-agent identities. An explicit credential that fails does not silently fall back to another mechanism. For encrypted keys, using `ssh-add` and the agent avoids keeping a passphrase in the process environment.

Unknown and changed host keys fail **before authentication**. Independently verify the server fingerprint and enrol the verified key first. Do not blindly trust `ssh-keyscan` output. Hashed known-host entries and `[host]:port` entries are handled by libssh2. Marked entries such as `@revoked` and `@cert-authority` are refused, not silently ignored; use a dedicated file containing independently verified ordinary host keys. There is no insecure/accept-any-host-key switch.

This backend does **not** implement OpenSSH configuration files, aliases, ProxyJump/ProxyCommand, host certificates, automatic default-key-file discovery, keyboard-interactive MFA or password prompts. Use a real hostname and the supported authentication methods. RDM does not upload, delete or modify remote files during normal operation.

## Resume and existing files

Partial data, private checkpoint metadata and locks live in a reserved `.rdm-sftp` directory beneath each output parent. Filenames are SHA-256-derived to avoid collisions with actual payload names or filesystem name-length limits. HTTP `.part`/`.rdm` state is never reused.

Resume requires the same canonical URL, verified host key, size and modification time. Missing timestamps force a restart. A checkpoint is written only after its byte prefix has been flushed, and bytes beyond the last committed checkpoint are discarded after a crash. Cancellation closes the SSH socket and waits for the blocking worker to stop; the queue cannot reuse a slot while that worker is still writing.

Queue and directory batches preserve existing regular files. A direct single-file download refuses an existing output instead of opening an interactive overwrite prompt; choose another `-o`, or use `sync` to refresh a mirror. Sync retains the old file until the replacement is complete, checks metadata again, preserves the remote mtime, and publishes atomically. Empty files are supported.

`RDM_MAX_FILE_BYTES` applies to advertised SFTP sizes as well: 64 GiB by default, `0` for no ceiling. Recognised transient failures use `max_retries` and capped exponential backoff. Authentication, permissions, invalid paths and host-key failures are not retried. Unclassified read errors fail conservatively; retrying the queue item or command can resume its saved checkpoint.

**Integrity limit:** SFTP v3 size/mtime is not a content hash or immutable version ID. A same-size rewrite that preserves the same second-resolution mtime cannot be detected by resume or sync. Use a stable source or independently verify published checksums where content integrity needs stronger guarantees. SSH still authenticates and protects the transport.

## Discovery and sync safety

- Listings are streamed and bounded: 100,000 files, 10,000 directories, 200,000 entries, depth 64, and a ten-minute scan budget. SSH calls and connection establishment have 15-second timeouts. A failed/over-budget scan is an error, not an empty directory.
- Remote symlinks and special files are not followed. A skipped entry forbids `sync --delete`; the generic scrape adapter also refuses such an incomplete listing. Ordinary directory downloads report their skip count.
- UTF-8 paths are required. Traversal, embedded separators, controls, invalid percent escapes, oversized names, and the reserved `.rdm-sftp` component are rejected rather than silently renamed.
- Local writes and deletion use the existing descriptor-anchored `safe_file` operations. Directory and state symlinks cannot redirect writes.
- A persisted queue output outside the configured download root is conservatively walked from `/`; symlinks in that path are refused because the old queue schema cannot retain a separate trusted root. Use a canonical output path in that case.
- `sync --delete` requires an explicit `-o` for a dedicated mirror, refuses filesystem/home roots, and only considers files matching the extension filter. It preserves local symlinks, special files and internal state. Empty directories are not mirrored or pruned.
- Deletion runs only after successful transfers and a matching second remote listing. Changed local orphan metadata or an active transfer lock prevents removal. There is no server-side snapshot, so use a quiescent source for destructive mirroring.

## Building and checks

Linux build prerequisites include a Rust toolchain, a C toolchain, pkg-config, OpenSSL and zlib development headers. The Nix package and dev shell include the native dependencies.

After applying the manual hooks:

```sh
cargo check --all-targets       # also reconciles Cargo.lock; review that diff
cargo test --lib sftp           # offline regression tests; live test is ignored
cargo test --all-targets
cargo clippy --all-targets
cargo fmt --all -- --check      # may also report existing project formatting
```

The opt-in live test creates a randomly named fixture on a **disposable loopback-only SSH server**, then exercises directory listing, percent-encoded names, cancellation/resume, zero-byte files, queue serialization, atomic sync replacement and orphan deletion. It removes its fixture after the assertions; an interrupted test may leave the uniquely named fixture behind. Never point it at a production account.

1. Start a local OpenSSH server with SFTP enabled and a writable test directory.
2. Verify/enrol its host key in a dedicated known-hosts file and configure a key, agent or password as above.
3. Set a canonical writable parent URL and run the ignored test:

```sh
export RDM_SFTP_TEST_URL='sftp://testuser@127.0.0.1:2222/absolute/writable/test-parent'
export RDM_SFTP_KNOWN_HOSTS='/absolute/path/to/test-known-hosts'
export RDM_SFTP_IDENTITY_FILE='/absolute/path/to/test-key'
cargo test --lib sftp::tests::live -- --ignored --nocapture
```

Verification status for this implementation is recorded separately in `SFTP-INTEGRATION.md`; the commands above are not a claim that they have already passed.
