# AGENTS.md — rdm contributor guide

## What this repository is

`rdm` is a Linux-focused Rust command-line download manager. It downloads
ordinary HTTP/HTTPS files, recursively discovers conventional HTTP directory
listings, and supports native SFTP. It also has first-class routes for MEGA,
Dropbox, OneDrive, Google Drive, and pixeldrain links. The project is
an application crate (`src/main.rs`) backed by a library crate (`src/lib.rs`),
so command dispatch remains thin and testable behavior lives in library
modules.

The core user-facing workflows are:

- `rdm <URL>` / `rdm download <URL>`: download one item; directory-looking
  HTTP/SFTP URLs are expanded and run through the queue.
- `rdm sync <URL>`: mirror a remote listing to a local directory, optionally
  filter extensions and delete safe, verified orphans.
- `rdm queue …`: persist, inspect, and process a download queue across
  invocations.
- `rdm config`: print the effective configuration.

Read `README.md` and the relevant file in `extraInfo/` before changing a
user-visible hoster workflow. These documents describe the supported link
shapes and configuration from the operator's point of view.

## Toolchain and routine commands

- Rust edition: **2024**; package name: `rdm`.
- Main dependencies: Tokio for async work, reqwest for HTTP, clap for the
  CLI, serde/toml for configuration and persisted queue data, ssh2 for SFTP,
  and aes/base64 for MEGA.
- Build/debug: `cargo build`.
- Run all tests: `cargo test`.
- Lint before submitting Rust changes: `cargo clippy --all-targets -- -D warnings`.
- Format before submitting Rust changes: `cargo fmt --check`; use `cargo fmt`
  to apply formatting.
- The release profile is intentionally small (`lto`, one codegen unit,
  `panic = "abort"`, stripped, `opt-level = "z"`). Do not relax it casually.

Some SFTP test code has live/network-oriented coverage. Prefer the normal
test suite first; do not make tests depend on third-party services unless that
is explicitly the purpose of the test.

## Repository map

| Area | Responsibility |
|---|---|
| `src/main.rs` | Parses CLI, loads configuration once, routes hoster/protocol-specific commands, installs cancellation handling, and owns top-level messages. |
| `src/lib.rs` | Declares library modules. `crate::mega` is a transitional re-export of `hoster::mega`; new code should use `crate::hoster::mega`. |
| `src/args/` | clap schema, subcommand/options types, parsers/limits, and CLI contract tests. Config-derived values intentionally stay `Option<T>` until dispatch. |
| `src/config.rs` | Config file location, defaults, safe parsing, config persistence, and credential/environment precedence. |
| `src/engine/` | Generic single-download engine: request/outcome types, HTTP client, URL/name handling, output collision policy, range/streaming transfer paths, and progress wrappers. |
| `src/net/` | Shared networking policy and scoped client behavior. |
| `src/scrape/` | Safe recursive directory-listing discovery: scope validation, DNS/address checks, redirect handling, body limits, HTML link parsing, and path sanitization. |
| `src/queue/` | Persistent queue items/state/store, cross-process locks, signals, hoster dispatch, table output, and concurrent runner. |
| `src/sync/` | Remote mirror planning, verification, extension filtering, controlled deletion, and hoster/SFTP-specific sync routes. |
| `src/sftp/` | Read-only native SFTP URL parsing, host-key/session handling, remote scanning, resumable transfer/checkpoint state, and sync support. |
| `src/hoster/` | Provider-specific parsing/resolution/download behavior. `mega/`, `dropbox/`, `onedrive/`, `gdrive/`, and `pixeldrain/` are independent integrations. |
| `src/ui/` | All terminal presentation: single-download bars, concurrent board, counters/spinners, formatting, terminal width, and terminal-text sanitization. |
| Root utility modules | `safe_path`, `safe_file`, `secret_file`, `secret_url`, `resume`, `retry`, `range_download`, `pressure`, `parallel`, and `signal` provide shared safety, retry, transfer, concurrency, and cancellation building blocks. |
| `extraInfo/` | Hoster- and SFTP-specific end-user documentation. |
| `flake.nix` | Nix development/build packaging metadata. |

## Execution and routing model

1. `main` parses `Cli`, calls `Config::load()` exactly once, and passes that
   same configuration to the engine and command handlers.
2. SFTP and known hosters must be identified **before** generic URL
   normalization or the directory-listing heuristic. Their share URLs are API
   handles or carry fragments/query data that generic handling can corrupt.
3. Generic single files go through `engine`; listings are discovered by
   `scrape` and become queue work. `sync` chooses its own hoster/SFTP/generic
   implementation because its metadata and deletion rules differ from a
   download.
4. Long-running async routes receive a `CancellationToken` via the top-level
   async runner. Preserve cancellation propagation when adding work or
   spawning tasks.
5. The engine reports progress through `ui::ProgressSink`; it does not print
   directly. Keep user-facing presentation in `ui` or the top-level command
   layer.

## CLI and configuration rules

- Keep the bare URL form equivalent to `download` where applicable.
- `DownloadOpts` contains only options shared by every download path:
  output, connections, private-address permission, and quiet mode. Do not add
  listing-only `--parallel` there; it belongs only to bare listing downloads,
  `sync`, and `queue start`.
- Do not give clap a static default for values that come from `config.toml`.
  Preserve `Option<T>` and resolve it after `Config::load()`.
- Update help footers and the parser/help tests whenever flags or their scope
  change. A command's help text must not advertise a flag it does not accept.
- `-o` is a filename/path for a download but a destination directory for
  `sync`; preserve that distinction.
- Config lives under the platform config directory at `rdm/config.toml`.
  Missing config creates defaults; unreadable or invalid existing config is an
  error and must never silently reset user settings.
- New config fields need serde defaults for backwards compatibility. Treat
  Drive/pixeldrain keys as secrets: do not print them, and
  preserve owner-only file permissions through `secret_file`.
- Environment overrides currently include `RDM_GDRIVE_API_KEY`,
  `RDM_PIXELDRAIN_API_KEY`, `RDM_ALLOW_PRIVATE`, `RDM_ALLOW_PROXY`, and
  `RDM_MAX_FILE_BYTES`. Document any new public variable in `README.md` and
  relevant hoster documentation.

## Security and data-integrity invariants

This is download software processing untrusted URLs, headers, HTML, remote
filenames, and credentials. Treat these requirements as compatibility
constraints, not optional hardening:

- Never let a remote name or listing path escape its requested download root.
  Use the existing safe-path/name helpers rather than hand-joining strings.
- Keep encoded traversal, Windows-reserved names, collisions, symlinks, and
  temporary/resume files in mind whenever mapping remote paths to disk.
- The generic scraper rejects private/loopback/link-local destinations by
  default, resolves names before connecting, pins checked addresses, controls
  redirects hop-by-hop, and disables system proxies unless explicitly allowed.
  Do not weaken any of those steps to make a request simpler.
- Preserve `--allow-private` / `RDM_ALLOW_PRIVATE` as explicit opt-ins. If
  proxy use is supported in a new client, reason about where name resolution
  occurs and retain the warning/opt-in model.
- Redact tokens, passwords, signed URLs, and query strings in errors/logging
  with `secret_url`; do not print credentials from config or auth flows.
- Sanitize every network-derived string before drawing it in a terminal using
  `ui::terminal_safe` (or the established presentation path). Terminal escape
  sequences are input, not decoration.
- Preserve resume semantics and output collision policies. Do not overwrite a
  completed file or discard valid partial state without an explicit policy.
- `sync --delete` must remain conservative: only delete paths proven to be
  in the selected mirror root, respect filters, avoid symlink traversal, and
  keep bulk-delete confirmation behavior.
- SFTP remains non-interactive and read-only. Do not add password prompts or
  bypass host-key trust checks; blocking SSH work must remain isolated from
  Tokio workers and cancellation must close/drain sessions safely.
- For MEGA, retain cryptographic integrity verification behavior unless a
  documented configuration option explicitly disables it.

## Concurrency, UI, and persistence

- Queue state and its processor are protected by cross-process locks. Use the
  queue state/store APIs for mutations; do not edit queue files ad hoc.
- Queue runners and provider folder downloads may run files concurrently.
  Bound new parallelism, propagate cancellation, and make status writes safe
  under interruption/retry.
- UI live blocks rely on exact terminal cursor accounting. Every rendered line
  must be clipped to `term_width() - 1` using column width (not character
  count); live blocks do not end with a newline.
- Do not hold a UI mutex while calling code that might acquire it again. These
  are non-reentrant standard mutexes and nested acquisition deadlocks.
- Write progress to the established stderr UI paths and retain useful plain,
  scroll-safe behavior when stderr is not a TTY.

## Change guidance

- Keep module boundaries: parsing in `args`, top-level routing in `main`,
  transport in `engine`/provider modules, presentation in `ui`, and persistent
  queue work in `queue`.
- Add focused unit tests beside the owning module (many modules use an inline
  `tests` module or `tests.rs`). Test parsing, unsafe input, URL/path edge
  cases, retries, collision behavior, and cancellation boundaries rather than
  only happy paths.
- Prefer existing abstractions and error context (`anyhow::Context`) over
  duplicate ad-hoc clients, URL parsing, path cleanup, or terminal rendering.
- Keep comments that explain non-obvious protocol, security, or race
  decisions. This code intentionally documents several historical failure
  modes; remove or rewrite such comments only when the invariant changes.
- If a user-visible command, option, config field, environment variable, or
  hoster capability changes, update `README.md` and the relevant `extraInfo`
  page in the same change.
- Use `cargo fmt`; do not add import-level `try`/`catch` patterns (Rust does
  not need them, and repository policy forbids wrapping imports that way).

## Before handing off a change

1. Run `cargo fmt --check`.
2. Run targeted tests for the touched module, then `cargo test` when practical.
3. Run `cargo clippy --all-targets -- -D warnings` for Rust changes.
4. Check `git diff --check` and review the diff for leaked URLs, keys, or
   accidental behavior/documentation drift.
5. Mention any test that could not run and why.
