# SFTP integration handoff

Target: `MasterZack69/rdm`, branch `sftp`.
Baseline: `f7178fa55f78c8eb9b02c56f925566da13a3a680`.

## Status — read first

The native transport, queue/engine dispatch, bounded recursive discovery, isolated directory batches and SFTP sync implementation are provided. Only new files and existing files with **200 lines or fewer at the baseline** are edited automatically.

**The hooks below are deliberately NOT applied.** They are required for the bare CLI, explicit `download`, `queue add` and generic scrape entry points to reach the new backend. Public engine and sync dispatch are wired in the permitted small modules.

`Cargo.lock` is also deliberately untouched. Reconcile it after the dependency additions; do not use `--locked` or expect the Nix package to build until that is done.

**Verification limitation:** this sandbox has no `cargo`, `rustc`, `rustfmt` or local SSH server, and cannot resolve GitHub for a clone/toolchain installation. No compilation, Rust tests, rustfmt, clippy or live SFTP check has been run here. Source review and static file/diff checks are not substitutes. Treat this as an unverified implementation pending the checks below, not a merge-ready release.

The test suite includes offline regression cases and one ignored OpenSSH fixture test. The live test exercises queue serialization, not the full persistent queue runner. Check CLI queue execution after applying the hooks.

## 1. `src/main.rs`: explicit download

Inside the existing match arm:

```rust
Some(Command::Download { url, opts }) => {
```

Insert this **before** the existing comment beginning `// Before normalize_download_url` and all HTTP/MEGA/hoster handling:

```rust
if rdm::sftp::is_sftp_url(&url) {
    return run_async(|cancel| {
        rdm::sftp::run(
            &cfg,
            &url,
            &opts,
            None,
            rdm::sftp::CommandMode::Download,
            cancel,
        )
    });
}
```

Do not remove or reorder the existing non-SFTP branches.

## 2. `src/main.rs`: bare `rdm <URL>`

At the very start of the existing `quick_download` function body, before its MEGA handling:

```rust
fn quick_download(
    cfg: &config::Config,
    url: &str,
    opts: &DownloadOpts,
    parallel: Option<usize>,
) -> Result<()> {
    // Insert the following block here; keep the existing body below it.
```

```rust
if rdm::sftp::is_sftp_url(url) {
    return run_async(|cancel| {
        rdm::sftp::run(
            cfg,
            url,
            opts,
            parallel,
            rdm::sftp::CommandMode::Download,
            cancel,
        )
    });
}
```

## 3. `src/main.rs`: `queue add`

At the very start of this existing function, before its folder-share handling:

```rust
fn queue_add(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
```

```rust
if rdm::sftp::is_sftp_url(url) {
    return run_async(|cancel| {
        rdm::sftp::run(
            cfg,
            url,
            opts,
            None,
            rdm::sftp::CommandMode::Enqueue,
            cancel,
        )
    });
}
```

This inspects remote metadata, persists individual credential-free file URLs and outputs, and retains `--allow-private` for the later queue run.

## 4. `src/scrape/mod.rs`: generic discovery

At the start of the existing function:

```rust
pub async fn discover_files(
    url: &str,
    wrap_in_folder: bool,
    allow_private: bool,
) -> Result<Option<Vec<DiscoveredFile>>> {
```

Insert before `parse_and_validate_url`:

```rust
if crate::sftp::is_sftp_url(url) {
    return crate::sftp::discover_files(url, wrap_in_folder, allow_private).await;
}
```

The existing interface has no cancellation-token parameter. This compatibility adapter therefore owns a token; the direct CLI/queue/sync paths use their caller's cancellation token. Do not interpret `None` as a failed listing: it means the URL names a regular file. Failed or incomplete SFTP scans return an error.

## 5. `Cargo.lock`: generate, do not hand-edit

On a full checkout of `sftp`, after applying the snippets:

```sh
cargo check --all-targets
```

Review and commit the generated lockfile changes. There are no fabricated package checksums or hand-written lock entries in this delivery. Native dependency setup and usage are documented in `extraInfo/sftp.md`.

## 6. Optional `README.md` entry

In the supported-protocol/usage documentation, add:

```markdown
- [SFTP](extraInfo/sftp.md): SSH-agent/key/password authentication, recursive directory discovery, queue downloads, resumable transfers and remote-to-local sync.
```

## Verification on a real checkout

```sh
cargo check --all-targets
cargo test --lib sftp
cargo test --all-targets
cargo clippy --all-targets
cargo fmt --all -- --check
```

The formatting command may also report pre-existing project formatting. Review formatting changes instead of blindly rewriting unrelated large files.

Set up a disposable loopback SFTP server as described in `extraInfo/sftp.md`, then run:

```sh
cargo test --lib sftp::tests::live -- --ignored --nocapture
```

Also exercise these CLI cases against that fixture after wiring `main.rs`:

- Bare and explicit single-file download, including extensionless and zero-byte files.
- Recursive directory download with a chosen output root and `-p`.
- `queue add`, restart the process, and `queue start`; verify credentials are not in queue JSON.
- Cancel/skip during transfer, then retry; compare the downloaded bytes with the fixture.
- Changed/unknown host key and disallowed private address: no authentication or local publication.
- `sync` with extension filtering, a stale local file and explicit `--delete -o`; verify unrelated extensions, local symlinks and internal state survive.
- Unreadable/incomplete/changed remote tree or a failed transfer: no orphan deletion.
- Existing HTTP/hoster tests remain green.

## Existing files edited automatically

These original contents were matched to their GitHub blob hashes before editing. Counts include comments and blank lines, making the limit stricter than a code-only count.

| File | Baseline lines |
|---|---:|
| `Cargo.toml` | 48 |
| `flake.nix` | 64 |
| `src/lib.rs` | 30 |
| `src/args/mod.rs` | 55 |
| `src/args/parse.rs` | 163 |
| `src/engine/mod.rs` | 38 |
| `src/engine/run.rs` | 146 |
| `src/queue/dispatch.rs` | 164 |
| `src/sync/mod.rs` | 31 |

No automatic edits to `src/main.rs`, `src/scrape/mod.rs`, `Cargo.lock`, `README.md` or the existing long HTTP/sync/config/filesystem implementations.
