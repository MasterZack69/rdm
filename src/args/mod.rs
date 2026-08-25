//! Command-line interface definition.
//!
//! All argument parsing lives here so `main.rs` is pure dispatch.
//!
//! ## Submodules
//!
//! - `cli`: the root command and the options every download path shares.
//! - `commands`: the subcommand tree and its argument value types.
//! - `parse`: value parsers, their limits, and `--ext` normalisation.
//!
//! ## Why the options are `Option<T>`
//!
//! Flags whose default comes from `config.toml` (`--connections`,
//! `--parallel`, `--output`) are deliberately modelled as `Option<T>` with no
//! clap-level `default_value`. clap cannot know the user's configured values,
//! so baking a default in here would either print a wrong default in `--help`
//! or silently override the config file. `main` resolves `None` against the
//! loaded [`crate::config::Config`] instead.
//!
//! ## Flag scoping
//!
//! `-o/--output`, `-c/--connections`, `--allow-private` (short form: `--ap`)
//! and `-q/--quiet` are shared by every download path via [`DownloadOpts`].
//!
//! `-p/--parallel` is scoped to the three places where more than one file can
//! be in flight: `sync`, `queue start`, and the root `rdm <URL>` form when the
//! URL turns out to be a directory listing. It is deliberately *not* part of
//! [`DownloadOpts`], which would wrongly add it to `download` and `queue add`.
//!
//! `-d/--delete` and `-e/--ext` belong to `sync` alone.
//!
//! ## Hoster links are not special here
//!
//! `parse_url` accepts any http(s) URL and rejects everything else. Which
//! hoster a link belongs to is `main`'s problem: a `1drv.ms` share and a
//! `mega.nz` link are ordinary URLs at this layer, and there are tests
//! pinning that, because a parser that got clever about hostnames would
//! start rejecting the links it had not been taught yet.
//!
//! Keep the `after_help` footers honest: a footer must never mention a flag
//! that its own command does not have. There is a test for this.

mod cli;
mod commands;
mod parse;

#[cfg(test)]
mod tests;

pub use cli::{Cli, DownloadOpts};
pub use commands::{ClearTarget, Command, QueueCommand, RetryTarget};
pub use parse::{
    MAX_CONNECTIONS, MAX_PARALLEL, normalize_extensions, parse_connections, parse_parallel,
    parse_retry_target, parse_url,
};
