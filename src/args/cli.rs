//! The root command and the options every download path shares.

use clap::{Args, Parser};

use super::commands::Command;
use super::parse::{parse_connections, parse_parallel, parse_url};

#[derive(Debug, Parser)]
#[command(
    name = "rdm",
    version,
    about = "RDM \u{2014} Rust Download Manager",
    arg_required_else_help = true,
    args_conflicts_with_subcommands = true,
    after_help = "Defaults for -c/-p and the download directory come from config.toml.\nRun `rdm config` to see the values currently in effect.\n\n-p applies only when <URL> is a directory listing, which is expanded into\nthe queue and downloaded concurrently.\n\nsync and queue have options of their own \u{2014} see `rdm sync --help` and\n`rdm queue --help`."
)]
pub struct Cli {
    /// URL to download (shorthand for `rdm download <URL>`)
    #[arg(value_name = "URL", value_parser = parse_url)]
    pub url: Option<String>,

    #[command(flatten)]
    pub opts: DownloadOpts,

    /// Files to download concurrently if <URL> is a directory listing
    /// [default: queue_parallel from config]
    #[arg(short, long, value_name = "N", value_parser = parse_parallel)]
    pub parallel: Option<usize>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Options shared by every code path that downloads something.
#[derive(Debug, Clone, Default, Args)]
pub struct DownloadOpts {
    /// Output file or directory [default: download_dir from config]
    #[arg(short, long, value_name = "PATH")]
    pub output: Option<String>,

    /// Connections per file [default: connections from config]
    #[arg(short, long, value_name = "N", value_parser = parse_connections)]
    pub connections: Option<usize>,

    /// Allow scanning private, loopback and link-local addresses
    ///
    /// `visible_alias` rather than `alias`: this flag is long and gets typed
    /// constantly on LAN hosts, and an alias nobody can find in `--help` is
    /// not really a shortcut.
    #[arg(long, visible_alias = "ap")]
    pub allow_private: bool,

    /// Suppress progress output
    #[arg(short, long)]
    pub quiet: bool,
}
