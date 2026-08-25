//! The subcommand tree and the value types its arguments parse into.

use clap::{Subcommand, ValueEnum};

use super::cli::DownloadOpts;
use super::parse::{parse_parallel, parse_retry_target, parse_url};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Download a single URL
    #[command(
        visible_alias = "d",
        after_help = "Defaults for -c and the download directory come from config.toml."
    )]
    Download {
        #[arg(value_name = "URL", value_parser = parse_url)]
        url: String,

        #[command(flatten)]
        opts: DownloadOpts,
    },

    /// Mirror a remote directory listing into a local directory
    #[command(
        after_help = "-o sets the destination directory for a sync, not a filename.\n\nDefaults for -c/-p and the download directory come from config.toml.\nRun `rdm config` to see the values currently in effect.\n\n-e accepts a comma separated list or repeated flags, with or without\nleading dots: `-e flac,mkv` and `-e .flac -e .MKV` are equivalent.\n\nA MEGA or OneDrive folder share is mirrored through that hoster's own path\nrather than the queue, so -p does not apply to it. On a OneDrive share -c\nsets how many files download at once."
    )]
    Sync {
        #[arg(value_name = "URL", value_parser = parse_url)]
        url: String,

        #[command(flatten)]
        opts: DownloadOpts,

        /// Files to download concurrently [default: queue_parallel from config]
        #[arg(short, long, value_name = "N", value_parser = parse_parallel)]
        parallel: Option<usize>,

        /// Delete local files that no longer exist on the remote
        #[arg(short, long)]
        delete: bool,

        /// Only sync these extensions, comma separated (e.g. flac,mkv)
        #[arg(short, long, value_name = "EXT", value_delimiter = ',')]
        ext: Vec<String>,
    },

    /// Manage the download queue
    #[command(visible_alias = "q", arg_required_else_help = true)]
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },

    /// Show the effective configuration
    Config,
}

#[derive(Debug, Subcommand)]
pub enum QueueCommand {
    /// Add a URL to the queue, expanding directory listings
    #[command(visible_alias = "a")]
    Add {
        #[arg(value_name = "URL", value_parser = parse_url)]
        url: String,

        #[command(flatten)]
        opts: DownloadOpts,
    },

    /// Show the queue
    #[command(visible_aliases = ["ls", "l"])]
    List,

    /// Start processing the queue
    #[command(
        visible_aliases = ["run", "s"],
        after_help = "-p defaults to queue_parallel from config.toml."
    )]
    Start {
        /// Files to download concurrently [default: queue_parallel from config]
        #[arg(short, long, value_name = "N", value_parser = parse_parallel)]
        parallel: Option<usize>,
    },

    /// Stop the queue after the current download finishes
    Stop,

    /// Skip the download(s) in flight
    #[command(visible_aliases = ["next", "n"])]
    Skip,

    /// Remove one item from the queue
    #[command(visible_alias = "rm")]
    Remove {
        #[arg(value_name = "ID")]
        id: u64,
    },

    /// Requeue items [default: everything failed or skipped]
    #[command(visible_alias = "r")]
    Retry {
        #[arg(value_name = "ID|failed|skipped", value_parser = parse_retry_target)]
        target: Option<RetryTarget>,
    },

    /// Clear queue items [default: the whole queue]
    #[command(visible_alias = "c")]
    Clear {
        #[arg(value_name = "pending|done", value_enum)]
        target: Option<ClearTarget>,
    },
}

/// What `rdm queue retry` should requeue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryTarget {
    /// Every failed item.
    Failed,
    /// Every skipped item.
    Skipped,
    /// One specific item.
    Id(u64),
}

/// What `rdm queue clear` should remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClearTarget {
    /// Items that have not started yet.
    #[value(alias = "p")]
    Pending,
    /// Items that already finished.
    #[value(aliases = ["d", "finished"])]
    Done,
}
