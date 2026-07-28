//! Command-line interface definition.
//!
//! All argument parsing lives here so `main.rs` is pure dispatch.
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
//! Keep the `after_help` footers honest: a footer must never mention a flag
//! that its own command does not have. There is a test for this.

use std::collections::HashSet;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// Upper bound for connections per file. Beyond this, servers start refusing
/// or throttling and the chunk bookkeeping stops paying for itself.
pub const MAX_CONNECTIONS: usize = 64;

/// Upper bound for how many queue items may download concurrently.
pub const MAX_PARALLEL: usize = 32;

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
        after_help = "-o sets the destination directory for a sync, not a filename.\n\nDefaults for -c/-p and the download directory come from config.toml.\nRun `rdm config` to see the values currently in effect.\n\n-e accepts a comma separated list or repeated flags, with or without\nleading dots: `-e flac,mkv` and `-e .flac -e .MKV` are equivalent."
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

// \u{2500}\u{2500} Value parsers \u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}

/// Accepts only http(s) URLs.
///
/// The previous parser treated any unrecognised first argument as "print usage
/// and exit 1", which meant a typo like `rdm dowload URL` produced the full
/// help text with no hint about what was wrong. Validating here lets clap
/// report the actual problem.
pub fn parse_url(value: &str) -> Result<String, String> {
    let trimmed = value.trim();

    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(trimmed.to_string());
    }

    if trimmed.is_empty() {
        return Err("URL must not be empty".to_string());
    }

    // The suggested URL is wrapped in backticks and nothing else. Doubled
    // braces in a format string emit literal braces, which produced a hint
    // nobody could paste into a shell.
    let hint = format!("https://{trimmed}");
    Err(format!(
        "`{trimmed}` is not an http(s) URL \u{2014} did you mean `{hint}`?"
    ))
}

/// Parses `--connections`, rejecting 0 and absurd values.
pub fn parse_connections(value: &str) -> Result<usize, String> {
    parse_bounded(value, MAX_CONNECTIONS, "connections")
}

/// Parses `--parallel`, rejecting 0 and absurd values.
pub fn parse_parallel(value: &str) -> Result<usize, String> {
    parse_bounded(value, MAX_PARALLEL, "parallel downloads")
}

fn parse_bounded(value: &str, max: usize, what: &str) -> Result<usize, String> {
    let parsed: usize = value
        .trim()
        .parse()
        .map_err(|_| format!("`{value}` is not a whole number"))?;

    if parsed == 0 {
        return Err(format!("{what} must be at least 1"));
    }

    if parsed > max {
        return Err(format!("{what} must be at most {max}"));
    }

    Ok(parsed)
}

/// Parses the `rdm queue retry` target.
pub fn parse_retry_target(value: &str) -> Result<RetryTarget, String> {
    let trimmed = value.trim();

    match trimmed.to_ascii_lowercase().as_str() {
        "failed" | "f" => Ok(RetryTarget::Failed),
        "skipped" | "s" => Ok(RetryTarget::Skipped),
        _ => trimmed
            .parse::<u64>()
            .map(RetryTarget::Id)
            .map_err(|_| {
                format!("expected an item ID, `failed` or `skipped`, got `{trimmed}`")
            }),
    }
}

/// Normalises `--ext` values into the set [`crate::sync`] expects.
///
/// Accepts repeated flags and comma separated lists, tolerates a leading dot,
/// and is case insensitive. Returns `None` when no usable extension was given,
/// so callers treat it as "no filter" rather than the old behaviour of building
/// an empty set that matched nothing.
pub fn normalize_extensions(raw: &[String]) -> Option<HashSet<String>> {
    let set: HashSet<String> = raw
        .iter()
        .flat_map(|value| value.split(','))
        .map(|ext| ext.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|ext| !ext.is_empty())
        .collect();

    if set.is_empty() { None } else { Some(set) }
}

// \u{2500}\u{2500} Tests \u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("expected args to parse")
    }

    fn render_help(args: &[&str]) -> String {
        let mut cmd = Cli::command();
        for name in &args[1..] {
            cmd = cmd
                .find_subcommand(name)
                .unwrap_or_else(|| panic!("no subcommand {name}"))
                .clone();
        }
        cmd.render_long_help().to_string()
    }

    /// Catches invalid clap configuration (duplicate IDs, bad aliases, illegal
    /// flag combinations) at test time instead of at runtime.
    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn bare_command_is_rejected() {
        assert!(Cli::try_parse_from(["rdm"]).is_err());
    }

    // \u{2500}\u{2500} Help text honesty \u{2500}\u{2500}

    /// The root footer once advertised `-c/-p` when `-p` was not a root option,
    /// sending people looking for a flag that wasn't there. Every footer must
    /// only mention options its own command really has.
    #[test]
    fn help_footers_only_mention_present_flags() {
        let root = render_help(&["rdm"]);
        assert!(root.contains("-c, --connections"));
        assert!(root.contains("-p, --parallel"), "root promises -p in its footer");
        // -d/-e are sync-only and must not leak into the root help.
        assert!(!root.contains("--delete"));
        assert!(!root.contains("--ext"));

        let sync = render_help(&["rdm", "sync"]);
        assert!(sync.contains("-p, --parallel"));
        assert!(sync.contains("-d, --delete"));
        assert!(sync.contains("-e, --ext"));
        assert!(sync.contains("-c/-p"));

        let start = render_help(&["rdm", "queue", "start"]);
        assert!(start.contains("-p, --parallel"));

        // Nothing runs concurrently for these, so -p must stay away.
        let download = render_help(&["rdm", "download"]);
        assert!(!download.contains("--parallel"));
        assert!(!download.contains("--delete"));
        assert!(!download.contains("--ext"));
        assert!(!download.contains("-c/-p"));

        let add = render_help(&["rdm", "queue", "add"]);
        assert!(!add.contains("--parallel"));
    }

    /// `-p` is meaningful at the root only because a listing URL is expanded
    /// into the queue. `-d`/`-e` have no root meaning and stay rejected.
    #[test]
    fn sync_only_flags_are_rejected_at_root() {
        for flag in ["-d", "--delete", "-e", "--ext"] {
            assert!(Cli::try_parse_from(["rdm", "https://e.com/f.zip", flag]).is_err());
        }
        assert!(Cli::try_parse_from(["rdm", "download", "https://e.com/f", "-d"]).is_err());
        // -p is not shared via DownloadOpts, so it must not reach these.
        assert!(Cli::try_parse_from(["rdm", "download", "https://e.com/f", "-p", "4"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "queue", "add", "https://e.com/f", "-p", "4"]).is_err());
    }

    // \u{2500}\u{2500} Quick download \u{2500}\u{2500}

    #[test]
    fn quick_download_takes_positional_url() {
        let cli = parse(&["rdm", "https://example.com/f.zip"]);
        assert_eq!(cli.url.as_deref(), Some("https://example.com/f.zip"));
        assert!(cli.command.is_none());
        assert_eq!(cli.parallel, None);
    }

    #[test]
    fn quick_download_accepts_options() {
        let cli = parse(&[
            "rdm",
            "https://example.com/f.zip",
            "-o",
            "out.zip",
            "-c",
            "16",
            "--allow-private",
            "-q",
        ]);
        assert_eq!(cli.opts.output.as_deref(), Some("out.zip"));
        assert_eq!(cli.opts.connections, Some(16));
        assert!(cli.opts.allow_private);
        assert!(cli.opts.quiet);
    }

    // \u{2500}\u{2500} --ap \u{2500}\u{2500}

    /// `--ap` must mean exactly `--allow-private`, everywhere the shared
    /// options are flattened in.
    #[test]
    fn ap_is_an_alias_for_allow_private() {
        assert!(parse(&["rdm", "https://example.com/f.zip", "--ap"]).opts.allow_private);

        match parse(&["rdm", "download", "https://example.com/f.zip", "--ap"]).command {
            Some(Command::Download { opts, .. }) => assert!(opts.allow_private),
            other => panic!("expected download, got {other:?}"),
        }

        match parse(&["rdm", "sync", "https://example.com/d/", "--ap"]).command {
            Some(Command::Sync { opts, .. }) => assert!(opts.allow_private),
            other => panic!("expected sync, got {other:?}"),
        }

        match parse(&["rdm", "queue", "add", "https://example.com/f.zip", "--ap"]).command {
            Some(Command::Queue { command: QueueCommand::Add { opts, .. } }) => {
                assert!(opts.allow_private)
            }
            other => panic!("expected queue add, got {other:?}"),
        }
    }

    #[test]
    fn allow_private_defaults_to_off() {
        assert!(!parse(&["rdm", "https://example.com/f.zip"]).opts.allow_private);
    }

    /// An alias the user cannot discover is not worth having.
    #[test]
    fn ap_is_advertised_in_help() {
        assert!(render_help(&["rdm"]).contains("--ap"));
        assert!(render_help(&["rdm", "download"]).contains("--ap"));
    }

    #[test]
    fn root_accepts_parallel_for_listings() {
        for flag in ["-p", "--parallel"] {
            let cli = parse(&["rdm", "https://example.com/music/", flag, "6"]);
            assert_eq!(cli.parallel, Some(6));
            assert!(cli.command.is_none());
        }

        // Combines with the shared options.
        let cli = parse(&["rdm", "https://example.com/music/", "-c", "8", "-p", "3"]);
        assert_eq!(cli.opts.connections, Some(8));
        assert_eq!(cli.parallel, Some(3));
    }

    #[test]
    fn root_parallel_is_validated() {
        assert!(Cli::try_parse_from(["rdm", "https://e.com/d/", "-p", "0"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "https://e.com/d/", "-p", "99"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "https://e.com/d/", "-p", "some"]).is_err());
    }

    #[test]
    fn non_http_url_is_rejected() {
        assert!(Cli::try_parse_from(["rdm", "example.com/f.zip"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "dowload"]).is_err());
    }

    /// MEGA links are ordinary https URLs as far as parsing is concerned \u{2014}
    /// the routing happens in `main`, so the parser must not reject them.
    #[test]
    fn mega_links_survive_url_parsing() {
        let link = "https://mega.nz/file/AbCdEfGh#somekey";
        assert_eq!(parse_url(link).as_deref(), Ok(link));
        assert_eq!(parse(&["rdm", link]).url.as_deref(), Some(link));
    }

    // \u{2500}\u{2500} download \u{2500}\u{2500}

    #[test]
    fn download_subcommand_and_alias_match() {
        for name in ["download", "d"] {
            let cli = parse(&["rdm", name, "https://example.com/f.zip", "-c", "4"]);
            match cli.command {
                Some(Command::Download { url, opts }) => {
                    assert_eq!(url, "https://example.com/f.zip");
                    assert_eq!(opts.connections, Some(4));
                }
                other => panic!("expected download, got {other:?}"),
            }
        }
    }

    #[test]
    fn download_requires_url() {
        assert!(Cli::try_parse_from(["rdm", "download"]).is_err());
    }

    #[test]
    fn connections_are_validated() {
        assert!(Cli::try_parse_from(["rdm", "download", "https://e.com/f", "-c", "0"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "download", "https://e.com/f", "-c", "999"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "download", "https://e.com/f", "-c", "lots"]).is_err());
    }

    #[test]
    fn connections_left_unset_when_absent() {
        let cli = parse(&["rdm", "download", "https://example.com/f.zip"]);
        match cli.command {
            Some(Command::Download { opts, .. }) => assert_eq!(opts.connections, None),
            other => panic!("expected download, got {other:?}"),
        }
    }

    // \u{2500}\u{2500} sync \u{2500}\u{2500}

    #[test]
    fn sync_parses_every_option() {
        let cli = parse(&[
            "rdm",
            "sync",
            "https://example.com/music/",
            "-o",
            "/data/music",
            "-c",
            "8",
            "-p",
            "4",
            "--delete",
            "--ext",
            "flac,mkv",
        ]);
        match cli.command {
            Some(Command::Sync { url, opts, parallel, delete, ext }) => {
                assert_eq!(url, "https://example.com/music/");
                assert_eq!(opts.output.as_deref(), Some("/data/music"));
                assert_eq!(opts.connections, Some(8));
                assert_eq!(parallel, Some(4));
                assert!(delete);
                assert_eq!(ext, vec!["flac".to_string(), "mkv".to_string()]);
            }
            other => panic!("expected sync, got {other:?}"),
        }
    }

    #[test]
    fn sync_short_flags_work() {
        let cli = parse(&["rdm", "sync", "https://e.com/d/", "-d", "-e", "mkv", "-p", "2"]);
        match cli.command {
            Some(Command::Sync { parallel, delete, ext, .. }) => {
                assert_eq!(parallel, Some(2));
                assert!(delete);
                assert_eq!(ext, vec!["mkv".to_string()]);
            }
            other => panic!("expected sync, got {other:?}"),
        }
    }

    #[test]
    fn sync_defaults_are_left_unresolved() {
        let cli = parse(&["rdm", "sync", "https://e.com/d/"]);
        match cli.command {
            Some(Command::Sync { opts, parallel, delete, ext, .. }) => {
                assert_eq!(opts.connections, None);
                assert_eq!(parallel, None);
                assert!(!delete);
                assert!(ext.is_empty());
            }
            other => panic!("expected sync, got {other:?}"),
        }
    }

    #[test]
    fn parallel_is_validated() {
        assert!(Cli::try_parse_from(["rdm", "sync", "https://e.com/d/", "-p", "0"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "sync", "https://e.com/d/", "-p", "99"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "queue", "start", "-p", "0"]).is_err());
    }

    // \u{2500}\u{2500} queue \u{2500}\u{2500}

    #[test]
    fn queue_requires_subcommand() {
        assert!(Cli::try_parse_from(["rdm", "queue"]).is_err());
    }

    #[test]
    fn queue_add_keeps_connections_unresolved() {
        let cli = parse(&["rdm", "q", "a", "https://example.com/f.zip", "-o", "name"]);
        match cli.command {
            Some(Command::Queue { command: QueueCommand::Add { url, opts } }) => {
                assert_eq!(url, "https://example.com/f.zip");
                assert_eq!(opts.output.as_deref(), Some("name"));
                assert_eq!(opts.connections, None);
            }
            other => panic!("expected queue add, got {other:?}"),
        }
    }

    #[test]
    fn queue_list_aliases_work() {
        for name in ["list", "ls", "l"] {
            let cli = parse(&["rdm", "queue", name]);
            assert!(matches!(
                cli.command,
                Some(Command::Queue { command: QueueCommand::List })
            ));
        }
    }

    #[test]
    fn queue_start_aliases_work() {
        for name in ["start", "run", "s"] {
            let cli = parse(&["rdm", "queue", name, "-p", "2"]);
            match cli.command {
                Some(Command::Queue { command: QueueCommand::Start { parallel } }) => {
                    assert_eq!(parallel, Some(2));
                }
                other => panic!("expected queue start, got {other:?}"),
            }
        }
    }

    #[test]
    fn queue_skip_aliases_work() {
        for name in ["skip", "next", "n"] {
            let cli = parse(&["rdm", "queue", name]);
            assert!(matches!(
                cli.command,
                Some(Command::Queue { command: QueueCommand::Skip })
            ));
        }
    }

    #[test]
    fn queue_remove_requires_numeric_id() {
        let cli = parse(&["rdm", "queue", "rm", "7"]);
        match cli.command {
            Some(Command::Queue { command: QueueCommand::Remove { id } }) => assert_eq!(id, 7),
            other => panic!("expected queue remove, got {other:?}"),
        }
        assert!(Cli::try_parse_from(["rdm", "queue", "rm", "abc"]).is_err());
        assert!(Cli::try_parse_from(["rdm", "queue", "rm"]).is_err());
    }

    #[test]
    fn queue_retry_targets_parse() {
        let cases = [
            ("failed", Some(RetryTarget::Failed)),
            ("f", Some(RetryTarget::Failed)),
            ("skipped", Some(RetryTarget::Skipped)),
            ("s", Some(RetryTarget::Skipped)),
            ("12", Some(RetryTarget::Id(12))),
        ];

        for (input, expected) in cases {
            let cli = parse(&["rdm", "queue", "retry", input]);
            match cli.command {
                Some(Command::Queue { command: QueueCommand::Retry { target } }) => {
                    assert_eq!(target, expected, "input {input}");
                }
                other => panic!("expected queue retry, got {other:?}"),
            }
        }
    }

    #[test]
    fn queue_retry_defaults_to_all() {
        let cli = parse(&["rdm", "queue", "r"]);
        match cli.command {
            Some(Command::Queue { command: QueueCommand::Retry { target } }) => {
                assert_eq!(target, None);
            }
            other => panic!("expected queue retry, got {other:?}"),
        }
    }

    #[test]
    fn queue_retry_rejects_nonsense() {
        assert!(Cli::try_parse_from(["rdm", "queue", "retry", "maybe"]).is_err());
    }

    #[test]
    fn queue_clear_targets_parse() {
        let cases = [
            ("pending", Some(ClearTarget::Pending)),
            ("p", Some(ClearTarget::Pending)),
            ("done", Some(ClearTarget::Done)),
            ("finished", Some(ClearTarget::Done)),
            ("d", Some(ClearTarget::Done)),
        ];

        for (input, expected) in cases {
            let cli = parse(&["rdm", "queue", "clear", input]);
            match cli.command {
                Some(Command::Queue { command: QueueCommand::Clear { target } }) => {
                    assert_eq!(target, expected, "input {input}");
                }
                other => panic!("expected queue clear, got {other:?}"),
            }
        }
    }

    #[test]
    fn queue_clear_defaults_to_everything() {
        let cli = parse(&["rdm", "queue", "c"]);
        match cli.command {
            Some(Command::Queue { command: QueueCommand::Clear { target } }) => {
                assert_eq!(target, None);
            }
            other => panic!("expected queue clear, got {other:?}"),
        }
    }

    #[test]
    fn queue_clear_rejects_unknown_target() {
        assert!(Cli::try_parse_from(["rdm", "queue", "clear", "everything"]).is_err());
    }

    // \u{2500}\u{2500} config \u{2500}\u{2500}

    #[test]
    fn config_subcommand_parses() {
        let cli = parse(&["rdm", "config"]);
        assert!(matches!(cli.command, Some(Command::Config)));
    }

    // \u{2500}\u{2500} Extension normalisation \u{2500}\u{2500}

    #[test]
    fn extensions_are_normalised() {
        let raw = vec![".FLAC".to_string(), " mkv ".to_string()];
        let set = normalize_extensions(&raw).expect("expected a filter");
        assert_eq!(set.len(), 2);
        assert!(set.contains("flac"));
        assert!(set.contains("mkv"));
    }

    #[test]
    fn extensions_split_on_commas_and_dedupe() {
        let raw = vec!["flac,mkv".to_string(), "FLAC".to_string()];
        let set = normalize_extensions(&raw).expect("expected a filter");
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn empty_extensions_mean_no_filter() {
        assert_eq!(normalize_extensions(&[]), None);
        assert_eq!(normalize_extensions(&[" ".to_string()]), None);
        assert_eq!(normalize_extensions(&[",".to_string(), ".".to_string()]), None);
    }

    #[test]
    fn repeated_ext_flags_accumulate() {
        let cli = parse(&[
            "rdm", "sync", "https://e.com/d/", "-e", "flac", "-e", "mkv",
        ]);
        match cli.command {
            Some(Command::Sync { ext, .. }) => {
                let set = normalize_extensions(&ext).expect("expected a filter");
                assert!(set.contains("flac") && set.contains("mkv"));
            }
            other => panic!("expected sync, got {other:?}"),
        }
    }

    // \u{2500}\u{2500} Value parser units \u{2500}\u{2500}

    #[test]
    fn parse_url_trims_and_validates() {
        assert_eq!(
            parse_url("  https://e.com/f.zip ").as_deref(),
            Ok("https://e.com/f.zip")
        );
        assert!(parse_url("ftp://e.com/f.zip").is_err());
        assert!(parse_url("").is_err());
    }

    #[test]
    fn parse_url_hint_has_no_stray_braces() {
        let err = parse_url("example.com/f.zip").expect_err("expected rejection");
        assert!(err.contains("https://example.com/f.zip"), "got: {err}");
        assert!(!err.contains('{') && !err.contains('}'), "got: {err}");
    }

    #[test]
    fn parse_bounded_edges() {
        assert_eq!(parse_connections("1"), Ok(1));
        assert_eq!(parse_connections(&MAX_CONNECTIONS.to_string()), Ok(MAX_CONNECTIONS));
        assert!(parse_connections(&(MAX_CONNECTIONS + 1).to_string()).is_err());
        assert_eq!(parse_parallel("1"), Ok(1));
        assert_eq!(parse_parallel(&MAX_PARALLEL.to_string()), Ok(MAX_PARALLEL));
        assert!(parse_parallel(&(MAX_PARALLEL + 1).to_string()).is_err());
    }

    #[test]
    fn parse_retry_target_is_case_insensitive() {
        assert_eq!(parse_retry_target("FAILED"), Ok(RetryTarget::Failed));
        assert_eq!(parse_retry_target(" 42 "), Ok(RetryTarget::Id(42)));
    }
}
