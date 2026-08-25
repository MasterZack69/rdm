//! Tests for the CLI shape: what parses, what is rejected, and what the help
//! text promises. The value parsers are unit tested next to themselves in
//! `parse.rs`.

use clap::{CommandFactory, Parser};

use super::{Cli, ClearTarget, Command, QueueCommand, RetryTarget, normalize_extensions, parse_url};

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
    assert!(
        root.contains("-p, --parallel"),
        "root promises -p in its footer"
    );
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
    assert!(
        parse(&["rdm", "https://example.com/f.zip", "--ap"])
            .opts
            .allow_private
    );

    match parse(&["rdm", "download", "https://example.com/f.zip", "--ap"]).command {
        Some(Command::Download { opts, .. }) => assert!(opts.allow_private),
        other => panic!("expected download, got {other:?}"),
    }

    match parse(&["rdm", "sync", "https://example.com/d/", "--ap"]).command {
        Some(Command::Sync { opts, .. }) => assert!(opts.allow_private),
        other => panic!("expected sync, got {other:?}"),
    }

    match parse(&["rdm", "queue", "add", "https://example.com/f.zip", "--ap"]).command {
        Some(Command::Queue {
            command: QueueCommand::Add { opts, .. },
        }) => {
            assert!(opts.allow_private)
        }
        other => panic!("expected queue add, got {other:?}"),
    }
}

#[test]
fn allow_private_defaults_to_off() {
    assert!(
        !parse(&["rdm", "https://example.com/f.zip"])
            .opts
            .allow_private
    );
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

/// Same for OneDrive, with less to go on: a `1drv.ms` link is a
/// shortener, so it carries no filename and no hint of file versus
/// folder. The parser must not try to be helpful about either.
#[test]
fn onedrive_links_survive_url_parsing() {
    for link in [
        "https://1drv.ms/f/c/abc123/AbCdEfGh",
        "https://onedrive.live.com/?id=ABC%21123&cid=ABC",
    ] {
        assert_eq!(parse_url(link).as_deref(), Ok(link));
        assert_eq!(parse(&["rdm", link]).url.as_deref(), Some(link));
    }
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
        Some(Command::Sync {
            url,
            opts,
            parallel,
            delete,
            ext,
        }) => {
            assert_eq!(url, "https://example.com/music/");
            assert_eq!(opts.output.as_deref(), Some("/data/music"));
            assert_eq!(opts.connections, Some(8));
            assert_eq!(parallel, Some(4));
            assert!(delete);
            assert_eq!(ext, vec!["flac".to_owned(), "mkv".to_owned()]);
        }
        other => panic!("expected sync, got {other:?}"),
    }
}

#[test]
fn sync_short_flags_work() {
    let cli = parse(&[
        "rdm",
        "sync",
        "https://e.com/d/",
        "-d",
        "-e",
        "mkv",
        "-p",
        "2",
    ]);
    match cli.command {
        Some(Command::Sync {
            parallel,
            delete,
            ext,
            ..
        }) => {
            assert_eq!(parallel, Some(2));
            assert!(delete);
            assert_eq!(ext, vec!["mkv".to_owned()]);
        }
        other => panic!("expected sync, got {other:?}"),
    }
}

#[test]
fn sync_defaults_are_left_unresolved() {
    let cli = parse(&["rdm", "sync", "https://e.com/d/"]);
    match cli.command {
        Some(Command::Sync {
            opts,
            parallel,
            delete,
            ext,
            ..
        }) => {
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
        Some(Command::Queue {
            command: QueueCommand::Add { url, opts },
        }) => {
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
            Some(Command::Queue {
                command: QueueCommand::List
            })
        ));
    }
}

#[test]
fn queue_start_aliases_work() {
    for name in ["start", "run", "s"] {
        let cli = parse(&["rdm", "queue", name, "-p", "2"]);
        match cli.command {
            Some(Command::Queue {
                command: QueueCommand::Start { parallel },
            }) => {
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
            Some(Command::Queue {
                command: QueueCommand::Skip
            })
        ));
    }
}

#[test]
fn queue_remove_requires_numeric_id() {
    let cli = parse(&["rdm", "queue", "rm", "7"]);
    match cli.command {
        Some(Command::Queue {
            command: QueueCommand::Remove { id },
        }) => assert_eq!(id, 7),
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
            Some(Command::Queue {
                command: QueueCommand::Retry { target },
            }) => {
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
        Some(Command::Queue {
            command: QueueCommand::Retry { target },
        }) => {
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
            Some(Command::Queue {
                command: QueueCommand::Clear { target },
            }) => {
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
        Some(Command::Queue {
            command: QueueCommand::Clear { target },
        }) => {
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

// \u{2500}\u{2500} --ext \u{2500}\u{2500}

#[test]
fn repeated_ext_flags_accumulate() {
    let cli = parse(&["rdm", "sync", "https://e.com/d/", "-e", "flac", "-e", "mkv"]);
    match cli.command {
        Some(Command::Sync { ext, .. }) => {
            let set = normalize_extensions(&ext).expect("expected a filter");
            assert!(set.contains("flac") && set.contains("mkv"));
        }
        other => panic!("expected sync, got {other:?}"),
    }
}
