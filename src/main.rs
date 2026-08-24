// segv's mom

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;

use rdm::args::{
    ClearTarget, Cli, Command, DownloadOpts, QueueCommand, RetryTarget, normalize_extensions,
};
use rdm::hoster::{dropbox, gdrive, gofile, onedrive, pixeldrain};
use rdm::ui::{self, ProgressSink};
use rdm::{config, engine, mega, queue, scrape, signal, sync};

fn main() -> Result<()> {
    let args = Cli::parse();
    let cfg = config::Config::load();

    match args.command {
        None => {
            // `arg_required_else_help` guarantees a URL when no subcommand ran.
            let url = args
                .url
                .as_deref()
                .expect("clap guarantees a URL when no subcommand is given");
            quick_download(&cfg, url, &args.opts, args.parallel)
        }

        Some(Command::Download { url, opts }) => {
            // Before `normalize_download_url`: a MEGA link's `#key` fragment is
            // load-bearing and must reach the parser untouched.
            if mega::is_mega_url(&url) {
                return mega_route(&cfg, &url, &opts);
            }

            // Likewise for GoFile: `/d/<id>` is an API handle, not a path, and
            // normalising it as one would corrupt the content id.
            if gofile::is_gofile_url(&url) {
                return gofile_download(&cfg, &url, &opts);
            }

            // And for Dropbox, for a different reason: the link is fetchable,
            // it just serves an HTML preview page until `dl=1` asks for the
            // file. The share key lives in the query string, so it needs
            // rewriting rather than normalising as a path.
            if dropbox::is_dropbox_url(&url) {
                return dropbox_download(&cfg, &url, &opts);
            }

            // And OneDrive, for the same reason as MEGA and GoFile: a share
            // link has no extension and no trailing slash, so the directory
            // heuristic would call it a listing, and the generic engine would
            // save an HTML preview page under a plausible filename. Only the
            // API knows what is behind it.
            if onedrive::is_onedrive_url(&url) {
                return onedrive_download(&cfg, &url, &opts);
            }

            // And Google Drive, the same story a fifth time: every shape of
            // Drive link is a viewer page or an API handle rather than the
            // bytes, so normalising it as a path would only rename the page it
            // gets saved under. Only resolution knows what is behind it.
            if gdrive::is_gdrive_url(&url) {
                return gdrive_download(&cfg, &url, &opts);
            }

            if pixeldrain::is_pixeldrain_url(&url) {
                return pixeldrain_download(&cfg, &url, &opts);
            }

            let url = engine::normalize_download_url(&url);
            let connections = opts.connections.unwrap_or(cfg.connections);
            let output_path = resolve_output(opts.output.clone(), &url, &cfg);

            run_async(|cancel| async move {
                engine::run_download(url, Some(output_path), connections, cancel, opts.quiet).await
            })
        }

        Some(Command::Sync {
            url,
            opts,
            parallel,
            delete,
            ext,
        }) => {
            // Sync mirrors a listing it can re-read on demand. A GoFile
            // content id is an API handle behind a throwaway account, with no
            // listing to diff and no per-file URLs to keep. Without this the
            // link falls through to the scraper, which finds an empty
            // JavaScript page and reports "no files found" \u{2014} an accusation
            // against a link that is perfectly fine.
            if gofile::is_gofile_url(&url) {
                anyhow::bail!(
                    "GoFile links cannot be synced \u{2014} run `rdm <gofile link>` instead; \
                     rerunning it skips whatever is already on disk"
                );
            }

            // Dropbox has the same problem from the other end: a share link is
            // one file, or one folder that Dropbox zips before serving. Either
            // way there is a single response and no listing to diff against
            // the local directory.
            if dropbox::is_dropbox_url(&url) {
                anyhow::bail!(
                    "Dropbox links cannot be synced \u{2014} run `rdm <dropbox link>` instead; \
                     a share link is a single download, and a folder share arrives as one zip"
                );
            }

            if pixeldrain::is_pixeldrain_url(&url) {
                // A list is re-readable, so this is a missing feature rather
                // than an impossible one.
                anyhow::bail!(
                    "pixeldrain links cannot be synced yet \u{2014} run `rdm <pixeldrain link>` instead; \
                     rerunning it skips whatever is already on disk"
                );
            }

            let parallel = parallel.unwrap_or(cfg.queue_parallel);
            let ext_filter = normalize_extensions(&ext);
            let allow_private = opts.allow_private;

            // Sync resolves the connection count itself: `-c` means MEGA
            // workers on a share and HTTP connections everywhere else, and
            // only sync knows which it is dealing with.
            let requested_connections = opts.connections;

            // Kept separate from the download_dir override below so sync can
            // tell an explicit destination from the configured default. That
            // distinction gates --delete on the MEGA path.
            let output_dir = opts.output.clone();

            // `-o` names the destination directory for a sync, not a file.
            let mut cfg = cfg;
            if let Some(dir) = opts.output {
                cfg.download_dir = dir;
            }

            run_async(|cancel| async move {
                sync::run(
                    &cfg,
                    &url,
                    requested_connections,
                    parallel,
                    delete,
                    ext_filter,
                    allow_private,
                    output_dir,
                    cancel,
                )
                .await
            })
        }

        Some(Command::Queue { command }) => run_queue(&cfg, command),

        Some(Command::Config) => {
            cfg.print();
            Ok(())
        }
    }
}

// -- Command handlers ------------------------------------------------

/// `rdm <URL>` \u{2014} download a file, or expand a directory listing into the queue
/// and immediately start working through it.
///
/// `parallel` only takes effect on the listing path, where several files are
/// downloaded at once. On the single-file path it has nothing to act on, so we
/// say so rather than accepting the flag and quietly doing nothing with it.
fn quick_download(
    cfg: &config::Config,
    url: &str,
    opts: &DownloadOpts,
    parallel: Option<usize>,
) -> Result<()> {
    // MEGA first: `looks_like_directory` sees `/file/AbCdEfGh#key` as an
    // extensionless segment and would hand the link to the scraper, which
    // finds nothing there.
    if mega::is_mega_url(url) {
        if parallel.is_some() && !opts.quiet {
            eprintln!("  \u{26a0} -p applies to directory listings; MEGA uses mega_workers.");
        }
        return mega_route(cfg, url, opts);
    }

    // GoFile falls into exactly the same trap: `/d/AbCdEf` has no extension,
    // so the listing heuristic below claims it and the scraper finds an empty
    // JavaScript page.
    if gofile::is_gofile_url(url) {
        if parallel.is_some() && !opts.quiet {
            eprintln!("  \u{26a0} -p applies to directory listings; GoFile uses gofile_workers.");
        }
        return gofile_download(cfg, url, opts);
    }

    // And Dropbox, for the third time: a folder share's `/scl/fo/<id>/h` has
    // no extension either, so the heuristic would call it a listing and send
    // it to the scraper, which finds a preview page.
    if dropbox::is_dropbox_url(url) {
        if parallel.is_some() && !opts.quiet {
            eprintln!(
                "  \u{26a0} -p applies to directory listings; a Dropbox link is one download."
            );
        }
        return dropbox_download(cfg, url, opts);
    }

    // OneDrive is the same trap a fourth time: `1drv.ms/u/s!Abc` has no
    // extension and no trailing slash either, so the listing heuristic claims
    // it, and the generic engine would save the HTML preview page under a
    // plausible filename. Hence the check sitting above it.
    if onedrive::is_onedrive_url(url) {
        if parallel.is_some() && !opts.quiet {
            eprintln!("  \u{26a0} -p applies to directory listings; a OneDrive link is one share.");
        }
        return onedrive_download(cfg, url, opts);
    }

    // Google Drive is the trap a fifth time: `/file/d/<id>/view` ends in an
    // extensionless segment too, so the heuristic claims it and hands a
    // viewer page to the scraper. A folder link dodges the heuristic the
    // other way \u{2014} its id is long enough to read as an opaque file id \u{2014}
    // which would land it in the generic engine instead. Worse, not better.
    if gdrive::is_gdrive_url(url) {
        if parallel.is_some() && !opts.quiet {
            eprintln!(
                "  \u{26a0} -p applies to directory listings; Google Drive uses gdrive_workers."
            );
        }
        return gdrive_download(cfg, url, opts);
    }

    if pixeldrain::is_pixeldrain_url(url) {
        if parallel.is_some() && !opts.quiet {
            eprintln!("  \u{26a0} -p applies to directory listings; use the list link itself.");
        }
        return pixeldrain_download(cfg, url, opts);
    }

    let url = engine::normalize_download_url(url);
    let connections = opts.connections.unwrap_or(cfg.connections);
    let scan_for_listing = opts.output.is_none() && looks_like_directory(&url);

    run_async(|cancel| async move {
        if scan_for_listing {
            // A failed scan is not fatal: fall through and treat the URL as a
            // single file, which is what it usually turns out to be.
            if let Ok(Some(files)) = scrape::discover_files(&url, true, opts.allow_private).await
                && !files.is_empty()
            {
                print_discovered(&files);

                queue::Queue::locked(|q| {
                    for file in &files {
                        q.add(
                            file.url.clone(),
                            Some(file.relative_path.clone()),
                            Some(connections),
                        );
                    }
                    Ok(())
                })?;

                let parallel = parallel.unwrap_or(cfg.queue_parallel);
                return queue::start(cfg, cancel, parallel).await;
            }
        }

        if parallel.is_some() && !opts.quiet {
            eprintln!(
                "  \u{26a0} -p applies to directory listings; ignoring it for a single file."
            );
        }

        let output_path = resolve_output(opts.output.clone(), &url, cfg);
        engine::run_download(url, Some(output_path), connections, cancel, opts.quiet).await
    })
}

/// Sends a MEGA link to the file downloader or the folder downloader.
///
/// The two share everything below the API call but differ completely above it:
/// a folder link has no file handle and no file key until its node tree has
/// been fetched and decrypted.
fn mega_route(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    if mega::folder::is_folder_link(url) {
        mega_folder_download(cfg, url, opts)
    } else {
        mega_download(cfg, url, opts)
    }
}

/// `rdm <mega link>` \u{2014} fetch, decrypt and verify a MEGA file.
///
/// Kept separate from the normal download path on purpose: MEGA needs its own
/// API round trip, its own chunk ladder and its own quota handling, and none
/// of that belongs in the generic engine.
fn mega_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let url = url.trim().to_owned();
    let (output, download_dir) = mega_destination(opts.output.clone(), cfg);
    let options = mega_options(cfg, opts);
    let quiet = opts.quiet;

    // The name is only known after the API call decrypts the attributes, so
    // the bar starts out labelled with the link's handle.
    let label = mega::parse_link(&url)
        .map(|link| link.handle)
        .unwrap_or_else(|_| "mega".to_owned());

    run_async(|cancel| async move {
        let sink = progress_sink(quiet, &label);
        let client = reqwest::Client::new();
        let outcome =
            mega::download(client, &url, output, &download_dir, options, cancel, sink).await?;

        report_mega(&outcome, quiet);
        Ok(())
    })
}

/// `rdm <mega folder link>` \u{2014} walk the share and download everything in it.
///
/// `-o` names the destination directory here, not a file: a share holds many
/// files and its own directory structure, so there is nothing sensible for a
/// single output filename to mean.
fn mega_folder_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let url = url.trim().to_owned();
    let options = mega_options(cfg, opts);
    let quiet = opts.quiet;

    let output = opts.output.as_deref().map(|o| {
        let trimmed = o.trim_end_matches('/').trim_end_matches("\\\\");
        resolve_relative_to_config(trimmed, cfg)
    });
    let download_dir = cfg.download_dir.clone();

    run_async(|cancel| async move {
        let client = reqwest::Client::new();
        let make_sink = |name: &str, _size: u64| progress_sink(quiet, name);

        let summary = mega::folder::download_folder(
            client,
            &url,
            output,
            &download_dir,
            options,
            cancel,
            &make_sink,
        )
        .await?;

        report_mega_folder(&summary, quiet);
        Ok(())
    })
}

/// `rdm <gofile link>` \u{2014} resolve the content id and download everything behind
/// it.
///
/// `-o` names a destination directory here rather than a filename, the same
/// deal as a MEGA share: one content id can hold a whole tree and the link
/// does not say which, so there is nothing a single filename could reliably
/// mean. Where an unqualified download lands is decided after the listing
/// comes back \u{2014} see `gofile::destination_root`.
fn gofile_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let url = url.trim().to_owned();
    let options = gofile_options(cfg, opts);
    let quiet = opts.quiet;

    let output = opts.output.as_deref().map(|o| {
        let trimmed = o.trim_end_matches('/').trim_end_matches("\\\\");
        resolve_relative_to_config(trimmed, cfg)
    });
    let download_dir = cfg.download_dir.clone();

    run_async(|cancel| async move {
        let client = reqwest::Client::new();

        let summary =
            gofile::download(client, &url, output, &download_dir, options, cancel, quiet).await?;

        report_gofile(&summary, quiet);
        Ok(())
    })
}

/// `rdm <onedrive link>` \u{2014} redeem the share and download whatever it turns
/// out to be.
///
/// The one request that says file-or-folder is also the one that redeems the
/// share for the anonymous token, so there is nothing cheaper to ask first and
/// no reason to ask twice. A file lands through the generic engine, exactly
/// like a resolved Dropbox link; a folder is walked and downloaded by the
/// OneDrive module itself.
fn onedrive_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let options = onedrive_options(cfg, opts);
    let quiet = opts.quiet;

    run_async(|cancel| async move {
        match onedrive::resolve(reqwest::Client::new(), url, &options).await? {
            // One file, one destination: from here it is an ordinary ranged
            // HTTPS download, exactly like a resolved Dropbox link.
            onedrive::Resolved::File(file) => {
                let output = resolve_output_named(opts.output.clone(), &file.name, cfg);
                engine::run_download_with_identity(
                    file.url,
                    Some(output),
                    opts.connections.unwrap_or(cfg.connections),
                    format!("onedrive:{}", file.id),
                    cancel,
                    opts.quiet,
                )
                .await
            }
            onedrive::Resolved::Folder(folder) => {
                let summary = onedrive::download_folder(
                    folder,
                    opts.output.clone(),
                    &cfg.download_dir,
                    options,
                    cancel,
                    if opts.quiet {
                        onedrive::Progress::Silent
                    } else {
                        onedrive::Progress::Board
                    },
                )
                .await?;
                report_onedrive(&summary, quiet);
                Ok(())
            }
        }
    })
}

/// `rdm <gdrive link>` \u{2014} work out what the link points at and download that.
///
/// The one question worth asking is also the only one there is: a Drive link
/// never carries its own bytes, so resolution is not an optimisation but the
/// whole job. A file or a Doc lands through the generic engine, exactly like a
/// resolved OneDrive share; a folder is walked and downloaded by the Drive
/// module itself.
fn gdrive_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let options = gdrive_options(cfg, opts);
    let quiet = opts.quiet;

    run_async(|cancel| async move {
        match gdrive::resolve(reqwest::Client::new(), url, &options).await? {
            // One file, one destination: from here it is an ordinary ranged
            // HTTPS download. Resume is keyed on the Drive id rather than the
            // URL, because a confirmed download URL carries a short-lived
            // token and would not survive a rerun.
            gdrive::Resolved::File(file) => {
                let output = resolve_output_named(opts.output.clone(), &file.name, cfg);
                engine::run_download_with_identity(
                    file.url,
                    Some(output),
                    opts.connections.unwrap_or(cfg.connections),
                    format!("gdrive:{}", file.id),
                    cancel,
                    opts.quiet,
                )
                .await
            }
            gdrive::Resolved::Folder(folder) => {
                let summary = gdrive::download_folder(
                    folder,
                    opts.output.clone(),
                    &cfg.download_dir,
                    options,
                    cancel,
                    quiet,
                )
                .await?;
                report_gdrive(&summary, quiet);
                Ok(())
            }
        }
    })
}

/// pixeldrain: `/u/<id>` is one file, `/l/<id>` is a list, and the link says
/// which — so unlike GoFile or OneDrive no request is needed just to find out
/// the shape of the download. `resolve` still makes one call, for a file's name
/// or a list's contents.
fn pixeldrain_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let options = pixeldrain_options(cfg, opts);
    let connections = opts.connections.unwrap_or(cfg.connections);
    // Read before `options` is moved into `download_list`. It decides only
    // whether the speed-cap note is worth printing.
    let has_api_key = options.api_key.is_some();
    let quiet = opts.quiet;

    run_async(|cancel| async move {
        // Bound before the match so the borrow ends here: the list arm moves
        // `options`.
        let resolved = pixeldrain::resolve(url, &options).await?;

        match resolved {
            pixeldrain::Resolved::File(file) => {
                // Stated by the API up front, not inferred by watching the
                // transfer and giving up on it.
                if !quiet
                    && let Some(note) = pixeldrain::speed_limit_note(file.speed_limit, has_api_key)
                {
                    eprintln!("  \u{26a0} {note}");
                }

                // The key lives in the client's headers rather than the URL, so
                // the transfer has to go out over that same client.
                engine::run_download_with_client(
                    file.url,
                    Some(resolve_output_named(opts.output.clone(), &file.name, cfg)),
                    connections,
                    file.client,
                    cancel,
                    quiet,
                )
                .await
            }
            pixeldrain::Resolved::List(list) => {
                let summary = pixeldrain::download_list(
                    list,
                    opts.output.clone(),
                    &cfg.download_dir,
                    options,
                    cancel,
                    quiet,
                )
                .await?;
                report_pixeldrain(&summary, quiet);
                Ok(())
            }
        }
    })
}

/// `rdm <dropbox link>` \u{2014} rewrite the share link, then let the engine do the
/// work.
///
/// There is deliberately no Dropbox downloader. `dl=1` redirects to a CDN that
/// honours `Range`, so resume, parallel connections, retries and the progress
/// bar all come free from the generic path; a second copy of that machinery
/// which happened to know the word "dropbox" would be strictly worse. All this
/// function decides is which URL to fetch and what to call the result.
///
/// Unlike MEGA and GoFile, `-o` here means a filename, because a share link is
/// always one response \u{2014} a folder share included, since Dropbox zips it.
///
/// A password-protected share is the one thing a rewritten URL cannot express,
/// being authorised by a session instead. `dropbox::open` performs that
/// handshake and hands back the client holding it, which the engine then
/// downloads with \u{2014} so even that case adds no second downloader.
fn dropbox_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let link = dropbox::resolve(url)?;
    let connections = opts.connections.unwrap_or(cfg.connections);

    // The name cannot come from the URL: a folder share's last path segment is
    // `h`, which would make for a memorable download.
    let output = resolve_output_named(opts.output.clone(), &link.fallback_name, cfg);
    let quiet = opts.quiet;

    let share = url.trim().to_owned();
    let password = dropbox::password_from_env();

    run_async(|cancel| async move {
        // A public share needs no session, and says so by handing back nothing.
        match dropbox::open(&share, password.as_deref()).await? {
            Some(client) => {
                engine::run_download_with_client(
                    link.url,
                    Some(output),
                    connections,
                    client,
                    cancel,
                    quiet,
                )
                .await
            }
            None => engine::run_download(link.url, Some(output), connections, cancel, quiet).await,
        }
    })
}

fn run_queue(cfg: &config::Config, command: QueueCommand) -> Result<()> {
    match command {
        QueueCommand::Add { url, opts } => queue_add(cfg, &url, &opts),

        QueueCommand::List => {
            queue::Queue::load_readonly().print_list();
            Ok(())
        }

        QueueCommand::Start { parallel } => {
            let parallel = parallel.unwrap_or(cfg.queue_parallel);
            run_async(|cancel| async move { queue::start(cfg, cancel, parallel).await })
        }

        QueueCommand::Stop => {
            queue::send_signal("stop")?;
            eprintln!("  \u{23f9}  Stop signal sent. Queue will stop after current download.");
            Ok(())
        }

        QueueCommand::Skip => {
            queue::send_signal("skip")?;
            eprintln!("  \u{23ed}  Skip signal sent.");
            Ok(())
        }

        QueueCommand::Remove { id } => {
            if queue::Queue::locked(|q| Ok(q.remove(id)))? {
                eprintln!("  Removed #{id}");
            } else {
                eprintln!("  No item with ID #{id}");
            }
            Ok(())
        }

        QueueCommand::Retry { target } => {
            match target {
                Some(RetryTarget::Failed) => {
                    let n = queue::Queue::locked(|q| Ok(q.retry_failed()))?;
                    eprintln!("  Requeued {n} failed item(s).");
                }
                Some(RetryTarget::Skipped) => {
                    let n = queue::Queue::locked(|q| Ok(q.retry_skipped()))?;
                    eprintln!("  Requeued {n} skipped item(s).");
                }
                Some(RetryTarget::Id(id)) => {
                    if queue::Queue::locked(|q| Ok(q.retry_item(id)))? {
                        eprintln!("  \u{2705} #{id} requeued.");
                    } else {
                        eprintln!("  #{id} is not failed or skipped.");
                    }
                }
                None => {
                    let n = queue::Queue::locked(|q| Ok(q.retry_failed() + q.retry_skipped()))?;
                    eprintln!("  Requeued {n} item(s).");
                }
            }
            Ok(())
        }

        QueueCommand::Clear { target } => {
            match target {
                Some(ClearTarget::Pending) => {
                    let n = queue::Queue::locked(|q| Ok(q.clear_pending()))?;
                    eprintln!("  Cleared {n} pending item(s).");
                }
                Some(ClearTarget::Done) => {
                    let n = queue::Queue::locked(|q| Ok(q.clear_finished()))?;
                    eprintln!("  Cleared {n} finished item(s).");
                }
                None => {
                    let n = queue::Queue::locked(|q| Ok(q.clear_all()))?;
                    eprintln!("  Cleared {n} item(s). Queue is empty.");
                }
            }
            Ok(())
        }
    }
}

/// `rdm queue add` \u{2014} enqueue a single file, or every file behind a listing.
fn queue_add(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    // A folder share is N files with no individual URLs to store, so it cannot
    // be represented as one queue item. Say so instead of accepting it and
    // failing later, in the runner, where the message would be less useful.
    if mega::folder::is_folder_link(url) {
        anyhow::bail!(
            "MEGA folder links cannot be queued \u{2014} run `rdm <folder link>` to download the whole share"
        );
    }

    // Same reasoning for GoFile, which has no single-file link shape at all:
    // every content id is a potential tree.
    if gofile::is_gofile_url(url) {
        anyhow::bail!(
            "GoFile links cannot be queued \u{2014} run `rdm <gofile link>` to download the whole content"
        );
    }

    if pixeldrain::is_list_link(url) {
        anyhow::bail!(
            "a pixeldrain list cannot be queued \u{2014} run `rdm {url}` to download all of it"
        );
    }

    // Unsigned and non-expiring, so it is still valid whenever the queue reaches
    // it. `?download` makes pixeldrain send a `Content-Disposition`, so unlike
    // Dropbox there is no filename to pin either.
    let pixeldrain_link = if pixeldrain::is_pixeldrain_url(url) {
        Some(pixeldrain::direct_url(url)?)
    } else {
        None
    };

    // Dropbox is the one hoster that queues cleanly, folder shares included:
    // rewriting the link yields an ordinary HTTPS URL for a single response,
    // so the runner can fetch it without knowing Dropbox exists. Resolving now
    // also means a bad link is rejected here, while the user is watching,
    // rather than at the front of the queue an hour later.
    //
    // The exception is a password-protected share, which is authorised by a
    // session the runner has no way to hold. Nothing here can detect that \u{2014}
    // it takes a request \u{2014} so such a link queues and then fetches the
    // password page. See extraInfo/dropbox.md.
    let dropbox_link = if dropbox::is_dropbox_url(url) {
        Some(dropbox::resolve(url)?)
    } else {
        None
    };

    // MEGA links go in verbatim: `normalize_download_url` would touch the
    // `#key` fragment, and there is no listing behind a file link to scrape.
    // The queue runner recognises them and dispatches to the MEGA downloader.
    let is_mega = mega::is_mega_url(url);
    let url = if is_mega {
        url.trim().to_owned()
    } else if let Some(link) = &dropbox_link {
        link.url.clone()
    } else if let Some(link) = &pixeldrain_link {
        link.clone()
    } else {
        engine::normalize_download_url(url)
    };

    let discovered = if !is_mega
        && dropbox_link.is_none()
        && pixeldrain_link.is_none()
        && looks_like_directory(&url)
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(scrape::discover_files(&url, true, opts.allow_private))
            .unwrap_or(None)
    } else {
        None
    };

    match discovered {
        Some(files) if !files.is_empty() => {
            queue::Queue::locked(|q| {
                for file in &files {
                    q.add(
                        file.url.clone(),
                        Some(file.relative_path.clone()),
                        opts.connections,
                    );
                }
                Ok(())
            })?;

            print_discovered(&files);
        }

        _ => {
            // A Dropbox item needs its name pinned now: by the time the runner
            // sees the rewritten URL, the only thing left to name it after is
            // the CDN path.
            let output = opts
                .output
                .as_deref()
                .map(|o| resolve_relative_to_config(o, cfg))
                .or_else(|| {
                    dropbox_link
                        .as_ref()
                        .map(|link| cfg.resolve_output_path(&link.fallback_name))
                });
            let id = queue::Queue::locked(|q| Ok(q.add(url.clone(), output, opts.connections)))?;
            let label = if is_mega {
                // Never echo the link back: the fragment is the decryption key.
                mega::parse_link(&url)
                    .map(|link| format!("MEGA {}", link.handle))
                    .unwrap_or_else(|_| "MEGA link".to_owned())
            } else if let Some(link) = &dropbox_link {
                // Same care as MEGA: `rlkey` is the share secret, so the link
                // does not go on screen.
                match link.share {
                    dropbox::Share::File => format!("Dropbox {}", link.fallback_name),
                    dropbox::Share::Folder => "Dropbox folder (zip)".to_owned(),
                }
            } else {
                engine::percent_decode(&url)
            };
            eprintln!("  \u{2705} Added #{}: {}", id, label);
        }
    }

    eprintln!(
        "  {} item(s) pending.",
        queue::Queue::load_readonly().pending_count()
    );
    Ok(())
}

// -- Helpers ---------------------------------------------------------

/// Builds a multi-threaded runtime, wires SIGINT/SIGTERM to a
/// [`CancellationToken`] so in-flight downloads can checkpoint their progress,
/// and always tears the handler down again.
fn run_async<F, Fut>(body: F) -> Result<()>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let cancel = CancellationToken::new();
            let handler = signal::spawn_signal_handler(cancel.clone());
            let result = body(cancel).await;
            handler.abort();
            result
        })
}

/// A single-file progress bar, or nothing at all under `-q`.
fn progress_sink(quiet: bool, label: &str) -> Arc<dyn ProgressSink> {
    if quiet {
        ui::silent()
    } else {
        let bar: Arc<dyn ProgressSink> = ui::SoloBar::new(label);
        bar
    }
}

/// Splits `-o` into the (exact path, fallback directory) pair MEGA needs.
///
/// MEGA is the one source where we do not know the filename up front \u{2014} it
/// arrives encrypted in the file attributes. So a directory-ish `-o` means
/// "use the real name, in here", and only a concrete path overrides it.
fn mega_destination(output: Option<String>, cfg: &config::Config) -> (Option<String>, String) {
    match output {
        Some(o) => {
            let path = Path::new(&o);
            if o.ends_with('/') || o.ends_with("\\\\") || path.is_dir() {
                let dir = o.trim_end_matches('/').trim_end_matches("\\\\").to_owned();
                (None, dir)
            } else {
                (
                    Some(resolve_relative_to_config(&o, cfg)),
                    cfg.download_dir.clone(),
                )
            }
        }
        None => (None, cfg.download_dir.clone()),
    }
}

/// `-c` doubles as the MEGA worker count: to the user it is still "how many
/// connections do I want to this file".
fn mega_options(cfg: &config::Config, opts: &DownloadOpts) -> mega::MegaOptions {
    mega::MegaOptions {
        workers: opts.connections.unwrap_or(cfg.mega_workers),
        verify_mac: cfg.mega_verify_mac,
        resume_on_ip_change: cfg.mega_resume_on_ip_change,
        max_retries: cfg.max_retries,
        overwrite: false,
    }
}

/// `-c` means files in flight here, not chunks per file \u{2014} GoFile rate-limits
/// per connection, so splitting one file gains nothing.
///
/// The password and account token come from the environment rather than flags:
/// a password on the command line ends up in shell history and in `ps` output
/// for every other user on the machine.
fn gofile_options(cfg: &config::Config, opts: &DownloadOpts) -> gofile::GofileOptions {
    gofile::GofileOptions {
        workers: opts.connections.unwrap_or(cfg.gofile_workers),
        max_retries: cfg.max_retries,
        password: std::env::var("RDM_GOFILE_PASSWORD")
            .ok()
            .filter(|p| !p.trim().is_empty()),
        token: std::env::var("RDM_GOFILE_TOKEN")
            .ok()
            .filter(|t| !t.trim().is_empty())
            .or_else(|| {
                let configured = cfg.gofile_token.trim();
                (!configured.is_empty()).then(|| configured.to_owned())
            }),
        overwrite: false,
    }
}

/// The key comes from the environment or the config file, never from a flag: an
/// argument ends up in shell history and in `ps` output for every other user on
/// the machine.
fn pixeldrain_options(cfg: &config::Config, opts: &DownloadOpts) -> pixeldrain::PixeldrainOptions {
    let api_key = std::env::var("RDM_PIXELDRAIN_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty())
        .or_else(|| {
            let configured = cfg.pixeldrain_api_key.trim();
            (!configured.is_empty()).then(|| configured.to_owned())
        });

    pixeldrain::PixeldrainOptions {
        // On a list, -c means files at once rather than chunks within one file.
        workers: opts.connections.unwrap_or(cfg.pixeldrain_workers),
        max_retries: cfg.max_retries,
        api_key,
        overwrite: false,
    }
}

/// `-c` means files in flight here, the same double meaning it has for GoFile:
/// a OneDrive folder share is downloaded one connection per file.
fn onedrive_options(cfg: &config::Config, opts: &DownloadOpts) -> onedrive::OneDriveOptions {
    onedrive::OneDriveOptions {
        workers: opts.connections.unwrap_or(cfg.onedrive_workers),
        max_retries: cfg.max_retries,
        overwrite: false,
    }
}

/// `-c` means files in flight here, the same double meaning it has for GoFile
/// and OneDrive: a Drive folder is downloaded one connection per file.
///
/// The API key comes from the environment ahead of the config: a billable key
/// is one people would rather not leave on disk. A blank key from either
/// source counts as absent \u{2014} sending one turns every call into a 400 instead
/// of falling back to anonymous access.
fn gdrive_options(cfg: &config::Config, opts: &DownloadOpts) -> gdrive::GdriveOptions {
    gdrive::GdriveOptions {
        workers: opts.connections.unwrap_or(cfg.gdrive_workers),
        max_retries: cfg.max_retries,
        api_key: std::env::var("RDM_GDRIVE_API_KEY")
            .ok()
            .or_else(|| Some(cfg.gdrive_api_key.clone()))
            .filter(|key| !key.trim().is_empty()),
        doc_format: cfg.gdrive_doc_format.clone(),
        overwrite: false,
    }
}

fn report_mega(outcome: &mega::MegaOutcome, quiet: bool) {
    if quiet {
        return;
    }

    match outcome {
        mega::MegaOutcome::Completed { path, bytes } => {
            eprintln!(
                "  \u{2705} {} ({})",
                path.display(),
                ui::format_size(*bytes)
            );
        }
        mega::MegaOutcome::AlreadyPresent { path } => {
            eprintln!("  \u{2713} Already downloaded: {}", path.display());
        }
        mega::MegaOutcome::Cancelled { path } => {
            eprintln!(
                "  \u{23f8} Stopped \u{2014} partial file kept at {}, rerun to resume.",
                path.display()
            );
        }
    }
}

/// Folder downloads report per file, because a share with one dead node in it
/// is still a successful download of everything else.
fn report_mega_folder(summary: &mega::folder::FolderSummary, quiet: bool) {
    if quiet {
        return;
    }

    eprintln!();
    eprintln!("  \u{1f4c1} {}", summary.root.display());

    // Where files land is the one thing this report exists to state, so an
    // absorbed folder level has to be visible rather than inferred.
    if let Some(folder) = summary.collapsed.as_deref() {
        eprintln!("     (already the share's '{folder}' folder, so its contents went");
        eprintln!("      straight in rather than into a second '{folder}' inside it)");
    }

    eprintln!(
        "     {} of {} file(s), {}",
        summary.completed,
        summary.total,
        ui::format_size(summary.bytes)
    );

    if summary.skipped > 0 {
        eprintln!("     {} already on disk", summary.skipped);
    }

    if !summary.failed.is_empty() {
        eprintln!();
        eprintln!("  \u{26a0} {} file(s) failed:", summary.failed.len());
        for (path, reason) in &summary.failed {
            eprintln!("     - {path}: {reason}");
        }
    }

    if summary.cancelled {
        eprintln!();
        eprintln!(
            "  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off."
        );
    }
}

/// Same shape as the MEGA folder report, for the same reason: one dead file in
/// a content id does not make the other forty a failure.
fn report_gofile(summary: &gofile::GofileSummary, quiet: bool) {
    if quiet {
        return;
    }

    eprintln!();
    eprintln!("  \u{1f4c1} {}", summary.root.display());
    eprintln!(
        "     {} of {} file(s), {}",
        summary.completed,
        summary.total,
        ui::format_size(summary.bytes)
    );

    if summary.skipped > 0 {
        eprintln!("     {} already on disk", summary.skipped);
    }

    if !summary.failed.is_empty() {
        eprintln!();
        eprintln!("  \u{26a0} {} file(s) failed:", summary.failed.len());
        for (path, reason) in &summary.failed {
            eprintln!("     - {path}: {reason}");
        }
    }

    if summary.cancelled {
        eprintln!();
        eprintln!(
            "  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off."
        );
    }
}

/// Same shape as the MEGA and GoFile folder reports, for the same reason: one
/// dead file in a OneDrive share does not make the other forty a failure.
fn report_onedrive(summary: &onedrive::OneDriveSummary, quiet: bool) {
    if quiet {
        return;
    }

    eprintln!();
    eprintln!("  \u{1f4c1} {}", summary.root.display());
    eprintln!(
        "     {} of {} file(s), {}",
        summary.completed,
        summary.total,
        ui::format_size(summary.bytes)
    );

    if summary.skipped > 0 {
        eprintln!("     {} already on disk", summary.skipped);
    }

    if !summary.failed.is_empty() {
        eprintln!();
        eprintln!("  \u{26a0} {} file(s) failed:", summary.failed.len());
        for (path, reason) in &summary.failed {
            eprintln!("     - {path}: {reason}");
        }
    }

    if summary.cancelled {
        eprintln!();
        eprintln!(
            "  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off."
        );
    }
}

/// Same shape as the MEGA, GoFile and OneDrive folder reports, for the same
/// reason: one dead file in a Drive folder does not make the other forty a
/// failure. The one line of its own is the unsupported count, because a
/// shortcut or an Apps Script project is not a failed download \u{2014} there was
/// never anything to download \u{2014} but comparing the folder against a local
/// copy needs to know its picture of it is short.
fn report_gdrive(summary: &gdrive::GdriveSummary, quiet: bool) {
    if quiet {
        return;
    }

    eprintln!();
    eprintln!("  \u{1f4c1} {}", summary.root.display());
    eprintln!(
        "     {} of {} file(s), {}",
        summary.completed,
        summary.total,
        ui::format_size(summary.bytes)
    );

    if summary.skipped > 0 {
        eprintln!("     {} already on disk", summary.skipped);
    }

    if summary.unsupported > 0 {
        eprintln!(
            "     {} with no downloadable form (shortcuts, Apps Script)",
            summary.unsupported
        );
    }

    if !summary.failed.is_empty() {
        eprintln!();
        eprintln!("  \u{26a0} {} file(s) failed:", summary.failed.len());
        for (path, reason) in &summary.failed {
            eprintln!("     - {path}: {reason}");
        }
    }

    if summary.cancelled {
        eprintln!();
        eprintln!(
            "  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off."
        );
    }
}

fn report_pixeldrain(summary: &pixeldrain::PixeldrainSummary, quiet: bool) {
    if quiet {
        return;
    }

    eprintln!();
    eprintln!("  \u{1f4c1} {}", summary.root.display());
    eprintln!(
        "     {} of {} file(s), {}",
        summary.completed,
        summary.total,
        ui::format_size(summary.bytes)
    );

    if summary.skipped > 0 {
        eprintln!("     {} already on disk", summary.skipped);
    }

    if summary.skipped_entries > 0 {
        eprintln!(
            "     {} entry(s) in the list had nothing to fetch",
            summary.skipped_entries
        );
    }

    if !summary.failed.is_empty() {
        eprintln!();
        eprintln!("  \u{26a0} {} file(s) failed:", summary.failed.len());
        for (path, reason) in &summary.failed {
            eprintln!("     - {path}: {reason}");
        }
    }

    if summary.cancelled {
        eprintln!();
        eprintln!(
            "  \u{23f8} Stopped \u{2014} rerun the same link to pick up where this left off."
        );
    }
}

/// Shows what was found without burying the terminal: a listing of 4000 files
/// used to print 4000 lines before a single byte was downloaded.
fn print_discovered(files: &[scrape::DiscoveredFile]) {
    const SAMPLE: usize = 20;

    eprintln!("  \u{1f4c1} Found {} file(s):", files.len());
    eprintln!();
    for file in files.iter().take(SAMPLE) {
        eprintln!("     + {}", engine::percent_decode(&file.relative_path));
    }
    if files.len() > SAMPLE {
        eprintln!("     \u{2026} and {} more", files.len() - SAMPLE);
    }
    eprintln!();
}

/// Resolves `-o` for a single-file download.
///
/// A trailing separator or an existing directory means "put the file in here
/// under its remote name"; anything else is taken as the filename itself.
/// Relative paths land under the configured download directory.
fn resolve_output(output: Option<String>, url: &str, cfg: &config::Config) -> String {
    let filename =
        engine::extract_filename_from_url(url).unwrap_or_else(|| "download.bin".to_owned());

    resolve_output_named(output, &filename, cfg)
}

/// The same rules, for a download whose name did not come from its URL.
///
/// Dropbox is why this is a separate function: a folder share's URL ends in
/// `/h`, so the name has to come from the share itself, while a directory-ish
/// `-o` must still keep that name rather than write a file called `h`.
fn resolve_output_named(output: Option<String>, filename: &str, cfg: &config::Config) -> String {
    match output {
        Some(o) => {
            let path = Path::new(&o);
            if o.ends_with('/') || o.ends_with("\\\\") || path.is_dir() {
                let dir = o.trim_end_matches('/').trim_end_matches("\\\\");
                format!("{}/{}", dir, filename)
            } else if path.is_absolute() {
                o
            } else {
                cfg.resolve_output_path(&o)
            }
        }
        None => cfg.resolve_output_path(filename),
    }
}

fn resolve_relative_to_config(output: &str, cfg: &config::Config) -> String {
    if Path::new(output).is_absolute() {
        output.to_owned()
    } else {
        cfg.resolve_output_path(output)
    }
}

/// Heuristic: does this URL point at a directory listing rather than a file?
///
/// A trailing slash or a last segment with no extension says "listing", except
/// for long hex-ish segments, which are almost always opaque file IDs.
fn looks_like_directory(url: &str) -> bool {
    if url.ends_with('/') {
        return true;
    }

    let last_segment = url
        .split('?')
        .next()
        .unwrap_or(url)
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("");

    if last_segment.is_empty() {
        return true;
    }

    if last_segment.contains('.') {
        return false;
    }

    let is_hex_like = last_segment.len() > 16
        && last_segment
            .chars()
            .all(|c| c.is_ascii_hexdigit() || c == '-' || c == '_');

    !is_hex_like
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trailing_slash_is_a_directory() {
        assert!(looks_like_directory("https://example.com/music/"));
        assert!(looks_like_directory("https://example.com/"));
    }

    #[test]
    fn extension_means_file() {
        assert!(!looks_like_directory("https://example.com/song.flac"));
        assert!(!looks_like_directory(
            "https://example.com/a/b/archive.tar.gz"
        ));
    }

    #[test]
    fn bare_segment_is_a_directory() {
        assert!(looks_like_directory("https://example.com/music"));
    }

    #[test]
    fn long_hex_segment_is_a_file_id() {
        assert!(!looks_like_directory(
            "https://example.com/0123456789abcdef01234"
        ));
    }

    #[test]
    fn query_string_is_ignored() {
        assert!(!looks_like_directory(
            "https://example.com/song.flac?token=1"
        ));
        assert!(looks_like_directory("https://example.com/music?page=2"));
    }

    // -- MEGA routing --

    /// The reason the MEGA check has to come first: the listing heuristic
    /// genuinely reads a MEGA link as a directory, so any code path that tests
    /// for a listing before testing for MEGA sends the link to the scraper.
    #[test]
    fn mega_links_would_be_mistaken_for_listings() {
        let link = "https://mega.nz/file/AbCdEfGh#thekey";
        assert!(mega::is_mega_url(link));
        assert!(
            looks_like_directory(link),
            "if this ever stops being true the ordering comment above is stale, not wrong"
        );
    }

    #[test]
    fn ordinary_links_are_not_sent_to_mega() {
        assert!(!mega::is_mega_url("https://example.com/mega.nz/file.zip"));
        assert!(!mega::is_mega_url("https://example.com/song.flac"));
    }

    /// Both link shapes are MEGA, but only one of them has a file key in it.
    /// Sending a folder link down the file path gets a -9 from the API rather
    /// than anything the user could act on.
    #[test]
    fn folder_links_and_file_links_take_different_paths() {
        let folder = "https://mega.nz/folder/s6lVFYbI#XKN8d1JVkhLYqpd9WPNQzA";
        let file = "https://mega.nz/file/AbCdEfGh#thekey";

        assert!(mega::is_mega_url(folder));
        assert!(mega::folder::is_folder_link(folder));
        assert!(!mega::folder::is_folder_link(file));

        // The file parser must not silently accept a folder link.
        assert!(mega::parse_link(folder).is_err());
    }

    #[test]
    fn mega_destination_prefers_the_real_filename() {
        let cfg = config::Config::default();

        // No -o: let MEGA name the file, inside the download dir.
        let (output, dir) = mega_destination(None, &cfg);
        assert_eq!(output, None);
        assert_eq!(dir, cfg.download_dir);

        // Directory-ish -o: same, but somewhere else.
        let (output, dir) = mega_destination(Some("/data/mega/".to_owned()), &cfg);
        assert_eq!(output, None);
        assert_eq!(dir, "/data/mega");

        // Concrete -o: the user's name wins.
        let (output, _) = mega_destination(Some("/data/movie.mkv".to_owned()), &cfg);
        assert_eq!(output.as_deref(), Some("/data/movie.mkv"));
    }

    #[test]
    fn mega_workers_come_from_connections_then_config() {
        let cfg = config::Config::default();

        let defaults = mega_options(&cfg, &DownloadOpts::default());
        assert_eq!(defaults.workers, cfg.mega_workers);
        assert_eq!(defaults.verify_mac, cfg.mega_verify_mac);
        assert!(!defaults.overwrite);

        let opts = DownloadOpts {
            connections: Some(3),
            ..DownloadOpts::default()
        };
        assert_eq!(mega_options(&cfg, &opts).workers, 3);
    }

    // -- GoFile routing --

    /// Exactly the MEGA trap again: `/d/AbCdEf` has no extension and is short
    /// enough not to read as an opaque id, so the heuristic calls it a
    /// listing. Hence the GoFile check sitting above it.
    #[test]
    fn gofile_links_would_be_mistaken_for_listings() {
        let link = "https://gofile.io/d/AbCdEf";
        assert!(gofile::is_gofile_url(link));
        assert!(
            looks_like_directory(link),
            "if this ever stops being true the ordering comment above is stale, not wrong"
        );
    }

    #[test]
    fn ordinary_links_are_not_sent_to_gofile() {
        assert!(!gofile::is_gofile_url(
            "https://example.com/gofile.io/d/abc"
        ));
        assert!(!gofile::is_gofile_url("https://example.com/song.flac"));
    }

    /// Sync has to refuse GoFile links for the same reason the scraper cannot
    /// handle them: there is no listing page behind the link, only an API the
    /// scraper knows nothing about. The refusal lives in the Sync arm, and
    /// this pins the condition it turns on.
    #[test]
    fn sync_can_tell_a_gofile_link_from_a_listing() {
        assert!(gofile::is_gofile_url("https://gofile.io/d/jWwmJp"));

        // An ordinary listing must still reach sync untouched.
        assert!(!gofile::is_gofile_url("https://example.com/music/"));
        assert!(!gofile::is_gofile_url("https://example.com/d/jWwmJp"));
    }

    #[test]
    fn gofile_workers_come_from_connections_then_config() {
        let cfg = config::Config::default();

        let defaults = gofile_options(&cfg, &DownloadOpts::default());
        assert_eq!(defaults.workers, cfg.gofile_workers);
        assert_eq!(defaults.max_retries, cfg.max_retries);
        assert!(!defaults.overwrite);

        let opts = DownloadOpts {
            connections: Some(2),
            ..DownloadOpts::default()
        };
        assert_eq!(gofile_options(&cfg, &opts).workers, 2);
    }

    /// A configured token is used when the environment does not override it.
    /// The environment variable itself is left alone here: tests share a
    /// process, and setting it would leak into every other test.
    #[test]
    fn a_configured_account_token_is_picked_up() {
        let cfg = config::Config {
            gofile_token: "tok-from-config".to_owned(),
            ..config::Config::default()
        };

        if std::env::var("RDM_GOFILE_TOKEN").is_err() {
            let options = gofile_options(&cfg, &DownloadOpts::default());
            assert_eq!(options.token.as_deref(), Some("tok-from-config"));
        }

        // A blank token means "guest", not an empty bearer header.
        let blank = config::Config {
            gofile_token: "   ".to_owned(),
            ..config::Config::default()
        };
        if std::env::var("RDM_GOFILE_TOKEN").is_err() {
            assert!(
                gofile_options(&blank, &DownloadOpts::default())
                    .token
                    .is_none()
            );
        }
    }

    // -- Dropbox routing --

    /// The same trap a third time, and the worst of the three: a folder share
    /// ends in `/h`, so the heuristic calls it a listing and the scraper finds
    /// a preview page. Hence the Dropbox check sitting above it.
    #[test]
    fn dropbox_folder_links_would_be_mistaken_for_listings() {
        let link = "https://www.dropbox.com/scl/fo/abc123/h?rlkey=k&dl=0";
        assert!(dropbox::is_dropbox_url(link));
        assert!(
            looks_like_directory(link),
            "if this ever stops being true the ordering comment above is stale, not wrong"
        );
    }

    #[test]
    fn ordinary_links_are_not_sent_to_dropbox() {
        assert!(!dropbox::is_dropbox_url(
            "https://example.com/dropbox.com/scl/fi/abc/f.zip"
        ));
        assert!(!dropbox::is_dropbox_url("https://example.com/song.flac"));
    }

    /// Sync refuses Dropbox for a different reason than GoFile: the link is
    /// fetchable, there is just nothing behind it to diff, because a folder
    /// share is zipped into a single response.
    #[test]
    fn sync_can_tell_a_dropbox_link_from_a_listing() {
        assert!(dropbox::is_dropbox_url(
            "https://www.dropbox.com/scl/fo/abc123/h?rlkey=k&dl=0"
        ));
        assert!(!dropbox::is_dropbox_url("https://example.com/music/"));
    }

    /// The Dropbox path is "rewrite, then hand to the engine", so the name has
    /// to survive that hand-off and land in the download directory.
    #[test]
    fn a_dropbox_file_share_keeps_its_name() {
        let cfg = config::Config::default();
        let link =
            dropbox::resolve("https://www.dropbox.com/scl/fi/abc123/holiday%20photos.zip?dl=0")
                .expect("a file share resolves without a request");

        assert_eq!(link.fallback_name, "holiday photos.zip");
        assert_eq!(
            resolve_output_named(None, &link.fallback_name, &cfg),
            cfg.resolve_output_path("holiday photos.zip")
        );
    }

    /// Why `resolve_output_named` exists: a folder share's own last path
    /// segment is `h`, so a directory-ish `-o` has to keep the share's name
    /// rather than the URL's.
    #[test]
    fn dropbox_output_directory_keeps_the_remote_name() {
        let cfg = config::Config::default();

        assert_eq!(
            resolve_output_named(Some("/data/dl/".to_owned()), "dropbox-abc123.zip", &cfg),
            "/data/dl/dropbox-abc123.zip"
        );

        // A concrete -o still wins outright.
        assert_eq!(
            resolve_output_named(
                Some("/data/mine.zip".to_owned()),
                "dropbox-abc123.zip",
                &cfg
            ),
            "/data/mine.zip"
        );
    }

    /// The password never comes from a flag, so nothing in `DownloadOpts` can
    /// carry it. This pins the variable's name and the blank-is-absent rule
    /// the Dropbox path relies on, without setting the variable: tests share a
    /// process and it would leak into every other one.
    #[test]
    fn a_dropbox_password_comes_only_from_the_environment() {
        match std::env::var("RDM_DROPBOX_PASSWORD") {
            Err(_) => assert!(dropbox::password_from_env().is_none()),
            Ok(set) if set.trim().is_empty() => {
                assert!(dropbox::password_from_env().is_none())
            }
            Ok(set) => assert_eq!(dropbox::password_from_env().as_deref(), Some(set.as_str())),
        }
    }

    // -- OneDrive routing --

    /// The same trap a fourth time: `u/s!AbCdEfGh` has no extension and no
    /// trailing slash, so the heuristic calls it a listing and the scraper
    /// would find a preview page. Hence the OneDrive check sitting above it.
    #[test]
    fn onedrive_links_would_be_mistaken_for_listings() {
        let link = "https://1drv.ms/u/s!AbCdEfGh";
        assert!(onedrive::is_onedrive_url(link));
        assert!(
            looks_like_directory(link),
            "if this ever stops being true the ordering comment above is stale, not wrong"
        );
    }

    #[test]
    fn ordinary_links_are_not_sent_to_onedrive() {
        assert!(!onedrive::is_onedrive_url("https://example.com/file.zip"));
        assert!(!onedrive::is_onedrive_url(
            "https://1drv.ms.evil.com/u/s!abc"
        ));
    }

    #[test]
    fn onedrive_workers_come_from_connections_then_config() {
        let cfg = config::Config::default();

        let defaults = onedrive_options(&cfg, &DownloadOpts::default());
        assert_eq!(defaults.workers, cfg.onedrive_workers);
        assert_eq!(defaults.max_retries, cfg.max_retries);
        assert!(!defaults.overwrite);

        let opts = DownloadOpts {
            connections: Some(4),
            ..DownloadOpts::default()
        };
        assert_eq!(onedrive_options(&cfg, &opts).workers, 4);
    }

    // -- Google Drive routing --

    /// The same trap a fifth time: `/file/d/<id>/view` has no extension and no
    /// trailing slash, so the heuristic calls it a listing and the scraper
    /// would find a viewer page. Hence the Drive check sitting above it.
    #[test]
    fn gdrive_links_would_be_mistaken_for_listings() {
        let link = "https://drive.google.com/file/d/1A2b3C4d5E6f/view";
        assert!(gdrive::is_gdrive_url(link));
        assert!(
            looks_like_directory(link),
            "if this ever stops being true the ordering comment above is stale, not wrong"
        );
    }

    #[test]
    fn ordinary_links_are_not_sent_to_gdrive() {
        assert!(!gdrive::is_gdrive_url("https://example.com/file.zip"));
        // A confirmed download URL is already direct: claiming it would send a
        // resolved link back through resolution.
        assert!(!gdrive::is_gdrive_url(
            "https://drive.usercontent.google.com/download?id=1A2b3C4d5E6f"
        ));
    }

    #[test]
    fn gdrive_workers_come_from_connections_then_config() {
        let cfg = config::Config::default();

        let defaults = gdrive_options(&cfg, &DownloadOpts::default());
        assert_eq!(defaults.workers, cfg.gdrive_workers);
        assert_eq!(defaults.max_retries, cfg.max_retries);
        assert_eq!(defaults.doc_format, cfg.gdrive_doc_format);
        assert!(!defaults.overwrite);

        let opts = DownloadOpts {
            connections: Some(6),
            ..DownloadOpts::default()
        };
        assert_eq!(gdrive_options(&cfg, &opts).workers, 6);
    }

    /// A configured key is used when the environment does not override it.
    /// The environment variable itself is left alone here: tests share a
    /// process, and setting it would leak into every other test.
    #[test]
    fn a_configured_api_key_is_picked_up() {
        let cfg = config::Config {
            gdrive_api_key: "AIzaSyExampleKey".to_owned(),
            ..config::Config::default()
        };

        if std::env::var("RDM_GDRIVE_API_KEY").is_err() {
            let options = gdrive_options(&cfg, &DownloadOpts::default());
            assert_eq!(options.api_key.as_deref(), Some("AIzaSyExampleKey"));
        }

        // A blank key means "anonymous", not an empty parameter on every call.
        let blank = config::Config {
            gdrive_api_key: "   ".to_owned(),
            ..config::Config::default()
        };
        if std::env::var("RDM_GDRIVE_API_KEY").is_err() {
            assert!(
                gdrive_options(&blank, &DownloadOpts::default())
                    .api_key
                    .is_none()
            );
        }
    }

    /// pixeldrain is the one host whose link says which it is, so routing must
    /// not treat every link as a listing the way GoFile has to.
    #[test]
    fn pixeldrain_lists_are_told_apart_from_files() {
        assert!(pixeldrain::is_list_link(
            "https://pixeldrain.com/l/AbCdEf12"
        ));
        assert!(!pixeldrain::is_list_link(
            "https://pixeldrain.com/u/AbCdEf12"
        ));
    }

    #[test]
    fn ordinary_links_are_not_sent_to_pixeldrain() {
        assert!(!pixeldrain::is_pixeldrain_url(
            "https://example.com/file.zip"
        ));
        assert!(!pixeldrain::is_pixeldrain_url(
            "https://pixeldrain.com.evil.net/u/AbCdEf12"
        ));
    }

    #[test]
    fn pixeldrain_workers_come_from_connections_then_config() {
        let cfg = config::Config {
            pixeldrain_workers: 7,
            ..config::Config::default()
        };
        let opts = DownloadOpts::default();
        assert_eq!(pixeldrain_options(&cfg, &opts).workers, 7);

        let opts = DownloadOpts {
            connections: Some(5),
            ..DownloadOpts::default()
        };
        assert_eq!(pixeldrain_options(&cfg, &opts).workers, 5);
    }

    #[test]
    fn no_key_configured_means_anonymous() {
        if std::env::var("RDM_PIXELDRAIN_API_KEY").is_err() {
            let opts = DownloadOpts::default();
            assert!(
                pixeldrain_options(&config::Config::default(), &opts)
                    .api_key
                    .is_none()
            );
        }
    }
}
