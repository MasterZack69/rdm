//! Directory transfers use the same SFTP worker and RDM progress board.

use anyhow::{Context, Result};
use futures_util::{StreamExt, stream};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

use crate::engine::{ExistingPolicy, Outcome};
use crate::ui;
use super::download::run_transfer;
use super::local::Destination;
use super::session::Cancelled;
use super::stamp::FileStamp;
use super::transfer::Transfer;
use super::{RemoteFile, SftpOptions, SftpUrl};

pub(crate) struct Batch {
    pub root: PathBuf,
    pub parallel: usize,
    pub allow_private: bool,
    pub policy: ExistingPolicy,
    pub quiet: bool,
}

struct Display {
    board: ui::Board,
    renderer: tokio::task::JoinHandle<()>,
}

impl Drop for Display {
    fn drop(&mut self) {
        self.board.finish();
        self.renderer.abort();
    }
}

pub(crate) async fn download_files(
    files: Vec<RemoteFile>,
    options: SftpOptions,
    batch: Batch,
    cancel: CancellationToken,
) -> Result<()> {
    let parallel = batch.parallel.clamp(1, crate::args::MAX_PARALLEL);
    let display = if batch.quiet {
        None
    } else {
        let board = ui::Board::new("SFTP", files.len(), parallel);
        let renderer = board.spawn_renderer();
        Some(Display { board, renderer })
    };
    let board = display.as_ref().map(|display| &display.board);
    let tasks = stream::iter(files.into_iter().enumerate()).map(|(index, file)| {
        let options = options.clone();
        let cancel = cancel.clone();
        let batch = &batch;
        async move {
            if cancel.is_cancelled() { return Err(Cancelled.into()); }
            let lane = board.map(|board| {
                board.claim(index as u64, &ui::terminal_safe(&file.relative_path))
                    .context("SFTP progress board has no free lane")
            }).transpose()?;
            let sink = lane.as_ref().map_or_else(ui::silent, |lane| lane.sink());
            let transfer = Transfer {
                target: SftpUrl::parse(&file.url)?,
                destination: Destination::beneath(&batch.root, &file.relative_path)?,
                expected: Some(FileStamp { size: file.size, modified: file.modified }),
                policy: batch.policy,
            };
            let result = run_transfer(transfer, options, batch.allow_private, cancel, sink).await;
            match result {
                Ok(Outcome::Completed { bytes, .. }) => {
                    if let Some(board) = board { board.file_completed(bytes); }
                    Ok(())
                }
                Ok(Outcome::AlreadyPresent { .. }) => {
                    if let Some(board) = board { board.file_skipped(); }
                    Ok(())
                }
                Ok(Outcome::Cancelled) => Err(Cancelled.into()),
                Err(error) => {
                    if let Some(board) = board { board.file_failed(); }
                    Err(error.context(format!("SFTP file: {}", ui::terminal_safe(&file.relative_path))))
                }
            }
        }
    }).buffer_unordered(parallel);
    tokio::pin!(tasks);
    let mut first_error = None;
    // Drain, don't try_collect: dropping active blocking workers on the first
    // failure would let a sync enter its next phase while writes still run.
    while let Some(result) = tasks.next().await {
        if let Err(error) = result {
            if first_error.is_none() { first_error = Some(error); }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
