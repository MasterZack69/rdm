//! Retry orchestration shared by the engine, queue and directory batches.

use anyhow::{Result, ensure};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::engine::{DownloadRequest, Outcome};
use crate::ui::ProgressSink;
use super::local::Destination;
use super::session::with_session;
use super::transfer::{RemoteRead, Transfer};
use super::{SftpOptions, SftpUrl};

pub async fn download(
    request: DownloadRequest,
    options: SftpOptions,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<Outcome> {
    ensure!(request.client.is_none(), "An HTTP client cannot be used for SFTP");
    ensure!(request.resume_identity.is_none(), "SFTP computes its own resume identity");
    let target = SftpUrl::parse(&request.url)?;
    let destination = match request.output.as_deref() {
        Some(output) => Destination::from_output(Path::new(output), &options.download_root)?,
        None => Destination::beneath(&options.download_root, target.folder_name())?,
    };
    if request.connections > 1 {
        sink.detail("SFTP uses one stream per file; use -p for concurrent files");
    }
    let transfer = Transfer { target, destination, expected: None, policy: request.policy };
    run_transfer(transfer, options, request.allow_private, cancel, sink).await
}

pub(crate) async fn run_transfer(
    transfer: Transfer,
    options: SftpOptions,
    allow_private: bool,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<Outcome> {
    let mut attempt = 0;
    let result = loop {
        if cancel.is_cancelled() {
            break Ok(Outcome::Cancelled);
        }
        let work = transfer.clone();
        let progress = Arc::clone(&sink);
        let result = with_session(
            transfer.target.clone(), Arc::clone(&options.authentication),
            allow_private, cancel.clone(),
            move |session, sftp, _, cancel| work.run(session, sftp, cancel, &progress),
        ).await;
        match result {
            Ok(outcome) => break Ok(outcome),
            Err(_) if cancel.is_cancelled() => break Ok(Outcome::Cancelled),
            Err(error) if attempt < options.max_retries && retryable(&error) => {
                let delay = Duration::from_secs((1_u64 << attempt.min(5)).min(30));
                attempt += 1;
                sink.note(&format!("SFTP transfer interrupted; retry {attempt} in {}s", delay.as_secs()));
                tokio::select! {
                    _ = cancel.cancelled() => break Ok(Outcome::Cancelled),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            Err(error) => break Err(error),
        }
    };
    sink.finish();
    result
}

pub(crate) fn retryable(error: &anyhow::Error) -> bool {
    if let Some(read) = error.downcast_ref::<RemoteRead>() {
        // ssh2's Read adapter erases some protocol codes into Other. Do not
        // guess that an unclassified error is transient (it may be an ACL).
        return matches!(read.0.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
        );
    }
    if let Some(error) = error.downcast_ref::<ssh2::Error>() {
        // libssh2 socket send, timeout, disconnect, socket timeout and receive;
        // SFTP v3 no-connection / connection-lost. Auth and host errors are not
        // retried, nor are permission failures or malformed remote metadata.
        return matches!(error.code(),
            ssh2::ErrorCode::Session(-7 | -9 | -13 | -30 | -43)
                | ssh2::ErrorCode::SFTP(6 | 7)
        );
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return matches!(error.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::NotConnected | std::io::ErrorKind::BrokenPipe
        );
    }
    error.is::<tokio::time::error::Elapsed>()
}
