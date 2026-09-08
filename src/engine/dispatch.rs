//! Route by protocol before HTTP normalisation or inspection.

use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::sftp::{self, SftpOptions};
use crate::ui::ProgressSink;
use super::{DownloadRequest, Outcome};

pub async fn download(
    request: DownloadRequest,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<Outcome> {
    if sftp::is_sftp_url(&request.url) {
        let options = SftpOptions::from_config(super::client::shared_config())?;
        sftp::download(request, options, cancel, sink).await
    } else {
        super::download::download(request, cancel, sink).await
    }
}
