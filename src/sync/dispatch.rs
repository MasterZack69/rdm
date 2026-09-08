//! Protocol routing, separated from the existing HTTP mirror.

use anyhow::Result;
use std::collections::HashSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    cfg: &Config,
    url: &str,
    requested_connections: Option<usize>,
    parallel: usize,
    delete: bool,
    ext_filter: Option<HashSet<String>>,
    allow_private: bool,
    output_dir: Option<String>,
    cancel: CancellationToken,
) -> Result<()> {
    if crate::sftp::is_sftp_url(url) {
        super::sftp::run(
            cfg, url, requested_connections, parallel, delete, ext_filter,
            allow_private, output_dir, cancel,
        ).await
    } else {
        super::run::run(
            cfg, url, requested_connections, parallel, delete, ext_filter,
            allow_private, output_dir, cancel,
        ).await
    }
}
