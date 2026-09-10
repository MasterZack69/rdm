//! Native, read-only SFTP support for downloads, directory discovery and sync.
//!
//! Authentication is non-interactive. Host keys must already be trusted.
//! Blocking SSH calls run in dedicated workers; cancellation closes their
//! sockets and drains them before a queue slot can be reused.

pub(crate) mod batch;
mod checkpoint;
mod commands;
mod download;
mod hostkeys;
pub(crate) mod local;
mod options;
mod scan;
mod session;
pub(crate) mod stamp;
mod transfer;
mod url;

pub(crate) use checkpoint::discard_state;
pub use commands::{CommandMode, run};
pub use download::download;
pub use options::SftpOptions;
pub use scan::{Listing, RemoteFile, discover_files, list};
pub use url::{SftpUrl, is_sftp_url};

#[cfg(test)]
mod tests;
