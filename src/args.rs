use clap::{Args, Parser, Subcommand};
use std::collections::HashSet;

#[derive(Parser, Debug)]
#[command(
    name = "rdm",
    version,
    about = "Rust Download Manager — a usable, efficient CLI download tool",
    long_about = None,
    arg_required_else_help = true,
)]
pub struct Cli {
    /// URL to download (quick mode; only used when no subcommand is given)
    pub url: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,

    /// Output file or directory
    #[arg(short, long, global = true)]
    pub output: Option<String>,

    /// Connections per file
    #[arg(short, long, global = true)]
    pub connections: Option<usize>,

    /// Allow scanning private/local IP addresses
    #[arg(long, global = true)]
    pub allow_private: bool,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Download a single URL
    #[command(alias = "d")]
    Download(DownloadArgs),

    /// Sync a remote directory listing to local
    Sync(SyncArgs),

    /// Show configuration
    Config,

    /// Manage the download queue
    #[command(alias = "q")]
    Queue(QueueArgs),
}

#[derive(Args, Debug, Clone)]
pub struct DownloadArgs {
    /// URL to download
    pub url: String,
}

#[derive(Args, Debug, Clone)]
pub struct SyncArgs {
    /// URL of remote directory listing
    pub url: String,

    /// Parallel downloads
    #[arg(short, long)]
    pub parallel: Option<usize>,

    /// Remove local files not on remote
    #[arg(short, long)]
    pub delete: bool,

    /// Only sync these file extensions
    #[arg(short, long, value_delimiter = ',')]
    pub ext: Vec<String>,
}

#[derive(Args, Debug, Clone)]
pub struct QueueArgs {
    #[command(subcommand)]
    pub command: QueueCommands,
}

#[derive(Subcommand, Debug, Clone)]
pub enum QueueCommands {
    /// Add a URL to the queue
    #[command(alias = "a")]
    Add(DownloadArgs),

    /// List queue items
    #[command(alias = "ls", alias = "l")]
    List,

    /// Start processing the queue
    #[command(alias = "s", alias = "run")]
    Start {
        /// Parallel downloads
        #[arg(short, long)]
        parallel: Option<usize>,
    },

    /// Stop the queue after the current download
    Stop,

    /// Skip the current download(s)
    #[command(alias = "n", alias = "next")]
    Skip,

    /// Remove a queue item by ID
    #[command(alias = "rm")]
    Remove {
        /// Queue item ID
        id: u64,
    },

    /// Retry failed or skipped items
    #[command(alias = "r")]
    Retry {
        /// ID, "failed", "skipped", or omitted for all
        target: Option<String>,
    },

    /// Clear queue items
    #[command(alias = "c")]
    Clear {
        /// Filter: "pending" or "done"
        filter: Option<String>,
    },
}

/// Resolve the output path for a download, matching the legacy behavior:
/// - trailing `/` or `\\` or an existing directory means "put the file inside this dir"
/// - absolute paths are used as-is
/// - relative paths are resolved against the configured download directory
pub fn resolve_output(output: Option<String>, url: &str, download_dir: &str) -> String {
    let filename_from_url = || -> String {
        crate::cli::extract_filename_from_url(url).unwrap_or_else(|| "download.bin".to_string())
    };

    match output {
        Some(o) => {
            let path = std::path::Path::new(&o);
            if o.ends_with('/') || o.ends_with('\\') || path.is_dir() {
                let dir = o.trim_end_matches('/').trim_end_matches('\\');
                format!("{}/ {}", dir, filename_from_url())
            } else if path.is_absolute() {
                o
            } else {
                resolve_relative_path(&o, download_dir)
            }
        }
        None => resolve_relative_path(&filename_from_url(), download_dir),
    }
}

fn resolve_relative_path(filename: &str, download_dir: &str) -> String {
    let path = std::path::Path::new(filename);
    if path.is_absolute() {
        filename.to_string()
    } else {
        std::path::PathBuf::from(download_dir)
            .join(filename)
            .to_string_lossy()
            .to_string()
    }
}

/// Convert a comma-separated extension list into a set of lowercase extensions.
pub fn parse_ext_filter(exts: Vec<String>) -> Option<HashSet<String>> {
    if exts.is_empty() {
        return None;
    }
    Some(
        exts.into_iter()
            .flat_map(|v| v.split(','))
            .map(|e| e.trim().trim_start_matches('.').to_lowercase())
            .filter(|e| !e.is_empty())
            .collect(),
    )
}
