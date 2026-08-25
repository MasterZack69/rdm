//! What a caller asks the engine for, and how the download ended.

/// What to do when the target file already exists on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingPolicy {
    /// Prompt the user to overwrite / rename / cancel. Interactive only.
    Ask,
    /// Treat the existing file as done. Used by every batch path, so a queue
    /// of 400 files never blocks on a hidden prompt behind the progress board.
    Reuse,
    /// Remove the file (and any `.part` / `.rdm` state) and download again.
    Overwrite,
}

#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub output: Option<String>,
    pub connections: usize,
    pub policy: ExistingPolicy,
    /// The client to download with, when it has to be a particular one.
    ///
    /// Normally `None`, and the shared client is used along with its connection
    /// pool. A hoster that had to authenticate passes its own client instead,
    /// because some authorisation cannot be expressed in a URL: a
    /// password-protected Dropbox share is authorised by the cookies in a jar,
    /// so the download has to go out over the client holding that jar.
    ///
    /// Keeping it on the request is what lets such a hoster stay a URL rewrite
    /// instead of growing a downloader: ranges, chunking, resume and retries
    /// all still come from this module.
    pub client: Option<reqwest::Client>,
    /// A stable identity for the remote content, when the URL is not one.
    ///
    /// The same reason `client` exists: a signed URL is a credential, not a
    /// name. A OneDrive download URL carries a fresh `tempauth` per run, so
    /// resume state keyed on it is discarded and the file restarts from zero.
    /// A hoster that knows something durable — a drive item id — puts it here.
    pub resume_identity: Option<String>,
}

impl DownloadRequest {
    pub fn new(url: String, output: Option<String>, connections: usize) -> Self {
        Self {
            url,
            output,
            connections,
            policy: ExistingPolicy::Ask,
            client: None,
            resume_identity: None,
        }
    }

    pub fn with_policy(mut self, policy: ExistingPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Downloads over `client` rather than the shared one, carrying whatever
    /// session it holds.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Identifies the remote content for resume purposes, when the URL cannot.
    pub fn with_resume_identity(mut self, identity: String) -> Self {
        self.resume_identity = Some(identity);
        self
    }
}

/// How a download ended. Errors are still returned as `Err`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Completed { path: String, bytes: u64 },
    AlreadyPresent { path: String },
    Cancelled,
}

/// Result of reconciling the requested output path with what is on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputDecision {
    Use(String),
    AlreadyPresent,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_defaults_to_asking_about_existing_files() {
        let req = DownloadRequest::new("https://example.com/f.zip".into(), None, 8);
        assert_eq!(req.policy, ExistingPolicy::Ask);
        assert_eq!(
            req.with_policy(ExistingPolicy::Reuse).policy,
            ExistingPolicy::Reuse
        );
    }

    /// A session cannot be expressed in a URL, so it travels on the request.
    /// Default is the shared client, which is what keeps the pool useful.
    #[test]
    fn a_request_uses_the_shared_client_unless_given_one() {
        let req = DownloadRequest::new("https://example.com/f.zip".into(), None, 8);
        assert!(req.client.is_none());

        let authenticated = DownloadRequest::new("https://example.com/f.zip".into(), None, 8)
            .with_client(reqwest::Client::new());
        assert!(authenticated.client.is_some());
    }
}
