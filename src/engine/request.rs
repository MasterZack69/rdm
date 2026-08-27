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
    pub client: Option<reqwest::Client>,
    pub resume_identity: Option<String>,
    pub allow_private: bool,
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
            allow_private: false,
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

    pub fn with_allow_private(mut self, allow_private: bool) -> Self {
        self.allow_private = allow_private;
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

    /// The default has to be the safe one: most requests are built by code that
    /// never heard of the flag.
    #[test]
    fn a_request_refuses_private_addresses_unless_told_otherwise() {
        let req = DownloadRequest::new("https://example.com/f.zip".into(), None, 8);
        assert!(!req.allow_private);
        assert!(req.with_allow_private(true).allow_private);
    }
}
