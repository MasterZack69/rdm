use std::time::Duration;

use reqwest::StatusCode;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        }
    }
}

impl RetryConfig {
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_ms = self.base_delay.as_millis() as u64;
        let factor = 2u64.saturating_pow(attempt);
        let delay_ms = base_ms.saturating_mul(factor);
        let capped = delay_ms.min(self.max_delay.as_millis() as u64);
        Duration::from_millis(capped)
    }
}

pub fn is_transient_status(status: StatusCode) -> bool {
    matches!(
        status.as_u16(),
        408 | 429 | 500 | 502 | 503 | 504
    )
}

#[derive(Debug)]
pub struct TransientError {
    pub message: String,
}

impl std::fmt::Display for TransientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TransientError {}

const RETRYABLE_IO_KINDS: &[std::io::ErrorKind] = &[
    std::io::ErrorKind::ConnectionReset,
    std::io::ErrorKind::ConnectionAborted,
    std::io::ErrorKind::TimedOut,
    std::io::ErrorKind::UnexpectedEof,
    std::io::ErrorKind::Interrupted,
];

const PERMANENT_IO_KINDS: &[std::io::ErrorKind] = &[
    std::io::ErrorKind::WriteZero,
    std::io::ErrorKind::PermissionDenied,
    std::io::ErrorKind::OutOfMemory,
    std::io::ErrorKind::InvalidInput,
    std::io::ErrorKind::InvalidData,
];

pub fn is_retryable(err: &anyhow::Error) -> bool {
    let mut found_transient = false;

    for cause in err.chain() {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if PERMANENT_IO_KINDS.contains(&io.kind()) {
                return false;
            }
            if RETRYABLE_IO_KINDS.contains(&io.kind()) {
                found_transient = true;
            }
        }

        if cause.downcast_ref::<TransientError>().is_some() {
            found_transient = true;
        }

        if let Some(re) = cause.downcast_ref::<reqwest::Error>() {
            if re.is_timeout() || re.is_connect() || re.is_body() {
                found_transient = true;
            }
        }
    }

    found_transient
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff() {
        let config = RetryConfig::default();
        assert_eq!(config.delay_for_attempt(0), Duration::from_secs(1));
        assert_eq!(config.delay_for_attempt(1), Duration::from_secs(2));
        assert_eq!(config.delay_for_attempt(2), Duration::from_secs(4));
        assert_eq!(config.delay_for_attempt(3), Duration::from_secs(8));
        assert_eq!(config.delay_for_attempt(4), Duration::from_secs(16));
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let config = RetryConfig {
            max_retries: 10,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        };
        assert_eq!(config.delay_for_attempt(5), Duration::from_secs(30));
        assert_eq!(config.delay_for_attempt(10), Duration::from_secs(30));
    }

    #[test]
    fn test_backoff_no_overflow() {
        let config = RetryConfig::default();
        let delay = config.delay_for_attempt(50);
        assert_eq!(delay, config.max_delay);
    }

    #[test]
    fn test_transient_status_codes() {
        assert!(is_transient_status(StatusCode::REQUEST_TIMEOUT));
        assert!(is_transient_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_transient_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_transient_status(StatusCode::BAD_GATEWAY));
        assert!(is_transient_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_transient_status(StatusCode::GATEWAY_TIMEOUT));
    }

    #[test]
    fn test_permanent_status_codes() {
        assert!(!is_transient_status(StatusCode::NOT_FOUND));
        assert!(!is_transient_status(StatusCode::FORBIDDEN));
        assert!(!is_transient_status(StatusCode::OK));
        assert!(!is_transient_status(StatusCode::PARTIAL_CONTENT));
    }

    #[test]
    fn test_transient_marker_is_retryable() {
        let err = anyhow::Error::new(TransientError {
            message: "server busy".into(),
        });
        assert!(is_retryable(&err));
    }

    #[test]
    fn test_generic_error_not_retryable() {
        let err = anyhow::anyhow!("permanent failure");
        assert!(!is_retryable(&err));
    }

    #[test]
    fn test_io_connection_reset_is_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_connection_aborted_is_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "aborted");
        assert!(is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_timeout_is_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
        assert!(is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_unexpected_eof_is_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof");
        assert!(is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_interrupted_is_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::Interrupted, "signal");
        assert!(is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_permission_denied_not_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        assert!(!is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_write_zero_not_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::WriteZero, "disk full");
        assert!(!is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_out_of_memory_not_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::OutOfMemory, "oom");
        assert!(!is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_invalid_input_not_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad input");
        assert!(!is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_io_invalid_data_not_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::InvalidData, "bad data");
        assert!(!is_retryable(&anyhow::Error::new(e)));
    }

    #[test]
    fn test_broken_pipe_not_permanent() {
        let e = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe");
        assert!(!PERMANENT_IO_KINDS.contains(&e.kind()));
    }

    #[test]
    fn test_already_exists_not_permanent() {
        let e = std::io::Error::new(std::io::ErrorKind::AlreadyExists, "exists");
        assert!(!PERMANENT_IO_KINDS.contains(&e.kind()));
    }

    #[test]
    fn test_not_found_not_permanent() {
        let e = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        assert!(!PERMANENT_IO_KINDS.contains(&e.kind()));
    }

    #[test]
    fn test_permanent_io_overrides_transient_marker() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let inner = anyhow::Error::new(io_err);
        let outer = inner.context(TransientError {
            message: "transient wrapper".into(),
        });
        assert!(!is_retryable(&outer));
    }

    #[test]
    fn test_write_zero_overrides_transient_marker() {
        let io_err = std::io::Error::new(std::io::ErrorKind::WriteZero, "disk full");
        let inner = anyhow::Error::new(io_err);
        let outer = inner.context(TransientError {
            message: "should not matter".into(),
        });
        assert!(!is_retryable(&outer));
    }

    #[test]
    fn test_unknown_io_kind_not_retryable() {
        let e = std::io::Error::new(std::io::ErrorKind::Other, "something weird");
        assert!(!is_retryable(&anyhow::Error::new(e)));
    }
}
