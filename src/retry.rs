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
        let factor = 2u64.saturating_pow(attempt.min(10));
        let delay_ms = base_ms.saturating_mul(factor);
        let capped = delay_ms.min(self.max_delay.as_millis() as u64);

        let jitter_range = capped / 4;
        let jittered = if jitter_range > 0 {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos() as u64;
            let offset = nanos % (jitter_range * 2);
            capped - jitter_range + offset
        } else {
            capped
        };

        Duration::from_millis(jittered.min(self.max_delay.as_millis() as u64))
    }
}

pub fn is_transient_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

#[derive(Debug)]
pub struct TransientError {
    pub message: String,
    /// What the server asked us to wait, when it said.
    ///
    /// A `429` or `503` carrying `Retry-After` is the one case where the
    /// server has told us exactly how to behave, and guessing an exponential
    /// backoff instead is how a client earns a longer ban.
    pub retry_after: Option<Duration>,
}

impl TransientError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn after(message: impl Into<String>, retry_after: Option<Duration>) -> Self {
        Self {
            message: message.into(),
            retry_after,
        }
    }
}

/// The longest pause a server may ask for and be believed.
///
/// `Retry-After: 86400` is a valid answer and not one a download session can
/// act on; honouring it literally would hang the run for a day.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(120);

/// Parses `Retry-After`, in either of its two forms.
///
/// Delta-seconds (`Retry-After: 30`) or an HTTP-date
/// (`Retry-After: Wed, 21 Oct 2015 07:28:00 GMT`). A date already in the past
/// means "now", which is `Duration::ZERO` rather than an error. The result is
/// capped at [`MAX_RETRY_AFTER`].
pub fn parse_retry_after(value: &str, now_unix: u64) -> Option<Duration> {
    let value = value.trim();

    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER));
    }

    let target = parse_http_date(value)?;
    Some(Duration::from_secs(target.saturating_sub(now_unix)).min(MAX_RETRY_AFTER))
}

/// Seconds since the epoch for an IMF-fixdate, the form servers must send.
///
/// `Sun, 06 Nov 1994 08:49:37 GMT`. The two obsolete formats in the spec are
/// not parsed; a client that cannot read one falls back to its own backoff,
/// which is the same thing it does for a missing header.
fn parse_http_date(value: &str) -> Option<u64> {
    let rest = value.split_once(", ").map(|(_, rest)| rest).unwrap_or(value);
    let mut parts = rest.split_whitespace();

    let day: i64 = parts.next()?.parse().ok()?;
    let month = match parts.next()? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i64 = parts.next()?.parse().ok()?;

    let time = parts.next()?;
    let mut hms = time.split(':');
    let hour: u64 = hms.next()?.parse().ok()?;
    let minute: u64 = hms.next()?.parse().ok()?;
    let second: u64 = hms.next()?.parse().ok()?;
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    let days = days_from_civil(year, month, day);
    if days < 0 {
        return None;
    }

    Some(days as u64 * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// Days between 1970-01-01 and the given civil date (Howard Hinnant's
/// `days_from_civil`, the standard branch-free form).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The pause the server asked for, anywhere in an error chain.
pub fn retry_after(err: &anyhow::Error) -> Option<Duration> {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<TransientError>())
        .find_map(|transient| transient.retry_after)
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

        if let Some(re) = cause.downcast_ref::<reqwest::Error>()
            && (re.is_timeout() || re.is_connect() || re.is_body())
        {
            found_transient = true;
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
        let d0 = config.delay_for_attempt(0).as_millis();
        assert!((700..=1300).contains(&d0), "attempt 0: {}ms", d0);
        let d2 = config.delay_for_attempt(2).as_millis();
        assert!((2800..=5200).contains(&d2), "attempt 2: {}ms", d2);
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let config = RetryConfig {
            max_retries: 10,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        };
        // Any attempt beyond cap should stay within max + jitter headroom
        for attempt in [5, 8, 10, 15] {
            let d = config.delay_for_attempt(attempt);
            assert!(d <= config.max_delay, "attempt {}: {:?}", attempt, d);
        }
    }

    #[test]
    fn test_backoff_no_overflow() {
        let config = RetryConfig::default();
        for attempt in [30, 50, 64, 100] {
            let delay = config.delay_for_attempt(attempt);
            assert!(
                delay <= config.max_delay,
                "attempt {}: {:?}",
                attempt,
                delay
            );
        }
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
        let err = anyhow::Error::new(TransientError::new("server busy"));
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
        let outer = inner.context(TransientError::new("transient wrapper"));
        assert!(!is_retryable(&outer));
    }

    #[test]
    fn test_write_zero_overrides_transient_marker() {
        let io_err = std::io::Error::new(std::io::ErrorKind::WriteZero, "disk full");
        let inner = anyhow::Error::new(io_err);
        let outer = inner.context(TransientError::new("should not matter"));
        assert!(!is_retryable(&outer));
    }

    #[test]
    fn retry_after_in_seconds_is_taken_literally() {
        assert_eq!(parse_retry_after("30", 0), Some(Duration::from_secs(30)));
        assert_eq!(parse_retry_after("  5 ", 0), Some(Duration::from_secs(5)));
        assert_eq!(parse_retry_after("0", 0), Some(Duration::ZERO));
    }

    /// A server may ask for a day. A download session cannot give it one.
    #[test]
    fn an_absurd_retry_after_is_capped() {
        assert_eq!(parse_retry_after("86400", 0), Some(MAX_RETRY_AFTER));
    }

    #[test]
    fn retry_after_as_a_date_is_the_distance_from_now() {
        // 1994-11-06 08:49:37 GMT
        let epoch = 784_111_777;
        assert_eq!(
            parse_retry_after("Sun, 06 Nov 1994 08:49:47 GMT", epoch),
            Some(Duration::from_secs(10))
        );
        // Already past: retry now rather than error.
        assert_eq!(
            parse_retry_after("Sun, 06 Nov 1994 08:49:00 GMT", epoch),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn a_retry_after_we_cannot_read_is_simply_absent() {
        assert_eq!(parse_retry_after("soon", 0), None);
        assert_eq!(parse_retry_after("", 0), None);
        assert_eq!(parse_retry_after("Sun, 06 Foo 1994 08:49:37 GMT", 0), None);
    }

    #[test]
    fn a_servers_pause_is_found_through_the_error_chain() {
        let err = anyhow::Error::new(TransientError::after(
            "HTTP 429",
            Some(Duration::from_secs(12)),
        ))
        .context("chunk #3 failed");
        assert_eq!(retry_after(&err), Some(Duration::from_secs(12)));

        let plain = anyhow::Error::new(TransientError::new("HTTP 503"));
        assert_eq!(retry_after(&plain), None);
    }

    #[test]
    fn test_unknown_io_kind_not_retryable() {
        let e = std::io::Error::other("something weird");
        assert!(!is_retryable(&anyhow::Error::new(e)));
    }
}
