use std::time::Duration;

/// Structured GCS error types for retry classification.
#[derive(Debug, thiserror::Error)]
pub enum GcsError {
    /// Transient error — safe to retry (HTTP 429, 5xx, network errors).
    #[error("transient: {0}")]
    Transient(String),
    /// Permanent error — do not retry (HTTP 4xx except 429).
    #[error("permanent: {0}")]
    Permanent(String),
    /// Request timed out.
    #[error("timeout after {0:?}")]
    Timeout(Duration),
}

impl GcsError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient(_) | Self::Timeout(_))
    }

    /// Classify an HTTP status code into transient or permanent.
    pub fn from_status(status: u16, body: &str) -> Self {
        match status {
            429 | 500 | 502 | 503 | 504 => {
                Self::Transient(format!("HTTP {status}: {}", truncate(body, 200)))
            }
            _ => Self::Permanent(format!("HTTP {status}: {}", truncate(body, 200))),
        }
    }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    // Find the largest char boundary at or before `max` to avoid
    // panicking on multi-byte UTF-8 sequences.
    s.get(..max).unwrap_or_else(|| {
        let end = s.floor_char_boundary(max);
        &s[..end]
    })
}

/// Retry policy with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (0 = no retries).
    pub max_retries: u32,
    /// Base delay for exponential backoff.
    pub base_delay_ms: u64,
    /// Maximum delay cap.
    pub max_delay_ms: u64,
    /// Per-request timeout.
    pub timeout_secs: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay_ms: 100,
            max_delay_ms: 5_000,
            timeout_secs: 30,
        }
    }
}

impl RetryPolicy {
    /// Compute delay for a given attempt (0-indexed).
    /// Uses exponential backoff: base * 2^attempt, capped at max_delay.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let delay_ms = self.base_delay_ms.saturating_mul(1u64 << attempt.min(12));
        Duration::from_millis(delay_ms.min(self.max_delay_ms))
    }

    /// Per-request timeout as Duration.
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_delays() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(800));
        // Should cap at max_delay
        assert_eq!(policy.delay_for_attempt(10), Duration::from_millis(5000));
        assert_eq!(policy.delay_for_attempt(20), Duration::from_millis(5000));
    }

    #[test]
    fn test_gcs_error_classification() {
        assert!(GcsError::from_status(503, "Service Unavailable").is_retryable());
        assert!(GcsError::from_status(429, "Too Many Requests").is_retryable());
        assert!(GcsError::from_status(500, "Internal").is_retryable());
        assert!(!GcsError::from_status(404, "Not Found").is_retryable());
        assert!(!GcsError::from_status(403, "Forbidden").is_retryable());
        assert!(!GcsError::from_status(400, "Bad Request").is_retryable());
    }

    #[test]
    fn test_timeout_is_retryable() {
        assert!(GcsError::Timeout(Duration::from_secs(30)).is_retryable());
    }
}
