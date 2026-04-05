//! Presto-inspired error categorization for the Ruxio wire protocol.
//!
//! Follows Presto's pattern of separating error **type** (who is at fault) from
//! error **code** (what went wrong) with explicit retryability flags.
//!
//! Error code ranges:
//! - `0x0000_xxxx` — Client/user errors
//! - `0x0001_xxxx` — Internal server errors
//! - `0x0002_xxxx` — Resource exhaustion
//! - `0x0003_xxxx` — External system errors (GCS, etc.)

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// Who is at fault for this error.
///
/// Modeled after Presto's `ErrorType` enum:
/// - `UserError` — the request was invalid (bad URI, missing params)
/// - `InternalError` — server bug or unexpected state (corrupt page, codec error)
/// - `InsufficientResources` — server is overloaded (connection limit, memory)
/// - `External` — an external dependency failed (GCS, etcd, DNS)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ErrorType {
    UserError = 0,
    InternalError = 1,
    InsufficientResources = 2,
    External = 3,
}

/// A structured error code with type and retryability metadata.
///
/// Modeled after Presto's `ErrorCode` class. Every error in the system
/// has a unique numeric code, a human-readable name, a category (ErrorType),
/// and a flag indicating whether the client should retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCode {
    /// Unique numeric code (e.g., 0x0000_0001).
    pub code: u32,
    /// Human-readable error name (e.g., "INVALID_REQUEST").
    pub name: Cow<'static, str>,
    /// Error category — who is at fault.
    pub error_type: ErrorType,
    /// Whether the client should retry this operation.
    pub retriable: bool,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:0x{:08X}", self.name, self.code)
    }
}

/// Helper to construct an `ErrorCode` constant.
macro_rules! error_code {
    ($code:expr, $name:expr, $etype:expr, $retriable:expr) => {
        ErrorCode {
            code: $code,
            name: Cow::Borrowed($name),
            error_type: $etype,
            retriable: $retriable,
        }
    };
}

// ── User errors (0x0000_xxxx) ───────────────────────────────────────

/// Generic user error (bad request, malformed payload).
pub fn invalid_request() -> ErrorCode {
    error_code!(0x0000_0001, "INVALID_REQUEST", ErrorType::UserError, false)
}

pub fn not_found() -> ErrorCode {
    error_code!(0x0000_0002, "NOT_FOUND", ErrorType::UserError, false)
}

pub fn permission_denied() -> ErrorCode {
    error_code!(
        0x0000_0003,
        "PERMISSION_DENIED",
        ErrorType::UserError,
        false
    )
}

pub fn not_supported() -> ErrorCode {
    error_code!(0x0000_0004, "NOT_SUPPORTED", ErrorType::UserError, false)
}

// ── Internal errors (0x0001_xxxx) ───────────────────────────────────

pub fn generic_internal_error() -> ErrorCode {
    error_code!(
        0x0001_0000,
        "GENERIC_INTERNAL_ERROR",
        ErrorType::InternalError,
        false
    )
}

pub fn corrupt_page() -> ErrorCode {
    error_code!(0x0001_0001, "CORRUPT_PAGE", ErrorType::InternalError, false)
}

pub fn disk_io_error() -> ErrorCode {
    error_code!(0x0001_0002, "DISK_IO_ERROR", ErrorType::InternalError, true)
}

pub fn page_not_cached() -> ErrorCode {
    error_code!(
        0x0001_0003,
        "PAGE_NOT_CACHED",
        ErrorType::InternalError,
        true
    )
}

pub fn metadata_parse_error() -> ErrorCode {
    error_code!(
        0x0001_0004,
        "METADATA_PARSE_ERROR",
        ErrorType::InternalError,
        false
    )
}

/// Retryable — client should reconnect to another node.
pub fn server_shutting_down() -> ErrorCode {
    error_code!(
        0x0001_0005,
        "SERVER_SHUTTING_DOWN",
        ErrorType::InternalError,
        true
    )
}

pub fn sendfile_error() -> ErrorCode {
    error_code!(
        0x0001_0006,
        "SENDFILE_ERROR",
        ErrorType::InternalError,
        true
    )
}

pub fn write_error() -> ErrorCode {
    error_code!(0x0001_0007, "WRITE_ERROR", ErrorType::InternalError, true)
}

pub fn codec_error() -> ErrorCode {
    error_code!(0x0001_0008, "CODEC_ERROR", ErrorType::InternalError, false)
}

// ── Resource exhaustion (0x0002_xxxx) ───────────────────────────────

pub fn server_overloaded() -> ErrorCode {
    error_code!(
        0x0002_0001,
        "SERVER_OVERLOADED",
        ErrorType::InsufficientResources,
        true
    )
}

pub fn connection_limit() -> ErrorCode {
    error_code!(
        0x0002_0002,
        "CONNECTION_LIMIT",
        ErrorType::InsufficientResources,
        true
    )
}

// ── External errors (0x0003_xxxx) ───────────────────────────────────

/// HTTP 429, 5xx, or network timeout — retryable.
pub fn gcs_transient_error() -> ErrorCode {
    error_code!(
        0x0003_0001,
        "GCS_TRANSIENT_ERROR",
        ErrorType::External,
        true
    )
}

/// HTTP 4xx (except 429) — not retryable.
pub fn gcs_permanent_error() -> ErrorCode {
    error_code!(
        0x0003_0002,
        "GCS_PERMANENT_ERROR",
        ErrorType::External,
        false
    )
}

pub fn gcs_timeout() -> ErrorCode {
    error_code!(0x0003_0003, "GCS_TIMEOUT", ErrorType::External, true)
}

pub fn gcs_response_invalid() -> ErrorCode {
    error_code!(
        0x0003_0004,
        "GCS_RESPONSE_INVALID",
        ErrorType::External,
        true
    )
}

pub fn connection_error() -> ErrorCode {
    error_code!(0x0003_0005, "CONNECTION_ERROR", ErrorType::External, true)
}

pub fn membership_error() -> ErrorCode {
    error_code!(0x0003_0006, "MEMBERSHIP_ERROR", ErrorType::External, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_display() {
        assert_eq!(
            format!("{}", invalid_request()),
            "INVALID_REQUEST:0x00000001"
        );
        assert_eq!(
            format!("{}", gcs_transient_error()),
            "GCS_TRANSIENT_ERROR:0x00030001"
        );
    }

    #[test]
    fn test_error_type_categories() {
        assert_eq!(invalid_request().error_type, ErrorType::UserError);
        assert_eq!(corrupt_page().error_type, ErrorType::InternalError);
        assert_eq!(
            server_overloaded().error_type,
            ErrorType::InsufficientResources
        );
        assert_eq!(gcs_transient_error().error_type, ErrorType::External);
    }

    #[test]
    fn test_retryability() {
        // User errors are never retriable
        assert!(!invalid_request().retriable);
        assert!(!not_found().retriable);
        assert!(!permission_denied().retriable);

        // Transient internal errors are retriable
        assert!(disk_io_error().retriable);
        assert!(page_not_cached().retriable);
        assert!(!corrupt_page().retriable);

        // Resource exhaustion is retriable
        assert!(server_overloaded().retriable);
        assert!(connection_limit().retriable);

        // External transient is retriable, permanent is not
        assert!(gcs_transient_error().retriable);
        assert!(!gcs_permanent_error().retriable);
        assert!(gcs_timeout().retriable);
    }

    #[test]
    fn test_error_code_serialization() {
        let ec = gcs_transient_error();
        let json = serde_json::to_string(&ec).unwrap();
        let deserialized: ErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.code, ec.code);
        assert_eq!(deserialized.error_type, ErrorType::External);
        assert!(deserialized.retriable);
    }

    #[test]
    fn test_no_code_collisions() {
        let codes: Vec<ErrorCode> = vec![
            invalid_request(),
            not_found(),
            permission_denied(),
            not_supported(),
            generic_internal_error(),
            corrupt_page(),
            disk_io_error(),
            page_not_cached(),
            metadata_parse_error(),
            server_shutting_down(),
            sendfile_error(),
            write_error(),
            codec_error(),
            server_overloaded(),
            connection_limit(),
            gcs_transient_error(),
            gcs_permanent_error(),
            gcs_timeout(),
            gcs_response_invalid(),
            connection_error(),
            membership_error(),
        ];
        let mut seen = std::collections::HashSet::new();
        for ec in &codes {
            assert!(seen.insert(ec.code), "duplicate error code: {ec}");
        }
    }
}
