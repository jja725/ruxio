//! Structured error types for the storage crate.
//!
//! Follows the Lance error pattern: each variant carries a `Location` for
//! automatic file/line tracking, and uses `BoxedError` for flexible source
//! wrapping. This gives typed error matching at boundaries (retry, error codes)
//! with zero-cost location tracking for debugging.

use snafu::{Location, Snafu};

use crate::retry::GcsError;

/// Heap-allocated, type-erased error for flexible source wrapping.
pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Convenience alias for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;

/// Structured error type for all storage operations.
///
/// Each variant carries `Location` (auto-captured at construction site)
/// for production debugging without backtraces.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum StorageError {
    /// GCS operation failed (wraps the retryable/permanent classification).
    #[snafu(display("GCS operation failed: {source}, {location}"))]
    Gcs {
        source: GcsError,
        #[snafu(implicit)]
        location: Location,
    },

    /// Disk I/O failed during page read/write.
    #[snafu(display("disk I/O failed for {path}: {source}, {location}"))]
    DiskIo {
        path: String,
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    /// Page assembly failed (e.g., page not found in cache).
    #[snafu(display("page assembly failed: {detail}, {location}"))]
    PageAssembly {
        detail: String,
        #[snafu(implicit)]
        location: Location,
    },

    /// HTTP response parsing failed.
    #[snafu(display("HTTP parse error: {detail}, {location}"))]
    HttpParse {
        detail: String,
        #[snafu(implicit)]
        location: Location,
    },

    /// TLS/TCP connection failed.
    #[snafu(display("connection failed: {detail}: {source}, {location}"))]
    Connection {
        detail: String,
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    /// JSON serialization/deserialization failed.
    #[snafu(display("serialization failed: {source}, {location}"))]
    Serialization {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    /// Parquet metadata parsing failed.
    #[snafu(display("metadata parse error: {source}, {location}"))]
    MetadataParse {
        source: parquet::errors::ParquetError,
        #[snafu(implicit)]
        location: Location,
    },

    /// TLS handshake or configuration error.
    #[snafu(display("TLS error: {detail}, {location}"))]
    Tls {
        detail: String,
        #[snafu(implicit)]
        location: Location,
    },

    /// Response body length mismatch.
    #[snafu(display("response validation failed: {detail}, {location}"))]
    ResponseValidation {
        detail: String,
        #[snafu(implicit)]
        location: Location,
    },
}

impl StorageError {
    /// Whether this error is transient and the operation should be retried.
    /// Derived from the error code's retryability flag to avoid redundant state.
    pub fn is_retryable(&self) -> bool {
        self.to_error_code().retriable
    }

    /// Map this storage error to a wire protocol error code.
    pub fn to_error_code(&self) -> ruxio_protocol::error_code::ErrorCode {
        use ruxio_protocol::error_code;
        match self {
            Self::Gcs { source, .. } => match source {
                GcsError::Transient { .. } => error_code::gcs_transient_error(),
                GcsError::Permanent { .. } => error_code::gcs_permanent_error(),
                GcsError::Timeout { .. } => error_code::gcs_timeout(),
            },
            Self::DiskIo { .. } => error_code::disk_io_error(),
            Self::PageAssembly { .. } => error_code::page_not_cached(),
            Self::HttpParse { .. } => error_code::gcs_response_invalid(),
            Self::Connection { .. } => error_code::connection_error(),
            Self::Serialization { .. } => error_code::codec_error(),
            Self::MetadataParse { .. } => error_code::metadata_parse_error(),
            Self::Tls { .. } => error_code::connection_error(),
            Self::ResponseValidation { .. } => error_code::gcs_response_invalid(),
        }
    }
}
