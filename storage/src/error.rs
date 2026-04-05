//! Structured error types for the storage crate.
//!
//! Inspired by Alluxio's status-based exception hierarchy with explicit
//! retryability classification. Each error variant carries enough context
//! for callers to decide whether to retry, propagate, or fail fast.

use snafu::Snafu;

use crate::retry::GcsError;

/// Structured error type for all storage operations.
///
/// Replaces `anyhow::Result` throughout the storage crate to give callers
/// machine-readable error classification (transient vs permanent).
#[derive(Debug, Snafu)]
pub enum StorageError {
    /// GCS operation failed (wraps the retryable/permanent classification).
    #[snafu(display("GCS operation failed: {source}"))]
    Gcs { source: GcsError },

    /// Disk I/O failed during page read/write.
    #[snafu(display("disk I/O failed for {path}: {source}"))]
    DiskIo {
        path: String,
        source: std::io::Error,
    },

    /// Page assembly failed (e.g., page not found in cache).
    #[snafu(display("page assembly failed: {detail}"))]
    PageAssembly { detail: String },

    /// HTTP response parsing failed.
    #[snafu(display("HTTP parse error: {detail}"))]
    HttpParse { detail: String },

    /// TLS/TCP connection failed.
    #[snafu(display("connection failed: {detail}: {source}"))]
    Connection {
        detail: String,
        source: std::io::Error,
    },

    /// JSON serialization/deserialization failed.
    #[snafu(display("serialization failed: {source}"))]
    Serialization { source: serde_json::Error },

    /// Parquet metadata parsing failed.
    #[snafu(display("metadata parse error: {source}"))]
    MetadataParse {
        source: parquet::errors::ParquetError,
    },

    /// TLS handshake or configuration error.
    #[snafu(display("TLS error: {detail}"))]
    Tls { detail: String },

    /// Response body length mismatch.
    #[snafu(display("response validation failed: {detail}"))]
    ResponseValidation { detail: String },
}

/// Convenience alias for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;

impl StorageError {
    /// Whether this error is transient and the operation should be retried.
    ///
    /// Follows Alluxio's pattern: GCS transient errors (429, 5xx, timeouts)
    /// are retryable; all other error categories are permanent.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Gcs { source } => source.is_retryable(),
            Self::DiskIo { .. } => true,
            Self::Connection { .. } => true,
            Self::PageAssembly { .. }
            | Self::HttpParse { .. }
            | Self::Serialization { .. }
            | Self::MetadataParse { .. }
            | Self::Tls { .. }
            | Self::ResponseValidation { .. } => false,
        }
    }

    /// Map this storage error to a wire protocol error code.
    ///
    /// Follows Presto's pattern: each error maps to a specific code with
    /// type (UserError/InternalError/External) and retryability metadata.
    pub fn to_error_code(&self) -> ruxio_protocol::error_code::ErrorCode {
        use ruxio_protocol::error_code;
        match self {
            Self::Gcs { source } => match source {
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
