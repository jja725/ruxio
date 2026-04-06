use bytes::Bytes;
use ruxio_protocol::frame::MessageType;

/// Parquet file metadata returned by `get_metadata`.
#[derive(Debug)]
pub struct MetadataResult {
    pub uri: String,
    pub file_size: u64,
    pub footer_size: u64,
}

/// Internal response from a single request exchange.
#[derive(Debug)]
pub(crate) enum Response {
    /// Data chunks accumulated from DataChunk frames.
    Data(Bytes),
    /// Server said this node doesn't own the file — try the redirect target.
    Redirect { host: String, port: u16 },
    /// Server returned an error.
    Error {
        error_code: ruxio_protocol::error_code::ErrorCode,
        message: String,
    },
    /// Metadata response.
    Metadata(MetadataResult),
}

impl Response {
    pub(crate) fn msg_type(&self) -> MessageType {
        match self {
            Self::Data(_) => MessageType::DataChunk,
            Self::Redirect { .. } => MessageType::Redirect,
            Self::Error { .. } => MessageType::Error,
            Self::Metadata(_) => MessageType::Metadata,
        }
    }
}
