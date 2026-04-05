use std::time::Duration;

use ruxio_protocol::error_code::ErrorCode;
use ruxio_protocol::frame::{FrameError, MessageType};
use snafu::{Location, Snafu};

/// Client-side errors for Ruxio operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ClientError {
    /// TCP connection to a server failed.
    #[snafu(display("connection failed to {addr}: {source}, {location}"))]
    ConnectionFailed {
        addr: String,
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    /// Operation timed out.
    #[snafu(display("request timed out after {timeout:?}, {location}"))]
    Timeout {
        timeout: Duration,
        #[snafu(implicit)]
        location: Location,
    },

    /// Server returned a structured error.
    #[snafu(display("server error: {message} (code: {error_code}), {location}"))]
    ServerError {
        error_code: ErrorCode,
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    /// No server available in the hash ring for the given URI.
    #[snafu(display("no server available for {uri}, {location}"))]
    NoServerAvailable {
        uri: String,
        #[snafu(implicit)]
        location: Location,
    },

    /// All retry attempts exhausted.
    #[snafu(display("max retries ({max}) exceeded for {uri}, {location}"))]
    RetriesExhausted {
        uri: String,
        max: u32,
        #[snafu(implicit)]
        location: Location,
    },

    /// Frame decode error.
    #[snafu(display("frame error: {source}, {location}"))]
    Frame {
        source: FrameError,
        #[snafu(implicit)]
        location: Location,
    },

    /// Unexpected response message type.
    #[snafu(display("unexpected response type: {msg_type:?}, {location}"))]
    UnexpectedResponse {
        msg_type: MessageType,
        #[snafu(implicit)]
        location: Location,
    },

    /// JSON deserialization failed.
    #[snafu(display("deserialization error: {source}, {location}"))]
    Deserialization {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    /// Write to server failed.
    #[snafu(display("write failed: {source}, {location}"))]
    WriteFailed {
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    /// Read from server failed.
    #[snafu(display("read failed: {source}, {location}"))]
    ReadFailed {
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    /// Membership discovery failed.
    #[snafu(display("membership error: {detail}, {location}"))]
    Membership {
        detail: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, ClientError>;
