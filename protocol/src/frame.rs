use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Message types for the data plane binary protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Read a byte range (engine already knows which bytes).
    ReadRange = 0x01,
    /// Read multiple byte ranges in one round-trip.
    BatchRead = 0x02,
    /// Response: a chunk of data (raw Parquet page bytes).
    DataChunk = 0x03,
    /// Response: error.
    Error = 0x04,
    /// Response: redirect to the correct node.
    Redirect = 0x05,
    /// Response: all data sent for this request.
    Done = 0x06,
    /// Request file metadata (Parquet footer).
    GetMetadata = 0x07,
    /// Response: file metadata.
    Metadata = 0x08,
    /// Scan with predicate pushdown (1 RPC instead of N+1).
    Scan = 0x09,
    /// Batch scan with predicates.
    BatchScan = 0x0A,
}

impl MessageType {
    pub fn from_u8(v: u8) -> Result<Self, FrameError> {
        match v {
            0x01 => Ok(MessageType::ReadRange),
            0x02 => Ok(MessageType::BatchRead),
            0x03 => Ok(MessageType::DataChunk),
            0x04 => Ok(MessageType::Error),
            0x05 => Ok(MessageType::Redirect),
            0x06 => Ok(MessageType::Done),
            0x07 => Ok(MessageType::GetMetadata),
            0x08 => Ok(MessageType::Metadata),
            0x09 => Ok(MessageType::Scan),
            0x0A => Ok(MessageType::BatchScan),
            _ => Err(FrameError::UnknownMessageType(v)),
        }
    }
}

/// A wire protocol frame.
///
/// Wire format:
/// ```text
/// [4 bytes: frame body length (big-endian u32)]
/// [1 byte:  message type]
/// [4 bytes: request ID (big-endian u32)]
/// [N bytes: payload]
/// ```
///
/// The 4-byte length field contains the size of everything after it
/// (1 + 4 + N = msg_type + request_id + payload).
#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: MessageType,
    pub request_id: u32,
    pub payload: Bytes,
}

/// Header size: 4 (length) + 1 (type) + 4 (request_id) = 9 bytes
pub const FRAME_HEADER_SIZE: usize = 9;
/// Body header: 1 (type) + 4 (request_id) = 5 bytes
const BODY_HEADER_SIZE: usize = 5;

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("unknown message type: 0x{0:02x}")]
    UnknownMessageType(u8),
    #[error("incomplete frame: need {needed} bytes, have {have}")]
    Incomplete { needed: usize, have: usize },
    #[error("frame too large: {size} bytes (max {max})")]
    TooLarge { size: usize, max: usize },
}

/// Maximum frame payload size (64MB).
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

impl Frame {
    /// Create a new frame with JSON-serialized payload.
    pub fn new_json<T: serde::Serialize>(
        msg_type: MessageType,
        request_id: u32,
        value: &T,
    ) -> Self {
        let payload = serde_json::to_vec(value).unwrap();
        Frame {
            msg_type,
            request_id,
            payload: Bytes::from(payload),
        }
    }

    /// Create a new frame with raw bytes payload (for DataChunk).
    pub fn new_raw(msg_type: MessageType, request_id: u32, payload: Bytes) -> Self {
        Frame {
            msg_type,
            request_id,
            payload,
        }
    }

    /// Create a Done frame (empty payload).
    pub fn done(request_id: u32) -> Self {
        Frame {
            msg_type: MessageType::Done,
            request_id,
            payload: Bytes::new(),
        }
    }

    /// Encode this frame into wire format bytes.
    pub fn encode(&self) -> Bytes {
        let body_len = BODY_HEADER_SIZE + self.payload.len();
        let mut buf = BytesMut::with_capacity(4 + body_len);
        buf.put_u32(body_len as u32);
        buf.put_u8(self.msg_type as u8);
        buf.put_u32(self.request_id);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Encode just the 9-byte header (for scatter writes where payload is sent separately).
    pub fn encode_header(&self) -> [u8; FRAME_HEADER_SIZE] {
        let body_len = BODY_HEADER_SIZE + self.payload.len();
        let mut header = [0u8; FRAME_HEADER_SIZE];
        header[0..4].copy_from_slice(&(body_len as u32).to_be_bytes());
        header[4] = self.msg_type as u8;
        header[5..9].copy_from_slice(&self.request_id.to_be_bytes());
        header
    }

    /// Attempt to decode a frame from the buffer (copies payload).
    ///
    /// For zero-copy decode, use `FrameReader::next_frame()` instead.
    pub fn decode(buf: &[u8]) -> Result<Option<(Frame, usize)>, FrameError> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let body_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

        if body_len > MAX_FRAME_SIZE {
            return Err(FrameError::TooLarge {
                size: body_len,
                max: MAX_FRAME_SIZE,
            });
        }

        let total_len = 4 + body_len;
        if buf.len() < total_len {
            return Ok(None);
        }

        let cursor = &buf[4..total_len];
        let msg_type = MessageType::from_u8(cursor[0])?;
        let request_id = u32::from_be_bytes([cursor[1], cursor[2], cursor[3], cursor[4]]);
        let payload = Bytes::copy_from_slice(&cursor[5..]);

        Ok(Some((
            Frame {
                msg_type,
                request_id,
                payload,
            },
            total_len,
        )))
    }
}

/// Accumulates bytes from a stream and yields complete frames.
///
/// Uses `BytesMut::split_to` + `freeze` for zero-copy payload extraction —
/// the payload `Bytes` shares the same underlying allocation as the read buffer,
/// avoiding a 4MB copy for DataChunk frames.
pub struct FrameReader {
    buf: BytesMut,
}

/// Default initial capacity — sized for typical page reads.
const FRAME_READER_INITIAL_CAPACITY: usize = 4 * 1024 * 1024 + 1024;

impl FrameReader {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(FRAME_READER_INITIAL_CAPACITY),
        }
    }

    /// Create with a specific initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
        }
    }

    /// Feed raw bytes into the reader.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to extract the next complete frame (zero-copy payload).
    pub fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        if self.buf.len() < 4 {
            return Ok(None);
        }

        let body_len =
            u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;

        if body_len > MAX_FRAME_SIZE {
            return Err(FrameError::TooLarge {
                size: body_len,
                max: MAX_FRAME_SIZE,
            });
        }

        let total_len = 4 + body_len;
        if self.buf.len() < total_len {
            return Ok(None);
        }

        // Split the complete frame out of the buffer
        let mut frame_bytes = self.buf.split_to(total_len);

        // Skip 4-byte length prefix
        frame_bytes.advance(4);

        // Parse header (5 bytes: type + request_id)
        let msg_type = MessageType::from_u8(frame_bytes[0])?;
        let request_id = u32::from_be_bytes([
            frame_bytes[1],
            frame_bytes[2],
            frame_bytes[3],
            frame_bytes[4],
        ]);

        // Skip body header
        frame_bytes.advance(BODY_HEADER_SIZE);

        // Zero-copy: freeze the remaining bytes as payload
        let payload = frame_bytes.freeze();

        Ok(Some(Frame {
            msg_type,
            request_id,
            payload,
        }))
    }
}

impl Default for FrameReader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::ReadRangeRequest;

    #[test]
    fn test_frame_roundtrip() {
        let req = ReadRangeRequest {
            uri: "gs://bucket/file.parquet".into(),
            offset: 4096,
            length: 4 * 1024 * 1024,
        };
        let frame = Frame::new_json(MessageType::ReadRange, 42, &req);
        let encoded = frame.encode();

        let (decoded, consumed) = Frame::decode(&encoded).unwrap().unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.msg_type, MessageType::ReadRange);
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[test]
    fn test_frame_reader_zero_copy() {
        let req = ReadRangeRequest {
            uri: "gs://bucket/file.parquet".into(),
            offset: 0,
            length: 1024,
        };
        let frame = Frame::new_json(MessageType::ReadRange, 42, &req);
        let encoded = frame.encode();

        let mut reader = FrameReader::new();
        reader.feed(&encoded);
        let decoded = reader.next_frame().unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::ReadRange);
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[test]
    fn test_frame_reader_partial() {
        let frame = Frame::done(1);
        let encoded = frame.encode();

        let mut reader = FrameReader::new();

        // Feed first half
        let mid = encoded.len() / 2;
        reader.feed(&encoded[..mid]);
        assert!(reader.next_frame().unwrap().is_none());

        // Feed second half
        reader.feed(&encoded[mid..]);
        let decoded = reader.next_frame().unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Done);
        assert_eq!(decoded.request_id, 1);
    }

    #[test]
    fn test_frame_reader_multiple() {
        let f1 = Frame::done(1);
        let f2 = Frame::done(2);

        let mut reader = FrameReader::new();
        reader.feed(&f1.encode());
        reader.feed(&f2.encode());

        let d1 = reader.next_frame().unwrap().unwrap();
        assert_eq!(d1.request_id, 1);
        let d2 = reader.next_frame().unwrap().unwrap();
        assert_eq!(d2.request_id, 2);
        assert!(reader.next_frame().unwrap().is_none());
    }

    #[test]
    fn test_unknown_message_type() {
        let mut buf = BytesMut::new();
        buf.put_u32(5); // body length
        buf.put_u8(0xFF); // unknown type
        buf.put_u32(1); // request_id
        assert!(Frame::decode(&buf).is_err());
    }

    #[test]
    fn test_encode_header() {
        let frame = Frame::done(42);
        let header = frame.encode_header();
        let encoded = frame.encode();
        assert_eq!(&header[..], &encoded[..FRAME_HEADER_SIZE]);
    }
}
