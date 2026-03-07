//! Zero-copy data transfer utilities.
//!
//! - `send_file_to_socket`: file → socket via sendfile (Linux) or read+write (macOS)
//! - `send_bytes_to_socket`: Bytes → socket via IoBuf (zero userspace copies)
//! - `IoBytes`: IoBuf adapter for `bytes::Bytes` — lets monoio read directly
//!   from Bytes' backing memory without .to_vec() copies.

use std::path::Path;

use bytes::Bytes;
use monoio::buf::IoBuf;
use monoio::io::AsyncWriteRentExt;
use monoio::net::TcpStream;

use ruxio_protocol::frame::{Frame, MessageType, FRAME_HEADER_SIZE};

// ── IoBuf adapter for Bytes ──────────────────────────────────────────

/// Zero-copy IoBuf wrapper for `bytes::Bytes`.
///
/// monoio's write_all requires `T: IoBuf` (owned buffer). `Bytes` doesn't
/// implement it natively, but it satisfies all requirements:
/// - Stable pointer (Arc-backed, won't move)
/// - 'static lifetime (reference-counted ownership)
/// - Unpin (no self-referential data)
///
/// This eliminates the .to_vec() copy when sending Bytes to a socket.
/// The kernel reads directly from Bytes' backing memory.
pub struct IoBytes(Bytes);

impl IoBytes {
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

unsafe impl IoBuf for IoBytes {
    fn read_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.0.len()
    }
}

// ── Zero-copy send helpers ───────────────────────────────────────────

/// Send raw Bytes as a DataChunk frame + Done frame — zero userspace copies.
///
/// Data path:
///   Bytes (Arc-backed) → IoBytes adapter → monoio write_all
///   → kernel reads directly from Bytes' memory → socket
///
/// No .to_vec(), no memcpy. The kernel accesses the same memory
/// that Bytes points to.
pub async fn send_bytes_to_socket(
    data: Bytes,
    request_id: u32,
    stream: &mut TcpStream,
) -> std::io::Result<()> {
    // Build and send the 9-byte DataChunk header
    let body_len = 5 + data.len() as u32; // type(1) + request_id(4) + payload
    let mut header = [0u8; FRAME_HEADER_SIZE];
    header[0..4].copy_from_slice(&body_len.to_be_bytes());
    header[4] = MessageType::DataChunk as u8;
    header[5..9].copy_from_slice(&request_id.to_be_bytes());

    let (result, _) = stream.write_all(header.to_vec()).await;
    result?;

    // Send payload — ZERO copies (IoBytes lets kernel read from Bytes' backing memory)
    if !data.is_empty() {
        let (result, _) = stream.write_all(IoBytes::new(data)).await;
        result?;
    }

    // Send Done frame
    let done = Frame::done(request_id);
    let (result, _) = stream.write_all(done.encode().to_vec()).await;
    result?;

    Ok(())
}

/// Send a DataChunk frame with Bytes payload (no Done frame).
/// Used for streaming where multiple chunks are sent before a final Done.
pub async fn send_data_chunk(
    data: Bytes,
    request_id: u32,
    stream: &mut TcpStream,
) -> std::io::Result<()> {
    let body_len = 5 + data.len() as u32;
    let mut header = [0u8; FRAME_HEADER_SIZE];
    header[0..4].copy_from_slice(&body_len.to_be_bytes());
    header[4] = MessageType::DataChunk as u8;
    header[5..9].copy_from_slice(&request_id.to_be_bytes());

    let (result, _) = stream.write_all(header.to_vec()).await;
    result?;

    if !data.is_empty() {
        let (result, _) = stream.write_all(IoBytes::new(data)).await;
        result?;
    }

    Ok(())
}

/// Send a cached page file to a TCP socket with zero-copy where possible.
///
/// Writes a DataChunk frame header, then sends the file contents, then Done.
pub async fn send_file_to_socket(
    file_path: &Path,
    file_size: u64,
    request_id: u32,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    // Write DataChunk frame header
    let body_len = 5 + file_size as usize; // type(1) + request_id(4) + payload
    let mut header = [0u8; FRAME_HEADER_SIZE];
    header[0..4].copy_from_slice(&(body_len as u32).to_be_bytes());
    header[4] = MessageType::DataChunk as u8;
    header[5..9].copy_from_slice(&request_id.to_be_bytes());

    let (result, _) = stream.write_all(header.to_vec()).await;
    result?;

    // Transfer file contents
    let transferred = send_file_contents(file_path, file_size, stream).await?;

    // Write Done frame
    let done = Frame::done(request_id);
    let (result, _) = stream.write_all(done.encode().to_vec()).await;
    result?;

    Ok(transferred)
}

/// Linux: use sendfile(2) for zero-copy file → socket.
#[cfg(target_os = "linux")]
async fn send_file_contents(
    file_path: &Path,
    file_size: u64,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    use std::os::unix::io::AsRawFd;

    let file = std::fs::File::open(file_path)?;
    let file_fd = file.as_raw_fd();
    let socket_fd = stream.as_raw_fd();

    let mut offset: libc::off_t = 0;
    let mut remaining = file_size as usize;
    let mut transferred: u64 = 0;

    while remaining > 0 {
        let n = unsafe { libc::sendfile(socket_fd, file_fd, &mut offset, remaining) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                monoio::time::sleep(std::time::Duration::from_micros(1)).await;
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            break;
        }
        remaining -= n as usize;
        transferred += n as u64;
    }

    Ok(transferred)
}

/// macOS fallback: read file into buffer, write to socket.
#[cfg(not(target_os = "linux"))]
async fn send_file_contents(
    file_path: &Path,
    _file_size: u64,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    let data = std::fs::read(file_path)?;
    let len = data.len() as u64;
    let (result, _) = stream.write_all(data).await;
    result?;
    Ok(len)
}
