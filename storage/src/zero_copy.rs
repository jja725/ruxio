//! Zero-copy data transfer utilities.
//!
//! - `send_file_to_socket`: full file → socket via sendfile (Linux) or read+write (macOS)
//! - `send_file_range_to_socket`: partial file → socket via sendfile with offset
//! - `send_bytes_to_socket`: Bytes → socket via IoBuf (zero userspace copies)
//! - `IoBytes`: IoBuf adapter for `bytes::Bytes`

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

// ── Frame header helper ──────────────────────────────────────────────

fn data_chunk_header(request_id: u32, payload_len: usize) -> [u8; FRAME_HEADER_SIZE] {
    let body_len = 5 + payload_len as u32; // type(1) + request_id(4) + payload
    let mut header = [0u8; FRAME_HEADER_SIZE];
    header[0..4].copy_from_slice(&body_len.to_be_bytes());
    header[4] = MessageType::DataChunk as u8;
    header[5..9].copy_from_slice(&request_id.to_be_bytes());
    header
}

// ── Zero-copy send: Bytes → socket ───────────────────────────────────

/// Send raw Bytes as a DataChunk frame + Done frame — zero userspace copies.
pub async fn send_bytes_to_socket(
    data: Bytes,
    request_id: u32,
    stream: &mut TcpStream,
) -> std::io::Result<()> {
    let header = data_chunk_header(request_id, data.len());
    let (result, _) = stream.write_all(header.to_vec()).await;
    result?;

    if !data.is_empty() {
        let (result, _) = stream.write_all(IoBytes::new(data)).await;
        result?;
    }

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
    let header = data_chunk_header(request_id, data.len());
    let (result, _) = stream.write_all(header.to_vec()).await;
    result?;

    if !data.is_empty() {
        let (result, _) = stream.write_all(IoBytes::new(data)).await;
        result?;
    }

    Ok(())
}

// ── Zero-copy send: file → socket (full file) ────────────────────────

/// Send a cached page file to a TCP socket with zero-copy where possible.
/// Sends the entire file as a DataChunk frame + Done.
pub async fn send_file_to_socket(
    file_path: &Path,
    file_size: u64,
    request_id: u32,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    send_file_range_to_socket(file_path, 0, file_size, request_id, stream).await
}

// ── Zero-copy send: file range → socket (partial page) ───────────────

/// A slice of a page file to send via sendfile.
pub struct FileSlice<'a> {
    pub path: &'a Path,
    pub offset: u64,
    pub length: u64,
}

/// Send a byte range from a file to a TCP socket with zero-copy.
///
/// Uses sendfile(2) with offset on Linux — the kernel reads the specified
/// slice directly from the file's page cache into the socket buffer.
/// No userspace buffer, no memcpy.
pub async fn send_file_range_to_socket(
    file_path: &Path,
    file_offset: u64,
    length: u64,
    request_id: u32,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    // Write DataChunk frame header
    let header = data_chunk_header(request_id, length as usize);
    let (result, _) = stream.write_all(header.to_vec()).await;
    result?;

    // Transfer file range
    let transferred = send_file_range_contents(file_path, file_offset, length, stream).await?;

    // Write Done frame
    let done = Frame::done(request_id);
    let (result, _) = stream.write_all(done.encode().to_vec()).await;
    result?;

    Ok(transferred)
}

/// Send multiple file slices as a single DataChunk frame + Done.
///
/// Writes one DataChunk header with total_length, then sendfiles each slice
/// in order. The TCP stream concatenates them on the wire — the client
/// receives one contiguous DataChunk.
///
/// Example: request spans 3 pages:
///   slices = [
///     (page_0, offset=3MB, len=1MB),  ← tail of page 0
///     (page_1, offset=0,   len=4MB),  ← full page 1
///     (page_2, offset=0,   len=1MB),  ← head of page 2
///   ]
///   → 1 DataChunk header (6MB) + 3 sendfile calls = zero userspace copies
pub async fn send_file_slices_to_socket(
    slices: &[FileSlice<'_>],
    request_id: u32,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    let total_length: u64 = slices.iter().map(|s| s.length).sum();

    // Write single DataChunk header for the combined payload
    let header = data_chunk_header(request_id, total_length as usize);
    let (result, _) = stream.write_all(header.to_vec()).await;
    result?;

    // Sendfile each slice — kernel concatenates on the wire
    let mut total_transferred = 0u64;
    for slice in slices {
        let transferred =
            send_file_range_contents(slice.path, slice.offset, slice.length, stream).await?;
        total_transferred += transferred;
    }

    // Done frame
    let done = Frame::done(request_id);
    let (result, _) = stream.write_all(done.encode().to_vec()).await;
    result?;

    Ok(total_transferred)
}

/// Linux: use sendfile(2) with offset for zero-copy file range → socket.
#[cfg(target_os = "linux")]
pub async fn send_file_range_contents(
    file_path: &Path,
    file_offset: u64,
    length: u64,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    use std::os::unix::io::AsRawFd;

    let file = std::fs::File::open(file_path)?;
    let file_fd = file.as_raw_fd();
    let socket_fd = stream.as_raw_fd();

    let mut offset = file_offset as libc::off_t;
    let mut remaining = length as usize;
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

/// macOS fallback: read file range into buffer, write to socket.
#[cfg(not(target_os = "linux"))]
pub async fn send_file_range_contents(
    file_path: &Path,
    file_offset: u64,
    length: u64,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(file_path)?;
    file.seek(SeekFrom::Start(file_offset))?;
    let mut buf = vec![0u8; length as usize];
    file.read_exact(&mut buf)?;
    let len = buf.len() as u64;
    let (result, _) = stream.write_all(buf).await;
    result?;
    Ok(len)
}
