//! Zero-copy file → socket transfer via splice.
//!
//! On Linux with io_uring, this uses splice to move data from a disk file
//! to a TCP socket through a kernel pipe, bypassing userspace memory copies.
//!
//! On macOS or when splice is unavailable, falls back to read + write.

use std::path::Path;

use monoio::io::AsyncWriteRentExt;
use monoio::net::TcpStream;

use ruxio_protocol::frame::{MessageType, FRAME_HEADER_SIZE};

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
    let done = ruxio_protocol::frame::Frame::done(request_id);
    let (result, _) = stream.write_all(done.encode().to_vec()).await;
    result?;

    Ok(transferred)
}

/// Linux: use splice via raw fd for zero-copy file → socket.
#[cfg(target_os = "linux")]
async fn send_file_contents(
    file_path: &Path,
    file_size: u64,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    use std::os::unix::io::AsRawFd;

    // Open file with std (we just need the fd for sendfile)
    let file = std::fs::File::open(file_path)?;
    let file_fd = file.as_raw_fd();
    let socket_fd = stream.as_raw_fd();

    // Use sendfile(2) — simpler than splice, one syscall, no pipe needed
    let mut offset: libc::off_t = 0;
    let mut remaining = file_size as usize;
    let mut transferred: u64 = 0;

    while remaining > 0 {
        let n = unsafe { libc::sendfile(socket_fd, file_fd, &mut offset, remaining) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                // Socket buffer full — yield and retry
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
