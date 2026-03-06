//! Zero-copy file → socket transfer via splice.
//!
//! On Linux with io_uring, this uses `IORING_OP_SPLICE` to move data
//! from a disk file to a TCP socket through a kernel pipe, completely
//! bypassing userspace memory copies.
//!
//! On macOS or when the splice feature is unavailable, falls back to
//! regular read + write (still async via monoio).

use std::path::Path;

#[cfg(target_os = "linux")]
use monoio::fs::File;
use monoio::io::AsyncWriteRentExt;
use monoio::net::TcpStream;

use ruxio_protocol::frame::{Frame, MessageType, FRAME_HEADER_SIZE};

/// Send a cached page file to a TCP socket with zero-copy where possible.
///
/// Protocol: writes a DataChunk frame header, then splices/sends the file
/// contents as the payload, then writes a Done frame.
///
/// On Linux with splice: file data goes through kernel pipe directly to
/// socket — no userspace buffer copy.
/// On macOS: falls back to read() + write() through a 256KB buffer.
pub async fn send_file_to_socket(
    file_path: &Path,
    file_size: u64,
    request_id: u32,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    // Write DataChunk frame header
    // Frame: [4B body_len][1B type][4B request_id] then payload follows
    let body_len = 5 + file_size as usize; // type(1) + request_id(4) + payload(file_size)
    let mut header = [0u8; FRAME_HEADER_SIZE];
    header[0..4].copy_from_slice(&(body_len as u32).to_be_bytes());
    header[4] = MessageType::DataChunk as u8;
    header[5..9].copy_from_slice(&request_id.to_be_bytes());

    let (result, _) = stream.write_all(header.to_vec()).await;
    result?;

    // Transfer file contents to socket
    let transferred = send_file_contents(file_path, file_size, stream).await?;

    // Write Done frame
    let done = Frame::done(request_id);
    let (result, _) = stream.write_all(done.encode().to_vec()).await;
    result?;

    Ok(transferred)
}

/// Send file contents to socket — splice on Linux, read+write on macOS.
#[cfg(target_os = "linux")]
async fn send_file_contents(
    file_path: &Path,
    file_size: u64,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    use monoio::io::splice::{SpliceDestination, SpliceSource};

    let mut file = File::open(file_path).await?;
    let (mut pipe_read, mut pipe_write) = monoio::net::unix::new_pipe()?;

    let mut remaining = file_size;
    let mut transferred: u64 = 0;
    // Use 256KB chunks for splice — balances syscall overhead vs latency
    const SPLICE_CHUNK: u32 = 256 * 1024;

    while remaining > 0 {
        let chunk = std::cmp::min(remaining, SPLICE_CHUNK as u64) as u32;
        let spliced_in = file.splice_to_pipe(&mut pipe_write, chunk).await?;
        if spliced_in == 0 {
            break;
        }
        let mut to_write = spliced_in;
        while to_write > 0 {
            let spliced_out = stream.splice_from_pipe(&mut pipe_read, to_write).await?;
            to_write -= spliced_out;
        }
        remaining -= spliced_in as u64;
        transferred += spliced_in as u64;
    }

    Ok(transferred)
}

/// Fallback for macOS: read file into buffer, write to socket.
#[cfg(not(target_os = "linux"))]
async fn send_file_contents(
    file_path: &Path,
    _file_size: u64,
    stream: &mut TcpStream,
) -> std::io::Result<u64> {
    // On macOS, just read the whole file and write it
    let data = std::fs::read(file_path)?;
    let len = data.len() as u64;
    let (result, _) = stream.write_all(data).await;
    result?;
    Ok(len)
}
