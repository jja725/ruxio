//! HTTP/1.1 data plane with zero-copy sendfile.
//!
//! Endpoints:
//! - `GET /read?uri=...&offset=...&length=...` — read cached byte range

use std::rc::Rc;

use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use ruxio_protocol::messages::ReadRangeRequest;
use ruxio_storage::zero_copy::{self, IoBytes};

use crate::data::ThreadContext;

// ── HTTP request parsing ─────────────────────────────────────────────

struct HttpRequest {
    path: String,
    query_params: Vec<(String, String)>,
    keep_alive: bool,
}

impl HttpRequest {
    fn query(&self, key: &str) -> Option<&str> {
        self.query_params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// Parse a minimal HTTP/1.1 request from raw header bytes.
/// Returns None if the request is malformed.
fn parse_request(header_bytes: &[u8]) -> Option<HttpRequest> {
    let header_str = std::str::from_utf8(header_bytes).ok()?;
    let mut lines = header_str.lines();

    // Request line: "GET /path?query HTTP/1.1"
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    if method != "GET" {
        return None;
    }
    let raw_path = parts.next()?;

    // Split path and query string
    let (path, query_string) = if let Some(idx) = raw_path.find('?') {
        (&raw_path[..idx], Some(&raw_path[idx + 1..]))
    } else {
        (raw_path, None)
    };

    // Parse query params
    let query_params = if let Some(qs) = query_string {
        qs.split('&')
            .filter_map(|pair| {
                let mut kv = pair.splitn(2, '=');
                let k = kv.next()?;
                let v = kv.next().unwrap_or("");
                Some((url_decode(k), url_decode(v)))
            })
            .collect()
    } else {
        Vec::new()
    };

    // Check Connection header (default keep-alive for HTTP/1.1)
    let mut keep_alive = true;
    for line in lines {
        if let Some(val) = line.strip_prefix("Connection: ") {
            if val.eq_ignore_ascii_case("close") {
                keep_alive = false;
            }
        } else if let Some(val) = line.strip_prefix("connection: ") {
            if val.eq_ignore_ascii_case("close") {
                keep_alive = false;
            }
        }
    }

    Some(HttpRequest {
        path: path.to_string(),
        query_params,
        keep_alive,
    })
}

/// Minimal percent-decoding for query param values.
fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        match b {
            b'%' => {
                let hi = chars.next().unwrap_or(b'0');
                let lo = chars.next().unwrap_or(b'0');
                let byte = hex_val(hi) << 4 | hex_val(lo);
                result.push(byte as char);
            }
            b'+' => result.push(' '),
            _ => result.push(b as char),
        }
    }
    result
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

// ── HTTP response helpers ────────────────────────────────────────────

async fn write_response_headers(
    stream: &mut TcpStream,
    status: u16,
    status_text: &str,
    content_type: &str,
    content_length: u64,
    keep_alive: bool,
) -> std::io::Result<()> {
    let conn = if keep_alive { "keep-alive" } else { "close" };
    let headers = format!(
        "HTTP/1.1 {status} {status_text}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {content_length}\r\n\
         Connection: {conn}\r\n\
         \r\n"
    );
    let (result, _) = stream.write_all(headers.into_bytes()).await;
    result.map(|_| ())
}

async fn write_error(
    stream: &mut TcpStream,
    status: u16,
    status_text: &str,
    message: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    let body = format!("{{\"error\":\"{message}\"}}");
    write_response_headers(
        stream,
        status,
        status_text,
        "application/json",
        body.len() as u64,
        keep_alive,
    )
    .await?;
    let (result, _) = stream.write_all(body.into_bytes()).await;
    result.map(|_| ())
}

// ── Connection handler ───────────────────────────────────────────────

pub async fn serve_http_connection(stream: TcpStream, ctx: Rc<ThreadContext>) {
    let mut stream = stream;
    let _ = stream.set_nodelay(true);
    let mut buf = vec![0u8; 8192];
    let mut filled = 0usize;

    loop {
        // Read until we have a complete header block (\r\n\r\n)
        let header_end;
        loop {
            if filled >= buf.len() {
                // Header too large
                let _ = write_error(
                    &mut stream,
                    431,
                    "Request Header Fields Too Large",
                    "header too large",
                    false,
                )
                .await;
                return;
            }
            let read_buf = buf.split_off(filled);
            let (result, returned_buf) = stream.read(read_buf).await;
            let n = match result {
                Ok(0) => return,
                Ok(n) => n,
                Err(_) => return,
            };
            // Reassemble buffer
            buf.extend_from_slice(&returned_buf[..n]);
            filled += n;

            if let Some(pos) = find_header_end(&buf[..filled]) {
                header_end = pos;
                break;
            }
        }

        let req = match parse_request(&buf[..header_end]) {
            Some(r) => r,
            None => {
                let _ =
                    write_error(&mut stream, 400, "Bad Request", "malformed request", false).await;
                return;
            }
        };

        let keep_alive = req.keep_alive;

        match req.path.as_str() {
            "/read" => {
                handle_read(&mut stream, &req, &ctx).await;
            }
            _ => {
                let _ = write_error(&mut stream, 404, "Not Found", "not found", keep_alive).await;
            }
        }

        // Shift remaining bytes to front for next request (keep-alive)
        let consumed = header_end + 4; // include \r\n\r\n
        let remaining = filled - consumed;
        if remaining > 0 {
            buf.copy_within(consumed..filled, 0);
        }
        filled = remaining;

        if !keep_alive {
            return;
        }
    }
}

/// Find the position of `\r\n\r\n` in the buffer.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

// ── Endpoint handlers ────────────────────────────────────────────────

async fn handle_read(stream: &mut TcpStream, req: &HttpRequest, ctx: &Rc<ThreadContext>) {
    let keep_alive = req.keep_alive;

    let uri = match req.query("uri") {
        Some(u) => u.to_string(),
        None => {
            let _ = write_error(stream, 400, "Bad Request", "missing uri param", keep_alive).await;
            return;
        }
    };
    let offset: u64 = req
        .query("offset")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let length: u64 = match req.query("length").and_then(|s| s.parse().ok()) {
        Some(l) => l,
        None => {
            let _ = write_error(
                stream,
                400,
                "Bad Request",
                "missing length param",
                keep_alive,
            )
            .await;
            return;
        }
    };

    let page_size = ctx.cache_manager.borrow().page_size();
    let first_page = offset / page_size;
    let last_page = (offset + length - 1) / page_size;

    // Try zero-copy path: collect file slices for all pages
    let mut slices: Vec<(std::path::PathBuf, u64, u64)> = Vec::new();
    let mut all_cached = true;

    for page_idx in first_page..=last_page {
        let file_info = ctx.cache_manager.borrow_mut().get_page_file(&uri, page_idx);
        if let Some((file_path, file_size)) = file_info {
            let page_start = page_idx * page_size;
            let page_end = page_start + file_size;
            let slice_start = offset.max(page_start) - page_start;
            let slice_end = (offset + length).min(page_end) - page_start;
            slices.push((file_path, slice_start, slice_end - slice_start));
        } else {
            all_cached = false;
            break;
        }
    }

    if all_cached && !slices.is_empty() {
        // Zero-copy path: HTTP headers, then sendfile each slice
        let total_length: u64 = slices.iter().map(|(_, _, len)| len).sum();
        if write_response_headers(
            stream,
            200,
            "OK",
            "application/octet-stream",
            total_length,
            keep_alive,
        )
        .await
        .is_err()
        {
            return;
        }

        #[cfg(target_os = "linux")]
        set_tcp_cork(stream, true);

        for (path, file_offset, len) in &slices {
            if zero_copy::send_file_range_contents(path, *file_offset, *len, stream)
                .await
                .is_err()
            {
                return;
            }
        }

        #[cfg(target_os = "linux")]
        set_tcp_cork(stream, false);

        return;
    }

    // Buffered fallback: fetch via cache manager
    let read_req = ReadRangeRequest {
        uri,
        offset,
        length,
    };
    match crate::data::read_range(ctx, &read_req).await {
        Ok(data) => {
            if write_response_headers(
                stream,
                200,
                "OK",
                "application/octet-stream",
                data.len() as u64,
                keep_alive,
            )
            .await
            .is_err()
            {
                return;
            }
            if !data.is_empty() {
                let (result, _) = stream.write_all(IoBytes::new(data)).await;
                if let Err(e) = result {
                    tracing::debug!("HTTP write error: {e}");
                }
            }
        }
        Err(e) => {
            let _ = write_error(
                stream,
                500,
                "Internal Server Error",
                &e.to_string(),
                keep_alive,
            )
            .await;
        }
    }
}

// ── TCP_CORK for coalescing headers + sendfile ───────────────────────

#[cfg(target_os = "linux")]
fn set_tcp_cork(stream: &TcpStream, cork: bool) {
    use std::os::unix::io::AsRawFd;
    let val: libc::c_int = if cork { 1 } else { 0 };
    unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_CORK,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
