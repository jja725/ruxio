use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use snafu::{IntoError, ResultExt};

use crate::error::{
    self, ConnectionSnafu, GcsSnafu, HttpParseSnafu, ResponseValidationSnafu, SerializationSnafu,
    TlsSnafu,
};
use crate::retry::{GcsError, RetryPolicy};

use ruxio_common::metrics::{GCS_RETRIES, GCS_TIMEOUTS};

/// Minimum buffer capacity for HTTP response reads.
const MIN_RESPONSE_BUFFER_BYTES: usize = 4_096;

/// Extra headroom added to the response buffer beyond the size hint.
const RESPONSE_BUFFER_HEADROOM: usize = 1_024;

/// Read chunk size for streaming HTTP response body.
const HTTP_READ_CHUNK_BYTES: usize = 64 * 1024;

/// Object metadata from GCS.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub name: String,
    pub size: u64,
    pub etag: Option<String>,
    pub content_type: Option<String>,
}

/// GCS JSON API metadata response.
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct GcsObjectMeta {
    name: Option<String>,
    size: Option<String>,
    etag: Option<String>,
    #[serde(rename = "contentType")]
    content_type: Option<String>,
}

/// Async GCS client built on monoio + rustls.
///
/// Stack: monoio TcpStream → TLS (rustls) → HTTP/1.1 → GCS JSON API.
/// All I/O is non-blocking. No tokio dependency.
///
/// Data is received directly into BytesMut and frozen into Bytes —
/// callers get zero-copy views via Bytes::slice() for page splitting.
pub struct GcsClient {
    bucket: String,
    access_token: Option<String>,
    tls_config: Arc<rustls::ClientConfig>,
    /// Connection pool: (TLS stream, last used timestamp).
    pool: RefCell<VecDeque<(monoio_rustls::ClientTlsStream<TcpStream>, Instant)>>,
    max_idle: usize,
    idle_timeout: Duration,
}

impl GcsClient {
    pub fn new(bucket: impl Into<String>) -> Self {
        let access_token = std::env::var("GCS_ACCESS_TOKEN").ok();

        // Build TLS config with webpki root certificates
        let root_store =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Self {
            bucket: bucket.into(),
            access_token,
            tls_config: Arc::new(tls_config),
            pool: RefCell::new(VecDeque::new()),
            max_idle: 4,
            idle_timeout: Duration::from_secs(60),
        }
    }

    pub fn with_access_token(mut self, token: impl Into<String>) -> Self {
        self.access_token = Some(token.into());
        self
    }

    /// Configure connection pool size and idle timeout.
    pub fn with_pool_config(mut self, max_idle: usize, idle_timeout_secs: u64) -> Self {
        self.max_idle = max_idle;
        self.idle_timeout = Duration::from_secs(idle_timeout_secs);
        self
    }

    /// Fetch a byte range from a GCS object.
    ///
    /// Data flows: GCS → TLS decrypt → BytesMut → freeze → Bytes
    /// The returned Bytes can be sliced for page splitting without copies.
    pub async fn get_range(&self, path: &str, range: Range<u64>) -> error::Result<Bytes> {
        let encoded_path = urlencoded(path);
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);
        let expected_len = (range.end - range.start) as usize;

        let request = format!(
            "GET /storage/v1/b/{}/o/{}?alt=media HTTP/1.1\r\n\
             Host: storage.googleapis.com\r\n\
             Range: {}\r\n\
             {}\
             Connection: keep-alive\r\n\
             \r\n",
            self.bucket,
            encoded_path,
            range_header,
            self.auth_header(),
        );

        let mut stream = self.get_connection().await?;

        // Send request
        let (result, _) = stream.write_all(request.into_bytes()).await;
        result.context(ConnectionSnafu {
            detail: "failed to send GCS request",
        })?;

        // Read response into BytesMut — single allocation, no intermediate copies
        let (status, body) = read_http_response(&mut stream, expected_len).await?;

        if !(200..300).contains(&status) {
            let body_str = String::from_utf8_lossy(&body);
            return Err(GcsSnafu.into_error(GcsError::from_status(status, &body_str)));
        }

        // Validate response body length matches expected range size
        if body.len() != expected_len {
            return Err(ResponseValidationSnafu {
                detail: format!(
                    "GCS response body length mismatch: expected {expected_len}, got {}",
                    body.len()
                ),
            }
            .build());
        }

        // Return connection to pool for reuse
        self.return_connection(stream);

        Ok(body)
    }

    /// HEAD-style request to get object metadata (size, etag).
    pub async fn head(&self, path: &str) -> error::Result<ObjectMeta> {
        let encoded_path = urlencoded(path);

        // Use the JSON metadata endpoint (not alt=media) to get size/etag
        let request = format!(
            "GET /storage/v1/b/{}/o/{} HTTP/1.1\r\n\
             Host: storage.googleapis.com\r\n\
             {}\
             Connection: keep-alive\r\n\
             \r\n",
            self.bucket,
            encoded_path,
            self.auth_header(),
        );

        let mut stream = self.get_connection().await?;

        let (result, _) = stream.write_all(request.into_bytes()).await;
        result.context(ConnectionSnafu {
            detail: "failed to send GCS head request",
        })?;

        let (status, body) = read_http_response(&mut stream, 0).await?;

        if !(200..300).contains(&status) {
            let body_str = String::from_utf8_lossy(&body);
            return Err(GcsSnafu.into_error(GcsError::from_status(status, &body_str)));
        }

        self.return_connection(stream);

        let meta: GcsObjectMeta = serde_json::from_slice(&body).context(SerializationSnafu)?;

        Ok(ObjectMeta {
            name: meta.name.unwrap_or_default(),
            size: meta
                .size
                .as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            etag: meta.etag,
            content_type: meta.content_type,
        })
    }

    /// List objects with a given prefix.
    pub async fn list(&self, prefix: &str) -> error::Result<Vec<ObjectMeta>> {
        let encoded_prefix = urlencoded(prefix);

        let request = format!(
            "GET /storage/v1/b/{}/o?prefix={} HTTP/1.1\r\n\
             Host: storage.googleapis.com\r\n\
             {}\
             Connection: keep-alive\r\n\
             \r\n",
            self.bucket,
            encoded_prefix,
            self.auth_header(),
        );

        let mut stream = self.get_connection().await?;

        let (result, _) = stream.write_all(request.into_bytes()).await;
        result.context(ConnectionSnafu {
            detail: "failed to send GCS list request",
        })?;

        let (status, body) = read_http_response(&mut stream, 0).await?;

        if !(200..300).contains(&status) {
            let body_str = String::from_utf8_lossy(&body);
            return Err(GcsSnafu.into_error(GcsError::from_status(status, &body_str)));
        }

        self.return_connection(stream);

        #[derive(serde::Deserialize)]
        struct ListResponse {
            items: Option<Vec<GcsObjectMeta>>,
        }

        let resp: ListResponse = serde_json::from_slice(&body).context(SerializationSnafu)?;

        Ok(resp
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|o| ObjectMeta {
                name: o.name.unwrap_or_default(),
                size: o.size.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0),
                etag: o.etag,
                content_type: o.content_type,
            })
            .collect())
    }

    // ── Connection pool ──────────────────────────────────────────────

    /// Get a TLS connection, reusing from pool if available.
    async fn get_connection(&self) -> error::Result<monoio_rustls::ClientTlsStream<TcpStream>> {
        // Try to reuse a pooled connection
        while let Some((stream, last_used)) = self.pool.borrow_mut().pop_front() {
            if last_used.elapsed() < self.idle_timeout {
                return Ok(stream);
            }
            // Connection too old, drop it
        }
        // No pooled connection available, create new
        self.connect_tls().await
    }

    /// Return a connection to the pool for reuse.
    fn return_connection(&self, stream: monoio_rustls::ClientTlsStream<TcpStream>) {
        let mut pool = self.pool.borrow_mut();
        if pool.len() < self.max_idle {
            pool.push_back((stream, Instant::now()));
        }
        // If pool is full, just drop the connection
    }

    // ── Retry wrappers ──────────────────────────────────────────────

    /// Fetch a byte range with retry and timeout.
    ///
    /// Only retries transient errors (429, 5xx, timeouts). Permanent errors
    /// (404, 403, etc.) fail immediately without retry.
    pub async fn get_range_with_retry(
        &self,
        path: &str,
        range: Range<u64>,
        policy: &RetryPolicy,
    ) -> error::Result<Bytes> {
        for attempt in 0..=policy.max_retries {
            if attempt > 0 {
                monoio::time::sleep(policy.delay_for_attempt(attempt - 1)).await;
            }
            match monoio::time::timeout(policy.timeout(), self.get_range(path, range.clone())).await
            {
                Ok(Ok(data)) => return Ok(data),
                Ok(Err(e)) => {
                    if !e.is_retryable() {
                        return Err(e);
                    }
                    GCS_RETRIES.inc();
                    if attempt == policy.max_retries {
                        return Err(e);
                    }
                    tracing::warn!(
                        "GCS get_range retry {}/{}: {e}",
                        attempt + 1,
                        policy.max_retries
                    );
                }
                Err(_) => {
                    GCS_TIMEOUTS.inc();
                    if attempt == policy.max_retries {
                        return Err(GcsSnafu.into_error(GcsError::Timeout {
                            duration: policy.timeout(),
                        }));
                    }
                    tracing::warn!(
                        "GCS get_range timeout, retry {}/{}",
                        attempt + 1,
                        policy.max_retries
                    );
                }
            }
        }
        // All retries exhausted (should not be reached due to loop logic above)
        Err(GcsSnafu.into_error(GcsError::Timeout {
            duration: policy.timeout(),
        }))
    }

    /// HEAD request with retry and timeout.
    ///
    /// Only retries transient errors. Permanent errors fail immediately.
    pub async fn head_with_retry(
        &self,
        path: &str,
        policy: &RetryPolicy,
    ) -> error::Result<ObjectMeta> {
        for attempt in 0..=policy.max_retries {
            if attempt > 0 {
                monoio::time::sleep(policy.delay_for_attempt(attempt - 1)).await;
            }
            match monoio::time::timeout(policy.timeout(), self.head(path)).await {
                Ok(Ok(meta)) => return Ok(meta),
                Ok(Err(e)) => {
                    if !e.is_retryable() {
                        return Err(e);
                    }
                    GCS_RETRIES.inc();
                    if attempt == policy.max_retries {
                        return Err(e);
                    }
                    tracing::warn!("GCS head retry {}/{}: {e}", attempt + 1, policy.max_retries);
                }
                Err(_) => {
                    GCS_TIMEOUTS.inc();
                    if attempt == policy.max_retries {
                        return Err(GcsSnafu.into_error(GcsError::Timeout {
                            duration: policy.timeout(),
                        }));
                    }
                    tracing::warn!(
                        "GCS head timeout, retry {}/{}",
                        attempt + 1,
                        policy.max_retries
                    );
                }
            }
        }
        Err(GcsSnafu.into_error(GcsError::Timeout {
            duration: policy.timeout(),
        }))
    }

    fn auth_header(&self) -> String {
        match &self.access_token {
            Some(token) => format!("Authorization: Bearer {token}\r\n"),
            None => String::new(),
        }
    }

    async fn connect_tls(&self) -> error::Result<monoio_rustls::ClientTlsStream<TcpStream>> {
        let tcp = TcpStream::connect("storage.googleapis.com:443")
            .await
            .context(ConnectionSnafu {
                detail: "failed to connect to storage.googleapis.com:443",
            })?;
        if let Err(e) = tcp.set_nodelay(true) {
            tracing::debug!("GCS connection set_nodelay failed: {e}");
        }

        let server_name = rustls::pki_types::ServerName::try_from("storage.googleapis.com")
            .map_err(|e| {
                TlsSnafu {
                    detail: format!("invalid server name: {e}"),
                }
                .build()
            })?
            .to_owned();

        let connector = monoio_rustls::TlsConnector::from(self.tls_config.clone());
        let tls_stream = connector.connect(server_name, tcp).await.map_err(|e| {
            ConnectionSnafu {
                detail: "TLS handshake failed",
            }
            .into_error(e.into())
        })?;

        Ok(tls_stream)
    }
}

/// Read an HTTP/1.1 response, returning (status_code, body).
///
/// Reads directly into BytesMut for zero-copy. The body is frozen
/// into Bytes so callers can slice it without copies.
async fn read_http_response<S: AsyncReadRent>(
    stream: &mut S,
    size_hint: usize,
) -> error::Result<(u16, Bytes)> {
    let mut buf = BytesMut::with_capacity(
        size_hint.max(MIN_RESPONSE_BUFFER_BYTES) + RESPONSE_BUFFER_HEADROOM,
    );
    let mut read_buf = vec![0u8; HTTP_READ_CHUNK_BYTES];

    // Read until we have the full headers
    let mut header_end = None;
    loop {
        let (result, returned_buf) = stream.read(read_buf).await;
        read_buf = returned_buf;
        let n = result.context(ConnectionSnafu {
            detail: "failed to read GCS response",
        })?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&read_buf[..n]);

        // Look for \r\n\r\n header terminator
        if header_end.is_none() {
            if let Some(pos) = find_header_end(&buf) {
                header_end = Some(pos);
                break;
            }
        }
    }

    let header_end = header_end.ok_or_else(|| {
        HttpParseSnafu {
            detail: "incomplete HTTP response headers",
        }
        .build()
    })?;
    let headers = std::str::from_utf8(&buf[..header_end]).map_err(|_| {
        HttpParseSnafu {
            detail: "invalid HTTP headers (not UTF-8)",
        }
        .build()
    })?;

    // Parse status code
    let status = parse_status_code(headers)?;

    // Parse Content-Length
    let content_length = parse_content_length(headers);

    // Body starts after \r\n\r\n
    let body_start = header_end + 4;
    let body_so_far = buf.len() - body_start;

    if let Some(cl) = content_length {
        // Known content length — read exactly that many bytes
        let remaining = cl.saturating_sub(body_so_far);
        if remaining > 0 {
            buf.reserve(remaining);
            let mut left = remaining;
            while left > 0 {
                let (result, returned_buf) = stream.read(read_buf).await;
                read_buf = returned_buf;
                let n = result.context(ConnectionSnafu {
                    detail: "failed to read GCS response body",
                })?;
                if n == 0 {
                    break;
                }
                let take = n.min(left);
                buf.extend_from_slice(&read_buf[..take]);
                left -= take;
            }
        }
    } else {
        // No content length — read until EOF (for metadata responses)
        loop {
            let (result, returned_buf) = stream.read(read_buf).await;
            read_buf = returned_buf;
            let n = result.context(ConnectionSnafu {
                detail: "failed to read GCS response body",
            })?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&read_buf[..n]);
        }
    }

    // Split off just the body and freeze — zero-copy from here on
    let _ = buf.split_to(body_start); // discard headers
    let body = buf.freeze();

    Ok((status, body))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_status_code(headers: &str) -> error::Result<u16> {
    // "HTTP/1.1 200 OK\r\n..."
    let status_line = headers.lines().next().ok_or_else(|| {
        HttpParseSnafu {
            detail: "empty HTTP response",
        }
        .build()
    })?;
    let parts: Vec<&str> = status_line.split_whitespace().collect();
    let code_str = parts.get(1).ok_or_else(|| {
        HttpParseSnafu {
            detail: "missing status code in HTTP response",
        }
        .build()
    })?;
    let code: u16 = code_str.parse().map_err(|_| {
        HttpParseSnafu {
            detail: format!("invalid status code: {code_str}"),
        }
        .build()
    })?;
    Ok(code)
}

fn parse_content_length(headers: &str) -> Option<usize> {
    for line in headers.lines() {
        if let Some(value) = line
            .strip_prefix("Content-Length: ")
            .or_else(|| line.strip_prefix("content-length: "))
        {
            return value.trim().parse().ok();
        }
    }
    None
}

/// URL-encode a path component.
fn urlencoded(s: &str) -> String {
    s.replace('%', "%25")
        .replace('/', "%2F")
        .replace(' ', "%20")
        .replace('?', "%3F")
        .replace('&', "%26")
        .replace('=', "%3D")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_encoding() {
        assert_eq!(urlencoded("path/to/file"), "path%2Fto%2Ffile");
        assert_eq!(urlencoded("file name"), "file%20name");
    }

    #[test]
    fn test_gcs_client_creation() {
        let client = GcsClient::new("my-bucket");
        assert_eq!(client.bucket, "my-bucket");
    }

    #[test]
    fn test_parse_status_code() {
        assert_eq!(parse_status_code("HTTP/1.1 200 OK\r\n").unwrap(), 200);
        assert_eq!(
            parse_status_code("HTTP/1.1 206 Partial Content\r\n").unwrap(),
            206
        );
        assert_eq!(
            parse_status_code("HTTP/1.1 404 Not Found\r\n").unwrap(),
            404
        );
    }

    #[test]
    fn test_parse_content_length() {
        assert_eq!(
            parse_content_length("Content-Length: 4194304\r\n"),
            Some(4194304)
        );
        assert_eq!(parse_content_length("X-Other: foo\r\n"), None);
    }

    #[test]
    fn test_find_header_end() {
        let header = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let pos = find_header_end(header).unwrap();
        // Verify the position points to \r\n\r\n
        assert_eq!(&header[pos..pos + 4], b"\r\n\r\n");
        assert_eq!(find_header_end(b"partial headers\r\n"), None);
    }
}
