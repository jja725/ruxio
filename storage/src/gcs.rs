use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

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
        }
    }

    pub fn with_access_token(mut self, token: impl Into<String>) -> Self {
        self.access_token = Some(token.into());
        self
    }

    /// Fetch a byte range from a GCS object.
    ///
    /// Data flows: GCS → TLS decrypt → BytesMut → freeze → Bytes
    /// The returned Bytes can be sliced for page splitting without copies.
    pub async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let encoded_path = urlencoded(path);
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);
        let expected_len = (range.end - range.start) as usize;

        let request = format!(
            "GET /storage/v1/b/{}/o/{}?alt=media HTTP/1.1\r\n\
             Host: storage.googleapis.com\r\n\
             Range: {}\r\n\
             {}\
             Connection: close\r\n\
             \r\n",
            self.bucket,
            encoded_path,
            range_header,
            self.auth_header(),
        );

        let mut stream = self.connect_tls().await?;

        // Send request
        let (result, _) = stream.write_all(request.into_bytes()).await;
        result.context("failed to send GCS request")?;

        // Read response into BytesMut — single allocation, no intermediate copies
        let (status, body) = read_http_response(&mut stream, expected_len).await?;

        if status < 200 || status >= 300 {
            let body_str = String::from_utf8_lossy(&body);
            anyhow::bail!("GCS returned HTTP {status}: {body_str}");
        }

        Ok(body)
    }

    /// HEAD-style request to get object metadata (size, etag).
    pub async fn head(&self, path: &str) -> Result<ObjectMeta> {
        let encoded_path = urlencoded(path);

        // Use the JSON metadata endpoint (not alt=media) to get size/etag
        let request = format!(
            "GET /storage/v1/b/{}/o/{} HTTP/1.1\r\n\
             Host: storage.googleapis.com\r\n\
             {}\
             Connection: close\r\n\
             \r\n",
            self.bucket,
            encoded_path,
            self.auth_header(),
        );

        let mut stream = self.connect_tls().await?;

        let (result, _) = stream.write_all(request.into_bytes()).await;
        result.context("failed to send GCS head request")?;

        let (status, body) = read_http_response(&mut stream, 0).await?;

        if status < 200 || status >= 300 {
            let body_str = String::from_utf8_lossy(&body);
            anyhow::bail!("GCS metadata returned HTTP {status}: {body_str}");
        }

        let meta: GcsObjectMeta =
            serde_json::from_slice(&body).context("failed to parse GCS metadata response")?;

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
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let encoded_prefix = urlencoded(prefix);

        let request = format!(
            "GET /storage/v1/b/{}/o?prefix={} HTTP/1.1\r\n\
             Host: storage.googleapis.com\r\n\
             {}\
             Connection: close\r\n\
             \r\n",
            self.bucket,
            encoded_prefix,
            self.auth_header(),
        );

        let mut stream = self.connect_tls().await?;

        let (result, _) = stream.write_all(request.into_bytes()).await;
        result.context("failed to send GCS list request")?;

        let (status, body) = read_http_response(&mut stream, 0).await?;

        if status < 200 || status >= 300 {
            let body_str = String::from_utf8_lossy(&body);
            anyhow::bail!("GCS list returned HTTP {status}: {body_str}");
        }

        #[derive(serde::Deserialize)]
        struct ListResponse {
            items: Option<Vec<GcsObjectMeta>>,
        }

        let resp: ListResponse =
            serde_json::from_slice(&body).context("failed to parse GCS list response")?;

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

    fn auth_header(&self) -> String {
        match &self.access_token {
            Some(token) => format!("Authorization: Bearer {token}\r\n"),
            None => String::new(),
        }
    }

    async fn connect_tls(&self) -> Result<monoio_rustls::ClientTlsStream<TcpStream>> {
        let tcp = TcpStream::connect("storage.googleapis.com:443")
            .await
            .context("failed to connect to storage.googleapis.com:443")?;
        let _ = tcp.set_nodelay(true);

        let server_name = rustls::pki_types::ServerName::try_from("storage.googleapis.com")
            .context("invalid server name")?
            .to_owned();

        let connector = monoio_rustls::TlsConnector::from(self.tls_config.clone());
        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .context("TLS handshake failed")?;

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
) -> Result<(u16, Bytes)> {
    let mut buf = BytesMut::with_capacity(size_hint.max(4096) + 1024);
    let mut read_buf = vec![0u8; 64 * 1024];

    // Read until we have the full headers
    let mut header_end = None;
    loop {
        let (result, returned_buf) = stream.read(read_buf).await;
        read_buf = returned_buf;
        let n = result.context("failed to read GCS response")?;
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

    let header_end = header_end.context("incomplete HTTP response headers")?;
    let headers = std::str::from_utf8(&buf[..header_end]).context("invalid HTTP headers")?;

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
                let n = result.context("failed to read GCS response body")?;
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
            let n = result.context("failed to read GCS response body")?;
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

fn parse_status_code(headers: &str) -> Result<u16> {
    // "HTTP/1.1 200 OK\r\n..."
    let status_line = headers.lines().next().context("empty HTTP response")?;
    let parts: Vec<&str> = status_line.split_whitespace().collect();
    let code: u16 = parts
        .get(1)
        .context("missing status code")?
        .parse()
        .context("invalid status code")?;
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
