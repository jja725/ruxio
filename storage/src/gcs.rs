use std::ops::Range;

use anyhow::Result;
use bytes::Bytes;

/// Object metadata from GCS.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub name: String,
    pub size: u64,
    pub etag: Option<String>,
    pub content_type: Option<String>,
}

/// GCS JSON API list response item.
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct GcsObject {
    name: String,
    size: Option<String>,
    etag: Option<String>,
    #[serde(rename = "contentType")]
    content_type: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct GcsListResponse {
    items: Option<Vec<GcsObject>>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// Async GCS client built on monoio.
///
/// Stack: monoio TcpStream (io_uring) → TLS (rustls) → HTTP/1.1 → GCS JSON API.
/// All I/O is non-blocking via io_uring. No tokio dependency.
///
/// Note: All existing Rust GCS clients (cloud-storage, google-cloud-storage,
/// object_store) require tokio. This is a minimal custom implementation.
pub struct GcsClient {
    bucket: String,
    /// OAuth2 access token (for now, loaded from env or metadata server).
    access_token: Option<String>,
}

impl GcsClient {
    pub fn new(bucket: impl Into<String>) -> Self {
        let access_token = std::env::var("GCS_ACCESS_TOKEN").ok();
        Self {
            bucket: bucket.into(),
            access_token,
        }
    }

    /// Set the access token for authentication.
    pub fn with_access_token(mut self, token: impl Into<String>) -> Self {
        self.access_token = Some(token.into());
        self
    }

    /// The GCS API base URL for object access.
    fn object_url(&self, path: &str) -> String {
        format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
            self.bucket,
            urlencoded(path)
        )
    }

    /// Fetch a byte range from a GCS object.
    ///
    /// Uses HTTP Range header for partial reads — ideal for fetching
    /// individual 4MB pages without downloading the entire file.
    pub async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let _url = self.object_url(path);
        let _range_header = format!("bytes={}-{}", range.start, range.end - 1);

        // TODO: Implement actual HTTP request over monoio TcpStream + TLS
        // For now, return placeholder to allow compilation and testing of
        // the rest of the architecture.
        anyhow::bail!(
            "GCS get_range not yet implemented (bucket={}, path={}, range={:?})",
            self.bucket,
            path,
            range
        )
    }

    /// Fetch the Parquet footer (last N bytes of the file).
    ///
    /// Parquet files store their metadata at the end. We first need the file
    /// size (via HEAD), then fetch the footer using a suffix range read.
    pub async fn get_footer(&self, path: &str, footer_size: u64) -> Result<Bytes> {
        let meta = self.head(path).await?;
        let start = meta.size.saturating_sub(footer_size);
        self.get_range(path, start..meta.size).await
    }

    /// HEAD request to get object metadata without downloading content.
    pub async fn head(&self, path: &str) -> Result<ObjectMeta> {
        let _url = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
            self.bucket,
            urlencoded(path)
        );

        // TODO: Implement actual HTTP HEAD over monoio TcpStream + TLS
        anyhow::bail!(
            "GCS head not yet implemented (bucket={}, path={})",
            self.bucket,
            path
        )
    }

    /// List objects with a given prefix.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let _url = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o?prefix={}",
            self.bucket,
            urlencoded(prefix)
        );

        // TODO: Implement actual HTTP GET over monoio TcpStream + TLS
        anyhow::bail!(
            "GCS list not yet implemented (bucket={}, prefix={})",
            self.bucket,
            prefix
        )
    }
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
}
