use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;

use xxhash_rust::xxh3::Xxh3DefaultBuilder;

/// Key for page cache lookups.
///
/// Identifies a specific page within a remote file using Alluxio-style
/// `(file_id, page_index)` addressing. The page_index is computed as
/// `offset / page_size` by the caller.
///
/// `file_id` uses `Arc<str>` for zero-cost sharing: many PageKeys
/// reference pages of the same file, so the URI string is allocated
/// once and shared via refcount. Clone is ~3ns (atomic increment)
/// instead of ~50-100ns (string allocation + copy).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PageKey {
    pub file_id: Arc<str>,
    pub page_index: u64,
}

impl PageKey {
    /// Create a PageKey from a string reference (allocates a new Arc<str>).
    /// For multiple pages of the same file, prefer `PageKey::with_arc()`.
    pub fn new(file_id: &str, page_index: u64) -> Self {
        Self {
            file_id: Arc::from(file_id),
            page_index,
        }
    }

    /// Create a PageKey sharing an existing Arc<str> (refcount bump only).
    /// Use this in loops over page indices to avoid repeated string allocation.
    pub fn with_arc(file_id: Arc<str>, page_index: u64) -> Self {
        Self {
            file_id,
            page_index,
        }
    }

    /// URL-encode a file ID so it's filesystem-safe.
    pub fn url_safe_file_id(file_id: &str) -> String {
        let mut encoded = String::with_capacity(file_id.len());
        for b in file_id.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                    encoded.push(b as char);
                }
                _ => {
                    encoded.push_str(&format!("%{:02X}", b));
                }
            }
        }
        encoded
    }

    /// Hash bucket for the file_id (0..999) to distribute across directories.
    pub fn file_bucket(file_id: &str) -> u64 {
        let mut hasher = Xxh3DefaultBuilder.build_hasher();
        file_id.hash(&mut hasher);
        hasher.finish() % 1000
    }

    /// Returns the Alluxio-style path component: `{bucket}/{url_safe_file_id}/{page_index}`
    ///
    /// The caller prepends `{root}/{page_size}/` to form the full path.
    pub fn to_path_component(&self) -> String {
        let bucket = Self::file_bucket(&self.file_id);
        let safe_id = Self::url_safe_file_id(&self.file_id);
        format!("{bucket:03}/{safe_id}/{}", self.page_index)
    }
}

impl Hash for PageKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.hash(state);
        self.page_index.hash(state);
    }
}

impl std::fmt::Display for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:page{}", self.file_id, self.page_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_key_hash_deterministic() {
        let k1 = PageKey::new("gs://b/f.parquet", 0);
        let k2 = PageKey::new("gs://b/f.parquet", 0);
        assert_eq!(k1.to_path_component(), k2.to_path_component());
    }

    #[test]
    fn test_page_key_different_indices() {
        let k1 = PageKey::new("gs://b/f.parquet", 0);
        let k2 = PageKey::new("gs://b/f.parquet", 1);
        assert_ne!(k1.to_path_component(), k2.to_path_component());
    }

    #[test]
    fn test_url_safe_encoding() {
        let encoded = PageKey::url_safe_file_id("gs://bucket/path/file.parquet");
        assert_eq!(encoded, "gs%3A%2F%2Fbucket%2Fpath%2Ffile.parquet");
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains(':'));
    }

    #[test]
    fn test_path_component_format() {
        let key = PageKey::new("gs://bucket/data.parquet", 5);
        let component = key.to_path_component();
        let parts: Vec<&str> = component.splitn(3, '/').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].len(), 3);
        assert_eq!(parts[1], "gs%3A%2F%2Fbucket%2Fdata.parquet");
        assert_eq!(parts[2], "5");
    }

    #[test]
    fn test_file_bucket_range() {
        for i in 0..100 {
            let bucket = PageKey::file_bucket(&format!("gs://bucket/file{i}.parquet"));
            assert!(bucket < 1000);
        }
    }

    #[test]
    fn test_arc_sharing() {
        let file_id: Arc<str> = Arc::from("gs://bucket/file.parquet");
        let k1 = PageKey::with_arc(file_id.clone(), 0);
        let k2 = PageKey::with_arc(file_id.clone(), 1);
        // Same underlying allocation
        assert!(Arc::ptr_eq(&k1.file_id, &k2.file_id));
        // But different keys
        assert_ne!(k1, k2);
    }
}
