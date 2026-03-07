use std::hash::{BuildHasher, Hash, Hasher};
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

/// Key for page cache lookups.
///
/// Identifies a specific page within a remote file using Alluxio-style
/// `(file_id, page_index)` addressing. The page_index is computed as
/// `offset / page_size` by the caller.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PageKey {
    pub file_id: String,
    pub page_index: u64,
}

impl PageKey {
    pub fn new(file_id: impl Into<String>, page_index: u64) -> Self {
        Self {
            file_id: file_id.into(),
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
        // No slashes or colons in the encoded string
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains(':'));
    }

    #[test]
    fn test_path_component_format() {
        let key = PageKey::new("gs://bucket/data.parquet", 5);
        let component = key.to_path_component();
        // Should be {bucket:03}/{url_safe_id}/{page_index}
        let parts: Vec<&str> = component.splitn(3, '/').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].len(), 3); // 3-digit bucket
        assert_eq!(parts[1], "gs%3A%2F%2Fbucket%2Fdata.parquet");
        assert_eq!(parts[2], "5");
    }

    #[test]
    fn test_file_bucket_range() {
        // Bucket should always be 0..999
        for i in 0..100 {
            let bucket = PageKey::file_bucket(&format!("gs://bucket/file{i}.parquet"));
            assert!(bucket < 1000);
        }
    }
}
