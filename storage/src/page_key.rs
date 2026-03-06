use std::hash::{BuildHasher, Hash, Hasher};
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

/// Key for page cache lookups.
///
/// Identifies a specific data page within a Parquet file.
/// The combination of (file_uri, row_group, page_offset) uniquely
/// identifies a cached page.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PageKey {
    pub file_uri: String,
    pub row_group: usize,
    pub page_offset: u64,
}

impl PageKey {
    pub fn new(file_uri: impl Into<String>, row_group: usize, page_offset: u64) -> Self {
        Self {
            file_uri: file_uri.into(),
            row_group,
            page_offset,
        }
    }

    /// Returns a filesystem-safe path component for this page key.
    pub fn to_path_component(&self) -> String {
        let mut hasher = Xxh3DefaultBuilder.build_hasher();
        self.hash(&mut hasher);
        let h = hasher.finish();
        let bucket = h % 1024;
        format!("{bucket:03x}/{h:016x}.page")
    }
}

impl Hash for PageKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_uri.hash(state);
        self.row_group.hash(state);
        self.page_offset.hash(state);
    }
}

impl std::fmt::Display for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:rg{}:off{}",
            self.file_uri, self.row_group, self.page_offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_key_hash_deterministic() {
        let k1 = PageKey::new("gs://b/f.parquet", 0, 4096);
        let k2 = PageKey::new("gs://b/f.parquet", 0, 4096);
        assert_eq!(k1.to_path_component(), k2.to_path_component());
    }

    #[test]
    fn test_page_key_different_offsets() {
        let k1 = PageKey::new("gs://b/f.parquet", 0, 0);
        let k2 = PageKey::new("gs://b/f.parquet", 0, 4096);
        assert_ne!(k1.to_path_component(), k2.to_path_component());
    }
}
