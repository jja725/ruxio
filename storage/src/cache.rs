use bytes::Bytes;

use ruxio_protocol::messages::ReadRangeRequest;

use crate::cache_trait::Cache;
use crate::metadata_cache::{CachedParquetMeta, MetadataCache};
use crate::page_cache::{CachedPage, PageCache};
use crate::page_key::PageKey;

/// Cache manager — pure synchronous cache operations.
///
/// Manages page cache and metadata cache. All I/O (GCS fetches) is handled
/// by the caller, keeping CacheManager borrow-safe across async boundaries.
///
/// The caller pattern is:
/// 1. `borrow_mut()` → check cache (sync) → drop borrow
/// 2. On miss: fetch from GCS (async, no borrow held)
/// 3. `borrow_mut()` → insert into cache (sync) → drop borrow
pub struct CacheManager {
    pub metadata_cache: MetadataCache,
    pub page_cache: PageCache,
}

/// Result of a cached range read.
pub enum RangeResult {
    /// Full cache hit — all pages were cached, here's the assembled data.
    Hit(Bytes),
    /// Partial or full miss — `cached_parts` has data from hit pages (keyed by page_idx),
    /// `misses` lists the (PageKey, page_offset) pairs to fetch from GCS.
    Miss { misses: Vec<(PageKey, u64)> },
}

impl CacheManager {
    pub fn new(metadata_cache: MetadataCache, page_cache: PageCache) -> Self {
        Self {
            metadata_cache,
            page_cache,
        }
    }

    /// The configured page size in bytes.
    pub fn page_size(&self) -> u64 {
        self.page_cache.page_size()
    }

    /// Read a byte range from cache only.
    ///
    /// Returns `RangeResult::Hit` on full cache hit (zero-copy for single full-page reads),
    /// or `RangeResult::Miss` with the pages that need fetching.
    pub fn read_range_cached(&mut self, req: &ReadRangeRequest) -> RangeResult {
        let page_size = self.page_size();
        let start = req.offset;
        let end = req.offset + req.length;

        let first_page = start / page_size;
        let last_page = (end - 1) / page_size;
        let is_single_page = first_page == last_page;

        let mut result_buf: Option<Vec<u8>> = None;
        let mut single_page_bytes: Option<Bytes> = None;
        let mut misses = Vec::new();

        for page_idx in first_page..=last_page {
            let page_offset = page_idx * page_size;
            let key = PageKey::new(&req.uri, page_idx);

            if let Some(page) = self.page_cache.get(&key) {
                let page_data = page.data.clone().unwrap_or_default();

                // Fast path: single full-page read — return Bytes directly (zero copy)
                if is_single_page
                    && start == page_offset
                    && end == page_offset + page_data.len() as u64
                {
                    single_page_bytes = Some(page_data);
                    continue;
                }

                // Multi-page or partial: slice into buffer
                let page_end = page_offset + page_data.len() as u64;
                let slice_start = (start.max(page_offset) - page_offset) as usize;
                let slice_end = (end.min(page_end) - page_offset) as usize;

                if slice_start < page_data.len() && slice_end <= page_data.len() {
                    let buf =
                        result_buf.get_or_insert_with(|| Vec::with_capacity(req.length as usize));
                    buf.extend_from_slice(&page_data[slice_start..slice_end]);
                }
            } else {
                misses.push((key, page_offset));
            }
        }

        if !misses.is_empty() {
            return RangeResult::Miss { misses };
        }

        // Full hit
        if let Some(bytes) = single_page_bytes {
            RangeResult::Hit(bytes)
        } else {
            RangeResult::Hit(Bytes::from(result_buf.unwrap_or_default()))
        }
    }

    /// Assemble a byte range after missing pages have been fetched and cached.
    ///
    /// Only re-reads the pages that were previously missing (their keys).
    /// The caller passes the miss keys so we don't re-scan already-cached pages.
    pub fn read_range_finish(
        &mut self,
        req: &ReadRangeRequest,
        miss_keys: &[(PageKey, u64)],
    ) -> Bytes {
        let page_size = self.page_size();
        let start = req.offset;
        let end = req.offset + req.length;

        let first_page = start / page_size;
        let last_page = (end - 1) / page_size;

        // Build a set of which page indices were misses for fast lookup
        let miss_indices: HashSet<u64> = miss_keys.iter().map(|(k, _)| k.page_index).collect();
        let _ = miss_indices; // suppress unused warning — used below

        // We need to re-read ALL pages to assemble in order, but we know they're all cached now
        let mut result = Vec::with_capacity(req.length as usize);

        for page_idx in first_page..=last_page {
            let page_offset = page_idx * page_size;
            let key = PageKey::new(&req.uri, page_idx);

            if let Some(page) = self.page_cache.get(&key) {
                let page_data = page.data.clone().unwrap_or_default();
                let page_end = page_offset + page_data.len() as u64;
                let slice_start = (start.max(page_offset) - page_offset) as usize;
                let slice_end = (end.min(page_end) - page_offset) as usize;

                if slice_start < page_data.len() && slice_end <= page_data.len() {
                    result.extend_from_slice(&page_data[slice_start..slice_end]);
                }
            }
        }

        Bytes::from(result)
    }

    /// Get the on-disk file path and size for a cached page (for zero-copy serving).
    ///
    /// Skips the `path.exists()` check — if the file is missing, sendfile will
    /// fail and the caller falls back to the buffered path.
    pub fn get_page_file(
        &mut self,
        file_id: &str,
        page_index: u64,
    ) -> Option<(std::path::PathBuf, u64)> {
        let key = PageKey::new(file_id, page_index);
        if let Some(page) = self.page_cache.get(&key) {
            Some((page.local_path.clone(), page.size))
        } else {
            None
        }
    }

    /// Cache a page: write to disk and insert into page cache.
    pub fn cache_page(&mut self, key: &PageKey, data: &Bytes) {
        let local_path = match self.page_cache.write_page_to_disk(key, data) {
            Ok(p) => p,
            Err(_) => self.page_cache.page_path(key),
        };

        let cached_page = CachedPage {
            local_path,
            size: data.len() as u64,
            data: Some(data.clone()),
        };
        self.page_cache.put(key.clone(), cached_page);
    }

    /// Check metadata cache. Returns None on miss or stale.
    pub fn get_metadata_cached(&mut self, uri: &str) -> Option<CachedParquetMeta> {
        if let Some(meta) = self.metadata_cache.get(&uri.to_string()) {
            if !meta.is_stale() {
                return Some(meta.clone());
            }
        }
        None
    }

    /// Insert parsed metadata into cache.
    pub fn put_metadata(&mut self, uri: &str, cached: CachedParquetMeta) {
        self.metadata_cache.put(uri.to_string(), cached);
    }

    /// Parse a Parquet footer and cache the resulting metadata.
    pub fn parse_and_cache_metadata(
        &mut self,
        uri: &str,
        footer_bytes: Bytes,
        file_size: u64,
        etag: Option<String>,
    ) -> anyhow::Result<CachedParquetMeta> {
        let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
            .parse_and_finish(&footer_bytes)?;

        let cached = CachedParquetMeta {
            metadata: std::sync::Arc::new(metadata),
            footer_bytes,
            file_size,
            etag,
            cached_at: std::time::Instant::now(),
            ttl: std::time::Duration::from_secs(300),
        };

        self.metadata_cache.put(uri.to_string(), cached.clone());
        Ok(cached)
    }
}

use std::collections::HashSet;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_manager_creation() {
        let metadata = MetadataCache::new(1000);
        let pages = PageCache::new("/tmp/ruxio_test", 1024 * 1024 * 1024, 4 * 1024 * 1024);
        let cm = CacheManager::new(metadata, pages);
        assert_eq!(cm.page_size(), 4 * 1024 * 1024);
    }

    #[test]
    fn test_cache_manager_custom_page_size() {
        let metadata = MetadataCache::new(1000);
        let pages = PageCache::new("/tmp/ruxio_test", 1024 * 1024 * 1024, 1024 * 1024);
        let cm = CacheManager::new(metadata, pages);
        assert_eq!(cm.page_size(), 1024 * 1024);
    }
}
