use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;

use ruxio_protocol::messages::ReadRangeRequest;

use crate::cache_trait::Cache;
use crate::metadata_cache::{CachedParquetMeta, MetadataCache};
use crate::page_cache::{CachedPage, PageCache};
use crate::page_key::PageKey;

/// Cache manager — pure synchronous cache operations.
///
/// Data lives on disk only — the OS page cache handles the memory tier.
/// All GCS I/O is handled by the caller (async), keeping CacheManager
/// borrow-safe across async boundaries.
pub struct CacheManager {
    pub metadata_cache: MetadataCache,
    pub page_cache: PageCache,
    /// Metadata TTL in seconds.
    metadata_ttl_secs: u64,
}

/// Result of a cached range read.
pub enum RangeResult {
    /// Full cache hit — all pages were cached, data read from disk.
    Hit(Bytes),
    /// Partial or full miss — `misses` lists the (PageKey, page_offset) pairs to fetch.
    /// `coalesced` provides merged ranges for efficient GCS fetching.
    Miss {
        misses: Vec<(PageKey, u64)>,
        coalesced: Vec<CoalescedRange>,
    },
}

/// A coalesced range merging adjacent cache misses into a single GCS request.
///
/// Example: misses at pages 5, 6, 7 (each 4MB) become one range:
///   `{ start_offset: 20MB, end_offset: 32MB, page_keys: [key5, key6, key7] }`
pub struct CoalescedRange {
    pub start_offset: u64,
    pub end_offset: u64,
    pub page_keys: Vec<PageKey>,
}

impl CacheManager {
    pub fn new(metadata_cache: MetadataCache, page_cache: PageCache) -> Self {
        Self {
            metadata_cache,
            page_cache,
            metadata_ttl_secs: 300,
        }
    }

    /// Create with explicit metadata TTL.
    pub fn with_ttl(metadata_cache: MetadataCache, page_cache: PageCache, ttl_secs: u64) -> Self {
        Self {
            metadata_cache,
            page_cache,
            metadata_ttl_secs: ttl_secs,
        }
    }

    /// The configured page size in bytes.
    pub fn page_size(&self) -> u64 {
        self.page_cache.page_size()
    }

    /// Read a byte range from cache only.
    ///
    /// On full hit: reads pages from disk (served from OS page cache if hot).
    /// On miss: returns miss list with coalesced ranges for efficient GCS fetch.
    pub fn read_range_cached(&mut self, req: &ReadRangeRequest) -> RangeResult {
        let page_size = self.page_size();
        let start = req.offset;
        let end = req.offset + req.length;

        let first_page = start / page_size;
        let last_page = (end - 1) / page_size;

        let mut misses = Vec::new();
        let file_id: Arc<str> = Arc::from(req.uri.as_str());

        // First pass: identify hits vs misses
        for page_idx in first_page..=last_page {
            let page_offset = page_idx * page_size;
            let key = PageKey::with_arc(file_id.clone(), page_idx);

            if self.page_cache.get(&key).is_some() {
                // Hit — will read from disk in assembly step
            } else {
                misses.push((key, page_offset));
            }
        }

        if !misses.is_empty() {
            let coalesced = Self::coalesce_ranges(&misses, page_size);
            return RangeResult::Miss { misses, coalesced };
        }

        // Full hit — read from disk and assemble
        RangeResult::Hit(self.assemble_from_disk(req))
    }

    /// Read pages from disk and assemble the requested byte range.
    ///
    /// Pages are read via std::fs::read — the OS page cache serves hot data
    /// from memory (~microseconds) and cold data from NVMe (~1ms per 4MB).
    fn assemble_from_disk(&mut self, req: &ReadRangeRequest) -> Bytes {
        let page_size = self.page_size();
        let start = req.offset;
        let end = req.offset + req.length;
        let first_page = start / page_size;
        let last_page = (end - 1) / page_size;
        let is_single_full_page = first_page == last_page
            && start == first_page * page_size
            && end == (first_page + 1) * page_size;

        let file_id: Arc<str> = Arc::from(req.uri.as_str());

        if is_single_full_page {
            // Fast path: single full page — read directly, no slicing
            let key = PageKey::with_arc(file_id, first_page);
            if let Some(page) = self.page_cache.get(&key) {
                if let Ok(data) = std::fs::read(&page.local_path) {
                    return Bytes::from(data);
                }
            }
            return Bytes::new();
        }

        // Multi-page or partial: read each page, slice, assemble
        let mut result = Vec::with_capacity(req.length as usize);

        for page_idx in first_page..=last_page {
            let page_offset = page_idx * page_size;
            let key = PageKey::with_arc(file_id.clone(), page_idx);

            if let Some(page) = self.page_cache.get(&key) {
                if let Ok(page_data) = std::fs::read(&page.local_path) {
                    let page_end = page_offset + page_data.len() as u64;
                    let slice_start = (start.max(page_offset) - page_offset) as usize;
                    let slice_end = (end.min(page_end) - page_offset) as usize;

                    if slice_start < page_data.len() && slice_end <= page_data.len() {
                        result.extend_from_slice(&page_data[slice_start..slice_end]);
                    }
                }
            }
        }

        Bytes::from(result)
    }

    /// Assemble a byte range after missing pages have been fetched and cached.
    pub fn read_range_finish(&mut self, req: &ReadRangeRequest) -> Bytes {
        self.assemble_from_disk(req)
    }

    /// Coalesce adjacent page misses into merged GCS range requests.
    ///
    /// Input:  [(key5, 20MB), (key6, 24MB), (key7, 28MB), (key10, 40MB)]
    /// Output: [{ 20MB-32MB, [key5,key6,key7] }, { 40MB-44MB, [key10] }]
    fn coalesce_ranges(misses: &[(PageKey, u64)], page_size: u64) -> Vec<CoalescedRange> {
        if misses.is_empty() {
            return Vec::new();
        }

        let mut ranges = Vec::new();
        let mut current = CoalescedRange {
            start_offset: misses[0].1,
            end_offset: misses[0].1 + page_size,
            page_keys: vec![misses[0].0.clone()],
        };

        for (key, offset) in &misses[1..] {
            if *offset == current.end_offset {
                // Adjacent — extend the current range
                current.end_offset = offset + page_size;
                current.page_keys.push(key.clone());
            } else {
                // Gap — start a new range
                ranges.push(current);
                current = CoalescedRange {
                    start_offset: *offset,
                    end_offset: offset + page_size,
                    page_keys: vec![key.clone()],
                };
            }
        }
        ranges.push(current);
        ranges
    }

    /// Get the on-disk file path and size for a cached page (for zero-copy serving).
    pub fn get_page_file(&mut self, file_id: &str, page_index: u64) -> Option<(PathBuf, u64)> {
        let key = PageKey::new(file_id, page_index);
        self.page_cache
            .get(&key)
            .map(|page| (page.local_path.clone(), page.size))
    }

    /// Cache a page: write to disk and insert into page cache.
    /// If disk write fails, the page is NOT cached (no phantom entries).
    pub fn cache_page(&mut self, key: &PageKey, data: &[u8]) {
        let size = data.len() as u64;
        let local_path = match self.page_cache.write_page_to_disk(key, data) {
            Ok(p) => p,
            Err(e) => {
                ruxio_common::metrics::CACHE_PUT_ERRORS.inc();
                tracing::warn!("Failed to cache page {}: {e}", key.file_id);
                return; // Don't insert phantom entry
            }
        };

        let cached_page = CachedPage { local_path, size };
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
        footer_bytes: bytes::Bytes,
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
            ttl: std::time::Duration::from_secs(self.metadata_ttl_secs),
        };

        self.metadata_cache.put(uri.to_string(), cached.clone());
        Ok(cached)
    }
}

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
    fn test_coalesce_adjacent() {
        let page_size = 4 * 1024 * 1024u64;
        let misses = vec![
            (PageKey::new("f", 5), 5 * page_size),
            (PageKey::new("f", 6), 6 * page_size),
            (PageKey::new("f", 7), 7 * page_size),
            (PageKey::new("f", 10), 10 * page_size),
        ];
        let coalesced = CacheManager::coalesce_ranges(&misses, page_size);
        assert_eq!(coalesced.len(), 2);
        assert_eq!(coalesced[0].page_keys.len(), 3); // pages 5,6,7
        assert_eq!(coalesced[0].start_offset, 5 * page_size);
        assert_eq!(coalesced[0].end_offset, 8 * page_size);
        assert_eq!(coalesced[1].page_keys.len(), 1); // page 10
    }

    #[test]
    fn test_coalesce_single() {
        let page_size = 4 * 1024 * 1024u64;
        let misses = vec![(PageKey::new("f", 3), 3 * page_size)];
        let coalesced = CacheManager::coalesce_ranges(&misses, page_size);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].page_keys.len(), 1);
    }

    #[test]
    fn test_coalesce_empty() {
        let coalesced = CacheManager::coalesce_ranges(&[], 4 * 1024 * 1024);
        assert!(coalesced.is_empty());
    }
}
