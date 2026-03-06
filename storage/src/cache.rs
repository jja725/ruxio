use anyhow::Result;
use bytes::Bytes;

use ruxio_protocol::messages::{ReadRangeRequest, ScanRequest};

use crate::cache_trait::Cache;
use crate::gcs::GcsClient;
use crate::metadata_cache::{CachedParquetMeta, MetadataCache};
use crate::page_cache::{CachedPage, PageCache};
use crate::page_key::PageKey;

/// Cache manager — orchestrates the range-read cache flow.
///
/// Ruxio is a pure range-request cache. The query engine (Presto, Spark, etc.)
/// does its own predicate pushdown and tells ruxio exactly which byte ranges
/// to read. Ruxio's job is:
///
/// 1. **Metadata caching**: Cache Parquet footers so engines get them in
///    sub-ms instead of 200+ms from GCS.
/// 2. **Page caching**: Cache data pages (4MB aligned) on local NVMe/SSD.
/// 3. **Predicate pushdown**: Engines can send a Scan with a predicate.
///    Ruxio evaluates it against cached row group statistics and streams
///    only matching pages — turning N+1 round-trips into 1 RPC.
///
/// Two access modes:
/// - `read_range()` — engine already knows which bytes (direct range read)
/// - `scan()` — engine sends predicate, ruxio evaluates and streams matches
///
/// Two serving modes:
/// - Normal: reads data into memory, returns Bytes (for small pages or non-Linux)
/// - Zero-copy: returns file path for splice-based serving (file→socket, no userspace copy)
pub struct CacheManager {
    pub gcs: GcsClient,
    pub metadata_cache: MetadataCache,
    pub page_cache: PageCache,
    page_size: u64,
}

impl CacheManager {
    pub fn new(gcs: GcsClient, metadata_cache: MetadataCache, page_cache: PageCache) -> Self {
        Self {
            gcs,
            metadata_cache,
            page_cache,
            page_size: 4 * 1024 * 1024, // 4MB
        }
    }

    /// Read a byte range from a cached file.
    ///
    /// Flow:
    /// 1. Compute which 4MB page(s) cover the requested range
    /// 2. For each page: cache hit → read from NVMe, miss → fetch from GCS + cache
    /// 3. Slice and return exactly the requested bytes
    pub async fn read_range(&mut self, req: &ReadRangeRequest) -> Result<Bytes> {
        let start = req.offset;
        let end = req.offset + req.length;

        // Compute which pages are needed
        let first_page = start / self.page_size;
        let last_page = (end - 1) / self.page_size;

        let mut result = Vec::with_capacity(req.length as usize);

        for page_idx in first_page..=last_page {
            let page_offset = page_idx * self.page_size;
            let key = PageKey::new(&req.uri, 0, page_offset);

            let page_data = if let Some(page) = self.page_cache.get(&key) {
                // Cache hit
                page.data.clone().unwrap_or_default()
            } else {
                // Cache miss → fetch from GCS
                let fetch_end = page_offset + self.page_size;
                let data = self.gcs.get_range(&req.uri, page_offset..fetch_end).await?;
                self.cache_page(&key, &data);
                data
            };

            // Slice out the portion of this page that overlaps with the request
            let page_start = page_offset;
            let page_end = page_offset + page_data.len() as u64;
            let slice_start = (start.max(page_start) - page_start) as usize;
            let slice_end = (end.min(page_end) - page_start) as usize;

            if slice_start < page_data.len() && slice_end <= page_data.len() {
                result.extend_from_slice(&page_data[slice_start..slice_end]);
            }
        }

        Ok(Bytes::from(result))
    }

    /// Get the on-disk file path and size for a cached page (for zero-copy serving).
    ///
    /// Returns `Some((path, size))` if the page is cached on disk,
    /// `None` if it's a cache miss. The caller can then splice the file
    /// directly to the TCP socket without any userspace buffer copies.
    pub fn get_page_file(
        &mut self,
        uri: &str,
        page_offset: u64,
    ) -> Option<(std::path::PathBuf, u64)> {
        let key = PageKey::new(uri, 0, page_offset);
        if let Some(page) = self.page_cache.get(&key) {
            if page.local_path.exists() {
                return Some((page.local_path.clone(), page.size));
            }
        }
        None
    }

    /// Cache a page: write to disk and insert into page cache.
    fn cache_page(&mut self, key: &PageKey, data: &Bytes) {
        // Write to disk
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

    /// Get file metadata (Parquet footer).
    ///
    /// Engines need the footer to decide which pages to request.
    /// Cached in deserialized form for sub-ms access.
    pub async fn get_metadata(&mut self, uri: &str) -> Result<CachedParquetMeta> {
        // Check metadata cache
        if let Some(meta) = self.metadata_cache.get(&uri.to_string()) {
            if !meta.is_stale() {
                return Ok(meta.clone());
            }
        }

        // Cache miss or stale → fetch footer from GCS
        let head = self.gcs.head(uri).await?;
        let footer_size = 64 * 1024u64; // read last 64KB
        let footer_start = head.size.saturating_sub(footer_size);
        let footer_bytes = self.gcs.get_range(uri, footer_start..head.size).await?;

        // Parse Parquet metadata
        let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
            .parse_and_finish(&footer_bytes)?;

        let cached = CachedParquetMeta {
            metadata: std::sync::Arc::new(metadata),
            footer_bytes: footer_bytes.clone(),
            file_size: head.size,
            etag: head.etag,
            cached_at: std::time::Instant::now(),
            ttl: std::time::Duration::from_secs(300),
        };

        self.metadata_cache.put(uri.to_string(), cached.clone());
        Ok(cached)
    }

    /// Scan with predicate pushdown — 1 RPC instead of N+1.
    ///
    /// Flow:
    /// 1. Get/fetch Parquet metadata (cached footer + row group stats)
    /// 2. Evaluate predicate against row group statistics → matching row groups
    /// 3. For each matching row group: cache hit → NVMe, miss → GCS + cache
    /// 4. Stream back raw page bytes for all matching row groups
    pub async fn scan(&mut self, req: &ScanRequest) -> Result<Vec<Bytes>> {
        let uri = &req.uri;
        let metadata = self.get_metadata(uri).await?;

        // Determine which row groups match the predicate
        let row_group_ranges = if let Some(pred) = &req.predicate {
            MetadataCache::find_matching_row_groups(&metadata, pred)
        } else {
            // No predicate → all row groups
            metadata
                .metadata
                .row_groups()
                .iter()
                .enumerate()
                .map(|(rg_idx, rg)| {
                    let offset = rg.column(0).byte_range().0;
                    let end = rg
                        .columns()
                        .iter()
                        .map(|c| {
                            let (off, len) = c.byte_range();
                            off + len
                        })
                        .max()
                        .unwrap_or(offset);
                    crate::metadata_cache::RowGroupRange {
                        row_group: rg_idx,
                        offset,
                        length: end - offset,
                    }
                })
                .collect()
        };

        // Fetch matching pages
        let mut result = Vec::with_capacity(row_group_ranges.len());
        for rg_range in &row_group_ranges {
            let range_req = ReadRangeRequest {
                uri: uri.clone(),
                offset: rg_range.offset,
                length: rg_range.length,
            };
            let data = self.read_range(&range_req).await?;
            result.push(data);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_manager_creation() {
        let gcs = GcsClient::new("test-bucket");
        let metadata = MetadataCache::new(1000);
        let pages = PageCache::new("/tmp/ruxio_test", 1024 * 1024 * 1024);
        let cm = CacheManager::new(gcs, metadata, pages);
        assert_eq!(cm.page_size, 4 * 1024 * 1024);
    }
}
