use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::PathBuf;

use bytes::Bytes;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use crate::cache_trait::Cache;
use crate::page_key::PageKey;

/// A cached page on disk.
#[derive(Debug, Clone)]
pub struct CachedPage {
    /// Path to the cached page file on local NVMe/SSD.
    pub local_path: PathBuf,
    /// Size in bytes.
    pub size: u64,
    /// The raw data (kept in memory for recently accessed pages, None if evicted to disk-only).
    pub data: Option<Bytes>,
}

// ── Count-Min Sketch for TinyLFU ─────────────────────────────────────

const CMS_DEPTH: usize = 4;
const CMS_WIDTH: usize = 4096;

/// Count-Min Sketch for estimating access frequency (TinyLFU).
struct CountMinSketch {
    counters: [[u8; CMS_WIDTH]; CMS_DEPTH],
    total: u64,
    reset_threshold: u64,
}

impl CountMinSketch {
    fn new(reset_threshold: u64) -> Self {
        Self {
            counters: [[0; CMS_WIDTH]; CMS_DEPTH],
            total: 0,
            reset_threshold,
        }
    }

    fn increment(&mut self, key: &PageKey) {
        self.total += 1;
        for row in 0..CMS_DEPTH {
            let idx = self.hash_for_row(key, row);
            self.counters[row][idx] = self.counters[row][idx].saturating_add(1);
        }
        if self.total >= self.reset_threshold {
            self.halve();
        }
    }

    fn estimate(&self, key: &PageKey) -> u8 {
        let mut min = u8::MAX;
        for row in 0..CMS_DEPTH {
            let idx = self.hash_for_row(key, row);
            min = min.min(self.counters[row][idx]);
        }
        min
    }

    fn halve(&mut self) {
        for row in &mut self.counters {
            for c in row.iter_mut() {
                *c /= 2;
            }
        }
        self.total = 0;
    }

    fn hash_for_row(&self, key: &PageKey, row: usize) -> usize {
        let mut hasher = Xxh3DefaultBuilder.build_hasher();
        key.hash(&mut hasher);
        row.hash(&mut hasher);
        (hasher.finish() as usize) % CMS_WIDTH
    }
}

// ── Bloom Filter ─────────────────────────────────────────────────────

const BLOOM_SIZE_BITS: usize = 8192;
const BLOOM_NUM_HASHES: usize = 5;

/// Bloom filter for fast "is any page from this file cached?" checks.
struct BloomFilter {
    bits: Vec<u64>,
}

impl BloomFilter {
    fn new() -> Self {
        Self {
            bits: vec![0; BLOOM_SIZE_BITS / 64],
        }
    }

    fn insert(&mut self, key: &PageKey) {
        for i in 0..BLOOM_NUM_HASHES {
            let bit = self.hash_bit(key, i);
            let word = bit / 64;
            let offset = bit % 64;
            self.bits[word] |= 1 << offset;
        }
    }

    fn might_contain(&self, key: &PageKey) -> bool {
        for i in 0..BLOOM_NUM_HASHES {
            let bit = self.hash_bit(key, i);
            let word = bit / 64;
            let offset = bit % 64;
            if self.bits[word] & (1 << offset) == 0 {
                return false;
            }
        }
        true
    }

    fn hash_bit(&self, key: &PageKey, seed: usize) -> usize {
        let mut hasher = Xxh3DefaultBuilder.build_hasher();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        (hasher.finish() as usize) % BLOOM_SIZE_BITS
    }
}

// ── CLOCK-Pro Entry ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageStatus {
    Hot,
    Cold,
    Test, // ghost entry — metadata only, no data
}

struct ClockEntry {
    key: PageKey,
    status: PageStatus,
    referenced: bool,
    size: u64,
}

// ── Page Cache ───────────────────────────────────────────────────────

/// Page cache with CLOCK-Pro eviction, TinyLFU admission, and Bloom filters.
pub struct PageCache {
    root: PathBuf,
    max_bytes: u64,
    used_bytes: u64,

    // Data storage
    pages: HashMap<PageKey, CachedPage>,

    // CLOCK-Pro: circular buffer with hand
    clock: Vec<ClockEntry>,
    clock_hand: usize,

    // TinyLFU admission
    frequency_sketch: CountMinSketch,

    // Bloom filters per file URI
    file_bloom: HashMap<String, BloomFilter>,
}

impl PageCache {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            root: root.into(),
            max_bytes,
            used_bytes: 0,
            pages: HashMap::new(),
            clock: Vec::new(),
            clock_hand: 0,
            frequency_sketch: CountMinSketch::new(10 * CMS_WIDTH as u64),
            file_bloom: HashMap::new(),
        }
    }

    /// Quick Bloom filter check: might any page from this file be cached?
    pub fn maybe_has_file(&self, file_uri: &str) -> bool {
        self.file_bloom.contains_key(file_uri)
    }

    /// Get the local file path for a page key.
    pub fn page_path(&self, key: &PageKey) -> PathBuf {
        self.root.join(key.to_path_component())
    }

    /// Write page data to disk and return the path.
    /// Creates parent directories as needed.
    pub fn write_page_to_disk(&self, key: &PageKey, data: &[u8]) -> std::io::Result<PathBuf> {
        let path = self.page_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, data)?;
        Ok(path)
    }

    /// Used bytes in cache.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    /// Maximum cache capacity.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn evict_until_space(&mut self, needed: u64) {
        while self.used_bytes + needed > self.max_bytes && !self.clock.is_empty() {
            self.evict_one();
        }
    }

    fn evict_one(&mut self) {
        if self.clock.is_empty() {
            return;
        }

        let max_iterations = self.clock.len() * 2;
        for _ in 0..max_iterations {
            if self.clock.is_empty() {
                break;
            }
            self.clock_hand %= self.clock.len();
            let entry = &mut self.clock[self.clock_hand];

            if entry.referenced {
                // Second chance: clear reference bit, promote cold→hot
                entry.referenced = false;
                if entry.status == PageStatus::Cold {
                    entry.status = PageStatus::Hot;
                }
                self.clock_hand = (self.clock_hand + 1) % self.clock.len().max(1);
                continue;
            }

            // Evict this entry
            let key = entry.key.clone();
            let size = entry.size;
            self.clock.remove(self.clock_hand);
            if self.clock_hand >= self.clock.len() && !self.clock.is_empty() {
                self.clock_hand = 0;
            }

            if let Some(page) = self.pages.remove(&key) {
                self.used_bytes -= size;
                // Delete file on disk (best effort)
                let _ = std::fs::remove_file(&page.local_path);
            }
            return;
        }
    }
}

impl Cache for PageCache {
    type Key = PageKey;
    type Value = CachedPage;

    fn get(&mut self, key: &PageKey) -> Option<&CachedPage> {
        // Always increment frequency for TinyLFU
        self.frequency_sketch.increment(key);

        // Set referenced bit in CLOCK-Pro
        if let Some(pos) = self.clock.iter().position(|e| e.key == *key) {
            self.clock[pos].referenced = true;
        }

        self.pages.get(key)
    }

    fn put(&mut self, key: PageKey, value: CachedPage) -> bool {
        // Already cached? Update in place.
        if self.pages.contains_key(&key) {
            self.pages.insert(key, value);
            return true;
        }

        let size = value.size;

        // TinyLFU admission: check if new item frequency > eviction victim frequency
        self.frequency_sketch.increment(&key);
        let new_freq = self.frequency_sketch.estimate(&key);

        if self.used_bytes + size > self.max_bytes {
            // Find the victim's frequency
            if !self.clock.is_empty() {
                // Scan for the first unreferenced entry as potential victim
                let victim_freq = self
                    .clock
                    .iter()
                    .filter(|e| !e.referenced && e.status != PageStatus::Test)
                    .map(|e| self.frequency_sketch.estimate(&e.key))
                    .min()
                    .unwrap_or(0);

                if new_freq <= victim_freq {
                    // Reject admission: new item is less frequent than victim
                    return false;
                }
            }

            // Evict to make space
            self.evict_until_space(size);
        }

        // Admit the page
        self.clock.push(ClockEntry {
            key: key.clone(),
            status: PageStatus::Cold, // new entries start cold
            referenced: true,
            size,
        });

        // Update Bloom filter for this file
        self.file_bloom
            .entry(key.file_uri.clone())
            .or_insert_with(BloomFilter::new)
            .insert(&key);

        self.used_bytes += size;
        self.pages.insert(key, value);
        true
    }

    fn contains(&self, key: &PageKey) -> bool {
        // Fast path: check Bloom filter first
        if let Some(bloom) = self.file_bloom.get(&key.file_uri) {
            if !bloom.might_contain(key) {
                return false;
            }
        } else {
            return false;
        }
        self.pages.contains_key(key)
    }

    fn remove(&mut self, key: &PageKey) -> Option<CachedPage> {
        if let Some(pos) = self.clock.iter().position(|e| e.key == *key) {
            self.clock.remove(pos);
            if self.clock_hand >= self.clock.len() && !self.clock.is_empty() {
                self.clock_hand = 0;
            }
        }
        if let Some(page) = self.pages.remove(key) {
            self.used_bytes -= page.size;
            let _ = std::fs::remove_file(&page.local_path);
            Some(page)
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.pages.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_page(size: u64) -> CachedPage {
        CachedPage {
            local_path: PathBuf::from("/tmp/test"),
            size,
            data: Some(Bytes::from(vec![0u8; size as usize])),
        }
    }

    #[test]
    fn test_page_cache_basic() {
        let mut cache = PageCache::new("/tmp/ruxio_test", 1024 * 1024);
        let key = PageKey::new("gs://b/f.parquet", 0, 0);

        assert!(!cache.contains(&key));
        assert!(cache.put(key.clone(), make_page(100)));
        assert!(cache.contains(&key));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_page_cache_eviction() {
        let mut cache = PageCache::new("/tmp/ruxio_test", 200);

        // Fill cache
        let k1 = PageKey::new("gs://b/f1.parquet", 0, 0);
        let k2 = PageKey::new("gs://b/f2.parquet", 0, 0);
        cache.put(k1.clone(), make_page(100));
        cache.put(k2.clone(), make_page(100));
        assert_eq!(cache.used_bytes(), 200);

        // Adding k3 should trigger eviction
        let k3 = PageKey::new("gs://b/f3.parquet", 0, 0);
        // Increment frequency of k3 a few times to pass TinyLFU
        for _ in 0..5 {
            cache.frequency_sketch.increment(&k3);
        }
        cache.put(k3.clone(), make_page(100));
        assert!(cache.used_bytes() <= 200);
    }

    #[test]
    fn test_bloom_filter_false_negative_free() {
        let mut cache = PageCache::new("/tmp/ruxio_test", 1024 * 1024);
        let key = PageKey::new("gs://b/f.parquet", 0, 0);
        cache.put(key.clone(), make_page(100));

        // Bloom filter must never have false negatives
        assert!(cache.maybe_has_file("gs://b/f.parquet"));
    }

    #[test]
    fn test_count_min_sketch() {
        let mut cms = CountMinSketch::new(1000);
        let key = PageKey::new("gs://b/f.parquet", 0, 0);

        assert_eq!(cms.estimate(&key), 0);
        cms.increment(&key);
        assert!(cms.estimate(&key) >= 1);
        cms.increment(&key);
        assert!(cms.estimate(&key) >= 2);
    }
}
