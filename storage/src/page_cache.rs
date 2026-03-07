use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use crate::cache_trait::Cache;
use crate::page_key::PageKey;

/// A cached page on disk.
///
/// Data lives on NVMe/SSD only — the OS page cache handles the memory tier.
/// This gives us full NVMe capacity without duplicating data in application RAM.
#[derive(Debug, Clone)]
pub struct CachedPage {
    /// Path to the cached page file on local NVMe/SSD.
    pub local_path: PathBuf,
    /// Size in bytes.
    pub size: u64,
}

/// Per-file cache tracking.
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub file_id: Arc<str>,
    pub cached_pages: HashSet<u64>,
    pub total_cached_bytes: u64,
}

/// Which eviction policy to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// CLOCK-Pro with second-chance promotion (hot/cold).
    ClockPro,
    /// Simple LRU with O(1) doubly-linked list.
    Lru,
}

// ── Count-Min Sketch for TinyLFU ─────────────────────────────────────

const CMS_DEPTH: usize = 4;
const CMS_WIDTH: usize = 4096;

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

// ── CLOCK-Pro state ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageStatus {
    Hot,
    Cold,
}

struct ClockEntry {
    key: PageKey,
    status: PageStatus,
    referenced: bool,
    size: u64,
}

struct ClockProState {
    entries: Vec<ClockEntry>,
    hand: usize,
    pos: HashMap<PageKey, usize>,
}

impl ClockProState {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            hand: 0,
            pos: HashMap::new(),
        }
    }

    fn mark_accessed(&mut self, key: &PageKey) {
        if let Some(&idx) = self.pos.get(key) {
            self.entries[idx].referenced = true;
        }
    }

    fn insert(&mut self, key: PageKey, size: u64) {
        let idx = self.entries.len();
        self.entries.push(ClockEntry {
            key: key.clone(),
            status: PageStatus::Cold,
            referenced: true,
            size,
        });
        self.pos.insert(key, idx);
    }

    /// Remove by key. Uses swap_remove for O(1).
    fn remove_key(&mut self, key: &PageKey) {
        if let Some(&idx) = self.pos.get(key) {
            self.swap_remove(idx);
        }
    }

    fn swap_remove(&mut self, idx: usize) -> ClockEntry {
        let entry = self.entries.swap_remove(idx);
        self.pos.remove(&entry.key);
        if idx < self.entries.len() {
            let moved_key = self.entries[idx].key.clone();
            self.pos.insert(moved_key, idx);
        }
        if self.hand >= self.entries.len() && !self.entries.is_empty() {
            self.hand = 0;
        }
        entry
    }

    /// Get the eviction victim key (the entry the clock hand evicts).
    fn victim_key(&self) -> Option<&PageKey> {
        if self.entries.is_empty() {
            return None;
        }
        Some(&self.entries[self.hand % self.entries.len()].key)
    }

    /// Evict one entry, returning its key and size.
    fn evict_one(&mut self) -> Option<(PageKey, u64)> {
        if self.entries.is_empty() {
            return None;
        }

        let max_iterations = self.entries.len() * 2;
        for _ in 0..max_iterations {
            if self.entries.is_empty() {
                break;
            }
            self.hand %= self.entries.len();
            let entry = &mut self.entries[self.hand];

            if entry.referenced {
                entry.referenced = false;
                if entry.status == PageStatus::Cold {
                    entry.status = PageStatus::Hot;
                }
                self.hand = (self.hand + 1) % self.entries.len().max(1);
                continue;
            }

            let key = entry.key.clone();
            let size = entry.size;
            self.swap_remove(self.hand);
            return Some((key, size));
        }
        None
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ── LRU state (O(1) index-based doubly-linked list) ──────────────────

const NONE: usize = usize::MAX;

struct LruNode {
    key: PageKey,
    size: u64,
    prev: usize,
    next: usize,
}

struct LruState {
    nodes: Vec<LruNode>,
    pos: HashMap<PageKey, usize>,
    head: usize, // oldest → evict from here
    tail: usize, // newest → insert here
    free: Vec<usize>,
}

impl LruState {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            pos: HashMap::new(),
            head: NONE,
            tail: NONE,
            free: Vec::new(),
        }
    }

    fn mark_accessed(&mut self, key: &PageKey) {
        if let Some(&idx) = self.pos.get(key) {
            self.move_to_tail(idx);
        }
    }

    fn insert(&mut self, key: PageKey, size: u64) {
        let idx = if let Some(free_idx) = self.free.pop() {
            self.nodes[free_idx] = LruNode {
                key: key.clone(),
                size,
                prev: NONE,
                next: NONE,
            };
            free_idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(LruNode {
                key: key.clone(),
                size,
                prev: NONE,
                next: NONE,
            });
            idx
        };

        self.pos.insert(key, idx);
        self.push_tail(idx);
    }

    fn remove_key(&mut self, key: &PageKey) {
        if let Some(&idx) = self.pos.get(key) {
            self.unlink(idx);
            self.pos.remove(key);
            self.free.push(idx);
        }
    }

    /// Get the eviction victim key (LRU = head of list).
    fn victim_key(&self) -> Option<&PageKey> {
        if self.head == NONE {
            return None;
        }
        Some(&self.nodes[self.head].key)
    }

    /// Evict the LRU entry (head), returning its key and size.
    fn evict_one(&mut self) -> Option<(PageKey, u64)> {
        if self.head == NONE {
            return None;
        }
        let idx = self.head;
        let key = self.nodes[idx].key.clone();
        let size = self.nodes[idx].size;
        self.unlink(idx);
        self.pos.remove(&key);
        self.free.push(idx);
        Some((key, size))
    }

    fn is_empty(&self) -> bool {
        self.head == NONE
    }

    fn len(&self) -> usize {
        self.pos.len()
    }

    // ── linked list operations ───────────────────────────────────────

    fn unlink(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        if prev != NONE {
            self.nodes[prev].next = next;
        } else {
            self.head = next;
        }

        if next != NONE {
            self.nodes[next].prev = prev;
        } else {
            self.tail = prev;
        }

        self.nodes[idx].prev = NONE;
        self.nodes[idx].next = NONE;
    }

    fn push_tail(&mut self, idx: usize) {
        self.nodes[idx].prev = self.tail;
        self.nodes[idx].next = NONE;

        if self.tail != NONE {
            self.nodes[self.tail].next = idx;
        } else {
            self.head = idx;
        }
        self.tail = idx;
    }

    fn move_to_tail(&mut self, idx: usize) {
        if idx == self.tail {
            return; // already at tail
        }
        self.unlink(idx);
        self.push_tail(idx);
    }
}

// ── Unified eviction state ───────────────────────────────────────────

enum EvictionState {
    ClockPro(ClockProState),
    Lru(LruState),
}

impl EvictionState {
    fn mark_accessed(&mut self, key: &PageKey) {
        match self {
            Self::ClockPro(s) => s.mark_accessed(key),
            Self::Lru(s) => s.mark_accessed(key),
        }
    }

    fn insert(&mut self, key: PageKey, size: u64) {
        match self {
            Self::ClockPro(s) => s.insert(key, size),
            Self::Lru(s) => s.insert(key, size),
        }
    }

    fn remove_key(&mut self, key: &PageKey) {
        match self {
            Self::ClockPro(s) => s.remove_key(key),
            Self::Lru(s) => s.remove_key(key),
        }
    }

    fn victim_key(&self) -> Option<&PageKey> {
        match self {
            Self::ClockPro(s) => s.victim_key(),
            Self::Lru(s) => s.victim_key(),
        }
    }

    fn evict_one(&mut self) -> Option<(PageKey, u64)> {
        match self {
            Self::ClockPro(s) => s.evict_one(),
            Self::Lru(s) => s.evict_one(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::ClockPro(s) => s.is_empty(),
            Self::Lru(s) => s.is_empty(),
        }
    }
}

// ── Page Cache ───────────────────────────────────────────────────────

/// Page cache with configurable eviction (CLOCK-Pro or LRU) and TinyLFU admission.
///
/// Disk layout (Alluxio-style):
/// ```text
/// {root}/{page_size}/{bucket}/{url_safe_file_id}/{page_index}
/// ```
pub struct PageCache {
    root: PathBuf,
    max_bytes: u64,
    used_bytes: u64,
    page_size: u64,

    pages: HashMap<PageKey, CachedPage>,
    eviction: EvictionState,
    frequency_sketch: CountMinSketch,
    files: HashMap<Arc<str>, FileInfo>,
    created_dirs: HashSet<PathBuf>,
}

impl PageCache {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64, page_size: u64) -> Self {
        Self::with_policy(root, max_bytes, page_size, EvictionPolicy::ClockPro)
    }

    pub fn with_policy(
        root: impl Into<PathBuf>,
        max_bytes: u64,
        page_size: u64,
        policy: EvictionPolicy,
    ) -> Self {
        let eviction = match policy {
            EvictionPolicy::ClockPro => EvictionState::ClockPro(ClockProState::new()),
            EvictionPolicy::Lru => EvictionState::Lru(LruState::new()),
        };
        Self {
            root: root.into(),
            max_bytes,
            used_bytes: 0,
            page_size,
            pages: HashMap::new(),
            eviction,
            frequency_sketch: CountMinSketch::new(10 * CMS_WIDTH as u64),
            files: HashMap::new(),
            created_dirs: HashSet::new(),
        }
    }

    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    pub fn has_file(&self, file_id: &str) -> bool {
        self.files.contains_key(file_id)
    }

    pub fn page_path(&self, key: &PageKey) -> PathBuf {
        self.root
            .join(self.page_size.to_string())
            .join(key.to_path_component())
    }

    pub fn write_page_to_disk(&mut self, key: &PageKey, data: &[u8]) -> std::io::Result<PathBuf> {
        let path = self.page_path(key);
        if let Some(parent) = path.parent() {
            if self.created_dirs.insert(parent.to_path_buf()) {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(&path, data)?;
        Ok(path)
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn file_info(&self, file_id: &str) -> Option<&FileInfo> {
        self.files.get(file_id)
    }

    fn track_file_add(&mut self, key: &PageKey, size: u64) {
        let info = self
            .files
            .entry(key.file_id.clone())
            .or_insert_with(|| FileInfo {
                file_id: key.file_id.clone(),
                cached_pages: HashSet::new(),
                total_cached_bytes: 0,
            });
        if info.cached_pages.insert(key.page_index) {
            info.total_cached_bytes += size;
        }
    }

    fn track_file_remove(&mut self, key: &PageKey, size: u64) {
        if let Some(info) = self.files.get_mut(&*key.file_id) {
            if info.cached_pages.remove(&key.page_index) {
                info.total_cached_bytes = info.total_cached_bytes.saturating_sub(size);
            }
            if info.cached_pages.is_empty() {
                self.files.remove(&*key.file_id);
            }
        }
    }

    fn evict_until_space(&mut self, needed: u64) {
        while self.used_bytes + needed > self.max_bytes && !self.eviction.is_empty() {
            if let Some((key, size)) = self.eviction.evict_one() {
                if let Some(page) = self.pages.remove(&key) {
                    self.used_bytes -= size;
                    self.track_file_remove(&key, size);
                    let _ = std::fs::remove_file(&page.local_path);
                }
            } else {
                break;
            }
        }
    }
}

impl Cache for PageCache {
    type Key = PageKey;
    type Value = CachedPage;

    fn get(&mut self, key: &PageKey) -> Option<&CachedPage> {
        self.frequency_sketch.increment(key);
        self.eviction.mark_accessed(key);
        self.pages.get(key)
    }

    fn put(&mut self, key: PageKey, value: CachedPage) -> bool {
        if self.pages.contains_key(&key) {
            self.pages.insert(key, value);
            return true;
        }

        let size = value.size;

        // TinyLFU admission
        self.frequency_sketch.increment(&key);
        let new_freq = self.frequency_sketch.estimate(&key);

        if self.used_bytes + size > self.max_bytes {
            if let Some(victim_key) = self.eviction.victim_key() {
                let victim_freq = self.frequency_sketch.estimate(victim_key);
                if new_freq <= victim_freq {
                    return false;
                }
            }
            self.evict_until_space(size);
        }

        self.eviction.insert(key.clone(), size);
        self.track_file_add(&key, size);
        self.used_bytes += size;
        self.pages.insert(key, value);
        true
    }

    fn contains(&self, key: &PageKey) -> bool {
        if let Some(info) = self.files.get(&*key.file_id) {
            info.cached_pages.contains(&key.page_index)
        } else {
            false
        }
    }

    fn remove(&mut self, key: &PageKey) -> Option<CachedPage> {
        self.eviction.remove_key(key);
        if let Some(page) = self.pages.remove(key) {
            self.used_bytes -= page.size;
            self.track_file_remove(key, page.size);
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

    const TEST_PAGE_SIZE: u64 = 4 * 1024 * 1024;

    fn make_page(size: u64) -> CachedPage {
        CachedPage {
            local_path: PathBuf::from("/tmp/test"),
            size,
        }
    }

    // Run the same tests for both policies
    fn test_basic(policy: EvictionPolicy) {
        let mut cache =
            PageCache::with_policy("/tmp/ruxio_test", 1024 * 1024, TEST_PAGE_SIZE, policy);
        let key = PageKey::new("gs://b/f.parquet", 0);

        assert!(!cache.contains(&key));
        assert!(cache.put(key.clone(), make_page(100)));
        assert!(cache.contains(&key));
        assert_eq!(cache.len(), 1);
    }

    fn test_eviction(policy: EvictionPolicy) {
        let mut cache = PageCache::with_policy("/tmp/ruxio_test", 200, TEST_PAGE_SIZE, policy);

        let k1 = PageKey::new("gs://b/f1.parquet", 0);
        let k2 = PageKey::new("gs://b/f2.parquet", 0);
        cache.put(k1.clone(), make_page(100));
        cache.put(k2.clone(), make_page(100));
        assert_eq!(cache.used_bytes(), 200);

        let k3 = PageKey::new("gs://b/f3.parquet", 0);
        for _ in 0..5 {
            cache.frequency_sketch.increment(&k3);
        }
        cache.put(k3.clone(), make_page(100));
        assert!(cache.used_bytes() <= 200);
    }

    fn test_file_tracking(policy: EvictionPolicy) {
        let mut cache =
            PageCache::with_policy("/tmp/ruxio_test", 1024 * 1024, TEST_PAGE_SIZE, policy);

        let k1 = PageKey::new("gs://b/f.parquet", 0);
        let k2 = PageKey::new("gs://b/f.parquet", 1);
        cache.put(k1.clone(), make_page(100));
        cache.put(k2.clone(), make_page(200));

        let info = cache.file_info("gs://b/f.parquet").unwrap();
        assert_eq!(info.cached_pages.len(), 2);
        assert_eq!(info.total_cached_bytes, 300);

        cache.remove(&k1);
        let info = cache.file_info("gs://b/f.parquet").unwrap();
        assert_eq!(info.cached_pages.len(), 1);

        cache.remove(&k2);
        assert!(cache.file_info("gs://b/f.parquet").is_none());
    }

    #[test]
    fn test_clockpro_basic() {
        test_basic(EvictionPolicy::ClockPro);
    }

    #[test]
    fn test_clockpro_eviction() {
        test_eviction(EvictionPolicy::ClockPro);
    }

    #[test]
    fn test_clockpro_file_tracking() {
        test_file_tracking(EvictionPolicy::ClockPro);
    }

    #[test]
    fn test_lru_basic() {
        test_basic(EvictionPolicy::Lru);
    }

    #[test]
    fn test_lru_eviction() {
        test_eviction(EvictionPolicy::Lru);
    }

    #[test]
    fn test_lru_file_tracking() {
        test_file_tracking(EvictionPolicy::Lru);
    }

    #[test]
    fn test_has_file() {
        for policy in [EvictionPolicy::ClockPro, EvictionPolicy::Lru] {
            let mut cache =
                PageCache::with_policy("/tmp/ruxio_test", 1024 * 1024, TEST_PAGE_SIZE, policy);
            let key = PageKey::new("gs://b/f.parquet", 0);
            cache.put(key.clone(), make_page(100));

            assert!(cache.has_file("gs://b/f.parquet"));
            assert!(!cache.has_file("gs://b/other.parquet"));

            cache.remove(&key);
            assert!(!cache.has_file("gs://b/f.parquet"));
        }
    }

    #[test]
    fn test_count_min_sketch() {
        let mut cms = CountMinSketch::new(1000);
        let key = PageKey::new("gs://b/f.parquet", 0);

        assert_eq!(cms.estimate(&key), 0);
        cms.increment(&key);
        assert!(cms.estimate(&key) >= 1);
        cms.increment(&key);
        assert!(cms.estimate(&key) >= 2);
    }

    #[test]
    fn test_page_size_accessor() {
        let cache = PageCache::new("/tmp/ruxio_test", 1024, 1024 * 1024);
        assert_eq!(cache.page_size(), 1024 * 1024);
    }

    #[test]
    fn test_page_path_alluxio_layout() {
        let cache = PageCache::new("/tmp/cache", 1024 * 1024, TEST_PAGE_SIZE);
        let key = PageKey::new("test://file.parquet", 3);
        let path = cache.page_path(&key);
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("/4194304/"));
        assert!(path_str.ends_with("/3"));
    }

    #[test]
    fn test_lru_ordering() {
        let mut cache =
            PageCache::with_policy("/tmp/ruxio_test", 300, TEST_PAGE_SIZE, EvictionPolicy::Lru);

        let k1 = PageKey::new("gs://b/f1.parquet", 0);
        let k2 = PageKey::new("gs://b/f2.parquet", 0);
        let k3 = PageKey::new("gs://b/f3.parquet", 0);

        cache.put(k1.clone(), make_page(100));
        cache.put(k2.clone(), make_page(100));
        cache.put(k3.clone(), make_page(100));
        assert_eq!(cache.len(), 3);

        // Access k1 — moves it to most recent
        cache.get(&k1);

        // Insert k4 — should evict k2 (oldest untouched)
        let k4 = PageKey::new("gs://b/f4.parquet", 0);
        for _ in 0..5 {
            cache.frequency_sketch.increment(&k4);
        }
        cache.put(k4.clone(), make_page(100));

        assert!(cache.contains(&k1), "k1 should survive (was accessed)");
        assert!(!cache.contains(&k2), "k2 should be evicted (oldest)");
        assert!(cache.contains(&k3), "k3 should survive");
        assert!(cache.contains(&k4), "k4 should be inserted");
    }
}
