/// Unified KV cache interface.
///
/// Both metadata cache and page cache implement this trait.
/// Callers simply do `get(key)` / `put(key, value)` — all eviction
/// (CLOCK-Pro), admission control (TinyLFU), and fast lookups (Bloom filters)
/// are internal implementation details.
pub trait Cache {
    type Key;
    type Value;

    /// Retrieve a cached value by key. Returns None on miss.
    fn get(&mut self, key: &Self::Key) -> Option<&Self::Value>;

    /// Insert a value into the cache. Returns true if the value was admitted.
    /// Admission may be rejected by TinyLFU if the new item's frequency
    /// is lower than the eviction victim's.
    fn put(&mut self, key: Self::Key, value: Self::Value) -> bool;

    /// Check if a key exists in the cache (does not update access tracking).
    fn contains(&self, key: &Self::Key) -> bool;

    /// Remove a specific key from the cache.
    fn remove(&mut self, key: &Self::Key) -> Option<Self::Value>;

    /// Number of entries in the cache.
    fn len(&self) -> usize;

    /// Whether the cache is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
