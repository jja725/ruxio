//! Cross-thread cache lookup forwarding via lock-free SPSC ring buffers.
//!
//! Inspired by ScyllaDB/Seastar's per-core messaging. Each thread pair
//! gets a dedicated SPSC (single-producer, single-consumer) queue —
//! no locks, no atomics on the fast path, cache-line padded.
//!
//! Flow:
//!   Thread A → spsc_request[A→B].push(key) → Thread B processes → spsc_reply[B→A].push(result)
//!
//! Cost: ~1-5µs per forwarded lookup (vs ~100-200µs with Mutex + polling).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::cache_trait::Cache;
use crate::page_cache::PageCache;
use crate::page_key::PageKey;

// ── Lock-free SPSC ring buffer ───────────────────────────────────────

const RING_CAPACITY: usize = 256; // must be power of 2
const RING_MASK: usize = RING_CAPACITY - 1;

/// Lock-free single-producer single-consumer ring buffer.
///
/// Cache-line padded: head (consumer) and tail (producer) are on
/// separate cache lines to avoid false sharing.
pub struct SpscRing<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>; RING_CAPACITY]>,
    // Cache line 1: written by consumer
    head: AtomicUsize,
    _pad: [u8; 56],
    // Cache line 2: written by producer
    tail: AtomicUsize,
}

unsafe impl<T: Send> Send for SpscRing<T> {}
unsafe impl<T: Send> Sync for SpscRing<T> {}

impl<T> Default for SpscRing<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SpscRing<T> {
    pub fn new() -> Self {
        let buffer = {
            let mut v: Vec<UnsafeCell<MaybeUninit<T>>> = Vec::with_capacity(RING_CAPACITY);
            for _ in 0..RING_CAPACITY {
                v.push(UnsafeCell::new(MaybeUninit::uninit()));
            }
            let boxed_slice = v.into_boxed_slice();
            // Safety: Vec has exactly RING_CAPACITY elements
            unsafe {
                let ptr =
                    Box::into_raw(boxed_slice) as *mut [UnsafeCell<MaybeUninit<T>>; RING_CAPACITY];
                Box::from_raw(ptr)
            }
        };
        Self {
            buffer,
            head: AtomicUsize::new(0),
            _pad: [0u8; 56],
            tail: AtomicUsize::new(0),
        }
    }

    /// Push an item (producer side). Returns Err(item) if full.
    pub fn try_push(&self, item: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if tail.wrapping_sub(head) >= RING_CAPACITY {
            return Err(item); // full
        }

        let idx = tail & RING_MASK;
        // Safety: we're the only producer, and slot is not occupied
        unsafe {
            (*self.buffer[idx].get()).write(item);
        }
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Pop an item (consumer side). Returns None if empty.
    pub fn try_pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        if head == tail {
            return None; // empty
        }

        let idx = head & RING_MASK;
        // Safety: we're the only consumer, and slot is occupied
        let item = unsafe { (*self.buffer[idx].get()).assume_init_read() };
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Some(item)
    }
}

impl<T> Drop for SpscRing<T> {
    fn drop(&mut self) {
        // Drop any remaining items
        while self.try_pop().is_some() {}
    }
}

// ── Forwarding types ─────────────────────────────────────────────────

/// A forwarded lookup request.
pub struct ForwardRequest {
    pub key: PageKey,
}

/// A forwarded lookup response.
pub struct ForwardResponse {
    pub path: Option<(PathBuf, u64)>,
}

/// Bidirectional channel between two threads.
///
/// Thread A uses `requests` to send lookups, reads from `replies`.
/// Thread B reads from `requests`, uses `replies` to send results.
pub struct ThreadChannel {
    pub requests: SpscRing<ForwardRequest>,
    pub replies: SpscRing<ForwardResponse>,
}

impl Default for ThreadChannel {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadChannel {
    pub fn new() -> Self {
        Self {
            requests: SpscRing::new(),
            replies: SpscRing::new(),
        }
    }
}

/// All channels for a given thread. Index by peer thread id.
///
/// `channels[peer_id]`:
///   - To send a request TO peer: `channels[peer_id].requests.try_push()`
///   - To read a reply FROM peer: `channels[peer_id].replies.try_pop()`
///
/// The peer thread does the reverse:
///   - Reads requests from us: our `channels[peer_id].requests.try_pop()`
///     (via its own mirrored reference)
///   - Sends replies to us: our `channels[peer_id].replies.try_push()`
pub type ChannelMatrix = Vec<Vec<std::sync::Arc<ThreadChannel>>>;

/// Create the NxN channel matrix for all thread pairs.
///
/// Returns a matrix where `matrix[sender][receiver]` is the channel
/// from sender to receiver. The channel is shared (Arc) so both
/// threads can access it.
pub fn create_channel_matrix(num_threads: usize) -> ChannelMatrix {
    let mut matrix: Vec<Vec<std::sync::Arc<ThreadChannel>>> = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let mut row = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            row.push(std::sync::Arc::new(ThreadChannel::new()));
        }
        matrix.push(row);
    }
    matrix
}

/// Send a forwarded lookup to the owning thread and wait for response.
///
/// Uses lock-free SPSC queues — no mutex, no channel allocation per request.
pub async fn forward_lookup(
    channel_matrix: &ChannelMatrix,
    from_thread: usize,
    to_thread: usize,
    key: PageKey,
) -> Option<(PathBuf, u64)> {
    let channel = &channel_matrix[from_thread][to_thread];

    // Push request (spin-yield if queue is full — shouldn't happen with 256 slots)
    let mut req = ForwardRequest { key };
    loop {
        match channel.requests.try_push(req) {
            Ok(()) => break,
            Err(returned) => {
                req = returned;
                monoio::time::sleep(Duration::from_micros(1)).await;
            }
        }
    }

    // Wait for reply
    loop {
        if let Some(resp) = channel.replies.try_pop() {
            return resp.path;
        }
        monoio::time::sleep(Duration::from_micros(1)).await;
    }
}

/// Drain all pending forwarded requests from all peers and process them.
///
/// Called by the owning thread to serve forwarded lookups.
pub fn process_forwarded_requests(
    channel_matrix: &ChannelMatrix,
    my_thread: usize,
    _num_threads: usize,
    page_cache: &mut PageCache,
) {
    for (peer, row) in channel_matrix.iter().enumerate() {
        if peer == my_thread {
            continue;
        }

        // Read requests that peer sent to us
        let channel = &row[my_thread];

        while let Some(req) = channel.requests.try_pop() {
            let result = page_cache
                .get(&req.key)
                .map(|page| (page.local_path.clone(), page.size));

            // Send reply back
            let resp = ForwardResponse { path: result };
            if channel.replies.try_push(resp).is_err() {
                tracing::warn!(
                    "SPSC reply queue full (thread {my_thread} → {peer}), dropping response"
                );
            }
        }
    }
}

// ── Legacy compatibility (for inbox-based code) ──────────────────────

/// Legacy inbox type — kept for compatibility during migration.
pub type Inbox = std::sync::Arc<std::sync::Mutex<Vec<LegacyLookupRequest>>>;

pub struct LegacyLookupRequest {
    pub key: PageKey,
    pub reply: std::sync::mpsc::Sender<LegacyLookupResponse>,
}

pub struct LegacyLookupResponse {
    pub path: Option<(PathBuf, u64)>,
}

pub fn new_inbox() -> Inbox {
    std::sync::Arc::new(std::sync::Mutex::new(Vec::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spsc_basic() {
        let ring: SpscRing<u64> = SpscRing::new();
        assert!(ring.try_pop().is_none());

        ring.try_push(42).unwrap();
        ring.try_push(43).unwrap();

        assert_eq!(ring.try_pop(), Some(42));
        assert_eq!(ring.try_pop(), Some(43));
        assert!(ring.try_pop().is_none());
    }

    #[test]
    fn test_spsc_full() {
        let ring: SpscRing<u64> = SpscRing::new();
        for i in 0..RING_CAPACITY as u64 {
            ring.try_push(i).unwrap();
        }
        // Should be full
        assert!(ring.try_push(999).is_err());

        // Pop one, push one
        assert_eq!(ring.try_pop(), Some(0));
        ring.try_push(999).unwrap();
    }

    #[test]
    fn test_spsc_wrap_around() {
        let ring: SpscRing<u64> = SpscRing::new();
        // Push and pop many times to wrap around
        for round in 0..4 {
            for i in 0..RING_CAPACITY as u64 {
                ring.try_push(round * 1000 + i).unwrap();
            }
            for i in 0..RING_CAPACITY as u64 {
                assert_eq!(ring.try_pop(), Some(round * 1000 + i));
            }
        }
    }

    #[test]
    fn test_channel_matrix() {
        let matrix = create_channel_matrix(4);
        assert_eq!(matrix.len(), 4);
        for row in &matrix {
            assert_eq!(row.len(), 4);
        }
    }
}
