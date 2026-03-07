//! Cross-thread cache lookup forwarding.
//!
//! Each monoio thread owns a partition of the cache. When a request arrives
//! for a file owned by a different thread, we forward the lookup:
//!
//!   Thread A → inbox → Thread B: "lookup key + mark accessed"
//!   Thread B: CLOCK-Pro marks referenced, returns file path
//!   Thread B → reply channel → Thread A: (path, size)
//!   Thread A: sendfile(path, socket)  ← zero-copy preserved
//!
//! Cost: ~100-200µs per forwarded lookup (channel + polling).
//! Local lookups: 0 overhead.

use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cache_trait::Cache;
use crate::page_cache::PageCache;
use crate::page_key::PageKey;

/// A lookup request sent from one thread to the owning thread.
pub struct LookupRequest {
    pub key: PageKey,
    pub reply: mpsc::Sender<LookupResponse>,
}

/// Response from the owning thread.
pub struct LookupResponse {
    /// File path on disk (None if not cached).
    pub path: Option<(PathBuf, u64)>,
}

/// Inbox for receiving forwarded lookup requests.
pub type Inbox = Arc<Mutex<Vec<LookupRequest>>>;

/// Create an inbox for a worker thread.
pub fn new_inbox() -> Inbox {
    Arc::new(Mutex::new(Vec::new()))
}

/// Send a forwarded lookup to the owning thread's inbox and wait for response.
///
/// Called from the requesting thread. Polls the reply channel with short
/// yields to avoid blocking the monoio event loop.
pub async fn forward_lookup(inbox: &Inbox, key: PageKey) -> Option<(PathBuf, u64)> {
    let (tx, rx) = mpsc::channel();
    {
        let mut queue = inbox.lock().unwrap();
        queue.push(LookupRequest { key, reply: tx });
    }

    // Poll for response — yields between checks so monoio can process other tasks
    loop {
        match rx.try_recv() {
            Ok(resp) => return resp.path,
            Err(mpsc::TryRecvError::Empty) => {
                monoio::time::sleep(Duration::from_micros(10)).await;
            }
            Err(mpsc::TryRecvError::Disconnected) => return None,
        }
    }
}

/// Drain the inbox and process all pending lookup requests.
///
/// Called by the owning thread to serve forwarded lookups from other threads.
/// Does CLOCK-Pro access tracking so eviction stays correct.
pub fn process_inbox(inbox: &Inbox, page_cache: &mut PageCache) {
    let requests: Vec<LookupRequest> = {
        let mut queue = inbox.lock().unwrap();
        std::mem::take(&mut *queue)
    };

    for req in requests {
        let result = if let Some(page) = page_cache.get(&req.key) {
            // CLOCK-Pro marks as referenced, TinyLFU increments frequency
            Some((page.local_path.clone(), page.size))
        } else {
            None
        };

        let _ = req.reply.send(LookupResponse { path: result });
    }
}
