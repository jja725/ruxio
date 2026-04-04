use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use tracing::warn;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use ruxio_cluster::membership::ClusterMembership;
use ruxio_common::metrics::{
    ACTIVE_OPERATIONS, BYTES_READ_CACHE, BYTES_READ_GCS, BYTES_SERVED_TOTAL, CACHE_GET_ERRORS,
    CACHE_HIT_COUNTER, CACHE_MISS_COUNTER, CACHE_READ_LATENCY, CONNECTED_CLIENTS,
    GCS_FETCH_COUNTER, GCS_FETCH_LATENCY, INCOMING_REQUESTS, INFLIGHT_COALESCED,
    METADATA_CACHE_HITS, METADATA_CACHE_MISSES, PREFETCH_PAGES_TOTAL, RESPONSE_TIME_COLLECTOR,
};
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::{
    BatchReadRequest, ErrorResponse, GetMetadataRequest, MetadataResponse, ReadRangeRequest,
    RedirectResponse, ScanRequest,
};
use ruxio_storage::cache::{CacheManager, RangeResult};
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::forwarding::{self, ChannelMatrix};
use ruxio_storage::gcs::GcsClient;
use ruxio_storage::metadata_cache::{CachedParquetMeta, MetadataCache};
use ruxio_storage::page_key::PageKey;
use ruxio_storage::retry::RetryPolicy;
use ruxio_storage::zero_copy;

// ── Server configuration ────────────────────────────────────────────

/// Runtime-configurable server settings (passed from config file).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Idle timeout per connection (default 300s).
    pub idle_timeout_secs: u64,
    /// Max bytes in flight per connection before pausing sends (default 64MB).
    pub max_inflight_bytes: u64,
    /// Sequential prefetch depth in pages (default 4).
    pub prefetch_pages: u64,
    /// Thundering herd wait timeout (default 30s).
    pub inflight_timeout_secs: u64,
    /// Max concurrent connections per worker thread (default 10000).
    pub max_connections_per_thread: u32,
    /// Max outstanding prefetch tasks per thread (default 16).
    pub max_prefetch_tasks: u32,
    /// Per-write timeout in seconds (default 60s).
    pub write_timeout_secs: u64,
    /// Max pending requests per thread before returning RESOURCE_EXHAUSTED (default 1000).
    pub max_pending_requests: u32,
    /// Response chunk size for large reads (default 4MB).
    pub response_chunk_bytes: u64,
    /// Sendfile deadline in seconds (default 300s).
    pub sendfile_timeout_secs: u64,
    /// Write buffer high water mark (bytes). Connection enters backpressured
    /// state when pending bytes exceed this. Default 64KB.
    pub write_buffer_high: u64,
    /// Write buffer low water mark (bytes). Connection exits backpressured
    /// state when pending bytes drain below this. Default 32KB.
    /// Hysteresis between high/low prevents flapping.
    pub write_buffer_low: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            idle_timeout_secs: 300,
            max_inflight_bytes: 64 * 1024 * 1024,
            prefetch_pages: 4,
            inflight_timeout_secs: 30,
            max_connections_per_thread: 10_000,
            max_prefetch_tasks: 16,
            write_timeout_secs: 60,
            max_pending_requests: 1_000,
            response_chunk_bytes: 4 * 1024 * 1024,
            sendfile_timeout_secs: 300,
            write_buffer_high: 64 * 1024,
            write_buffer_low: 32 * 1024,
        }
    }
}

/// Per-thread context.
pub struct ThreadContext {
    pub cache_manager: Rc<RefCell<CacheManager>>,
    pub gcs: Rc<GcsClient>,
    pub membership: Rc<ClusterMembership>,
    pub thread_id: usize,
    pub num_threads: usize,

    /// SPSC channel matrix for lock-free cross-thread forwarding.
    pub channels: ChannelMatrix,

    /// In-flight GCS fetches for thundering herd prevention.
    pub inflight: Rc<RefCell<HashSet<PageKey>>>,

    /// Sequential access tracking for prefetching.
    pub access_tracker: Rc<RefCell<HashMap<Arc<str>, u64>>>,

    /// Retry policy for GCS operations.
    pub retry_policy: RetryPolicy,

    /// Shutdown flag — set by signal handler.
    pub shutdown: Arc<AtomicBool>,

    /// Server readiness flag — false during startup/shutdown.
    pub ready: Arc<AtomicBool>,

    /// Server configuration.
    pub config: ServerConfig,

    /// Active connection count for this thread.
    pub active_connections: Rc<AtomicU32>,

    /// Outstanding prefetch task count for this thread.
    pub prefetch_outstanding: Rc<AtomicU32>,

    /// Active in-flight request count for overload rejection.
    pub pending_requests: Rc<AtomicU32>,
}

impl ThreadContext {
    fn owning_thread(&self, uri: &str) -> usize {
        let mut hasher = Xxh3DefaultBuilder.build_hasher();
        uri.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_threads
    }
}

// ── Connection handling ──────────────────────────────────────────────

/// RAII guard to decrement connected clients gauge on drop.
struct ConnectionGuard {
    counter: Rc<AtomicU32>,
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        CONNECTED_CLIENTS.dec();
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// RAII guard to decrement active operations gauge on drop.
struct ActiveOpGuard;
impl Drop for ActiveOpGuard {
    fn drop(&mut self) {
        ACTIVE_OPERATIONS.dec();
    }
}

/// RAII guard to decrement prefetch counter on drop.
struct PrefetchGuard {
    counter: Rc<AtomicU32>,
}
impl Drop for PrefetchGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// RAII guard to decrement pending request counter on drop.
struct RequestGuard {
    counter: Rc<AtomicU32>,
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Write a frame to the stream with a timeout. Returns bytes written or error.
async fn write_frame_with_timeout(
    stream: &mut TcpStream,
    frame: Frame,
    timeout: Duration,
) -> std::io::Result<u64> {
    let encoded = frame.encode();
    let len = encoded.len() as u64;
    match monoio::time::timeout(timeout, stream.write_all(encoded.to_vec())).await {
        Ok((Ok(_), _)) => Ok(len),
        Ok((Err(e), _)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "write timeout",
        )),
    }
}

pub async fn serve_connection(stream: TcpStream, ctx: Rc<ThreadContext>) {
    // Connection limit check
    let current = ctx.active_connections.fetch_add(1, Ordering::Relaxed);
    if current >= ctx.config.max_connections_per_thread {
        ctx.active_connections.fetch_sub(1, Ordering::Relaxed);
        tracing::warn!(
            "Connection limit reached ({}/{}), rejecting",
            current,
            ctx.config.max_connections_per_thread
        );
        return;
    }

    CONNECTED_CLIENTS.inc();
    let _guard = ConnectionGuard {
        counter: ctx.active_connections.clone(),
    };

    let mut stream = stream;
    let _ = stream.set_nodelay(true);
    let mut reader = FrameReader::new();
    let mut buf = vec![0u8; 128 * 1024];
    let idle_timeout = Duration::from_secs(ctx.config.idle_timeout_secs);
    let write_timeout = Duration::from_secs(ctx.config.write_timeout_secs);
    let high_water = ctx.config.write_buffer_high;
    let low_water = ctx.config.write_buffer_low;
    // Connection-level bytes-in-flight counter for watermark backpressure.
    // When pending exceeds high_water, we enter backpressured state and
    // yield until it drains below low_water. Hysteresis prevents flapping.
    let mut conn_bytes_pending: u64 = 0;
    let mut backpressured = false;

    loop {
        if ctx.shutdown.load(Ordering::Relaxed) {
            tracing::debug!("Shutdown: closing connection");
            return;
        }

        let read_result = monoio::time::timeout(idle_timeout, stream.read(buf)).await;
        match read_result {
            Err(_) => {
                tracing::debug!("Connection idle timeout, closing");
                return;
            }
            Ok((result, read_buf)) => {
                buf = read_buf;
                let n = match result {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(e) => {
                        warn!("Read error: {e}");
                        return;
                    }
                };
                reader.feed(&buf[..n]);
            }
        }

        loop {
            match reader.next_frame() {
                Ok(Some(frame)) => {
                    let response_frames = process_frame(frame, &ctx, &mut stream).await;
                    for resp in response_frames {
                        // Watermark-based backpressure (Netty-style hysteresis):
                        // - Enter backpressured state when pending > high_water
                        // - Stay backpressured until pending drains below low_water
                        // - Prevents flapping between yield/resume on boundary
                        if backpressured {
                            while conn_bytes_pending > low_water {
                                monoio::time::sleep(Duration::from_millis(1)).await;
                                // write_all is synchronous from our perspective —
                                // once it returns, kernel accepted the bytes.
                                // So reset after yield.
                                conn_bytes_pending = 0;
                            }
                            backpressured = false;
                        } else if conn_bytes_pending >= high_water {
                            backpressured = true;
                            monoio::time::sleep(Duration::from_millis(1)).await;
                            conn_bytes_pending = 0;
                        }

                        match write_frame_with_timeout(&mut stream, resp, write_timeout).await {
                            Ok(n) => conn_bytes_pending += n,
                            Err(e) => {
                                tracing::debug!("Write error: {e}");
                                return;
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("Frame decode error: {e}");
                    return;
                }
            }
        }
    }
}

// ── Thundering herd ──────────────────────────────────────────────────

/// Wait for an in-flight fetch to complete, with timeout.
/// Returns true if the fetch completed, false if timed out.
async fn wait_for_inflight(ctx: &ThreadContext, key: &PageKey) -> bool {
    let timeout = Duration::from_secs(ctx.config.inflight_timeout_secs);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if !ctx.inflight.borrow().contains(key) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            // Timed out — remove the stale inflight entry so it doesn't
            // block future requests. The page will be re-fetched.
            ctx.inflight.borrow_mut().remove(key);
            return false;
        }
        monoio::time::sleep(Duration::from_micros(100)).await;
    }
}

/// RAII guard that removes a PageKey from the inflight set on drop.
/// Ensures cleanup even if the async task panics.
struct InflightGuard {
    inflight: Rc<RefCell<HashSet<PageKey>>>,
    key: PageKey,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.inflight.borrow_mut().remove(&self.key);
    }
}

// ── Sequential prefetching ───────────────────────────────────────────

fn maybe_prefetch(ctx: &Rc<ThreadContext>, file_id: &Arc<str>, current_page: u64) {
    let is_sequential = {
        let mut tracker = ctx.access_tracker.borrow_mut();
        let last = tracker.get(&**file_id).copied();
        tracker.insert(file_id.clone(), current_page);
        last.map_or(false, |last| current_page == last + 1)
    };

    if !is_sequential {
        return;
    }

    let page_size = ctx.cache_manager.borrow().page_size();

    for i in 1..=ctx.config.prefetch_pages {
        // Bound outstanding prefetch tasks
        let outstanding = ctx.prefetch_outstanding.load(Ordering::Relaxed);
        if outstanding >= ctx.config.max_prefetch_tasks {
            break;
        }

        let prefetch_idx = current_page + i;
        let key = PageKey::with_arc(file_id.clone(), prefetch_idx);

        let already_cached = ctx.cache_manager.borrow_mut().page_cache.contains(&key);
        let already_inflight = ctx.inflight.borrow().contains(&key);

        if already_cached || already_inflight {
            continue;
        }

        ctx.inflight.borrow_mut().insert(key.clone());
        ctx.prefetch_outstanding.fetch_add(1, Ordering::Relaxed);
        PREFETCH_PAGES_TOTAL.inc();

        let ctx = ctx.clone();
        let file_id = file_id.clone();
        monoio::spawn(async move {
            // Guard ensures inflight entry is removed even on panic
            let _guard = InflightGuard {
                inflight: ctx.inflight.clone(),
                key: key.clone(),
            };
            let _prefetch_guard = PrefetchGuard {
                counter: ctx.prefetch_outstanding.clone(),
            };
            let page_offset = prefetch_idx * page_size;
            let fetch_end = page_offset + page_size;
            if let Ok(data) = ctx
                .gcs
                .get_range_with_retry(&file_id, page_offset..fetch_end, &ctx.retry_policy)
                .await
            {
                ctx.cache_manager.borrow_mut().cache_page(&key, &data);
            }
        });
    }
}

// ── Core read path (send-then-cache) ─────────────────────────────────

/// Read a byte range. On cache miss, fetches from GCS and returns data
/// immediately — disk caching happens asynchronously in the background.
pub(crate) async fn read_range(ctx: &Rc<ThreadContext>, req: &ReadRangeRequest) -> Result<Bytes> {
    let cache_result = ctx.cache_manager.borrow_mut().read_range_cached(req);

    match cache_result {
        RangeResult::Hit(data) => {
            CACHE_HIT_COUNTER.inc();
            let data_len = data.len() as f64;
            BYTES_READ_CACHE.inc_by(data_len);
            BYTES_SERVED_TOTAL.inc_by(data_len);
            let _cache_timer = CACHE_READ_LATENCY.start_timer();
            let page_size = ctx.cache_manager.borrow().page_size();
            let file_id: Arc<str> = Arc::from(req.uri.as_str());
            maybe_prefetch(ctx, &file_id, req.offset / page_size);
            Ok(data)
        }
        RangeResult::Miss { misses, coalesced } => {
            CACHE_MISS_COUNTER.inc();
            let page_size = ctx.cache_manager.borrow().page_size();
            let file_id: Arc<str> = Arc::from(req.uri.as_str());

            // Fetch from GCS using coalesced ranges, keep data in memory
            let mut fetched: HashMap<u64, Bytes> = HashMap::new();

            for range in &coalesced {
                // Thundering herd: wait for pages already being fetched
                let mut needs_fetch = Vec::new();
                for key in &range.page_keys {
                    if ctx.inflight.borrow().contains(key) {
                        INFLIGHT_COALESCED.inc();
                        wait_for_inflight(ctx, key).await;
                        // Now cached — will be read from disk during assembly
                    } else if !ctx.cache_manager.borrow_mut().page_cache.contains(key) {
                        needs_fetch.push(key.clone());
                    }
                }

                if needs_fetch.is_empty() {
                    continue;
                }

                // Mark as in-flight
                for key in &needs_fetch {
                    ctx.inflight.borrow_mut().insert(key.clone());
                }

                // Single coalesced GCS request
                GCS_FETCH_COUNTER.inc();
                let _gcs_timer = GCS_FETCH_LATENCY.start_timer();
                let data = match ctx
                    .gcs
                    .get_range_with_retry(
                        &req.uri,
                        range.start_offset..range.end_offset,
                        &ctx.retry_policy,
                    )
                    .await
                {
                    Ok(d) => {
                        BYTES_READ_GCS.inc_by(d.len() as f64);
                        BYTES_SERVED_TOTAL.inc_by(d.len() as f64);
                        d
                    }
                    Err(e) => {
                        // GCS fetch failed — remove all inflight entries
                        for key in &needs_fetch {
                            ctx.inflight.borrow_mut().remove(key);
                        }
                        return Err(e);
                    }
                };

                // Split into pages, keep in memory for immediate return
                for key in &needs_fetch {
                    let start = (key.page_index * page_size - range.start_offset) as usize;
                    let end = (start + page_size as usize).min(data.len());
                    if start < data.len() {
                        let page_data = data.slice(start..end);
                        fetched.insert(key.page_index, page_data.clone());

                        // Async cache to disk with guard for cleanup
                        let cm = ctx.cache_manager.clone();
                        let inflight = ctx.inflight.clone();
                        let key_owned = key.clone();
                        monoio::spawn(async move {
                            let _guard = InflightGuard {
                                inflight,
                                key: key_owned.clone(),
                            };
                            cm.borrow_mut().cache_page(&key_owned, &page_data);
                            // _guard dropped → inflight.remove()
                        });
                    } else {
                        ctx.inflight.borrow_mut().remove(key);
                    }
                }
            }

            // Trigger prefetch
            let last_page = misses.last().map(|(k, _)| k.page_index).unwrap_or(0);
            maybe_prefetch(ctx, &file_id, last_page);

            // Assemble result: cached pages from disk, fetched pages from memory
            let start = req.offset;
            let end = req.offset + req.length;
            let first_page = start / page_size;
            let last_page_idx = (end - 1) / page_size;

            if first_page == last_page_idx {
                // Single page — fast path
                if let Some(data) = fetched.get(&first_page) {
                    let slice_start = (start - first_page * page_size) as usize;
                    let slice_end = (end - first_page * page_size) as usize;
                    if start == first_page * page_size && end == (first_page + 1) * page_size {
                        return Ok(data.clone());
                    }
                    return Ok(data.slice(slice_start..slice_end.min(data.len())));
                }
            }

            // Multi-page assembly
            let mut result = Vec::with_capacity(req.length as usize);
            for page_idx in first_page..=last_page_idx {
                let page_offset = page_idx * page_size;
                let page_data = if let Some(data) = fetched.get(&page_idx) {
                    data.clone()
                } else {
                    // Read from disk (OS page cache handles memory)
                    let key = PageKey::with_arc(file_id.clone(), page_idx);
                    let path = ctx
                        .cache_manager
                        .borrow_mut()
                        .page_cache
                        .get(&key)
                        .map(|p| p.local_path.clone());
                    if let Some(path) = path {
                        match std::fs::read(&path) {
                            Ok(data) => {
                                // Validate page size
                                if data.len() as u64 > page_size * 2 {
                                    CACHE_GET_ERRORS.inc();
                                    warn!(
                                        "Corrupt page detected: {} (size {} > expected {})",
                                        path.display(),
                                        data.len(),
                                        page_size
                                    );
                                    ctx.cache_manager.borrow_mut().page_cache.remove(&key);
                                    continue;
                                }
                                Bytes::from(data)
                            }
                            Err(e) => {
                                CACHE_GET_ERRORS.inc();
                                warn!("Page read error {}: {e}", path.display());
                                // Remove corrupted/missing entry from cache
                                ctx.cache_manager.borrow_mut().page_cache.remove(&key);
                                continue;
                            }
                        }
                    } else {
                        continue;
                    }
                };

                let page_end = page_offset + page_data.len() as u64;
                let slice_start = (start.max(page_offset) - page_offset) as usize;
                let slice_end = (end.min(page_end) - page_offset) as usize;
                if slice_start < page_data.len() && slice_end <= page_data.len() {
                    result.extend_from_slice(&page_data[slice_start..slice_end]);
                }
            }

            Ok(Bytes::from(result))
        }
    }
}

/// Fetch metadata with cache check.
pub(crate) async fn get_metadata(ctx: &ThreadContext, uri: &str) -> Result<CachedParquetMeta> {
    if let Some(meta) = ctx.cache_manager.borrow_mut().get_metadata_cached(uri) {
        METADATA_CACHE_HITS.inc();
        return Ok(meta);
    }

    METADATA_CACHE_MISSES.inc();

    let head = ctx.gcs.head_with_retry(uri, &ctx.retry_policy).await?;
    let footer_size = 64 * 1024u64;
    let footer_start = head.size.saturating_sub(footer_size);
    let footer_bytes = ctx
        .gcs
        .get_range_with_retry(uri, footer_start..head.size, &ctx.retry_policy)
        .await?;

    let cached = ctx.cache_manager.borrow_mut().parse_and_cache_metadata(
        uri,
        footer_bytes,
        head.size,
        head.etag,
    )?;
    Ok(cached)
}

// ── Streaming scan ───────────────────────────────────────────────────

async fn scan_streaming(
    ctx: &Rc<ThreadContext>,
    req: &ScanRequest,
    request_id: u32,
    stream: &mut TcpStream,
) -> Result<()> {
    let uri = &req.uri;
    let metadata = get_metadata(ctx, uri).await?;
    let write_timeout = Duration::from_secs(ctx.config.write_timeout_secs);
    let chunk_size = ctx.config.response_chunk_bytes as usize;

    let row_group_ranges = if let Some(pred) = &req.predicate {
        MetadataCache::find_matching_row_groups(&metadata, pred)
    } else {
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
                ruxio_storage::metadata_cache::RowGroupRange {
                    row_group: rg_idx,
                    offset,
                    length: end - offset,
                }
            })
            .collect()
    };

    for rg_range in &row_group_ranges {
        let range_req = ReadRangeRequest {
            uri: uri.clone(),
            offset: rg_range.offset,
            length: rg_range.length,
        };
        let data = read_range(ctx, &range_req).await?;

        // Chunk large responses to enable backpressure between chunks
        if data.len() <= chunk_size {
            let chunk = Frame::new_raw(MessageType::DataChunk, request_id, data);
            if write_frame_with_timeout(stream, chunk, write_timeout)
                .await
                .is_err()
            {
                return Ok(());
            }
        } else {
            let mut offset = 0;
            while offset < data.len() {
                let end = (offset + chunk_size).min(data.len());
                let slice = data.slice(offset..end);
                let chunk = Frame::new_raw(MessageType::DataChunk, request_id, slice);
                if write_frame_with_timeout(stream, chunk, write_timeout)
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                offset = end;
            }
        }
    }

    let done = Frame::done(request_id);
    if let Err(e) = write_frame_with_timeout(stream, done, write_timeout).await {
        tracing::debug!("Failed to write Done frame: {e}");
    }
    Ok(())
}

// ── Frame processing ─────────────────────────────────────────────────

async fn process_frame(
    frame: Frame,
    ctx: &Rc<ThreadContext>,
    stream: &mut TcpStream,
) -> Vec<Frame> {
    INCOMING_REQUESTS.inc();
    ACTIVE_OPERATIONS.inc();
    let _active_guard = ActiveOpGuard;
    let _timer = RESPONSE_TIME_COLLECTOR.start_timer();
    let request_id = frame.request_id;

    // Overload rejection: if too many requests pending, reject early
    let pending = ctx.pending_requests.fetch_add(1, Ordering::Relaxed);
    let _req_guard = RequestGuard {
        counter: ctx.pending_requests.clone(),
    };
    if pending >= ctx.config.max_pending_requests {
        return vec![Frame::new_json(
            MessageType::Error,
            request_id,
            &ErrorResponse {
                code: 503,
                message: "Server overloaded, try again later".to_string(),
            },
        )];
    }

    match frame.msg_type {
        MessageType::ReadRange => handle_read_range(ctx, stream, request_id, &frame.payload).await,

        MessageType::BatchRead => {
            let req: BatchReadRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 400,
                            message: format!("Invalid batch read request: {e}"),
                        },
                    )];
                }
            };

            // Process all ranges, streaming each result immediately
            for read in &req.reads {
                let result = read_range(ctx, read).await;
                match result {
                    Ok(data) => {
                        let chunk = Frame::new_raw(MessageType::DataChunk, request_id, data);
                        let (r, _) = stream.write_all(chunk.encode().to_vec()).await;
                        if r.is_err() {
                            return vec![];
                        }
                    }
                    Err(e) => {
                        return vec![Frame::new_json(
                            MessageType::Error,
                            request_id,
                            &ErrorResponse {
                                code: 500,
                                message: format!("Batch read failed: {e}"),
                            },
                        )];
                    }
                }
            }
            vec![Frame::done(request_id)]
        }

        MessageType::GetMetadata => {
            let req: GetMetadataRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 400,
                            message: format!("Invalid metadata request: {e}"),
                        },
                    )];
                }
            };

            let result = get_metadata(ctx, &req.uri).await;
            match result {
                Ok(meta) => {
                    let meta_resp = MetadataResponse {
                        uri: req.uri,
                        file_size: meta.file_size,
                        footer_size: meta.footer_bytes.len() as u64,
                    };
                    vec![
                        Frame::new_json(MessageType::Metadata, request_id, &meta_resp),
                        Frame::new_raw(MessageType::DataChunk, request_id, meta.footer_bytes),
                        Frame::done(request_id),
                    ]
                }
                Err(e) => vec![Frame::new_json(
                    MessageType::Error,
                    request_id,
                    &ErrorResponse {
                        code: 500,
                        message: format!("Metadata fetch failed: {e}"),
                    },
                )],
            }
        }

        MessageType::Scan => {
            let req: ScanRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 400,
                            message: format!("Invalid scan request: {e}"),
                        },
                    )];
                }
            };

            if let Err(e) = scan_streaming(ctx, &req, request_id, stream).await {
                return vec![Frame::new_json(
                    MessageType::Error,
                    request_id,
                    &ErrorResponse {
                        code: 500,
                        message: format!("Scan failed: {e}"),
                    },
                )];
            }
            vec![]
        }

        MessageType::Heartbeat => {
            // Activity already tracked by the read timeout reset.
            vec![]
        }

        MessageType::Cancel => {
            tracing::debug!("Cancel received for request_id={}", frame.request_id);
            vec![]
        }

        _ => vec![Frame::new_json(
            MessageType::Error,
            request_id,
            &ErrorResponse {
                code: 400,
                message: format!("Unexpected message type: {:?}", frame.msg_type),
            },
        )],
    }
}

/// Handle a single ReadRange request.
async fn handle_read_range(
    ctx: &Rc<ThreadContext>,
    stream: &mut TcpStream,
    request_id: u32,
    payload: &Bytes,
) -> Vec<Frame> {
    let req: ReadRangeRequest = match serde_json::from_slice(payload) {
        Ok(r) => r,
        Err(e) => {
            return vec![Frame::new_json(
                MessageType::Error,
                request_id,
                &ErrorResponse {
                    code: 400,
                    message: format!("Invalid read request: {e}"),
                },
            )];
        }
    };

    // Inter-node routing
    if !ctx.membership.is_local(&req.uri) {
        if let Some(owner) = ctx.membership.owner(&req.uri) {
            let parts: Vec<&str> = owner.0.split(':').collect();
            let host = parts.first().unwrap_or(&"unknown").to_string();
            let port: u16 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(51234);
            return vec![Frame::new_json(
                MessageType::Redirect,
                request_id,
                &RedirectResponse {
                    target_host: host,
                    target_data_port: port,
                },
            )];
        }
    }

    // Intra-node routing
    let owner_thread = ctx.owning_thread(&req.uri);
    let page_size = ctx.cache_manager.borrow().page_size();
    let page_index = req.offset / page_size;

    if owner_thread == ctx.thread_id {
        // LOCAL — try zero-copy sendfile for any cached range
        let first_page = req.offset / page_size;
        let last_page = (req.offset + req.length - 1) / page_size;

        // Collect file paths for all pages in the range
        let mut slices: Vec<(std::path::PathBuf, u64, u64)> = Vec::new();
        let mut all_cached = true;

        for page_idx in first_page..=last_page {
            let file_info = ctx
                .cache_manager
                .borrow_mut()
                .get_page_file(&req.uri, page_idx);
            if let Some((file_path, file_size)) = file_info {
                let page_start = page_idx * page_size;
                let page_end = page_start + file_size;

                // Compute the slice of this page that overlaps with the request
                let slice_start = req.offset.max(page_start) - page_start;
                let slice_end = (req.offset + req.length).min(page_end) - page_start;
                let slice_len = slice_end - slice_start;

                slices.push((file_path, slice_start, slice_len));
            } else {
                all_cached = false;
                break;
            }
        }

        if all_cached && !slices.is_empty() {
            // All pages cached — sendfile each slice, zero userspace copies
            let file_slices: Vec<zero_copy::FileSlice<'_>> = slices
                .iter()
                .map(|(path, offset, length)| zero_copy::FileSlice {
                    path: path.as_path(),
                    offset: *offset,
                    length: *length,
                })
                .collect();

            if zero_copy::send_file_slices_to_socket(&file_slices, request_id, stream)
                .await
                .is_ok()
            {
                let file_id: Arc<str> = Arc::from(req.uri.as_str());
                maybe_prefetch(ctx, &file_id, last_page);
                return vec![];
            }
        }

        // Buffered fallback with send-then-cache
        let result = read_range(ctx, &req).await;
        match result {
            Ok(data) => vec![
                Frame::new_raw(MessageType::DataChunk, request_id, data),
                Frame::done(request_id),
            ],
            Err(e) => vec![Frame::new_json(
                MessageType::Error,
                request_id,
                &ErrorResponse {
                    code: 500,
                    message: format!("Read failed: {e}"),
                },
            )],
        }
    } else {
        // FORWARDED via SPSC channels
        let key = PageKey::new(&req.uri, page_index);

        if let Some((file_path, file_size)) =
            forwarding::forward_lookup(&ctx.channels, ctx.thread_id, owner_thread, key).await
        {
            if zero_copy::send_file_to_socket(&file_path, file_size, request_id, stream)
                .await
                .is_ok()
            {
                return vec![];
            }
        }

        vec![Frame::new_json(
            MessageType::Error,
            request_id,
            &ErrorResponse {
                code: 404,
                message: format!("Page not cached for {}", req.uri),
            },
        )]
    }
}
