use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
    METADATA_CACHE_HITS, METADATA_CACHE_MISSES, RESPONSE_TIME_COLLECTOR,
};
use ruxio_protocol::error_code;
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::{
    BatchReadRequest, ErrorResponse, GetMetadataRequest, MetadataResponse, ReadRangeRequest,
    ScanRequest,
};
use ruxio_storage::cache::{CacheManager, RangeResult};
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::error::{DiskIoSnafu, PageAssemblySnafu, StorageError};
use ruxio_storage::forwarding::{self, ChannelMatrix};
use ruxio_storage::gcs::GcsClient;
use ruxio_storage::metadata_cache::{CachedParquetMeta, MetadataCache};
use ruxio_storage::page_key::PageKey;
use ruxio_storage::retry::RetryPolicy;
use ruxio_storage::zero_copy;
use snafu::IntoError;

// ── Server configuration ────────────────────────────────────────────

/// Initial receive buffer size for the binary data plane.
const RECV_BUFFER_BYTES: usize = 128 * 1024;

/// Parquet footer read size — large enough for most footers in a single fetch.
const FOOTER_FETCH_BYTES: u64 = 64 * 1024;

/// Runtime-configurable server settings (passed from config file).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Idle timeout per connection (default 300s).
    pub idle_timeout_secs: u64,
    /// Thundering herd wait timeout (default 30s).
    pub inflight_timeout_secs: u64,
    /// Max concurrent connections per worker thread (default 10000).
    pub max_connections_per_thread: u32,
    /// Per-write timeout in seconds (default 60s).
    pub write_timeout_secs: u64,
    /// Max pending requests per thread before returning RESOURCE_EXHAUSTED (default 1000).
    pub max_pending_requests: u32,
    /// Response chunk size for large reads (default 4MB).
    pub response_chunk_bytes: u64,
    /// Write buffer high water mark (bytes). Default 64KB.
    pub write_buffer_high: u64,
    /// Write buffer low water mark (bytes). Default 32KB.
    pub write_buffer_low: u64,
    /// Forward lookup timeout in seconds (default 5).
    pub forward_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            idle_timeout_secs: 300,
            inflight_timeout_secs: 30,
            max_connections_per_thread: 10_000,
            write_timeout_secs: 60,
            max_pending_requests: 1_000,
            response_chunk_bytes: 4 * 1024 * 1024,
            write_buffer_high: 64 * 1024,
            write_buffer_low: 32 * 1024,
            forward_timeout_secs: 5,
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

    /// Retry policy for GCS operations.
    pub retry_policy: RetryPolicy,

    /// Shutdown flag — set by signal handler.
    pub shutdown: Arc<AtomicBool>,

    /// Server configuration.
    pub config: ServerConfig,

    /// Active connection count for this thread.
    pub active_connections: Rc<AtomicU32>,

    /// Active in-flight request count for overload rejection.
    pub pending_requests: Rc<AtomicU32>,
}

impl ThreadContext {
    fn owning_thread(&self, uri: &str) -> usize {
        (Xxh3DefaultBuilder.hash_one(uri) as usize) % self.num_threads
    }
}

// ── Request state machine ───────────────────────────────────────────
//
// Tracks request lifecycle to prevent silent failures.
//
// Key invariant: if a request enters STREAMING state (partial data
// sent to client), any subsequent error MUST send an Error frame
// before the request can complete.

/// Request processing state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestState {
    Idle,
    Streaming,
    Completed,
    Errored,
}

/// Tracks a single request's lifecycle.
struct RequestTracker {
    state: RequestState,
    request_id: u32,
}

impl RequestTracker {
    fn new(request_id: u32) -> Self {
        Self {
            state: RequestState::Idle,
            request_id,
        }
    }

    fn enter_streaming(&mut self) {
        if self.state == RequestState::Idle {
            self.state = RequestState::Streaming;
        }
    }

    fn complete(&mut self) {
        self.state = RequestState::Completed;
    }

    fn error(&mut self) {
        self.state = RequestState::Errored;
    }

    fn is_streaming(&self) -> bool {
        self.state == RequestState::Streaming
    }

    fn error_frame(
        &self,
        error_code: ruxio_protocol::error_code::ErrorCode,
        message: String,
    ) -> Frame {
        error_response_frame(self.request_id, error_code, message)
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

/// Build an error response frame with structured error code.
fn error_response_frame(
    request_id: u32,
    error_code: ruxio_protocol::error_code::ErrorCode,
    message: String,
) -> Frame {
    Frame::new_json_unchecked(
        MessageType::Error,
        request_id,
        &ErrorResponse {
            error_code,
            message,
        },
    )
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
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!("set_nodelay failed: {e}");
    }
    let mut reader = FrameReader::new();
    let mut buf = vec![0u8; RECV_BUFFER_BYTES];
    let idle_timeout = Duration::from_secs(ctx.config.idle_timeout_secs);
    let write_timeout = Duration::from_secs(ctx.config.write_timeout_secs);
    let high_water = ctx.config.write_buffer_high;
    let low_water = ctx.config.write_buffer_low;
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
                    if response_frames.is_empty() {
                        continue;
                    }

                    // Flush consolidation: encode all response frames into a
                    // single buffer and write once, reducing syscalls.
                    // For a typical ReadRange response (DataChunk + Done),
                    // this turns 2 write() syscalls into 1.
                    let mut batch = Vec::new();
                    for resp in &response_frames {
                        let encoded = resp.encode();
                        batch.extend_from_slice(&encoded);
                    }
                    let batch_len = batch.len() as u64;

                    // Watermark-based backpressure (hysteresis):
                    // Approximation: after sleeping, assume the kernel drained
                    // at least low_water bytes. Accurate tracking would require
                    // ioctl(TIOCOUTQ) which monoio doesn't expose yet.
                    if backpressured {
                        monoio::time::sleep(Duration::from_millis(1)).await;
                        conn_bytes_pending = conn_bytes_pending.saturating_sub(low_water);
                        if conn_bytes_pending <= low_water {
                            backpressured = false;
                        }
                    } else if conn_bytes_pending >= high_water {
                        backpressured = true;
                        monoio::time::sleep(Duration::from_millis(1)).await;
                        conn_bytes_pending = conn_bytes_pending.saturating_sub(low_water);
                    }

                    // Single batched write with timeout
                    match monoio::time::timeout(write_timeout, stream.write_all(batch)).await {
                        Ok((Ok(_), _)) => conn_bytes_pending += batch_len,
                        Ok((Err(e), _)) => {
                            tracing::debug!("Write error: {e}");
                            return;
                        }
                        Err(_) => {
                            tracing::debug!("Write timeout");
                            return;
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
async fn wait_for_inflight(ctx: &ThreadContext, key: &PageKey) -> bool {
    let timeout = Duration::from_secs(ctx.config.inflight_timeout_secs);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if !ctx.inflight.borrow().contains(key) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            ctx.inflight.borrow_mut().remove(key);
            return false;
        }
        monoio::time::sleep(Duration::from_micros(100)).await;
    }
}

/// RAII guard that removes a PageKey from the inflight set on drop.
struct InflightGuard {
    inflight: Rc<RefCell<HashSet<PageKey>>>,
    key: PageKey,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.inflight.borrow_mut().remove(&self.key);
    }
}

// ── Core read path (send-then-cache) ─────────────────────────────────

/// Read a byte range. On cache miss, fetches from GCS and returns data
/// immediately — disk caching happens asynchronously in the background.
pub(crate) async fn read_range(
    ctx: &Rc<ThreadContext>,
    req: &ReadRangeRequest,
) -> std::result::Result<Bytes, StorageError> {
    let cache_result = ctx.cache_manager.borrow_mut().read_range_cached(req)?;

    match cache_result {
        RangeResult::Hit(data) => {
            CACHE_HIT_COUNTER.inc();
            let data_len = data.len() as f64;
            BYTES_READ_CACHE.inc_by(data_len);
            BYTES_SERVED_TOTAL.inc_by(data_len);
            let _cache_timer = CACHE_READ_LATENCY.start_timer();
            Ok(data)
        }
        RangeResult::Miss { coalesced, .. } => {
            CACHE_MISS_COUNTER.inc();
            let page_size = ctx.cache_manager.borrow().page_size();
            let file_id: Arc<str> = Arc::from(req.uri.as_str());

            let mut fetched: HashMap<u64, Bytes> = HashMap::new();

            for range in &coalesced {
                let mut needs_fetch = Vec::new();
                for key in &range.page_keys {
                    if ctx.inflight.borrow().contains(key) {
                        INFLIGHT_COALESCED.inc();
                        wait_for_inflight(ctx, key).await;
                    } else if !ctx.cache_manager.borrow_mut().page_cache.contains(key) {
                        needs_fetch.push(key.clone());
                    }
                }

                if needs_fetch.is_empty() {
                    continue;
                }

                for key in &needs_fetch {
                    ctx.inflight.borrow_mut().insert(key.clone());
                }

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
                        for key in &needs_fetch {
                            ctx.inflight.borrow_mut().remove(key);
                        }
                        return Err(e);
                    }
                };

                for key in &needs_fetch {
                    let offset_in_range =
                        (key.page_index * page_size).saturating_sub(range.start_offset) as usize;
                    let end = (offset_in_range + page_size as usize).min(data.len());
                    if offset_in_range < data.len() {
                        let page_data = data.slice(offset_in_range..end);
                        fetched.insert(key.page_index, page_data.clone());

                        let cm = ctx.cache_manager.clone();
                        let inflight = ctx.inflight.clone();
                        let key_owned = key.clone();
                        monoio::spawn(async move {
                            let _guard = InflightGuard {
                                inflight,
                                key: key_owned.clone(),
                            };
                            cm.borrow_mut().cache_page(&key_owned, &page_data);
                        });
                    } else {
                        ctx.inflight.borrow_mut().remove(key);
                    }
                }
            }

            // Assemble result
            let start = req.offset;
            let end = req.offset + req.length;
            let first_page = start / page_size;
            let last_page_idx = (end - 1) / page_size;

            if first_page == last_page_idx {
                if let Some(data) = fetched.get(&first_page) {
                    let slice_start = (start - first_page * page_size) as usize;
                    let slice_end = (end - first_page * page_size) as usize;
                    if start == first_page * page_size && end == (first_page + 1) * page_size {
                        return Ok(data.clone());
                    }
                    return Ok(data.slice(slice_start..slice_end.min(data.len())));
                }
            }

            let mut result = Vec::with_capacity(req.length as usize);
            for page_idx in first_page..=last_page_idx {
                let page_offset = page_idx * page_size;
                let page_data = if let Some(data) = fetched.get(&page_idx) {
                    data.clone()
                } else {
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
                                if data.len() as u64 > page_size * 2 {
                                    CACHE_GET_ERRORS.inc();
                                    warn!(
                                        "Corrupt page detected: {} (size {} > expected {})",
                                        path.display(),
                                        data.len(),
                                        page_size
                                    );
                                    ctx.cache_manager.borrow_mut().page_cache.remove(&key);
                                    return Err(PageAssemblySnafu {
                                        detail: format!(
                                            "corrupt page {} (size {} > expected {})",
                                            path.display(),
                                            data.len(),
                                            page_size
                                        ),
                                    }
                                    .build());
                                }
                                Bytes::from(data)
                            }
                            Err(e) => {
                                CACHE_GET_ERRORS.inc();
                                ctx.cache_manager.borrow_mut().page_cache.remove(&key);
                                return Err(DiskIoSnafu {
                                    path: path.display().to_string(),
                                }
                                .into_error(e));
                            }
                        }
                    } else {
                        return Err(PageAssemblySnafu {
                            detail: format!("page {} not found in cache for {}", page_idx, req.uri),
                        }
                        .build());
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
pub(crate) async fn get_metadata(
    ctx: &ThreadContext,
    uri: &str,
) -> std::result::Result<CachedParquetMeta, StorageError> {
    if let Some(meta) = ctx.cache_manager.borrow_mut().get_metadata_cached(uri) {
        METADATA_CACHE_HITS.inc();
        return Ok(meta);
    }

    METADATA_CACHE_MISSES.inc();

    let head = ctx.gcs.head_with_retry(uri, &ctx.retry_policy).await?;
    let footer_size = FOOTER_FETCH_BYTES;
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
) -> std::result::Result<(), StorageError> {
    let uri = &req.uri;
    let metadata = get_metadata(ctx, uri).await?;
    let write_timeout = Duration::from_secs(ctx.config.write_timeout_secs);
    let chunk_size = ctx.config.response_chunk_bytes as usize;
    let mut tracker = RequestTracker::new(request_id);

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
        let data = match read_range(ctx, &range_req).await {
            Ok(d) => d,
            Err(e) => {
                if tracker.is_streaming() {
                    let err_frame =
                        tracker.error_frame(e.to_error_code(), format!("Scan read failed: {e}"));
                    let _ = write_frame_with_timeout(stream, err_frame, write_timeout).await;
                }
                tracker.error();
                return Err(e);
            }
        };

        tracker.enter_streaming();

        if data.len() <= chunk_size {
            let chunk = Frame::new_raw(MessageType::DataChunk, request_id, data);
            if write_frame_with_timeout(stream, chunk, write_timeout)
                .await
                .is_err()
            {
                let err_frame = tracker.error_frame(
                    error_code::write_error(),
                    "Write failed during scan".to_string(),
                );
                let _ = write_frame_with_timeout(stream, err_frame, write_timeout).await;
                tracker.error();
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
                    let err_frame = tracker.error_frame(
                        error_code::write_error(),
                        "Write failed during scan".to_string(),
                    );
                    let _ = write_frame_with_timeout(stream, err_frame, write_timeout).await;
                    tracker.error();
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
    tracker.complete();
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

    // Overload rejection
    let pending = ctx.pending_requests.fetch_add(1, Ordering::Relaxed);
    let _req_guard = RequestGuard {
        counter: ctx.pending_requests.clone(),
    };
    if pending >= ctx.config.max_pending_requests {
        return vec![Frame::new_json_unchecked(
            MessageType::Error,
            request_id,
            &ErrorResponse {
                error_code: error_code::server_overloaded(),
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
                    return vec![Frame::new_json_unchecked(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            error_code: error_code::invalid_request(),
                            message: format!("Invalid batch read request: {e}"),
                        },
                    )];
                }
            };

            let write_timeout = Duration::from_secs(ctx.config.write_timeout_secs);
            let mut tracker = RequestTracker::new(request_id);

            for read in &req.reads {
                match read_range(ctx, read).await {
                    Ok(data) => {
                        tracker.enter_streaming();
                        let chunk = Frame::new_raw(MessageType::DataChunk, request_id, data);
                        if write_frame_with_timeout(stream, chunk, write_timeout)
                            .await
                            .is_err()
                        {
                            tracker.error();
                            return vec![tracker.error_frame(
                                error_code::write_error(),
                                "Write failed during batch read".to_string(),
                            )];
                        }
                    }
                    Err(e) => {
                        tracker.error();
                        return vec![error_response_frame(
                            request_id,
                            e.to_error_code(),
                            format!("Batch read failed: {e}"),
                        )];
                    }
                }
            }
            tracker.complete();
            vec![Frame::done(request_id)]
        }

        MessageType::GetMetadata => {
            let req: GetMetadataRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![error_response_frame(
                        request_id,
                        error_code::invalid_request(),
                        format!("Invalid metadata request: {e}"),
                    )];
                }
            };

            match get_metadata(ctx, &req.uri).await {
                Ok(meta) => {
                    let meta_resp = MetadataResponse {
                        uri: req.uri,
                        file_size: meta.file_size,
                        footer_size: meta.footer_bytes.len() as u64,
                    };
                    vec![
                        Frame::new_json_unchecked(MessageType::Metadata, request_id, &meta_resp),
                        Frame::new_raw(MessageType::DataChunk, request_id, meta.footer_bytes),
                        Frame::done(request_id),
                    ]
                }
                Err(e) => vec![error_response_frame(
                    request_id,
                    e.to_error_code(),
                    format!("Metadata fetch failed: {e}"),
                )],
            }
        }

        MessageType::Scan => {
            let req: ScanRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![error_response_frame(
                        request_id,
                        error_code::invalid_request(),
                        format!("Invalid scan request: {e}"),
                    )];
                }
            };

            if let Err(e) = scan_streaming(ctx, &req, request_id, stream).await {
                return vec![error_response_frame(
                    request_id,
                    e.to_error_code(),
                    format!("Scan failed: {e}"),
                )];
            }
            vec![]
        }

        MessageType::Heartbeat => vec![],

        MessageType::Cancel => {
            tracing::debug!("Cancel received for request_id={}", frame.request_id);
            vec![]
        }

        // Response types should never be received by the server.
        MessageType::DataChunk
        | MessageType::Error
        | MessageType::Redirect
        | MessageType::Done
        | MessageType::Metadata
        | MessageType::BatchScan => vec![error_response_frame(
            request_id,
            error_code::not_supported(),
            format!("Unexpected message type: {:?}", frame.msg_type),
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
            return vec![error_response_frame(
                request_id,
                error_code::invalid_request(),
                format!("Invalid read request: {e}"),
            )];
        }
    };

    // Intra-node routing
    let owner_thread = ctx.owning_thread(&req.uri);
    let page_size = ctx.cache_manager.borrow().page_size();
    let page_index = req.offset / page_size;

    if owner_thread == ctx.thread_id {
        // LOCAL — try zero-copy sendfile for any cached range
        let first_page = req.offset / page_size;
        let last_page = (req.offset + req.length - 1) / page_size;

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
            let file_slices: Vec<zero_copy::FileSlice<'_>> = slices
                .iter()
                .map(|(path, offset, length)| zero_copy::FileSlice {
                    path: path.as_path(),
                    offset: *offset,
                    length: *length,
                })
                .collect();

            match zero_copy::send_file_slices_to_socket(&file_slices, request_id, stream).await {
                Ok(_) => return vec![],
                Err(e) => {
                    warn!("Sendfile failed for {}: {e}", req.uri);
                    return vec![error_response_frame(
                        request_id,
                        error_code::sendfile_error(),
                        format!("Sendfile failed: {e}"),
                    )];
                }
            }
        }

        // Buffered fallback
        match read_range(ctx, &req).await {
            Ok(data) => vec![
                Frame::new_raw(MessageType::DataChunk, request_id, data),
                Frame::done(request_id),
            ],
            Err(e) => vec![error_response_frame(
                request_id,
                e.to_error_code(),
                format!("Read failed: {e}"),
            )],
        }
    } else {
        // FORWARDED via SPSC channels (with timeout)
        let key = PageKey::new(&req.uri, page_index);
        let forward_timeout = Duration::from_secs(ctx.config.forward_timeout_secs);

        match monoio::time::timeout(
            forward_timeout,
            forwarding::forward_lookup(&ctx.channels, ctx.thread_id, owner_thread, key),
        )
        .await
        {
            Ok(Some((file_path, file_size))) => {
                match zero_copy::send_file_to_socket(&file_path, file_size, request_id, stream)
                    .await
                {
                    Ok(_) => return vec![],
                    Err(e) => {
                        warn!("Forwarded sendfile failed for {}: {e}", req.uri);
                        return vec![error_response_frame(
                            request_id,
                            error_code::sendfile_error(),
                            format!("Sendfile failed: {e}"),
                        )];
                    }
                }
            }
            Ok(None) => {}
            Err(_) => {
                warn!("Forward lookup timeout for {}", req.uri);
            }
        }

        vec![error_response_frame(
            request_id,
            error_code::page_not_cached(),
            format!("Page not cached for {}", req.uri),
        )]
    }
}
