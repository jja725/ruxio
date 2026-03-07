use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use tracing::warn;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use ruxio_cluster::membership::ClusterMembership;
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
use ruxio_storage::zero_copy;

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
}

impl ThreadContext {
    fn owning_thread(&self, uri: &str) -> usize {
        let mut hasher = Xxh3DefaultBuilder.build_hasher();
        uri.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_threads
    }
}

// ── Connection handling ──────────────────────────────────────────────

pub async fn serve_connection(stream: TcpStream, ctx: Rc<ThreadContext>) {
    let mut stream = stream;
    let _ = stream.set_nodelay(true);
    let mut reader = FrameReader::new();
    let mut buf = vec![0u8; 128 * 1024];

    loop {
        let (result, read_buf) = stream.read(buf).await;
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

        while let Some(frame) = reader.next_frame().unwrap() {
            let response_frames = process_frame(frame, &ctx, &mut stream).await;
            for resp in response_frames {
                let encoded = resp.encode();
                let (result, _) = stream.write_all(encoded.to_vec()).await;
                if result.is_err() {
                    return;
                }
            }
        }
    }
}

// ── Thundering herd ──────────────────────────────────────────────────

/// Maximum time to wait for an in-flight fetch before giving up.
/// Prevents infinite wait if the fetching task panics or gets stuck.
const INFLIGHT_TIMEOUT: Duration = Duration::from_secs(30);

/// Wait for an in-flight fetch to complete, with timeout.
/// Returns true if the fetch completed, false if timed out.
async fn wait_for_inflight(ctx: &ThreadContext, key: &PageKey) -> bool {
    let deadline = std::time::Instant::now() + INFLIGHT_TIMEOUT;
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

const PREFETCH_PAGES: u64 = 4;

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

    for i in 1..=PREFETCH_PAGES {
        let prefetch_idx = current_page + i;
        let key = PageKey::with_arc(file_id.clone(), prefetch_idx);

        let already_cached = ctx.cache_manager.borrow_mut().page_cache.contains(&key);
        let already_inflight = ctx.inflight.borrow().contains(&key);

        if already_cached || already_inflight {
            continue;
        }

        ctx.inflight.borrow_mut().insert(key.clone());

        let ctx = ctx.clone();
        let file_id = file_id.clone();
        monoio::spawn(async move {
            // Guard ensures inflight entry is removed even on panic
            let _guard = InflightGuard {
                inflight: ctx.inflight.clone(),
                key: key.clone(),
            };
            let page_offset = prefetch_idx * page_size;
            let fetch_end = page_offset + page_size;
            if let Ok(data) = ctx.gcs.get_range(&file_id, page_offset..fetch_end).await {
                ctx.cache_manager.borrow_mut().cache_page(&key, &data);
            }
            // _guard dropped here → inflight.remove(&key)
        });
    }
}

// ── Core read path (send-then-cache) ─────────────────────────────────

/// Read a byte range. On cache miss, fetches from GCS and returns data
/// immediately — disk caching happens asynchronously in the background.
async fn read_range(ctx: &Rc<ThreadContext>, req: &ReadRangeRequest) -> Result<Bytes> {
    let cache_result = ctx.cache_manager.borrow_mut().read_range_cached(req);

    match cache_result {
        RangeResult::Hit(data) => {
            let page_size = ctx.cache_manager.borrow().page_size();
            let file_id: Arc<str> = Arc::from(req.uri.as_str());
            maybe_prefetch(ctx, &file_id, req.offset / page_size);
            Ok(data)
        }
        RangeResult::Miss { misses, coalesced } => {
            let page_size = ctx.cache_manager.borrow().page_size();
            let file_id: Arc<str> = Arc::from(req.uri.as_str());

            // Fetch from GCS using coalesced ranges, keep data in memory
            let mut fetched: HashMap<u64, Bytes> = HashMap::new();

            for range in &coalesced {
                // Thundering herd: wait for pages already being fetched
                let mut needs_fetch = Vec::new();
                for key in &range.page_keys {
                    if ctx.inflight.borrow().contains(key) {
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
                let data = match ctx
                    .gcs
                    .get_range(&req.uri, range.start_offset..range.end_offset)
                    .await
                {
                    Ok(d) => d,
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
                        Bytes::from(std::fs::read(&path).unwrap_or_default())
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
async fn get_metadata(ctx: &ThreadContext, uri: &str) -> Result<CachedParquetMeta> {
    if let Some(meta) = ctx.cache_manager.borrow_mut().get_metadata_cached(uri) {
        return Ok(meta);
    }

    let head = ctx.gcs.head(uri).await?;
    let footer_size = 64 * 1024u64;
    let footer_start = head.size.saturating_sub(footer_size);
    let footer_bytes = ctx.gcs.get_range(uri, footer_start..head.size).await?;

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

        let chunk = Frame::new_raw(MessageType::DataChunk, request_id, data);
        let (result, _) = stream.write_all(chunk.encode().to_vec()).await;
        if result.is_err() {
            return Ok(());
        }
    }

    let done = Frame::done(request_id);
    let (result, _) = stream.write_all(done.encode().to_vec()).await;
    let _ = result;
    Ok(())
}

// ── Frame processing ─────────────────────────────────────────────────

async fn process_frame(
    frame: Frame,
    ctx: &Rc<ThreadContext>,
    stream: &mut TcpStream,
) -> Vec<Frame> {
    let request_id = frame.request_id;

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
        // LOCAL — try zero-copy sendfile first
        if req.length == page_size && req.offset == page_index * page_size {
            let file_info = ctx
                .cache_manager
                .borrow_mut()
                .get_page_file(&req.uri, page_index);
            if let Some((file_path, file_size)) = file_info {
                if zero_copy::send_file_to_socket(&file_path, file_size, request_id, stream)
                    .await
                    .is_ok()
                {
                    let file_id: Arc<str> = Arc::from(req.uri.as_str());
                    maybe_prefetch(ctx, &file_id, page_index);
                    return vec![];
                }
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
