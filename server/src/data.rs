use std::cell::RefCell;
use std::hash::{BuildHasher, Hash, Hasher};
use std::rc::Rc;

use anyhow::Result;
use bytes::Bytes;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use tracing::warn;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use ruxio_cluster::membership::ClusterMembership;
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::{
    ErrorResponse, GetMetadataRequest, MetadataResponse, ReadRangeRequest, RedirectResponse,
    ScanRequest,
};
use ruxio_storage::cache::{CacheManager, RangeResult};
use ruxio_storage::forwarding::{self, Inbox};
use ruxio_storage::gcs::GcsClient;
use ruxio_storage::metadata_cache::{CachedParquetMeta, MetadataCache};
use ruxio_storage::page_key::PageKey;
use ruxio_storage::zero_copy;

/// Per-thread context holding the cache, GCS client, and inboxes.
///
/// CacheManager is sync-only (behind RefCell). GcsClient is stateless (&self)
/// so it's behind Rc — no RefCell needed. This ensures we never hold a
/// RefCell borrow across an await point.
pub struct ThreadContext {
    pub cache_manager: Rc<RefCell<CacheManager>>,
    pub gcs: Rc<GcsClient>,
    pub membership: Rc<ClusterMembership>,
    pub thread_id: usize,
    pub num_threads: usize,
    /// Inboxes for all threads — index by owning thread id.
    pub inboxes: Vec<Inbox>,
}

impl ThreadContext {
    /// Determine which thread owns a given file URI.
    fn owning_thread(&self, uri: &str) -> usize {
        let mut hasher = Xxh3DefaultBuilder.build_hasher();
        uri.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_threads
    }
}

/// Handle a single TCP connection.
pub async fn serve_connection(stream: TcpStream, ctx: Rc<ThreadContext>) {
    let mut stream = stream;
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

/// Read a byte range — checks cache (sync), fetches GCS on miss (async),
/// inserts into cache (sync). RefCell is never held across await.
async fn read_range(ctx: &ThreadContext, req: &ReadRangeRequest) -> Result<Bytes> {
    // Phase 1: check cache (sync borrow, dropped immediately)
    let cache_result = ctx.cache_manager.borrow_mut().read_range_cached(req);

    match cache_result {
        RangeResult::Hit(data) => Ok(data), // full cache hit (zero-copy for single pages)
        RangeResult::Miss { misses } => {
            // Phase 2: fetch missing pages from GCS (no borrow held)
            let page_size = ctx.cache_manager.borrow().page_size();
            for (key, page_offset) in &misses {
                let fetch_end = page_offset + page_size;
                let data = ctx.gcs.get_range(&req.uri, *page_offset..fetch_end).await?;

                // Phase 3: insert into cache (sync borrow, dropped immediately)
                ctx.cache_manager.borrow_mut().cache_page(key, &data);
            }

            // Phase 4: assemble result from now-cached pages (sync borrow)
            Ok(ctx
                .cache_manager
                .borrow_mut()
                .read_range_finish(req, &misses))
        }
    }
}

/// Fetch metadata — checks cache (sync), fetches GCS on miss (async).
async fn get_metadata(ctx: &ThreadContext, uri: &str) -> Result<CachedParquetMeta> {
    // Phase 1: check metadata cache (sync borrow)
    if let Some(meta) = ctx.cache_manager.borrow_mut().get_metadata_cached(uri) {
        return Ok(meta);
    }

    // Phase 2: fetch from GCS (no borrow held)
    let head = ctx.gcs.head(uri).await?;
    let footer_size = 64 * 1024u64;
    let footer_start = head.size.saturating_sub(footer_size);
    let footer_bytes = ctx.gcs.get_range(uri, footer_start..head.size).await?;

    // Phase 3: parse and cache (sync borrow)
    let cached = ctx.cache_manager.borrow_mut().parse_and_cache_metadata(
        uri,
        footer_bytes,
        head.size,
        head.etag,
    )?;
    Ok(cached)
}

async fn process_frame(
    frame: Frame,
    ctx: &Rc<ThreadContext>,
    stream: &mut TcpStream,
) -> Vec<Frame> {
    let request_id = frame.request_id;

    match frame.msg_type {
        MessageType::ReadRange => {
            let req: ReadRangeRequest = match serde_json::from_slice(&frame.payload) {
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

            // Check cluster-level consistent hash ring (inter-node routing)
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

            // Intra-node routing: which thread owns this file?
            let owner_thread = ctx.owning_thread(&req.uri);
            let page_size = ctx.cache_manager.borrow().page_size();
            let page_index = req.offset / page_size;

            if owner_thread == ctx.thread_id {
                // LOCAL: this thread owns the file — direct serve
                // Try zero-copy sendfile for page-aligned requests
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
                            return vec![];
                        }
                    }
                }

                // Buffered fallback — borrow-safe across await
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
                // FORWARDED: another thread owns this file
                let key = PageKey::new(&req.uri, page_index);
                let inbox = &ctx.inboxes[owner_thread];

                if let Some((file_path, file_size)) = forwarding::forward_lookup(inbox, key).await {
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

            // Borrow-safe: get_metadata handles borrow scoping internally
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

            // Borrow-safe scan: metadata fetch + page reads all scope borrows correctly
            let result = scan(ctx, &req).await;
            match result {
                Ok(pages) => {
                    let mut frames = Vec::with_capacity(pages.len() + 1);
                    for page_data in pages {
                        frames.push(Frame::new_raw(
                            MessageType::DataChunk,
                            request_id,
                            page_data,
                        ));
                    }
                    frames.push(Frame::done(request_id));
                    frames
                }
                Err(e) => vec![Frame::new_json(
                    MessageType::Error,
                    request_id,
                    &ErrorResponse {
                        code: 500,
                        message: format!("Scan failed: {e}"),
                    },
                )],
            }
        }

        _ => {
            vec![Frame::new_json(
                MessageType::Error,
                request_id,
                &ErrorResponse {
                    code: 400,
                    message: format!("Unexpected message type: {:?}", frame.msg_type),
                },
            )]
        }
    }
}

/// Scan with predicate pushdown. Borrow-safe across all await points.
async fn scan(ctx: &ThreadContext, req: &ScanRequest) -> Result<Vec<Bytes>> {
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

    let mut result = Vec::with_capacity(row_group_ranges.len());
    for rg_range in &row_group_ranges {
        let range_req = ReadRangeRequest {
            uri: uri.clone(),
            offset: rg_range.offset,
            length: rg_range.length,
        };
        let data = read_range(ctx, &range_req).await?;
        result.push(data);
    }

    Ok(result)
}
