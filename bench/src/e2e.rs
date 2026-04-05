use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use ruxio_cluster::membership::ClusterMembership;
use ruxio_cluster::ring::NodeId;
use ruxio_cluster::static_membership::StaticMembership;
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::ReadRangeRequest;
use ruxio_storage::cache::{CacheManager, RangeResult};
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::metadata_cache::MetadataCache;
use ruxio_storage::page_cache::{CachedPage, PageCache};
use ruxio_storage::page_key::PageKey;
use ruxio_storage::zero_copy;

const PAGE_SIZE: u64 = 4 * 1024 * 1024; // 4MB
const NUM_PAGES: u64 = 100;
const BASE_PORT: u16 = 19900;

/// Pre-populate the page cache with test data on disk.
fn populate_cache(page_cache: &mut PageCache) {
    let page_size = page_cache.page_size();
    let data = vec![0xABu8; page_size as usize];
    for i in 0..NUM_PAGES {
        let key = PageKey::new("test://file.parquet", i);
        let local_path = page_cache
            .write_page_to_disk(&key, &data)
            .expect("failed to write test page to disk");
        let page = CachedPage {
            local_path,
            size: page_size,
        };
        page_cache.put(key, page);
    }
}

// ── Server ───────────────────────────────────────────────────────────

fn start_server_thread(port: u16, use_zero_copy: bool) {
    std::thread::spawn(move || {
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async move {
            // Each server thread gets its own cache partition (thread-per-core, no sharing)
            let metadata_cache = MetadataCache::new(1000);
            let mut page_cache = PageCache::new(
                format!("/tmp/ruxio_e2e_test/{port}"),
                2 * 1024 * 1024 * 1024,
                PAGE_SIZE,
            );
            populate_cache(&mut page_cache);
            let cache_manager = CacheManager::new(metadata_cache, page_cache);
            let self_id = NodeId::new("127.0.0.1", port);
            let membership =
                ClusterMembership::new(Box::new(StaticMembership::new(self_id, vec![])), 150);

            // Rc<RefCell<>> for sharing within the same monoio thread (no Send needed)
            let cm = Rc::new(RefCell::new(cache_manager));
            let mem = Rc::new(membership);

            let addr = format!("127.0.0.1:{port}");
            let listener = monoio::net::TcpListener::bind(&addr).unwrap();

            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let cm = cm.clone();
                let mem = mem.clone();
                monoio::spawn(async move {
                    serve_connection(stream, cm, mem, use_zero_copy).await;
                });
            }
        });
    });
}

async fn serve_connection(
    mut stream: monoio::net::TcpStream,
    cache_manager: Rc<RefCell<CacheManager>>,
    _membership: Rc<ClusterMembership>,
    use_zero_copy: bool,
) {
    let _ = stream.set_nodelay(true);
    let mut reader = FrameReader::new();
    let mut buf = vec![0u8; 128 * 1024];

    loop {
        let (result, read_buf) = stream.read(buf).await;
        buf = read_buf;
        let n = match result {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        reader.feed(&buf[..n]);

        while let Some(frame) = reader.next_frame().unwrap() {
            let rid = frame.request_id;
            if frame.msg_type != MessageType::ReadRange {
                let done = Frame::done(rid);
                let (r, _) = stream.write_all(done.encode().to_vec()).await;
                if r.is_err() {
                    return;
                }
                continue;
            }

            let req: ReadRangeRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(_) => continue,
            };

            // Zero-copy path
            if use_zero_copy {
                let page_index = req.offset / PAGE_SIZE;
                if req.length == PAGE_SIZE && req.offset == page_index * PAGE_SIZE {
                    let file_info = cache_manager
                        .borrow_mut()
                        .get_page_file(&req.uri, page_index);
                    if let Some((file_path, file_size)) = file_info {
                        if zero_copy::send_file_to_socket(&file_path, file_size, rid, &mut stream)
                            .await
                            .is_ok()
                        {
                            continue;
                        }
                    }
                }
            }

            // Buffered path (cache-only, no GCS in bench)
            let result = cache_manager.borrow_mut().read_range_cached(&req);
            match result {
                Ok(RangeResult::Hit(data)) => {
                    let chunk = Frame::new_raw(MessageType::DataChunk, rid, data);
                    let (r, _) = stream.write_all(chunk.encode().to_vec()).await;
                    if r.is_err() {
                        return;
                    }
                    let done = Frame::done(rid);
                    let (r, _) = stream.write_all(done.encode().to_vec()).await;
                    if r.is_err() {
                        return;
                    }
                }
                Ok(RangeResult::Miss { .. }) | Err(_) => {
                    let done = Frame::done(rid);
                    let (r, _) = stream.write_all(done.encode().to_vec()).await;
                    if r.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

// ── Client ───────────────────────────────────────────────────────────

fn start_client_thread(
    port: u16,
    _num_requests: u64,
    total_bytes: Arc<AtomicU64>,
    total_ops: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    duration_secs: u64,
) {
    std::thread::spawn(move || {
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async move {
            // Wait for server
            let mut stream;
            loop {
                match TcpStream::connect(format!("127.0.0.1:{port}")).await {
                    Ok(s) => {
                        stream = s;
                        let _ = stream.set_nodelay(true);
                        break;
                    }
                    Err(_) => {
                        monoio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }

            // Warmup
            for i in 0..20u64 {
                let _ = send_and_recv(&mut stream, i as u32, (i % NUM_PAGES) * PAGE_SIZE).await;
            }

            // Signal ready — wait for all clients
            while !running.load(Ordering::Relaxed) {
                monoio::time::sleep(Duration::from_millis(1)).await;
            }

            let deadline = Instant::now() + Duration::from_secs(duration_secs);
            let mut i = 0u64;

            while Instant::now() < deadline {
                let page_offset = (i % NUM_PAGES) * PAGE_SIZE;
                let bytes = send_and_recv(&mut stream, i as u32, page_offset).await;
                total_bytes.fetch_add(bytes, Ordering::Relaxed);
                total_ops.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        });
    });
}

async fn send_and_recv(stream: &mut TcpStream, request_id: u32, page_offset: u64) -> u64 {
    let req = ReadRangeRequest {
        uri: "test://file.parquet".into(),
        offset: page_offset,
        length: PAGE_SIZE,
    };
    let frame = Frame::new_json_unchecked(MessageType::ReadRange, request_id, &req);
    let encoded = frame.encode();
    let (result, _) = stream.write_all(encoded.to_vec()).await;
    if result.is_err() {
        return 0;
    }

    let mut reader = FrameReader::new();
    let mut bytes_received: u64 = 0;
    let mut buf = vec![0u8; PAGE_SIZE as usize + 4096];

    loop {
        let (result, read_buf) = stream.read(buf).await;
        buf = read_buf;
        let n = match result {
            Ok(0) | Err(_) => return bytes_received,
            Ok(n) => n,
        };
        reader.feed(&buf[..n]);

        while let Some(resp) = reader.next_frame().unwrap() {
            match resp.msg_type {
                MessageType::DataChunk => bytes_received += resp.payload.len() as u64,
                MessageType::Done => return bytes_received,
                _ => {}
            }
        }
    }
}

// ── Main ─────────────────────────────────────────────────────────────

fn main() {
    let num_server_threads: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let num_client_conns: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(num_server_threads);
    let duration_secs: u64 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let mode = std::env::args().nth(4).unwrap_or_default();
    let use_zero_copy = mode != "buffered";

    println!("=== Ruxio E2E Throughput Benchmark ===");
    println!();
    println!("Config:");
    println!("  Server threads:     {num_server_threads}");
    println!("  Client connections: {num_client_conns}");
    println!("  Duration:           {duration_secs}s");
    println!("  Page size:          {} MB", PAGE_SIZE / (1024 * 1024));
    println!("  Cached pages/core:  {NUM_PAGES}");
    println!(
        "  Mode:               {}",
        if use_zero_copy {
            "zero-copy (sendfile)"
        } else {
            "buffered"
        }
    );
    if cfg!(target_os = "linux") {
        println!("  Platform:           Linux");
    } else {
        println!("  Platform:           macOS");
    }
    println!();

    // Clean up
    let _ = std::fs::remove_dir_all("/tmp/ruxio_e2e_test");

    // Start server threads (one per core, each on its own port)
    println!(
        "Starting {num_server_threads} server threads (ports {}-{})...",
        BASE_PORT,
        BASE_PORT + num_server_threads as u16 - 1
    );
    for i in 0..num_server_threads {
        start_server_thread(BASE_PORT + i as u16, use_zero_copy);
    }
    std::thread::sleep(Duration::from_millis(500)); // let servers bind

    // Start client threads
    let total_bytes = Arc::new(AtomicU64::new(0));
    let total_ops = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(false));

    println!("Starting {num_client_conns} client connections...");
    let _client_handles: Vec<std::thread::JoinHandle<()>> = Vec::new();
    for i in 0..num_client_conns {
        let port = BASE_PORT + (i % num_server_threads) as u16;
        let tb = total_bytes.clone();
        let to = total_ops.clone();
        let r = running.clone();
        start_client_thread(port, 0, tb, to, r, duration_secs);
    }

    // Let clients warm up then start timing
    std::thread::sleep(Duration::from_millis(500));
    println!("Running benchmark for {duration_secs}s...");
    println!();
    running.store(true, Ordering::Relaxed);

    let start = Instant::now();

    // Print progress every second
    let mut last_bytes = 0u64;
    let mut last_ops = 0u64;
    for sec in 1..=duration_secs {
        std::thread::sleep(Duration::from_secs(1));
        let cur_bytes = total_bytes.load(Ordering::Relaxed);
        let cur_ops = total_ops.load(Ordering::Relaxed);
        let delta_bytes = cur_bytes - last_bytes;
        let delta_ops = cur_ops - last_ops;
        let mb_s = delta_bytes as f64 / (1024.0 * 1024.0);
        println!("  [{sec:>2}s] {mb_s:>8.1} MB/s  {delta_ops:>6} ops/s");
        last_bytes = cur_bytes;
        last_ops = cur_ops;
    }

    let elapsed = start.elapsed();
    let final_bytes = total_bytes.load(Ordering::Relaxed);
    let final_ops = total_ops.load(Ordering::Relaxed);

    let throughput_mb = final_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    let throughput_gb = throughput_mb / 1024.0;
    let ops_per_sec = final_ops as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Results ===");
    println!();
    println!("  Throughput: {throughput_gb:.2} GB/s ({throughput_mb:.0} MB/s)");
    println!("  Ops/sec:    {ops_per_sec:.0} (4MB page reads)");
    println!(
        "  Total:      {:.1} GB in {:.1}s",
        final_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        elapsed.as_secs_f64()
    );
    println!(
        "  Per-core:   {:.2} GB/s",
        throughput_gb / num_server_threads as f64
    );

    std::process::exit(0); // exit cleanly (server threads are still running)
}
