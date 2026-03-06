use std::time::{Duration, Instant};

use bytes::Bytes;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use ruxio_cluster::membership::{ClusterMembership, DiscoveryMode};
use ruxio_cluster::ring::NodeId;
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::ReadRangeRequest;
use ruxio_storage::cache::CacheManager;
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::gcs::GcsClient;
use ruxio_storage::metadata_cache::MetadataCache;
use ruxio_storage::page_cache::{CachedPage, PageCache};
use ruxio_storage::page_key::PageKey;
use ruxio_storage::zero_copy;

const PAGE_SIZE: u64 = 4 * 1024 * 1024; // 4MB
const NUM_PAGES: u64 = 100;
const SERVER_ADDR: &str = "127.0.0.1:19876";

/// Pre-populate the page cache with test data on disk (enables zero-copy).
fn populate_cache(page_cache: &mut PageCache) {
    let data = Bytes::from(vec![0xABu8; PAGE_SIZE as usize]);
    for i in 0..NUM_PAGES {
        let key = PageKey::new("test://file.parquet", 0, i * PAGE_SIZE);
        // Write to disk for zero-copy splice
        let local_path = page_cache
            .write_page_to_disk(&key, &data)
            .expect("failed to write test page to disk");
        let page = CachedPage {
            local_path,
            size: PAGE_SIZE,
            data: Some(data.clone()),
        };
        page_cache.put(key, page);
    }
}

/// Server task: accept connections and serve cache hits.
async fn run_server(
    mut cache_manager: CacheManager,
    membership: ClusterMembership,
    use_zero_copy: bool,
) {
    let listener = monoio::net::TcpListener::bind(SERVER_ADDR).unwrap();
    // Signal ready by accepting connections
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut reader = FrameReader::new();
        let mut buf = vec![0u8; 64 * 1024];

        loop {
            let (result, read_buf) = stream.read(buf).await;
            buf = read_buf;
            let n = match result {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            reader.feed(&buf[..n]);

            while let Some(frame) = reader.next_frame().unwrap() {
                let responses = handle_frame(
                    frame,
                    &mut cache_manager,
                    &membership,
                    &mut stream,
                    use_zero_copy,
                )
                .await;
                for resp in responses {
                    let encoded = resp.encode();
                    let (result, _) = stream.write_all(encoded.to_vec()).await;
                    if result.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

async fn handle_frame(
    frame: Frame,
    cache_manager: &mut CacheManager,
    _membership: &ClusterMembership,
    stream: &mut TcpStream,
    use_zero_copy: bool,
) -> Vec<Frame> {
    let rid = frame.request_id;
    match frame.msg_type {
        MessageType::ReadRange => {
            let req: ReadRangeRequest = serde_json::from_slice(&frame.payload).unwrap();

            // Try zero-copy path if enabled
            if use_zero_copy {
                let page_offset = (req.offset / PAGE_SIZE) * PAGE_SIZE;
                if req.length == PAGE_SIZE && req.offset == page_offset {
                    if let Some((file_path, file_size)) =
                        cache_manager.get_page_file(&req.uri, page_offset)
                    {
                        if zero_copy::send_file_to_socket(&file_path, file_size, rid, stream)
                            .await
                            .is_ok()
                        {
                            return vec![]; // already sent directly
                        }
                    }
                }
            }

            // Fallback: buffered path
            match cache_manager.read_range(&req).await {
                Ok(data) => vec![
                    Frame::new_raw(MessageType::DataChunk, rid, data),
                    Frame::done(rid),
                ],
                Err(e) => vec![Frame::new_json(
                    MessageType::Error,
                    rid,
                    &ruxio_protocol::messages::ErrorResponse {
                        code: 500,
                        message: e.to_string(),
                    },
                )],
            }
        }
        _ => vec![Frame::done(rid)],
    }
}

/// Client: send ReadRange requests and measure throughput + latency.
async fn run_client(num_requests: u64) {
    // Wait for server to be ready
    let mut stream;
    loop {
        match TcpStream::connect(SERVER_ADDR).await {
            Ok(s) => {
                stream = s;
                break;
            }
            Err(_) => {
                monoio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    // Warmup
    for i in 0..100 {
        send_and_recv(&mut stream, i, (i % NUM_PAGES) * PAGE_SIZE).await;
    }

    // Timed run
    let mut latencies: Vec<Duration> = Vec::with_capacity(num_requests as usize);
    let mut total_bytes: u64 = 0;
    let start = Instant::now();

    for i in 0..num_requests {
        let page_offset = (i % NUM_PAGES) * PAGE_SIZE;
        let t0 = Instant::now();
        let bytes_received = send_and_recv(&mut stream, i as u64, page_offset).await;
        latencies.push(t0.elapsed());
        total_bytes += bytes_received;
    }

    let elapsed = start.elapsed();

    // Sort latencies for percentiles
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[latencies.len() * 99 / 100];
    let p999 = latencies[latencies.len() * 999 / 1000];
    let min = latencies[0];
    let max = latencies[latencies.len() - 1];

    let throughput_mb = total_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    let ops_per_sec = num_requests as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== End-to-End Benchmark Results ===");
    println!();
    println!("Config:");
    println!("  Page size:    {} MB", PAGE_SIZE / (1024 * 1024));
    println!("  Cached pages: {NUM_PAGES}");
    println!("  Requests:     {num_requests}");
    println!();
    println!("Throughput:");
    println!("  {throughput_mb:.1} MB/s");
    println!("  {ops_per_sec:.0} ops/sec");
    println!(
        "  Total: {total_bytes} bytes in {:.3}s",
        elapsed.as_secs_f64()
    );
    println!();
    println!("Latency (per request round-trip):");
    println!("  min:  {min:?}");
    println!("  p50:  {p50:?}");
    println!("  p99:  {p99:?}");
    println!("  p999: {p999:?}");
    println!("  max:  {max:?}");
}

async fn send_and_recv(stream: &mut TcpStream, request_id: u64, page_offset: u64) -> u64 {
    let req = ReadRangeRequest {
        uri: "test://file.parquet".into(),
        offset: page_offset,
        length: PAGE_SIZE,
    };
    let frame = Frame::new_json(MessageType::ReadRange, request_id as u32, &req);
    let encoded = frame.encode();
    let (result, _) = stream.write_all(encoded.to_vec()).await;
    result.unwrap();

    // Read responses until Done
    let mut reader = FrameReader::new();
    let mut bytes_received: u64 = 0;
    let mut buf = vec![0u8; PAGE_SIZE as usize + 1024];

    loop {
        let (result, read_buf) = stream.read(buf).await;
        buf = read_buf;
        let n = result.unwrap();
        if n == 0 {
            break;
        }
        reader.feed(&buf[..n]);

        while let Some(resp) = reader.next_frame().unwrap() {
            match resp.msg_type {
                MessageType::DataChunk => {
                    bytes_received += resp.payload.len() as u64;
                }
                MessageType::Done => return bytes_received,
                MessageType::Error => {
                    let err: ruxio_protocol::messages::ErrorResponse =
                        serde_json::from_slice(&resp.payload).unwrap();
                    panic!("Server error: {} {}", err.code, err.message);
                }
                _ => {}
            }
        }
    }
    bytes_received
}

#[monoio::main(timer_enabled = true)]
async fn main() {
    let num_requests: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);

    let mode = std::env::args().nth(2).unwrap_or_default();
    let use_zero_copy = mode != "buffered";

    println!(
        "Starting E2E benchmark ({num_requests} requests, {NUM_PAGES} cached 4MB pages, mode={})",
        if use_zero_copy {
            "zero-copy"
        } else {
            "buffered"
        }
    );
    if cfg!(target_os = "linux") {
        println!("  Platform: Linux (splice via io_uring)");
    } else {
        println!("  Platform: macOS (fallback: read+write, no splice)");
    }

    // Clean up old test cache
    let _ = std::fs::remove_dir_all("/tmp/ruxio_e2e_test");

    // Build server components with pre-populated cache
    let gcs = GcsClient::new("test-bucket");
    let metadata_cache = MetadataCache::new(1000);
    let mut page_cache = PageCache::new("/tmp/ruxio_e2e_test", 1024 * 1024 * 1024);
    populate_cache(&mut page_cache);
    let cache_manager = CacheManager::new(gcs, metadata_cache, page_cache);

    let self_id = NodeId::new("127.0.0.1", 19876);
    let membership = ClusterMembership::new(self_id, 150, DiscoveryMode::Static { peers: vec![] });

    // Spawn server
    monoio::spawn(run_server(cache_manager, membership, use_zero_copy));

    // Run client benchmark
    run_client(num_requests).await;
}
