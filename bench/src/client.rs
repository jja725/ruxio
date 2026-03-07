use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::ReadRangeRequest;

const PAGE_SIZE: u64 = 4 * 1024 * 1024;
const NUM_PAGES: u64 = 100;

async fn send_and_recv(stream: &mut TcpStream, request_id: u32, page_offset: u64) -> u64 {
    let req = ReadRangeRequest {
        uri: "test://file.parquet".into(),
        offset: page_offset,
        length: PAGE_SIZE,
    };
    let frame = Frame::new_json(MessageType::ReadRange, request_id, &req);
    let (result, _) = stream.write_all(frame.encode().to_vec()).await;
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

fn start_client_thread(
    server: String,
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
            let mut stream;
            loop {
                match TcpStream::connect(&server).await {
                    Ok(s) => {
                        stream = s;
                        break;
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to {server}: {e}, retrying...");
                        monoio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }

            // Warmup
            for i in 0..20u64 {
                let _ = send_and_recv(&mut stream, i as u32, (i % NUM_PAGES) * PAGE_SIZE).await;
            }

            // Wait for start signal
            while !running.load(Ordering::Relaxed) {
                monoio::time::sleep(Duration::from_millis(1)).await;
            }

            let deadline = Instant::now() + Duration::from_secs(duration_secs);
            let mut i = 0u64;

            while Instant::now() < deadline {
                let page_offset = (i % NUM_PAGES) * PAGE_SIZE;
                let bytes = send_and_recv(&mut stream, i as u32, page_offset).await;
                if bytes == 0 {
                    eprintln!("Connection lost, stopping client");
                    break;
                }
                total_bytes.fetch_add(bytes, Ordering::Relaxed);
                total_ops.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        });
    });
}

fn main() {
    let host = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port: u16 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(51234);
    let num_conns: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let duration_secs: u64 = std::env::args()
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let server = format!("{host}:{port}");

    println!("=== Ruxio Benchmark Client ===");
    println!();
    println!("  Server:       {server}");
    println!("  Connections:  {num_conns}");
    println!("  Duration:     {duration_secs}s");
    println!("  Page size:    {} MB", PAGE_SIZE / (1024 * 1024));
    println!();

    let total_bytes = Arc::new(AtomicU64::new(0));
    let total_ops = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(false));

    println!("Connecting {num_conns} clients...");
    for _ in 0..num_conns {
        start_client_thread(
            server.clone(),
            total_bytes.clone(),
            total_ops.clone(),
            running.clone(),
            duration_secs,
        );
    }

    std::thread::sleep(Duration::from_millis(2000));
    println!("Running benchmark for {duration_secs}s...");
    println!();
    running.store(true, Ordering::Relaxed);

    let start = Instant::now();
    let mut last_bytes = 0u64;
    let mut last_ops = 0u64;

    for sec in 1..=duration_secs {
        std::thread::sleep(Duration::from_secs(1));
        let cur_bytes = total_bytes.load(Ordering::Relaxed);
        let cur_ops = total_ops.load(Ordering::Relaxed);
        let delta_bytes = cur_bytes - last_bytes;
        let delta_ops = cur_ops - last_ops;
        let mb_s = delta_bytes as f64 / (1024.0 * 1024.0);
        let gb_s = mb_s / 1024.0;
        println!("  [{sec:>2}s] {gb_s:>6.2} GB/s  {mb_s:>8.1} MB/s  {delta_ops:>6} ops/s");
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

    std::process::exit(0);
}
