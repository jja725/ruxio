use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpListener;
use tracing::info;

/// Shared server stats — updated by data plane threads via atomics.
#[derive(Clone)]
pub struct ServerStats {
    pub total_requests: Arc<AtomicU64>,
    pub total_bytes_sent: Arc<AtomicU64>,
    pub cache_hits: Arc<AtomicU64>,
    pub cache_misses: Arc<AtomicU64>,
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            total_requests: Arc::new(AtomicU64::new(0)),
            total_bytes_sent: Arc::new(AtomicU64::new(0)),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Read process memory usage from /proc/self/status (Linux).
fn get_memory_usage_kb() -> (u64, u64) {
    // VmRSS = physical memory, VmSize = virtual memory
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let mut rss_kb = 0u64;
    let mut vm_kb = 0u64;
    for line in status.lines() {
        if let Some(val) = line.strip_prefix("VmRSS:") {
            rss_kb = val
                .trim()
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("VmSize:") {
            vm_kb = val
                .trim()
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    (rss_kb, vm_kb)
}

/// Simple HTTP/1.1 health endpoint. Returns JSON stats on GET /health.
///
/// Usage: `curl http://<host>:<control_port>/health`
pub fn start_health_server(addr: String, stats: ServerStats) {
    std::thread::Builder::new()
        .name("ruxio-health".to_string())
        .spawn(move || {
            let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .enable_timer()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = TcpListener::bind(&addr).unwrap();
                info!("Health endpoint on {addr}");

                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let stats = stats.clone();

                    monoio::spawn(async move {
                        let mut buf = vec![0u8; 4096];
                        let (result, read_buf) = stream.read(buf).await;
                        buf = read_buf;
                        if result.unwrap_or(0) == 0 {
                            return;
                        }

                        let (rss_kb, vm_kb) = get_memory_usage_kb();
                        let rss_mb = rss_kb as f64 / 1024.0;
                        let vm_mb = vm_kb as f64 / 1024.0;

                        let body = format!(
                            concat!(
                                "{{\n",
                                "  \"status\": \"ok\",\n",
                                "  \"memory_rss_mb\": {:.1},\n",
                                "  \"memory_virtual_mb\": {:.1},\n",
                                "  \"total_requests\": {},\n",
                                "  \"total_bytes_sent\": {},\n",
                                "  \"total_bytes_sent_gb\": {:.2},\n",
                                "  \"cache_hits\": {},\n",
                                "  \"cache_misses\": {}\n",
                                "}}\n"
                            ),
                            rss_mb,
                            vm_mb,
                            stats.total_requests.load(Ordering::Relaxed),
                            stats.total_bytes_sent.load(Ordering::Relaxed),
                            stats.total_bytes_sent.load(Ordering::Relaxed) as f64
                                / (1024.0 * 1024.0 * 1024.0),
                            stats.cache_hits.load(Ordering::Relaxed),
                            stats.cache_misses.load(Ordering::Relaxed),
                        );

                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );

                        let _ = stream.write_all(response.into_bytes()).await;
                    });
                }
            });
        })
        .unwrap();
}
