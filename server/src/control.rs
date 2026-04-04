use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpListener;
use tracing::{info, warn};

/// Read process memory usage from /proc/self/status (Linux).
fn get_memory_usage_kb() -> (u64, u64) {
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

/// Get available disk space in bytes for a given path.
fn get_disk_free_bytes(path: &str) -> u64 {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            let c_path = std::ffi::CString::new(path).unwrap_or_default();
            if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                return stat.f_bavail as u64 * stat.f_frsize as u64;
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        0 // Not implemented on non-Linux
    }
}

/// Health/metrics/readiness HTTP server.
///
/// Endpoints:
/// - `GET /health` — liveness check (always 200 if server is running)
/// - `GET /ready` — readiness check (503 during startup/shutdown)
/// - `GET /metrics` — Prometheus metrics
pub fn start_health_server(addr: String, ready: Arc<AtomicBool>, shutdown: Arc<AtomicBool>) {
    std::thread::Builder::new()
        .name("ruxio-health".to_string())
        .spawn(move || {
            let mut rt = match monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .enable_timer()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("Failed to build health server runtime: {e}");
                    return;
                }
            };
            rt.block_on(async move {
                let listener = match TcpListener::bind(&addr) {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!("Failed to bind health endpoint on {addr}: {e}");
                        return;
                    }
                };
                info!("Health endpoint on {addr}");

                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        info!("Health server shutting down");
                        return;
                    }

                    let accept_result = monoio::time::timeout(
                        std::time::Duration::from_secs(1),
                        listener.accept(),
                    )
                    .await;

                    let (mut stream, _) = match accept_result {
                        Ok(Ok(v)) => v,
                        Ok(Err(e)) => {
                            warn!("Health accept error: {e}");
                            continue;
                        }
                        Err(_) => continue, // Timeout, re-check shutdown
                    };

                    let ready = ready.clone();
                    let shutdown = shutdown.clone();
                    monoio::spawn(async move {
                        let mut buf = vec![0u8; 4096];
                        let (result, read_buf) = stream.read(buf).await;
                        buf = read_buf;
                        if result.unwrap_or(0) == 0 {
                            return;
                        }

                        // Parse request path
                        let request_str = std::str::from_utf8(&buf).unwrap_or("");
                        let path = request_str
                            .lines()
                            .next()
                            .and_then(|line| line.split_whitespace().nth(1))
                            .unwrap_or("/");

                        let (status_code, content_type, body) = match path {
                            "/metrics" => {
                                let metrics = ruxio_common::metrics::metrics_result();
                                (200, "text/plain; version=0.0.4; charset=utf-8", metrics)
                            }
                            "/ready" => {
                                let is_ready = ready.load(Ordering::Relaxed)
                                    && !shutdown.load(Ordering::Relaxed);
                                if is_ready {
                                    (200, "application/json", "{\"ready\":true}\n".to_string())
                                } else {
                                    (
                                        503,
                                        "application/json",
                                        "{\"ready\":false}\n".to_string(),
                                    )
                                }
                            }
                            "/health" | "/" => {
                                let (rss_kb, vm_kb) = get_memory_usage_kb();
                                let rss_mb = rss_kb as f64 / 1024.0;
                                let vm_mb = vm_kb as f64 / 1024.0;
                                let json = format!(
                                    concat!(
                                        "{{\n",
                                        "  \"status\": \"ok\",\n",
                                        "  \"memory_rss_mb\": {:.1},\n",
                                        "  \"memory_virtual_mb\": {:.1}\n",
                                        "}}\n"
                                    ),
                                    rss_mb, vm_mb,
                                );
                                (200, "application/json", json)
                            }
                            _ => {
                                let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                                let _ = stream.write_all(response.to_string().into_bytes()).await;
                                return;
                            }
                        };

                        let status_text = match status_code {
                            200 => "OK",
                            503 => "Service Unavailable",
                            _ => "Unknown",
                        };

                        let response = format!(
                            "HTTP/1.1 {status_code} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len(),
                        );

                        let _ = stream.write_all(response.into_bytes()).await;
                    });
                }
            });
        })
        .expect("failed to spawn health server thread");
}

/// Check available disk space and return true if above threshold.
pub fn check_disk_space(cache_dir: &str, min_free_bytes: u64) -> bool {
    get_disk_free_bytes(cache_dir) >= min_free_bytes
}
