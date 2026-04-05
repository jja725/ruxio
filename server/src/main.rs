mod control;
mod data;
mod http;

use std::cell::RefCell;
use std::hash::BuildHasher;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing::info;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use ruxio_cluster::membership::ClusterMembership;
use ruxio_cluster::ring::NodeId;
use ruxio_cluster::static_membership::StaticMembership;
use ruxio_common::settings::Settings;
use ruxio_storage::cache::CacheManager;
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::forwarding;
use ruxio_storage::gcs::GcsClient;
use ruxio_storage::metadata_cache::MetadataCache;
use ruxio_storage::page_cache::{CachedPage, EvictionPolicy, PageCache};
use ruxio_storage::page_key::PageKey;
use ruxio_storage::retry::RetryPolicy;

/// Minimal CLI args — all real configuration lives in ruxio_config.toml or env vars.
#[derive(Parser, Debug)]
#[command(name = "ruxio-server", about = "Ruxio distributed cache server")]
struct Args {
    /// Config file path (without extension). Also set via RUXIO_CONFIG env var.
    #[arg(long)]
    config: Option<String>,

    /// Populate cache with test data for benchmarking
    #[arg(long, default_value_t = false)]
    bench_populate: bool,
}

const BENCH_NUM_PAGES: u64 = 100;

/// Determine which thread owns a file URI.
fn owning_thread(uri: &str, num_threads: usize) -> usize {
    (Xxh3DefaultBuilder.hash_one(uri) as usize) % num_threads
}

/// Populate cache only with pages that this thread owns.
fn populate_bench_data(page_cache: &mut PageCache, thread_id: usize, num_threads: usize) {
    let page_size = page_cache.page_size();
    let data = vec![0xABu8; page_size as usize];
    let uri = "test://file.parquet";
    let owner = owning_thread(uri, num_threads);

    if owner != thread_id {
        return;
    }

    for i in 0..BENCH_NUM_PAGES {
        let key = PageKey::new(uri, i);
        match page_cache.write_page_to_disk(&key, &data) {
            Ok(local_path) => {
                let page = CachedPage {
                    local_path,
                    size: page_size,
                };
                page_cache.put(key, page);
            }
            Err(e) => {
                tracing::error!("Failed to write bench page {i}: {e}");
                return;
            }
        }
    }
    info!(
        "Thread {thread_id} populated {} pages (owns '{uri}')",
        BENCH_NUM_PAGES
    );
}

/// Scan existing cache directory and restore page index on startup.
fn restore_cache_index(page_cache: &mut PageCache, cache_dir: &str, page_size: u64) -> usize {
    let page_size_dir = std::path::Path::new(cache_dir).join(page_size.to_string());
    if !page_size_dir.exists() {
        return 0;
    }

    let mut restored = 0usize;
    let buckets = match std::fs::read_dir(&page_size_dir) {
        Ok(d) => d,
        Err(_) => return 0,
    };

    for bucket_entry in buckets.flatten() {
        if !bucket_entry.path().is_dir() {
            continue;
        }
        let file_dirs = match std::fs::read_dir(bucket_entry.path()) {
            Ok(d) => d,
            Err(_) => continue,
        };
        for file_entry in file_dirs.flatten() {
            if !file_entry.path().is_dir() {
                continue;
            }
            let file_id =
                PageKey::decode_url_safe(file_entry.file_name().to_str().unwrap_or_default());
            if file_id.is_empty() {
                continue;
            }
            let pages = match std::fs::read_dir(file_entry.path()) {
                Ok(d) => d,
                Err(_) => continue,
            };
            for page_file in pages.flatten() {
                let path = page_file.path();
                if !path.is_file() {
                    continue;
                }
                let page_index: u64 = match path
                    .file_name()
                    .and_then(|f| f.to_str())
                    .and_then(|s| s.parse().ok())
                {
                    Some(idx) => idx,
                    None => continue,
                };
                let size = match std::fs::metadata(&path) {
                    Ok(m) => m.len(),
                    Err(_) => continue,
                };
                if size > page_size * 2 || size == 0 {
                    tracing::warn!("Skipping corrupt page file: {}", path.display());
                    if let Err(e) = std::fs::remove_file(&path) {
                        tracing::warn!(
                            "Failed to remove corrupt page file {}: {e}",
                            path.display()
                        );
                    }
                    continue;
                }
                let key = PageKey::new(&file_id, page_index);
                let page = CachedPage {
                    local_path: path,
                    size,
                };
                page_cache.put(key, page);
                restored += 1;
            }
        }
    }
    restored
}

/// Create a TCP listener with production socket options.
///
/// Uses socket2 for full control over:
/// - SO_BACKLOG (default 1024, prevents dropped connections under burst)
/// - SO_REUSEADDR (allows quick restart without TIME_WAIT)
/// - TCP_FASTOPEN (saves 1 RTT on new connections, Linux only)
fn create_listener(
    addr: &str,
    settings: &ruxio_common::settings::ServerSettings,
) -> anyhow::Result<monoio::net::TcpListener> {
    let sock_addr: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid address {addr}: {e}"))?;

    let socket = socket2::Socket::new(
        if sock_addr.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        },
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;

    if settings.so_reuseaddr {
        socket.set_reuse_address(true)?;
    }
    socket.set_nonblocking(true)?;
    socket.set_nodelay(true)?;

    // TCP_FASTOPEN (Linux only) — allows data in the SYN packet
    #[cfg(target_os = "linux")]
    if settings.tcp_fastopen {
        let optval = settings.tcp_fastopen_qlen as libc::c_int;
        // SAFETY: socket fd is valid (obtained from socket2::Socket), optval is a
        // stack-allocated c_int with correct size passed via size_of. The setsockopt
        // call only reads from the pointer for the specified length.
        unsafe {
            libc::setsockopt(
                std::os::unix::io::AsRawFd::as_raw_fd(&socket),
                libc::IPPROTO_TCP,
                libc::TCP_FASTOPEN,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }

    // TCP_USER_TIMEOUT (Linux only) — kernel drops connection if no ACK
    // for this many ms. More reliable than app-level idle timeout.
    #[cfg(target_os = "linux")]
    {
        let timeout_ms = (settings.idle_timeout_secs * 1000) as libc::c_int;
        // SAFETY: socket fd is valid (obtained from socket2::Socket), timeout_ms is a
        // stack-allocated c_int with correct size passed via size_of. The setsockopt
        // call only reads from the pointer for the specified length.
        unsafe {
            libc::setsockopt(
                std::os::unix::io::AsRawFd::as_raw_fd(&socket),
                libc::IPPROTO_TCP,
                libc::TCP_USER_TIMEOUT,
                &timeout_ms as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }

    // SO_LINGER — ensure in-flight data is transmitted before close.
    // Linger for 5 seconds; kernel will attempt to send remaining data.
    socket.set_linger(Some(std::time::Duration::from_secs(5)))?;

    socket.bind(&sock_addr.into())?;
    socket.listen(settings.so_backlog as i32)?;

    let std_listener: std::net::TcpListener = socket.into();
    let listener = monoio::net::TcpListener::from_std(std_listener)?;
    Ok(listener)
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Set config path if provided via CLI
    if let Some(config_path) = &args.config {
        std::env::set_var("RUXIO_CONFIG", config_path);
    }

    // Load settings from config file + env vars
    let settings = Settings::new()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive(
                format!("ruxio={}", settings.log_level)
                    .parse()
                    .unwrap_or_else(|_| {
                        tracing_subscriber::filter::Directive::from(
                            tracing::level_filters::LevelFilter::INFO,
                        )
                    }),
            ),
        )
        .init();

    let bind = &settings.local_ip;
    let addr = format!("{bind}:{}", settings.data_port);
    let http_addr = format!("{bind}:{}", settings.http_port);
    let threads = settings.threads;
    let page_size = settings.cache.page_size_bytes as u64;
    let eviction_policy = match settings.cache.eviction_policy.as_str() {
        "clockpro" | "clock-pro" => EvictionPolicy::ClockPro,
        "lru" => EvictionPolicy::Lru,
        other => {
            tracing::error!("Unknown eviction policy: {other}. Use 'clockpro' or 'lru'.");
            std::process::exit(1);
        }
    };

    info!("Starting ruxio-server");
    info!("  bind:            {addr}");
    info!("  threads:         {threads}");
    info!("  cache_dir:       {}", settings.cache.root_path);
    info!(
        "  max_cache:       {} bytes",
        settings.cache.max_cache_bytes
    );
    info!(
        "  page_size:       {} bytes ({} MB)",
        page_size,
        page_size / (1024 * 1024)
    );
    info!("  eviction:        {}", settings.cache.eviction_policy);
    info!("  http_port:       {}", settings.http_port);
    info!(
        "  max_connections: {}/thread",
        settings.server.max_connections_per_thread
    );
    info!("  idle_timeout:    {}s", settings.server.idle_timeout_secs);
    info!("  cache_restore:   {}", settings.cache.restore_on_startup);

    std::fs::create_dir_all(&settings.cache.root_path)?;
    let per_thread_cache_bytes = settings.cache.max_cache_bytes / threads as u64;

    // Set up graceful shutdown + readiness
    let shutdown = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(AtomicBool::new(false));

    let shutdown_signal = shutdown.clone();
    ctrlc::set_handler(move || {
        info!("Shutdown signal received, draining...");
        shutdown_signal.store(true, Ordering::SeqCst);
    })
    .expect("failed to set signal handler");

    // Start health HTTP endpoint
    let health_addr = format!("{bind}:{}", settings.control_port);
    control::start_health_server(health_addr, ready.clone(), shutdown.clone());

    // Create SPSC channel matrix
    let channels = forwarding::create_channel_matrix(threads);

    // Build server config from settings
    let server_config = data::ServerConfig {
        idle_timeout_secs: settings.server.idle_timeout_secs,
        inflight_timeout_secs: settings.cluster.inflight_timeout_secs,
        max_connections_per_thread: settings.server.max_connections_per_thread,
        write_timeout_secs: settings.server.write_timeout_secs,
        max_pending_requests: settings.server.max_pending_requests,
        response_chunk_bytes: settings.server.response_chunk_bytes,
        write_buffer_high: settings.server.write_buffer_high,
        write_buffer_low: settings.server.write_buffer_low,
        forward_timeout_secs: 5,
    };

    let retry_policy = RetryPolicy {
        max_retries: settings.gcs.max_retries,
        base_delay_ms: settings.gcs.retry_base_delay_ms,
        max_delay_ms: settings.gcs.retry_max_delay_ms,
        timeout_secs: settings.gcs.request_timeout_secs,
    };

    // Spawn worker threads
    let mut handles = Vec::new();
    for thread_id in 0..threads {
        let addr = addr.clone();
        let http_addr = http_addr.clone();
        let cache_dir = format!("{}/{thread_id}", settings.cache.root_path);
        let bucket = settings.gcs.bucket.clone();
        let bench_populate = args.bench_populate;
        let max_metadata_entries = settings.cache.max_metadata_entries;
        let metadata_ttl_secs = settings.cache.metadata_ttl_secs;
        let cache_restore = settings.cache.restore_on_startup;
        let gcs_pool_size = settings.gcs.max_idle_connections;
        let gcs_idle_timeout_secs = settings.gcs.idle_timeout_secs;
        let bind = settings.local_ip.clone();
        let data_port = settings.data_port;
        let peers = settings.static_service_list.clone();
        let channels = channels.clone();
        let shutdown = shutdown.clone();
        let ready = ready.clone();
        let server_config = server_config.clone();
        let retry_policy = retry_policy.clone();
        let discovery = settings.service_discovery_type.clone();
        let etcd_endpoints = settings.etcd_uris.clone();
        let etcd_prefix = settings.etcd_prefix.clone();
        let etcd_lease_ttl = settings.cluster.etcd_lease_ttl_secs;
        let vnodes = settings.cluster.vnodes_per_node;
        let server_config_for_socket = settings.server.clone();

        let handle = std::thread::Builder::new()
            .name(format!("ruxio-worker-{thread_id}"))
            .spawn(move || {
                let mut rt = match monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!("Worker {thread_id}: failed to build runtime: {e}");
                        shutdown.store(true, Ordering::SeqCst);
                        return;
                    }
                };

                rt.block_on(async move {
                    if let Err(e) = std::fs::create_dir_all(&cache_dir) {
                        tracing::error!("Worker {thread_id}: failed to create cache dir: {e}");
                        shutdown.store(true, Ordering::SeqCst);
                        return;
                    }
                    let gcs = GcsClient::new(&bucket)
                        .with_pool_config(gcs_pool_size, gcs_idle_timeout_secs);
                    let metadata_cache =
                        MetadataCache::with_ttl(max_metadata_entries, metadata_ttl_secs);
                    let mut page_cache = PageCache::with_policy(
                        &cache_dir,
                        per_thread_cache_bytes,
                        page_size,
                        eviction_policy,
                    );

                    // Restore cache index from disk
                    if cache_restore {
                        let restored = restore_cache_index(&mut page_cache, &cache_dir, page_size);
                        if restored > 0 {
                            info!("Worker {thread_id}: restored {restored} cached pages from disk");
                        }
                    }

                    if bench_populate {
                        populate_bench_data(&mut page_cache, thread_id, threads);
                    }

                    let cache_manager =
                        CacheManager::with_ttl(metadata_cache, page_cache, metadata_ttl_secs);

                    let self_id = NodeId::new(&bind, data_port);
                    let membership_service: Box<dyn ruxio_cluster::service::MembershipService> =
                        match discovery.as_str() {
                            "etcd" => {
                                let config = ruxio_cluster::etcd::EtcdConfig {
                                    endpoints: etcd_endpoints.clone(),
                                    prefix: etcd_prefix.clone(),
                                    lease_ttl_secs: etcd_lease_ttl,
                                };
                                Box::new(ruxio_cluster::etcd::EtcdMembership::new(
                                    self_id.clone(),
                                    config,
                                ))
                            }
                            _ => {
                                let peer_nodes: Vec<NodeId> = peers
                                    .iter()
                                    .filter(|p| *p != &self_id.0)
                                    .map(|p| NodeId(p.clone()))
                                    .collect();
                                Box::new(StaticMembership::new(self_id.clone(), peer_nodes))
                            }
                        };
                    let membership = ClusterMembership::new(membership_service, vnodes);
                    if let Err(e) = membership.start() {
                        tracing::warn!("Membership start failed: {e}, running standalone");
                    }

                    let ctx = Rc::new(data::ThreadContext {
                        cache_manager: Rc::new(RefCell::new(cache_manager)),
                        gcs: Rc::new(gcs),
                        membership: Rc::new(membership),
                        thread_id,
                        num_threads: threads,
                        channels: channels.clone(),
                        inflight: Rc::new(RefCell::new(std::collections::HashSet::new())),
                        retry_policy,
                        shutdown: shutdown.clone(),
                        config: server_config,
                        active_connections: Rc::new(AtomicU32::new(0)),
                        pending_requests: Rc::new(AtomicU32::new(0)),
                    });

                    // Spawn SPSC inbox processor
                    let ctx_inbox = ctx.clone();
                    monoio::spawn(async move {
                        loop {
                            monoio::time::sleep(Duration::from_micros(10)).await;
                            forwarding::process_forwarded_requests(
                                &ctx_inbox.channels,
                                thread_id,
                                threads,
                                &mut ctx_inbox.cache_manager.borrow_mut().page_cache,
                            );
                        }
                    });

                    // Spawn membership event poller
                    let ctx_membership = ctx.clone();
                    monoio::spawn(async move {
                        loop {
                            monoio::time::sleep(Duration::from_millis(100)).await;
                            ctx_membership.membership.poll_events();
                        }
                    });

                    let listener = match create_listener(&addr, &server_config_for_socket) {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::error!(
                                "Worker {thread_id}: failed to bind data port {addr}: {e}"
                            );
                            shutdown.store(true, Ordering::SeqCst);
                            return;
                        }
                    };
                    let http_listener = match create_listener(&http_addr, &server_config_for_socket)
                    {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::error!(
                                "Worker {thread_id}: failed to bind HTTP port {http_addr}: {e}"
                            );
                            shutdown.store(true, Ordering::SeqCst);
                            return;
                        }
                    };

                    // Mark server as ready
                    ready.store(true, Ordering::SeqCst);
                    info!("Worker {thread_id} ready");

                    // Spawn HTTP/1.1 listener
                    let ctx_http = ctx.clone();
                    let shutdown_http = shutdown.clone();
                    monoio::spawn(async move {
                        loop {
                            if shutdown_http.load(Ordering::Relaxed) {
                                break;
                            }
                            match monoio::time::timeout(
                                Duration::from_secs(1),
                                http_listener.accept(),
                            )
                            .await
                            {
                                Ok(Ok((stream, _))) => {
                                    let ctx = ctx_http.clone();
                                    monoio::spawn(async move {
                                        http::serve_http_connection(stream, ctx).await;
                                    });
                                }
                                Ok(Err(e)) => tracing::warn!("HTTP accept error: {e}"),
                                Err(_) => continue,
                            }
                        }
                    });

                    // Main accept loop
                    loop {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        match monoio::time::timeout(Duration::from_secs(1), listener.accept()).await
                        {
                            Ok(Ok((stream, _))) => {
                                let ctx = ctx.clone();
                                monoio::spawn(async move {
                                    data::serve_connection(stream, ctx).await;
                                });
                            }
                            Ok(Err(e)) => tracing::warn!("Accept error: {e}"),
                            Err(_) => continue,
                        }
                    }

                    // Graceful shutdown
                    ready.store(false, Ordering::SeqCst);
                    info!("Worker {thread_id} shutting down");
                    ctx.membership.leave();
                });
            })?;
        handles.push(handle);
    }

    info!("Server ready: {threads} threads on {addr}");

    for h in handles {
        if let Err(e) = h.join() {
            tracing::error!("Worker thread panicked: {e:?}");
        }
    }
    Ok(())
}
