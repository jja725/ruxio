mod control;
mod data;

use std::cell::RefCell;
use std::rc::Rc;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use tracing::info;

use ruxio_cluster::membership::{ClusterMembership, DiscoveryMode};
use ruxio_cluster::ring::NodeId;
use ruxio_storage::cache::CacheManager;
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::gcs::GcsClient;
use ruxio_storage::metadata_cache::MetadataCache;
use ruxio_storage::page_cache::{CachedPage, PageCache};
use ruxio_storage::page_key::PageKey;

#[derive(Parser, Debug)]
#[command(name = "ruxio-server", about = "Ruxio distributed cache server")]
struct Args {
    /// Data plane port (binary protocol)
    #[arg(long, default_value_t = 51234)]
    data_port: u16,

    /// Health/stats HTTP port
    #[arg(long, default_value_t = 51235)]
    health_port: u16,

    /// Number of worker threads (defaults to number of CPUs)
    #[arg(long, default_value_t = 16)]
    threads: usize,

    /// GCS bucket name
    #[arg(long)]
    bucket: Option<String>,

    /// Page cache root directory
    #[arg(long, default_value = "/tmp/ruxio_cache")]
    cache_dir: String,

    /// Maximum cache size in bytes (total across all threads)
    #[arg(long, default_value_t = 10 * 1024 * 1024 * 1024)]
    max_cache_bytes: u64,

    /// Maximum metadata cache entries
    #[arg(long, default_value_t = 100_000)]
    max_metadata_entries: usize,

    /// Bind address
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// Static peer list (comma-separated host:port)
    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,

    /// Populate cache with test data for benchmarking (100 x 4MB pages per thread)
    #[arg(long, default_value_t = false)]
    bench_populate: bool,
}

const PAGE_SIZE: u64 = 4 * 1024 * 1024;
const BENCH_NUM_PAGES: u64 = 100;

fn populate_bench_data(page_cache: &mut PageCache) {
    let data = Bytes::from(vec![0xABu8; PAGE_SIZE as usize]);
    for i in 0..BENCH_NUM_PAGES {
        let key = PageKey::new("test://file.parquet", 0, i * PAGE_SIZE);
        let local_path = page_cache
            .write_page_to_disk(&key, &data)
            .expect("failed to write bench page");
        let page = CachedPage {
            local_path,
            size: PAGE_SIZE,
            data: Some(data.clone()),
        };
        page_cache.put(key, page);
    }
}

/// Server entry point.
///
/// Uses monoio's built-in multi-threading: each thread gets its own io_uring
/// event loop and TcpListener on the same port (SO_REUSEPORT). The kernel
/// distributes incoming connections across threads.
fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ruxio=info".parse().unwrap()),
        )
        .init();

    let addr = format!("{}:{}", args.bind, args.data_port);
    let threads = args.threads;

    info!("Starting ruxio-server");
    info!("  bind:         {addr}");
    info!("  threads:      {threads}");
    info!("  cache_dir:    {}", args.cache_dir);
    info!("  max_cache:    {} bytes", args.max_cache_bytes);
    if args.bench_populate {
        info!(
            "  bench:        populating {} x {}MB pages per thread",
            BENCH_NUM_PAGES,
            PAGE_SIZE / (1024 * 1024)
        );
    }

    std::fs::create_dir_all(&args.cache_dir)?;
    let per_thread_cache_bytes = args.max_cache_bytes / threads as u64;

    // Start health HTTP endpoint (memory stats)
    let health_addr = format!("{}:{}", args.bind, args.health_port);
    control::start_health_server(health_addr);

    // Spawn N monoio threads — each owns its own cache partition, listens on
    // the same port via SO_REUSEPORT. The kernel distributes connections.
    let mut handles = Vec::new();
    for thread_id in 0..threads {
        let addr = addr.clone();
        let cache_dir = format!("{}/{thread_id}", args.cache_dir);
        let bucket = args.bucket.clone().unwrap_or_default();
        let bench_populate = args.bench_populate;
        let max_metadata_entries = args.max_metadata_entries;
        let bind = args.bind.clone();
        let data_port = args.data_port;
        let peers = args.peers.clone();

        let handle = std::thread::Builder::new()
            .name(format!("ruxio-worker-{thread_id}"))
            .spawn(move || {
                let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    std::fs::create_dir_all(&cache_dir).unwrap();
                    let gcs = GcsClient::new(&bucket);
                    let metadata_cache = MetadataCache::new(max_metadata_entries);
                    let mut page_cache = PageCache::new(&cache_dir, per_thread_cache_bytes);

                    if bench_populate {
                        populate_bench_data(&mut page_cache);
                    }

                    let cache_manager = CacheManager::new(gcs, metadata_cache, page_cache);
                    let cm = Rc::new(RefCell::new(cache_manager));

                    let self_id = NodeId::new(&bind, data_port);
                    let discovery = if peers.is_empty() {
                        DiscoveryMode::Static { peers: vec![] }
                    } else {
                        DiscoveryMode::Static { peers }
                    };
                    let membership = ClusterMembership::new(self_id, 150, discovery);
                    let mem = Rc::new(membership);

                    let listener = monoio::net::TcpListener::bind(&addr).unwrap();
                    info!("Worker {thread_id} ready");

                    loop {
                        let (stream, _) = listener.accept().await.unwrap();
                        let cm = cm.clone();
                        let mem = mem.clone();
                        monoio::spawn(async move {
                            data::serve_connection(stream, cm, mem).await;
                        });
                    }
                });
            })?;
        handles.push(handle);
    }

    info!("Server ready: {threads} threads on {addr}");

    for h in handles {
        h.join().unwrap();
    }
    Ok(())
}
