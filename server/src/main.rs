mod control;
mod data;

use std::cell::RefCell;
use std::hash::{BuildHasher, Hash, Hasher};
use std::rc::Rc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use tracing::info;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use ruxio_cluster::membership::{ClusterMembership, DiscoveryMode};
use ruxio_cluster::ring::NodeId;
use ruxio_storage::cache::CacheManager;
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::forwarding;
use ruxio_storage::gcs::GcsClient;
use ruxio_storage::metadata_cache::MetadataCache;
use ruxio_storage::page_cache::{CachedPage, EvictionPolicy, PageCache};
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

    /// Number of worker threads (defaults to 16)
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

    /// Page size in bytes (default 4MB)
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    page_size: u64,

    /// Eviction policy: "clockpro" or "lru" (both use TinyLFU admission)
    #[arg(long, default_value = "lru")]
    eviction_policy: String,

    /// Populate cache with test data for benchmarking (100 pages)
    #[arg(long, default_value_t = false)]
    bench_populate: bool,
}

const BENCH_NUM_PAGES: u64 = 100;

/// Determine which thread owns a file URI.
fn owning_thread(uri: &str, num_threads: usize) -> usize {
    let mut hasher = Xxh3DefaultBuilder.build_hasher();
    uri.hash(&mut hasher);
    (hasher.finish() as usize) % num_threads
}

/// Populate cache only with pages that this thread owns.
fn populate_bench_data(page_cache: &mut PageCache, thread_id: usize, num_threads: usize) {
    let page_size = page_cache.page_size();
    let data = Bytes::from(vec![0xABu8; page_size as usize]);
    let uri = "test://file.parquet";
    let owner = owning_thread(uri, num_threads);

    if owner != thread_id {
        return; // This thread doesn't own this file
    }

    for i in 0..BENCH_NUM_PAGES {
        let key = PageKey::new(uri, i);
        let local_path = page_cache
            .write_page_to_disk(&key, &data)
            .expect("failed to write bench page");
        let page = CachedPage {
            local_path,
            size: page_size,
            data: Some(data.clone()),
        };
        page_cache.put(key, page);
    }
    info!(
        "Thread {thread_id} populated {} pages (owns '{uri}')",
        BENCH_NUM_PAGES
    );
}

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
    let page_size = args.page_size;
    let eviction_policy = match args.eviction_policy.as_str() {
        "clockpro" | "clock-pro" => EvictionPolicy::ClockPro,
        "lru" => EvictionPolicy::Lru,
        other => {
            eprintln!("Unknown eviction policy: {other}. Use 'clockpro' or 'lru'.");
            std::process::exit(1);
        }
    };

    info!("Starting ruxio-server");
    info!("  bind:         {addr}");
    info!("  threads:      {threads}");
    info!("  cache_dir:    {}", args.cache_dir);
    info!("  max_cache:    {} bytes", args.max_cache_bytes);
    info!(
        "  page_size:    {} bytes ({} MB)",
        page_size,
        page_size / (1024 * 1024)
    );
    info!("  eviction:     {:?}", eviction_policy);
    if args.bench_populate {
        info!(
            "  bench:        {} x {}MB pages (partitioned by hash)",
            BENCH_NUM_PAGES,
            page_size / (1024 * 1024)
        );
    }

    std::fs::create_dir_all(&args.cache_dir)?;
    let per_thread_cache_bytes = args.max_cache_bytes / threads as u64;

    // Start health HTTP endpoint
    let health_addr = format!("{}:{}", args.bind, args.health_port);
    control::start_health_server(health_addr);

    // Create inboxes for all threads (shared across threads for forwarding)
    let inboxes: Vec<forwarding::Inbox> = (0..threads).map(|_| forwarding::new_inbox()).collect();

    // Spawn worker threads
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
        let inboxes = inboxes.clone();

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
                    let mut page_cache = PageCache::with_policy(
                        &cache_dir,
                        per_thread_cache_bytes,
                        page_size,
                        eviction_policy,
                    );

                    if bench_populate {
                        populate_bench_data(&mut page_cache, thread_id, threads);
                    }

                    let cache_manager = CacheManager::new(metadata_cache, page_cache);

                    let self_id = NodeId::new(&bind, data_port);
                    let discovery = if peers.is_empty() {
                        DiscoveryMode::Static { peers: vec![] }
                    } else {
                        DiscoveryMode::Static { peers }
                    };
                    let membership = ClusterMembership::new(self_id, 150, discovery);

                    let ctx = Rc::new(data::ThreadContext {
                        cache_manager: Rc::new(RefCell::new(cache_manager)),
                        gcs: Rc::new(gcs),
                        membership: Rc::new(membership),
                        thread_id,
                        num_threads: threads,
                        inboxes: inboxes.clone(),
                    });

                    // Spawn inbox processor — drains forwarded lookup requests
                    let inbox = inboxes[thread_id].clone();
                    let ctx_inbox = ctx.clone();
                    monoio::spawn(async move {
                        loop {
                            monoio::time::sleep(Duration::from_micros(50)).await;
                            forwarding::process_inbox(
                                &inbox,
                                &mut ctx_inbox.cache_manager.borrow_mut().page_cache,
                            );
                        }
                    });

                    let listener = monoio::net::TcpListener::bind(&addr).unwrap();
                    info!("Worker {thread_id} ready");

                    loop {
                        let (stream, _) = listener.accept().await.unwrap();
                        let ctx = ctx.clone();
                        monoio::spawn(async move {
                            data::serve_connection(stream, ctx).await;
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
