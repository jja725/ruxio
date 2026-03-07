mod control;
mod data;

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

    /// GCS bucket name
    #[arg(long)]
    bucket: Option<String>,

    /// Page cache root directory
    #[arg(long, default_value = "/tmp/ruxio_cache")]
    cache_dir: String,

    /// Maximum cache size in bytes
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

    /// Populate cache with test data for benchmarking (100 x 4MB pages)
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

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ruxio=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!("Starting ruxio-server");
    info!("  data_port: {}", args.data_port);
    info!("  cache_dir: {}", args.cache_dir);
    info!("  max_cache: {} bytes", args.max_cache_bytes);

    // Create cache directory
    std::fs::create_dir_all(&args.cache_dir)?;

    // Initialize components
    let bucket = args.bucket.unwrap_or_default();
    let gcs = GcsClient::new(&bucket);
    let metadata_cache = MetadataCache::new(args.max_metadata_entries);
    let mut page_cache = PageCache::new(&args.cache_dir, args.max_cache_bytes);

    if args.bench_populate {
        info!(
            "Populating cache with {} x {}MB test pages",
            BENCH_NUM_PAGES,
            PAGE_SIZE / (1024 * 1024)
        );
        populate_bench_data(&mut page_cache);
        info!("Cache populated");
    }

    let cache_manager = CacheManager::new(gcs, metadata_cache, page_cache);

    // Initialize cluster membership
    let self_id = NodeId::new(&args.bind, args.data_port);
    let discovery = if args.peers.is_empty() {
        DiscoveryMode::Static { peers: vec![] }
    } else {
        DiscoveryMode::Static {
            peers: args.peers.clone(),
        }
    };
    let mut membership = ClusterMembership::new(self_id, 150, discovery);
    membership.discover_peers().await?;

    info!(
        "Cluster: {} nodes, self={}",
        membership.ring().node_count(),
        membership.self_id()
    );

    // Start data plane listener
    let data_addr = format!("{}:{}", args.bind, args.data_port);
    info!("Data plane listening on {data_addr}");

    data::serve_data_plane(&data_addr, cache_manager, membership).await?;

    Ok(())
}
