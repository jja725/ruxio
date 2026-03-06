mod control;
mod data;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use ruxio_cluster::membership::{ClusterMembership, DiscoveryMode};
use ruxio_cluster::ring::NodeId;
use ruxio_storage::cache::CacheManager;
use ruxio_storage::gcs::GcsClient;
use ruxio_storage::metadata_cache::MetadataCache;
use ruxio_storage::page_cache::PageCache;

#[derive(Parser, Debug)]
#[command(name = "ruxio-server", about = "Ruxio distributed cache server")]
struct Args {
    /// Control plane port (HTTP/2)
    #[arg(long, default_value_t = 8080)]
    control_port: u16,

    /// Data plane port (binary protocol)
    #[arg(long, default_value_t = 8081)]
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

    /// Use O_DIRECT for disk I/O (bypass OS page cache)
    #[arg(long, default_value_t = false)]
    direct_io: bool,

    /// Bind address
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// Static peer list (comma-separated host:port)
    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,
}

#[monoio::main(timer_enabled = true)]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ruxio=debug".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!("Starting ruxio-server");
    info!("  control_port: {}", args.control_port);
    info!("  data_port: {}", args.data_port);
    info!("  cache_dir: {}", args.cache_dir);
    info!("  max_cache: {} bytes", args.max_cache_bytes);

    // Create cache directory
    std::fs::create_dir_all(&args.cache_dir)?;

    // Initialize components
    let bucket = args.bucket.unwrap_or_default();
    let gcs = GcsClient::new(&bucket);
    let metadata_cache = MetadataCache::new(args.max_metadata_entries);
    let page_cache =
        PageCache::new(&args.cache_dir, args.max_cache_bytes).with_direct_io(args.direct_io);
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
