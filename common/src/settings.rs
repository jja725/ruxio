use std::env;

use lazy_static::lazy_static;

use anyhow::Result;
use config::{Config, Environment, File};
use serde::Deserialize;

use local_ip_address::local_ip;
use tracing::info;

/// Default port for control plane (health, metrics, readiness).
const DEFAULT_CONTROL_PORT: u16 = 51_235;

/// Default port for binary data plane (frame protocol).
const DEFAULT_DATA_PORT: u16 = 51_234;

/// Default port for HTTP/1.1 data plane (zero-copy reads).
const DEFAULT_HTTP_PORT: u16 = 51_236;

/// Default number of worker threads.
const DEFAULT_THREADS: usize = 16;

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::new().expect(
        "Failed to load ruxio configuration. Check RUXIO_CONFIG env var and config file path."
    );
}

/// Top-level configuration.
///
/// Loaded from (in order of priority):
/// 1. TOML config file (path from `RUXIO_CONFIG` env var, default `ruxio_config.toml`)
/// 2. Environment variables with `RUXIO_` prefix (e.g., `RUXIO_DATA_PORT=51234`)
///    Nested keys use `__` separator (e.g., `RUXIO_CACHE__PAGE_SIZE_BYTES=4194304`)
#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub debug: bool,
    pub log_level: String,
    pub hostname: String,
    pub local_ip: String,
    pub control_port: u16,
    pub data_port: u16,
    pub http_port: u16,
    pub threads: usize,
    pub service_discovery_type: String,
    pub etcd_uris: Vec<String>,
    pub etcd_prefix: String,
    pub static_service_list: Vec<String>,
    pub metrics_push_uri: Option<String>,
    pub cache: CacheSettings,
    pub gcs: GcsSettings,
    pub server: ServerSettings,
    pub cluster: ClusterSettings,
}

/// Page cache and metadata cache configuration.
#[derive(Clone, Debug, Deserialize)]
pub struct CacheSettings {
    pub page_size_bytes: usize,
    pub max_cache_bytes: u64,
    pub max_metadata_entries: usize,
    pub metadata_ttl_secs: u64,
    pub root_path: String,
    pub eviction_policy: String,
    pub restore_on_startup: bool,
}

/// GCS client configuration.
#[derive(Clone, Debug, Deserialize)]
pub struct GcsSettings {
    pub bucket: String,
    pub max_idle_connections: usize,
    pub idle_timeout_secs: u64,
    pub credentials_path: Option<String>,
    pub max_retries: u32,
    pub retry_base_delay_ms: u64,
    pub retry_max_delay_ms: u64,
    pub request_timeout_secs: u64,
}

/// Server networking and connection settings.
#[derive(Clone, Debug, Deserialize)]
pub struct ServerSettings {
    pub idle_timeout_secs: u64,
    pub max_inflight_bytes: u64,
    pub max_connections_per_thread: u32,
    /// Per-write timeout in seconds (prevents blocking on stalled clients).
    pub write_timeout_secs: u64,
    /// Max pending requests per thread before rejecting with RESOURCE_EXHAUSTED.
    pub max_pending_requests: u32,
    /// Response chunk size for large reads (bytes). Large responses are split
    /// into chunks of this size to enable backpressure between chunks.
    pub response_chunk_bytes: u64,
    /// Sendfile deadline in seconds. If a sendfile transfer takes longer
    /// than this, the connection is closed.
    pub sendfile_timeout_secs: u64,
    /// TCP listen backlog (max pending connections queued by kernel).
    /// Too small = dropped connections under burst. Default 1024.
    pub so_backlog: u32,
    /// Enable SO_REUSEADDR on server sockets. Default true.
    pub so_reuseaddr: bool,
    /// Enable TCP_FASTOPEN on server sockets (saves 1 RTT). Default true on Linux.
    pub tcp_fastopen: bool,
    /// TCP_FASTOPEN queue length. Only used if tcp_fastopen=true. Default 256.
    pub tcp_fastopen_qlen: u32,
    /// Write buffer high water mark (bytes). When pending bytes exceed this,
    /// the connection enters backpressured state and pauses accepting new frames.
    /// Default 64KB.
    pub write_buffer_high: u64,
    /// Write buffer low water mark (bytes). When pending bytes drain below this,
    /// the connection exits backpressured state and resumes. Default 32KB.
    /// Must be < write_buffer_high. Hysteresis prevents flapping.
    pub write_buffer_low: u64,
}

/// Cluster membership settings.
#[derive(Clone, Debug, Deserialize)]
pub struct ClusterSettings {
    pub vnodes_per_node: usize,
    pub etcd_lease_ttl_secs: u64,
    pub inflight_timeout_secs: u64,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            page_size_bytes: 4 * 1024 * 1024,         // 4MB
            max_cache_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            max_metadata_entries: 100_000,
            metadata_ttl_secs: 300,
            root_path: "/tmp/ruxio_cache".into(),
            eviction_policy: "lru".into(),
            restore_on_startup: true,
        }
    }
}

impl Default for GcsSettings {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            max_idle_connections: 4,
            idle_timeout_secs: 60,
            credentials_path: None,
            max_retries: 5,
            retry_base_delay_ms: 100,
            retry_max_delay_ms: 5_000,
            request_timeout_secs: 30,
        }
    }
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            idle_timeout_secs: 300,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_connections_per_thread: 10_000,
            write_timeout_secs: 60,
            max_pending_requests: 1_000,
            response_chunk_bytes: 4 * 1024 * 1024, // 4MB chunks
            sendfile_timeout_secs: 300,
            so_backlog: 1_024,
            so_reuseaddr: true,
            tcp_fastopen: true,
            tcp_fastopen_qlen: 256,
            write_buffer_high: 64 * 1024, // 64KB
            write_buffer_low: 32 * 1024,  // 32KB
        }
    }
}

impl Default for ClusterSettings {
    fn default() -> Self {
        Self {
            vnodes_per_node: 150,
            etcd_lease_ttl_secs: 10,
            inflight_timeout_secs: 30,
        }
    }
}

impl From<Config> for Settings {
    fn from(config: Config) -> Self {
        let debug = config.get_bool("debug").unwrap_or(false);
        let log_level = config
            .get::<String>("log_level")
            .unwrap_or_else(|_| "info".into());
        let hostname = config.get::<String>("hostname").unwrap_or_else(|_| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string())
        });
        let local_ip = config.get::<String>("local_ip").unwrap_or_else(|_| {
            local_ip()
                .map(|ip| ip.to_string())
                .unwrap_or_else(|_| "127.0.0.1".to_string())
        });
        let control_port = config
            .get::<u16>("control_port")
            .unwrap_or(DEFAULT_CONTROL_PORT);
        let data_port = config.get::<u16>("data_port").unwrap_or(DEFAULT_DATA_PORT);
        let http_port = config.get::<u16>("http_port").unwrap_or(DEFAULT_HTTP_PORT);
        let threads = config.get::<usize>("threads").unwrap_or(DEFAULT_THREADS);
        let service_discovery_type = config
            .get_string("service_discovery_type")
            .unwrap_or_else(|_| "static".into());
        let etcd_prefix = config
            .get_string("etcd_prefix")
            .unwrap_or_else(|_| "/ruxio/nodes/".into());
        let static_service_list = config
            .get_string("static_service_list")
            .map(|s| s.split(',').map(String::from).collect())
            .unwrap_or_default();
        let etcd_uris = config
            .get_string("etcd_uris")
            .map(|s| s.split(',').map(String::from).collect())
            .unwrap_or_default();
        let metrics_push_uri = config.get_string("metrics_push_uri").ok();

        let cache_defaults = CacheSettings::default();
        let cache = CacheSettings {
            page_size_bytes: config
                .get::<usize>("cache.page_size_bytes")
                .unwrap_or(cache_defaults.page_size_bytes),
            max_cache_bytes: config
                .get::<u64>("cache.max_cache_bytes")
                .unwrap_or(cache_defaults.max_cache_bytes),
            max_metadata_entries: config
                .get::<usize>("cache.max_metadata_entries")
                .unwrap_or(cache_defaults.max_metadata_entries),
            metadata_ttl_secs: config
                .get::<u64>("cache.metadata_ttl_secs")
                .unwrap_or(cache_defaults.metadata_ttl_secs),
            root_path: config
                .get::<String>("cache.root_path")
                .unwrap_or(cache_defaults.root_path),
            eviction_policy: config
                .get::<String>("cache.eviction_policy")
                .unwrap_or(cache_defaults.eviction_policy),
            restore_on_startup: config
                .get::<bool>("cache.restore_on_startup")
                .unwrap_or(cache_defaults.restore_on_startup),
        };

        let gcs_defaults = GcsSettings::default();
        let gcs = GcsSettings {
            bucket: config
                .get::<String>("gcs.bucket")
                .unwrap_or(gcs_defaults.bucket),
            max_idle_connections: config
                .get::<usize>("gcs.max_idle_connections")
                .unwrap_or(gcs_defaults.max_idle_connections),
            idle_timeout_secs: config
                .get::<u64>("gcs.idle_timeout_secs")
                .unwrap_or(gcs_defaults.idle_timeout_secs),
            credentials_path: config.get::<String>("gcs.credentials_path").ok(),
            max_retries: config
                .get::<u32>("gcs.max_retries")
                .unwrap_or(gcs_defaults.max_retries),
            retry_base_delay_ms: config
                .get::<u64>("gcs.retry_base_delay_ms")
                .unwrap_or(gcs_defaults.retry_base_delay_ms),
            retry_max_delay_ms: config
                .get::<u64>("gcs.retry_max_delay_ms")
                .unwrap_or(gcs_defaults.retry_max_delay_ms),
            request_timeout_secs: config
                .get::<u64>("gcs.request_timeout_secs")
                .unwrap_or(gcs_defaults.request_timeout_secs),
        };

        let server_defaults = ServerSettings::default();
        let server = ServerSettings {
            idle_timeout_secs: config
                .get::<u64>("server.idle_timeout_secs")
                .unwrap_or(server_defaults.idle_timeout_secs),
            max_inflight_bytes: config
                .get::<u64>("server.max_inflight_bytes")
                .unwrap_or(server_defaults.max_inflight_bytes),
            max_connections_per_thread: config
                .get::<u32>("server.max_connections_per_thread")
                .unwrap_or(server_defaults.max_connections_per_thread),
            write_timeout_secs: config
                .get::<u64>("server.write_timeout_secs")
                .unwrap_or(server_defaults.write_timeout_secs),
            max_pending_requests: config
                .get::<u32>("server.max_pending_requests")
                .unwrap_or(server_defaults.max_pending_requests),
            response_chunk_bytes: config
                .get::<u64>("server.response_chunk_bytes")
                .unwrap_or(server_defaults.response_chunk_bytes),
            sendfile_timeout_secs: config
                .get::<u64>("server.sendfile_timeout_secs")
                .unwrap_or(server_defaults.sendfile_timeout_secs),
            so_backlog: config
                .get::<u32>("server.so_backlog")
                .unwrap_or(server_defaults.so_backlog),
            so_reuseaddr: config
                .get::<bool>("server.so_reuseaddr")
                .unwrap_or(server_defaults.so_reuseaddr),
            tcp_fastopen: config
                .get::<bool>("server.tcp_fastopen")
                .unwrap_or(server_defaults.tcp_fastopen),
            tcp_fastopen_qlen: config
                .get::<u32>("server.tcp_fastopen_qlen")
                .unwrap_or(server_defaults.tcp_fastopen_qlen),
            write_buffer_high: config
                .get::<u64>("server.write_buffer_high")
                .unwrap_or(server_defaults.write_buffer_high),
            write_buffer_low: config
                .get::<u64>("server.write_buffer_low")
                .unwrap_or(server_defaults.write_buffer_low),
        };

        let cluster_defaults = ClusterSettings::default();
        let cluster = ClusterSettings {
            vnodes_per_node: config
                .get::<usize>("cluster.vnodes_per_node")
                .unwrap_or(cluster_defaults.vnodes_per_node),
            etcd_lease_ttl_secs: config
                .get::<u64>("cluster.etcd_lease_ttl_secs")
                .unwrap_or(cluster_defaults.etcd_lease_ttl_secs),
            inflight_timeout_secs: config
                .get::<u64>("cluster.inflight_timeout_secs")
                .unwrap_or(cluster_defaults.inflight_timeout_secs),
        };

        let settings = Settings {
            debug,
            log_level,
            hostname,
            local_ip,
            control_port,
            data_port,
            http_port,
            threads,
            service_discovery_type,
            etcd_uris,
            etcd_prefix,
            static_service_list,
            metrics_push_uri,
            cache,
            gcs,
            server,
            cluster,
        };
        info!("Settings loaded {:?}", settings);
        settings
    }
}

impl Settings {
    pub fn new() -> Result<Self> {
        let config_filename = env::var("RUXIO_CONFIG").unwrap_or_else(|_| "ruxio_config".into());
        let settings: Settings = Config::builder()
            .add_source(File::with_name(config_filename.as_str()).required(false))
            .add_source(Environment::with_prefix("RUXIO").separator("__"))
            .build()?
            .into();
        Ok(settings)
    }
}
