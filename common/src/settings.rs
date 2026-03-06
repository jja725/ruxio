use std::env;

use lazy_static::lazy_static;

use anyhow::Result;
use config::{Config, Environment, File};
use serde::Deserialize;

use local_ip_address::local_ip;
use log::info;

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::new().unwrap();
}

#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub debug: bool,
    pub log_level: String,
    pub hostname: String,
    pub local_ip: String,
    pub control_port: u16,
    pub data_port: u16,
    pub service_discovery_type: String,
    pub etcd_uris: Vec<String>,
    pub static_service_list: Vec<String>,
    pub metrics_push_uri: Option<String>,
    pub cache: CacheSettings,
    pub gcs: GcsSettings,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CacheSettings {
    pub page_size_bytes: usize,
    pub max_cache_bytes: u64,
    pub max_metadata_entries: usize,
    pub metadata_ttl_secs: u64,
    pub root_path: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GcsSettings {
    pub bucket: String,
    pub max_idle_connections: usize,
    pub idle_timeout_secs: u64,
    pub credentials_path: Option<String>,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            page_size_bytes: 4 * 1024 * 1024,         // 4MB
            max_cache_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            max_metadata_entries: 100_000,
            metadata_ttl_secs: 300,
            root_path: "/tmp/ruxio_cache".into(),
        }
    }
}

impl Default for GcsSettings {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            max_idle_connections: 8,
            idle_timeout_secs: 60,
            credentials_path: None,
        }
    }
}

impl From<Config> for Settings {
    fn from(config: Config) -> Self {
        let debug = config.get_bool("is_debug").unwrap_or(false);
        let log_level = config
            .get::<String>("log_level")
            .unwrap_or_else(|_| "INFO".into());
        let hostname = config
            .get::<String>("ruxio_hostname")
            .unwrap_or_else(|_| hostname::get().unwrap().into_string().unwrap());
        let local_ip = config
            .get::<String>("local_ip")
            .unwrap_or_else(|_| local_ip().unwrap().to_string());
        let control_port = config.get::<u16>("control_port").unwrap_or(8080);
        let data_port = config.get::<u16>("data_port").unwrap_or(8081);
        let service_discovery_type = config
            .get_string("service_discovery_type")
            .unwrap_or_else(|_| "static".into());
        let static_service_list = if service_discovery_type == "static" {
            config
                .get_string("static_service_list")
                .unwrap_or_else(|_| format!("localhost:{}", control_port))
                .split(',')
                .map(String::from)
                .collect()
        } else {
            Vec::new()
        };
        let etcd_uris = if service_discovery_type == "etcd" {
            config
                .get_string("etcd_uris")
                .unwrap_or_else(|_| "localhost:2379".into())
                .split(',')
                .map(String::from)
                .collect()
        } else {
            Vec::new()
        };
        let metrics_push_uri = config.get_string("metrics_push_uri").ok();

        let cache = CacheSettings {
            page_size_bytes: config
                .get::<usize>("cache.page_size_bytes")
                .unwrap_or(4 * 1024 * 1024),
            max_cache_bytes: config
                .get::<u64>("cache.max_cache_bytes")
                .unwrap_or(10 * 1024 * 1024 * 1024),
            max_metadata_entries: config
                .get::<usize>("cache.max_metadata_entries")
                .unwrap_or(100_000),
            metadata_ttl_secs: config.get::<u64>("cache.metadata_ttl_secs").unwrap_or(300),
            root_path: config
                .get::<String>("cache.root_path")
                .unwrap_or_else(|_| "/tmp/ruxio_cache".into()),
        };

        let gcs = GcsSettings {
            bucket: config.get::<String>("gcs.bucket").unwrap_or_default(),
            max_idle_connections: config.get::<usize>("gcs.max_idle_connections").unwrap_or(8),
            idle_timeout_secs: config.get::<u64>("gcs.idle_timeout_secs").unwrap_or(60),
            credentials_path: config.get::<String>("gcs.credentials_path").ok(),
        };

        let settings = Settings {
            debug,
            log_level,
            hostname,
            local_ip,
            control_port,
            data_port,
            service_discovery_type,
            etcd_uris,
            static_service_list,
            metrics_push_uri,
            cache,
            gcs,
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
