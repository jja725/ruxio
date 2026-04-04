use prometheus::{
    register_counter, register_histogram, register_int_counter_vec, register_int_gauge,
};
use prometheus::{Counter, Histogram, IntCounterVec, IntGauge, Opts, Registry};

use lazy_static::lazy_static;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // ── Request metrics ─────────────────────────────────────────────

    /// Total incoming requests (frames processed).
    pub static ref INCOMING_REQUESTS: Counter =
        register_counter!("incoming_requests", "Incoming Requests").unwrap();
    /// Currently connected clients (gauge).
    pub static ref CONNECTED_CLIENTS: IntGauge =
        register_int_gauge!("connected_clients", "Connected Clients").unwrap();
    /// Currently active (in-flight) operations.
    pub static ref ACTIVE_OPERATIONS: IntGauge =
        register_int_gauge!("active_operations", "Active in-flight operations").unwrap();
    /// Response codes by type.
    pub static ref RESPONSE_CODE_COLLECTOR: IntCounterVec = register_int_counter_vec!(
        Opts::new("response_code", "Response Codes"),
        &["statuscode", "type"]
    )
    .unwrap();
    /// Request processing latency (end-to-end per frame).
    pub static ref RESPONSE_TIME_COLLECTOR: Histogram =
        register_histogram!("response_time_seconds", "Request processing latency").unwrap();

    // ── Cache hit/miss metrics ──────────────────────────────────────

    /// Page cache hits (count).
    pub static ref CACHE_HIT_COUNTER: Counter =
        register_counter!("cache_hits_total", "Total cache hits").unwrap();
    /// Page cache misses (count).
    pub static ref CACHE_MISS_COUNTER: Counter =
        register_counter!("cache_misses_total", "Total cache misses").unwrap();

    // ── Cache throughput metrics (bytes) ────────────────────────────

    /// Bytes served from local page cache.
    pub static ref BYTES_READ_CACHE: Counter =
        register_counter!("bytes_read_cache_total", "Bytes read from cache").unwrap();
    /// Bytes fetched from GCS (cache miss path).
    pub static ref BYTES_READ_GCS: Counter =
        register_counter!("bytes_read_gcs_total", "Bytes read from GCS").unwrap();
    /// Bytes served to clients (total, cache + GCS).
    pub static ref BYTES_SERVED_TOTAL: Counter =
        register_counter!("bytes_served_total", "Total bytes served to clients").unwrap();

    // ── Cache capacity metrics ──────────────────────────────────────

    /// Current page cache size in bytes.
    pub static ref PAGE_CACHE_BYTES: IntGauge =
        register_int_gauge!("page_cache_bytes", "Current page cache size in bytes").unwrap();
    /// Total page cache capacity in bytes.
    pub static ref PAGE_CACHE_CAPACITY_BYTES: IntGauge =
        register_int_gauge!("page_cache_capacity_bytes", "Total page cache capacity in bytes").unwrap();
    /// Number of cached pages.
    pub static ref PAGE_CACHE_PAGES: IntGauge =
        register_int_gauge!("page_cache_pages", "Number of pages in cache").unwrap();

    // ── Eviction metrics ────────────────────────────────────────────

    /// Total pages evicted from cache.
    pub static ref PAGES_EVICTED: Counter =
        register_counter!("pages_evicted_total", "Total pages evicted from cache").unwrap();
    /// Total bytes evicted from cache.
    pub static ref BYTES_EVICTED: Counter =
        register_counter!("bytes_evicted_total", "Total bytes evicted from cache").unwrap();

    // ── Cache error metrics ─────────────────────────────────────────

    /// Page cache put (write-to-disk) errors.
    pub static ref CACHE_PUT_ERRORS: Counter =
        register_counter!("cache_put_errors_total", "Cache put errors").unwrap();
    /// Page cache get (read-from-disk) errors.
    pub static ref CACHE_GET_ERRORS: Counter =
        register_counter!("cache_get_errors_total", "Cache get errors").unwrap();
    /// Page cache delete (eviction file removal) errors.
    pub static ref CACHE_DELETE_ERRORS: Counter =
        register_counter!("cache_delete_errors_total", "Cache delete errors").unwrap();

    // ── Metadata cache metrics ──────────────────────────────────────

    /// Metadata cache hits.
    pub static ref METADATA_CACHE_HITS: Counter =
        register_counter!("metadata_cache_hits_total", "Metadata cache hits").unwrap();
    /// Metadata cache misses.
    pub static ref METADATA_CACHE_MISSES: Counter =
        register_counter!("metadata_cache_misses_total", "Metadata cache misses").unwrap();
    /// Metadata entries invalidated by TTL.
    pub static ref METADATA_INVALIDATED: Counter =
        register_counter!("metadata_invalidated_total", "Metadata entries invalidated by TTL").unwrap();

    // ── GCS metrics ─────────────────────────────────────────────────

    /// Total GCS fetch requests.
    pub static ref GCS_FETCH_COUNTER: Counter =
        register_counter!("gcs_fetches_total", "Total GCS fetch requests").unwrap();
    /// GCS fetch latency distribution.
    pub static ref GCS_FETCH_LATENCY: Histogram =
        register_histogram!("gcs_fetch_latency_seconds", "GCS fetch latency").unwrap();
    /// GCS retry attempts.
    pub static ref GCS_RETRIES: Counter =
        register_counter!("gcs_retries_total", "Total GCS retry attempts").unwrap();
    /// GCS request timeouts.
    pub static ref GCS_TIMEOUTS: Counter =
        register_counter!("gcs_timeouts_total", "Total GCS request timeouts").unwrap();

    // ── Cache read latency ──────────────────────────────────────────

    /// Latency for cache hit reads (disk + OS page cache).
    pub static ref CACHE_READ_LATENCY: Histogram =
        register_histogram!("cache_read_latency_seconds", "Cache hit read latency").unwrap();

    // ── Cluster metrics ─────────────────────────────────────────────

    /// Number of nodes in the cluster ring.
    pub static ref CLUSTER_MEMBERS: IntGauge =
        register_int_gauge!("cluster_members", "Number of nodes in the cluster").unwrap();

    // ── Thundering herd metrics ─────────────────────────────────────

    /// Requests that waited on in-flight fetches (thundering herd coalesced).
    pub static ref INFLIGHT_COALESCED: Counter =
        register_counter!("inflight_coalesced_total", "Requests coalesced via thundering herd prevention").unwrap();
}

pub fn metrics_result() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        eprintln!("could not encode custom metrics: {}", e);
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        eprintln!("could not encode prometheus metrics: {}", e);
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("prometheus metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    res.push_str(&res_custom);
    res
}
