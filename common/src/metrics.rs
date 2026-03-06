use prometheus::{
    labels, register_counter, register_histogram, register_int_counter_vec, register_int_gauge,
};
use prometheus::{Counter, Histogram, IntCounterVec, IntGauge, Opts, Registry};

use lazy_static::lazy_static;

use std::error::Error;

use log::{error, trace};

use crate::settings::SETTINGS;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref INCOMING_REQUESTS: Counter =
        register_counter!("incoming_requests", "Incoming Requests").unwrap();
    pub static ref CONNECTED_CLIENTS: IntGauge =
        register_int_gauge!("connected_clients", "Connected Clients").unwrap();
    pub static ref RESPONSE_CODE_COLLECTOR: IntCounterVec = register_int_counter_vec!(
        Opts::new("response_code", "Response Codes"),
        &["env", "statuscode", "type"]
    )
    .unwrap();
    pub static ref RESPONSE_TIME_COLLECTOR: Histogram =
        register_histogram!("response_time", "Response Times").unwrap();
    pub static ref CACHE_HIT_COUNTER: Counter =
        register_counter!("cache_hits_total", "Total cache hits").unwrap();
    pub static ref CACHE_MISS_COUNTER: Counter =
        register_counter!("cache_misses_total", "Total cache misses").unwrap();
    pub static ref PAGE_CACHE_BYTES: IntGauge =
        register_int_gauge!("page_cache_bytes", "Current page cache size in bytes").unwrap();
    pub static ref GCS_FETCH_COUNTER: Counter =
        register_counter!("gcs_fetches_total", "Total GCS fetch requests").unwrap();
    pub static ref GCS_FETCH_LATENCY: Histogram =
        register_histogram!("gcs_fetch_latency_seconds", "GCS fetch latency").unwrap();
    static ref PUSH_COUNTER: Counter =
        register_counter!("push_counter", "Total number of prometheus client pushed.").unwrap();
    static ref PUSH_REQ_HISTOGRAM: Histogram = register_histogram!(
        "push_request_latency_seconds",
        "The push request latencies in seconds."
    )
    .unwrap();
}

pub fn push_metrics() -> Result<(), Box<dyn Error>> {
    let push_uri = match SETTINGS.metrics_push_uri.as_deref() {
        Some(uri) => uri,
        None => return Ok(()),
    };
    trace!("Pushing metrics to gateway {}", push_uri);

    PUSH_COUNTER.inc();
    let metric_families = prometheus::gather();
    let _timer = PUSH_REQ_HISTOGRAM.start_timer();
    let push_result = prometheus::push_metrics(
        "ruxio_worker",
        labels! {"instance".to_owned() => format!("{}:{}", SETTINGS.local_ip, SETTINGS.control_port),},
        push_uri,
        metric_families,
        None,
    );
    match push_result {
        Ok(_) => {
            trace!("Pushing metrics to gateway {} succeed", push_uri);
            Ok(())
        }
        Err(e) => {
            error!("Push metrics failed: {}", e);
            Ok(())
        }
    }
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
