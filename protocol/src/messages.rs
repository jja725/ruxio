use serde::{Deserialize, Serialize};

use crate::predicate::PredicateExpr;

/// Read a byte range from a cached file (engine already knows which bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRangeRequest {
    pub uri: String,
    pub offset: u64,
    pub length: u64,
}

/// Read multiple byte ranges in a single round-trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchReadRequest {
    pub reads: Vec<ReadRangeRequest>,
}

/// Scan with predicate pushdown (saves N+1 round-trips → 1 RPC).
///
/// Instead of: engine fetches metadata → evaluates predicate → requests N pages,
/// the engine sends one Scan RPC. Ruxio evaluates the predicate against cached
/// Parquet row group statistics and streams back only matching pages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRequest {
    pub uri: String,
    pub predicate: Option<PredicateExpr>,
    pub projection: Option<Vec<String>>,
}

/// Scan multiple files with predicates in one round-trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchScanRequest {
    pub scans: Vec<ScanRequest>,
}

/// Request file metadata (Parquet footer) from cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetadataRequest {
    pub uri: String,
}

/// Metadata response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub uri: String,
    pub file_size: u64,
    pub footer_size: u64,
}

/// Error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: u32,
    pub message: String,
}

/// Redirect to the correct node (consistent hash miss).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedirectResponse {
    pub target_host: String,
    pub target_data_port: u16,
}
