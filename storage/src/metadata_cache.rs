use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parquet::file::metadata::ParquetMetaData;

use crate::cache_trait::Cache;

/// Cached Parquet metadata for a single file.
///
/// The engine (Presto, Spark, etc.) needs the Parquet footer to determine
/// which row groups/pages to request. By caching deserialized metadata,
/// we serve it in sub-ms instead of a 200+ms GCS round-trip.
#[derive(Debug, Clone)]
pub struct CachedParquetMeta {
    /// Parsed Parquet footer including schema and row group metadata.
    pub metadata: Arc<ParquetMetaData>,
    /// Raw footer bytes (sent to engines so they can parse it themselves).
    pub footer_bytes: bytes::Bytes,
    /// Total file size in bytes.
    pub file_size: u64,
    /// ETag from GCS for staleness detection.
    pub etag: Option<String>,
    /// When this entry was cached.
    pub cached_at: Instant,
    /// TTL for stale-while-revalidate.
    pub ttl: Duration,
}

impl CachedParquetMeta {
    /// Whether this metadata entry has expired its TTL.
    pub fn is_stale(&self) -> bool {
        self.cached_at.elapsed() > self.ttl
    }

    /// Number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.metadata.num_row_groups()
    }
}

/// Metadata cache implementing the KV Cache trait.
///
/// Key = file URI (String), Value = CachedParquetMeta.
/// Simple LRU eviction (metadata is small, CLOCK-Pro overkill).
pub struct MetadataCache {
    entries: HashMap<String, CachedParquetMeta>,
    access_order: Vec<String>,
    max_entries: usize,
}

/// Represents a byte range of a row group within a Parquet file.
#[derive(Debug, Clone)]
pub struct RowGroupRange {
    pub row_group: usize,
    pub offset: u64,
    pub length: u64,
}

impl MetadataCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(max_entries),
            access_order: Vec::with_capacity(max_entries),
            max_entries,
        }
    }

    /// Evaluate a predicate against Parquet row group statistics.
    ///
    /// Returns byte ranges for row groups that potentially contain matching data.
    /// This is the key optimization: instead of N+1 round-trips (1 metadata +
    /// N page fetches), the engine sends 1 Scan RPC and ruxio evaluates the
    /// predicate against cached stats, streaming back only matching pages.
    pub fn find_matching_row_groups(
        meta: &CachedParquetMeta,
        predicate: &ruxio_protocol::predicate::PredicateExpr,
    ) -> Vec<RowGroupRange> {
        let mut matches = Vec::new();

        for (rg_idx, rg) in meta.metadata.row_groups().iter().enumerate() {
            if Self::row_group_might_match(meta, rg, predicate) {
                let rg_offset = rg.column(0).byte_range().0;
                let rg_end = rg
                    .columns()
                    .iter()
                    .map(|c| {
                        let (off, len) = c.byte_range();
                        off + len
                    })
                    .max()
                    .unwrap_or(rg_offset);

                matches.push(RowGroupRange {
                    row_group: rg_idx,
                    offset: rg_offset,
                    length: rg_end - rg_offset,
                });
            }
        }

        matches
    }

    fn row_group_might_match(
        meta: &CachedParquetMeta,
        rg: &parquet::file::metadata::RowGroupMetaData,
        predicate: &ruxio_protocol::predicate::PredicateExpr,
    ) -> bool {
        use ruxio_protocol::predicate::PredicateExpr;

        match predicate {
            PredicateExpr::Comparison { column, op, value } => {
                if let Some(col_idx) = find_column_index(meta, column) {
                    if let Some(stats) = rg.column(col_idx).statistics() {
                        return evaluate_stats_comparison(stats, op, value);
                    }
                }
                true // no stats or unknown column → assume match (safe)
            }
            PredicateExpr::And(left, right) => {
                Self::row_group_might_match(meta, rg, left)
                    && Self::row_group_might_match(meta, rg, right)
            }
            PredicateExpr::Or(left, right) => {
                Self::row_group_might_match(meta, rg, left)
                    || Self::row_group_might_match(meta, rg, right)
            }
            PredicateExpr::Not(inner) => {
                // NOT can't prune row groups safely with only min/max stats
                // (a row group with min=1,max=10 could have NOT(col=5) matching)
                let _ = inner;
                true
            }
            PredicateExpr::In { column, values } => {
                // Check if any value in the IN list falls within the row group's range
                if let Some(col_idx) = find_column_index(meta, column) {
                    if let Some(stats) = rg.column(col_idx).statistics() {
                        return values.iter().any(|v| {
                            evaluate_stats_comparison(
                                stats,
                                &ruxio_protocol::predicate::CompOp::Eq,
                                v,
                            )
                        });
                    }
                }
                true
            }
            PredicateExpr::Between { column, low, high } => {
                // BETWEEN is equivalent to (col >= low AND col <= high)
                if let Some(col_idx) = find_column_index(meta, column) {
                    if let Some(stats) = rg.column(col_idx).statistics() {
                        let ge_low = evaluate_stats_comparison(
                            stats,
                            &ruxio_protocol::predicate::CompOp::Ge,
                            low,
                        );
                        let le_high = evaluate_stats_comparison(
                            stats,
                            &ruxio_protocol::predicate::CompOp::Le,
                            high,
                        );
                        return ge_low && le_high;
                    }
                }
                true
            }
            PredicateExpr::IsNull { .. } => true, // can't prune with min/max stats
        }
    }

    fn evict_oldest(&mut self) {
        if let Some(oldest_key) = self.access_order.first().cloned() {
            self.entries.remove(&oldest_key);
            self.access_order.remove(0);
        }
    }
}

impl Cache for MetadataCache {
    type Key = String;
    type Value = CachedParquetMeta;

    fn get(&mut self, key: &String) -> Option<&CachedParquetMeta> {
        if self.entries.contains_key(key) {
            // Move to end of access order (most recent)
            self.access_order.retain(|k| k != key);
            self.access_order.push(key.clone());
            self.entries.get(key)
        } else {
            None
        }
    }

    fn put(&mut self, key: String, value: CachedParquetMeta) -> bool {
        if self.entries.len() >= self.max_entries && !self.entries.contains_key(&key) {
            self.evict_oldest();
        }
        self.access_order.retain(|k| k != &key);
        self.access_order.push(key.clone());
        self.entries.insert(key, value);
        true
    }

    fn contains(&self, key: &String) -> bool {
        self.entries.contains_key(key)
    }

    fn remove(&mut self, key: &String) -> Option<CachedParquetMeta> {
        self.access_order.retain(|k| k != key);
        self.entries.remove(key)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

fn find_column_index(meta: &CachedParquetMeta, column_name: &str) -> Option<usize> {
    let schema = meta.metadata.file_metadata().schema_descr();
    schema
        .columns()
        .iter()
        .position(|field| field.name() == column_name)
}

fn evaluate_stats_comparison(
    stats: &parquet::file::statistics::Statistics,
    op: &ruxio_protocol::predicate::CompOp,
    value: &ruxio_protocol::predicate::ScalarValue,
) -> bool {
    use parquet::file::statistics::Statistics;
    use ruxio_protocol::predicate::{CompOp, ScalarValue};

    match (stats, value) {
        (Statistics::Int64(s), ScalarValue::Int64(v)) => {
            match (s.min_opt().copied(), s.max_opt().copied()) {
                (Some(min), Some(max)) => match op {
                    CompOp::Eq => *v >= min && *v <= max,
                    CompOp::Ne => true,
                    CompOp::Lt => min < *v,
                    CompOp::Gt => max > *v,
                    CompOp::Le => min <= *v,
                    CompOp::Ge => max >= *v,
                },
                _ => true,
            }
        }
        (Statistics::Double(s), ScalarValue::Float64(v)) => {
            match (s.min_opt().copied(), s.max_opt().copied()) {
                (Some(min), Some(max)) => match op {
                    CompOp::Eq => *v >= min && *v <= max,
                    CompOp::Ne => true,
                    CompOp::Lt => min < *v,
                    CompOp::Gt => max > *v,
                    CompOp::Le => min <= *v,
                    CompOp::Ge => max >= *v,
                },
                _ => true,
            }
        }
        (Statistics::Float(s), ScalarValue::Float64(v)) => {
            match (s.min_opt().map(|x| *x as f64), s.max_opt().map(|x| *x as f64)) {
                (Some(min), Some(max)) => match op {
                    CompOp::Eq => *v >= min && *v <= max,
                    CompOp::Ne => true,
                    CompOp::Lt => min < *v,
                    CompOp::Gt => max > *v,
                    CompOp::Le => min <= *v,
                    CompOp::Ge => max >= *v,
                },
                _ => true,
            }
        }
        _ => true, // type mismatch or unsupported → assume match (safe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_cache_basic() {
        let mut cache = MetadataCache::new(2);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_metadata_cache_eviction() {
        let mut cache = MetadataCache::new(2);

        let make_meta = || CachedParquetMeta {
            metadata: Arc::new(ParquetMetaData::new(
                parquet::file::metadata::FileMetaData::new(
                    0,
                    0,
                    None,
                    None,
                    Arc::new(parquet::schema::types::SchemaDescriptor::new(Arc::new(
                        parquet::schema::types::Type::group_type_builder("schema")
                            .build()
                            .unwrap(),
                    ))),
                    None,
                ),
                Vec::new(),
            )),
            footer_bytes: bytes::Bytes::new(),
            file_size: 1000,
            etag: None,
            cached_at: Instant::now(),
            ttl: Duration::from_secs(300),
        };

        cache.put("a".into(), make_meta());
        cache.put("b".into(), make_meta());
        assert_eq!(cache.len(), 2);

        // Adding a third should evict "a" (oldest)
        cache.put("c".into(), make_meta());
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&"a".into()));
        assert!(cache.contains(&"b".into()));
        assert!(cache.contains(&"c".into()));
    }
}
