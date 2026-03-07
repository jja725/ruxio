//! Benchmark: CLOCK-Pro+TinyLFU vs LRU+TinyLFU
//!
//! Both use the same PageCache with configurable EvictionPolicy.
//! TinyLFU admission is shared — this isolates eviction policy impact.
//!
//! Workloads:
//! 1. Zipf — realistic skewed access (a few hot files, long tail of cold)
//! 2. Scan-resistant — hot working set polluted by sequential scans

use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion};
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::page_cache::{CachedPage, EvictionPolicy, PageCache};
use ruxio_storage::page_key::PageKey;

const PAGE_SIZE: u64 = 4 * 1024 * 1024;

// ── Access pattern generators ────────────────────────────────────────

fn zipf_sequence(num_keys: u64, length: usize, seed: u64) -> Vec<u64> {
    let mut cdf = Vec::with_capacity(num_keys as usize);
    let mut total = 0.0f64;
    for i in 0..num_keys {
        total += 1.0 / (i as f64 + 1.0);
        cdf.push(total);
    }
    for v in &mut cdf {
        *v /= total;
    }

    let mut rng = seed;
    let mut result = Vec::with_capacity(length);
    for _ in 0..length {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let r = (rng >> 33) as f64 / (1u64 << 31) as f64;
        let idx = cdf.partition_point(|&v| v < r) as u64;
        result.push(idx.min(num_keys - 1));
    }
    result
}

fn scan_resistant_sequence(hot_size: u64, total_keys: u64, length: usize, seed: u64) -> Vec<u64> {
    let mut rng = seed;
    let mut scan_pos = hot_size;
    let mut result = Vec::with_capacity(length);

    for _ in 0..length {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let r = (rng >> 33) as f64 / (1u64 << 31) as f64;

        if r < 0.8 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let idx = (rng >> 33) as u64 % hot_size;
            result.push(idx);
        } else {
            result.push(scan_pos);
            scan_pos += 1;
            if scan_pos >= total_keys {
                scan_pos = hot_size;
            }
        }
    }
    result
}

// ── Hit rate measurement ─────────────────────────────────────────────

fn make_page() -> CachedPage {
    CachedPage {
        local_path: PathBuf::from("/tmp/test"),
        size: PAGE_SIZE,
        data: None,
    }
}

fn measure_hit_rate(policy: EvictionPolicy, cache_pages: u64, sequence: &[u64]) -> f64 {
    let mut cache = PageCache::with_policy(
        "/tmp/bench_eviction",
        cache_pages * PAGE_SIZE,
        PAGE_SIZE,
        policy,
    );
    let mut hits = 0u64;
    for &idx in sequence {
        let key = PageKey::new("test://file", idx);
        if cache.get(&key).is_some() {
            hits += 1;
        } else {
            cache.put(key, make_page());
        }
    }
    hits as f64 / sequence.len() as f64
}

// ── Benchmarks ───────────────────────────────────────────────────────

fn bench_eviction_policies(c: &mut Criterion) {
    let cache_pages: u64 = 100;
    let total_keys: u64 = 1000;
    let seq_len = 100_000;

    let zipf_seq = zipf_sequence(total_keys, seq_len, 42);
    let scan_seq = scan_resistant_sequence(cache_pages / 2, total_keys, seq_len, 42);

    // ── Hit Rate Comparison ──────────────────────────────────────────
    println!();
    println!("=== Hit Rate (cache={cache_pages} pages, keys={total_keys}, both with TinyLFU) ===");
    println!();
    println!("  {:20} {:>15} {:>15}", "Workload", "CLOCK-Pro", "LRU");
    println!("  {:20} {:>15} {:>15}", "--------", "---------", "---");

    let cp_zipf = measure_hit_rate(EvictionPolicy::ClockPro, cache_pages, &zipf_seq);
    let lr_zipf = measure_hit_rate(EvictionPolicy::Lru, cache_pages, &zipf_seq);
    println!(
        "  {:20} {:>14.1}% {:>14.1}%",
        "Zipf",
        cp_zipf * 100.0,
        lr_zipf * 100.0,
    );

    let cp_scan = measure_hit_rate(EvictionPolicy::ClockPro, cache_pages, &scan_seq);
    let lr_scan = measure_hit_rate(EvictionPolicy::Lru, cache_pages, &scan_seq);
    println!(
        "  {:20} {:>14.1}% {:>14.1}%",
        "Scan-resistant",
        cp_scan * 100.0,
        lr_scan * 100.0,
    );
    println!();

    // ── Throughput Benchmarks ────────────────────────────────────────
    let mut group = c.benchmark_group("eviction_throughput");

    for (name, seq) in [("zipf", &zipf_seq), ("scan", &scan_seq)] {
        for (policy_name, policy) in [
            ("clockpro", EvictionPolicy::ClockPro),
            ("lru", EvictionPolicy::Lru),
        ] {
            group.bench_function(format!("{policy_name}_{name}"), |b| {
                let mut cache = PageCache::with_policy(
                    "/tmp/bench_eviction",
                    cache_pages * PAGE_SIZE,
                    PAGE_SIZE,
                    policy,
                );
                let mut i = 0usize;
                b.iter(|| {
                    let idx = seq[i % seq.len()];
                    let key = PageKey::new("test://file", idx);
                    if cache.get(&key).is_none() {
                        cache.put(key, make_page());
                    }
                    i += 1;
                });
            });
        }
    }

    group.finish();
}

criterion_group!(eviction_benches, bench_eviction_policies);
criterion_main!(eviction_benches);
