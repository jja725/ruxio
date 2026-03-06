use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::page_cache::{CachedPage, PageCache};
use ruxio_storage::page_key::PageKey;
use std::path::PathBuf;

const NUM_KEYS: u64 = 1000;

fn make_key(i: u64) -> PageKey {
    PageKey::new(
        "gs://bucket/file.parquet",
        (i / 100) as usize,
        (i % 100) * 4096,
    )
}

fn make_page() -> CachedPage {
    CachedPage {
        local_path: PathBuf::from("/tmp/test"),
        size: 4096,
        data: Some(Bytes::from_static(&[0u8; 64])), // small data for bench speed
    }
}

fn bench_page_cache_put(c: &mut Criterion) {
    c.bench_function("page_cache_put", |b| {
        let mut cache = PageCache::new("/tmp/ruxio_bench_cache", 1024 * 1024 * 1024);
        let mut i = 0u64;
        b.iter(|| {
            let key = make_key(i % NUM_KEYS);
            cache.put(key, make_page());
            i += 1;
        });
    });
}

fn bench_page_cache_get_hit(c: &mut Criterion) {
    let mut cache = PageCache::new("/tmp/ruxio_bench_cache", 1024 * 1024 * 1024);
    for i in 0..NUM_KEYS {
        cache.put(make_key(i), make_page());
    }

    c.bench_function("page_cache_get_hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = make_key(i % NUM_KEYS);
            let _ = cache.get(&key);
            i += 1;
        });
    });
}

fn bench_page_cache_get_miss(c: &mut Criterion) {
    let mut cache = PageCache::new("/tmp/ruxio_bench_cache", 1024 * 1024 * 1024);
    // populate with keys 0..999
    for i in 0..NUM_KEYS {
        cache.put(make_key(i), make_page());
    }

    c.bench_function("page_cache_get_miss", |b| {
        let mut i = 0u64;
        b.iter(|| {
            // lookup keys that don't exist (offset by NUM_KEYS)
            let key = PageKey::new("gs://bucket/other.parquet", 0, i % 1000 * 4096);
            let _ = cache.get(&key);
            i += 1;
        });
    });
}

fn bench_page_cache_contains_bloom(c: &mut Criterion) {
    let mut cache = PageCache::new("/tmp/ruxio_bench_cache", 1024 * 1024 * 1024);
    for i in 0..NUM_KEYS {
        cache.put(make_key(i), make_page());
    }

    c.bench_function("page_cache_bloom_hit", |b| {
        b.iter(|| {
            cache.maybe_has_file("gs://bucket/file.parquet");
        });
    });

    c.bench_function("page_cache_bloom_miss", |b| {
        b.iter(|| {
            cache.maybe_has_file("gs://bucket/nonexistent.parquet");
        });
    });
}

criterion_group!(
    page_cache_benches,
    bench_page_cache_put,
    bench_page_cache_get_hit,
    bench_page_cache_get_miss,
    bench_page_cache_contains_bloom,
);
criterion_main!(page_cache_benches);
