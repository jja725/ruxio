use criterion::{criterion_group, criterion_main, Criterion};
use ruxio_storage::cache_trait::Cache;
use ruxio_storage::page_cache::{CachedPage, PageCache};
use ruxio_storage::page_key::PageKey;
use bytes::Bytes;
use std::path::PathBuf;

fn bench_page_cache_put(c: &mut Criterion) {
    c.bench_function("page_cache_put", |b| {
        let mut cache = PageCache::new("/tmp/ruxio_bench_cache", 1024 * 1024 * 1024);
        let mut i = 0u64;
        b.iter(|| {
            let key = PageKey::new(format!("gs://b/f{i}.parquet"), 0, 0);
            let page = CachedPage {
                local_path: PathBuf::from("/tmp/test"),
                size: 4096,
                data: Some(Bytes::from(vec![0u8; 4096])),
            };
            cache.put(key, page);
            i += 1;
        });
    });
}

fn bench_page_cache_get_hit(c: &mut Criterion) {
    let mut cache = PageCache::new("/tmp/ruxio_bench_cache", 1024 * 1024 * 1024);
    // Pre-populate
    for i in 0..1000 {
        let key = PageKey::new(format!("gs://b/f{i}.parquet"), 0, 0);
        let page = CachedPage {
            local_path: PathBuf::from("/tmp/test"),
            size: 4096,
            data: Some(Bytes::from(vec![0u8; 4096])),
        };
        cache.put(key, page);
    }

    c.bench_function("page_cache_get_hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = PageKey::new(format!("gs://b/f{}.parquet", i % 1000), 0, 0);
            cache.get(&key);
            i += 1;
        });
    });
}

fn bench_page_cache_get_miss(c: &mut Criterion) {
    let mut cache = PageCache::new("/tmp/ruxio_bench_cache", 1024 * 1024 * 1024);

    c.bench_function("page_cache_get_miss", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = PageKey::new(format!("gs://b/miss{i}.parquet"), 0, 0);
            cache.get(&key);
            i += 1;
        });
    });
}

criterion_group!(page_cache_benches, bench_page_cache_put, bench_page_cache_get_hit, bench_page_cache_get_miss);
criterion_main!(page_cache_benches);
