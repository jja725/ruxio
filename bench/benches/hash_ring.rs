use criterion::{criterion_group, criterion_main, Criterion};
use ruxio_cluster::ring::{HashRing, NodeId};

fn bench_hash_ring_lookup(c: &mut Criterion) {
    let mut ring = HashRing::new(150);
    for i in 0..10 {
        ring.add_node(NodeId::new(format!("node{i}"), 8080));
    }

    c.bench_function("hash_ring_lookup_10_nodes", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("gs://bucket/table/part-{i:05}.parquet");
            ring.get_node(&key);
            i += 1;
        });
    });
}

fn bench_hash_ring_lookup_100_nodes(c: &mut Criterion) {
    let mut ring = HashRing::new(150);
    for i in 0..100 {
        ring.add_node(NodeId::new(format!("node{i}"), 8080));
    }

    c.bench_function("hash_ring_lookup_100_nodes", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("gs://bucket/table/part-{i:05}.parquet");
            ring.get_node(&key);
            i += 1;
        });
    });
}

fn bench_hash_ring_add_node(c: &mut Criterion) {
    c.bench_function("hash_ring_add_node", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let mut ring = HashRing::new(150);
            ring.add_node(NodeId::new(format!("node{i}"), 8080));
            i += 1;
        });
    });
}

criterion_group!(
    hash_ring_benches,
    bench_hash_ring_lookup,
    bench_hash_ring_lookup_100_nodes,
    bench_hash_ring_add_node
);
criterion_main!(hash_ring_benches);
