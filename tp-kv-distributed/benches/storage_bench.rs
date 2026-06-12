// =============================================================================
// storage_bench.rs – Benchmarks Criterion du StorageEngine et du ring
// =============================================================================
//
// Mesure le throughput des opérations PUT/GET en single-thread et la latence
// du lookup ConsistentHashRing.
//
// Cibles attendues (machine moderne, release) :
//   - GET hit single-thread : > 1 000 000 ops/s
//   - PUT single-thread     : > 200 000 ops/s
//   - Lookup ring (3n×150v) : < 500 ns / lookup
// =============================================================================

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use kv_distributed::{consistent_hash::ConsistentHashRing, storage::StorageEngine};

fn bench_storage_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_put");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_thread", |b| {
        let engine = StorageEngine::with_capacity(200_000);
        let mut i = 0u64;
        b.iter(|| {
            engine.put(
                black_box(format!("key-{i}")),
                black_box(b"payload-128-bytes".repeat(8).to_vec()),
                None,
            );
            i = i.wrapping_add(1);
        });
    });
    group.finish();
}

fn bench_storage_get_hit(c: &mut Criterion) {
    let engine = StorageEngine::with_capacity(100_000);
    for i in 0..10_000u64 {
        engine.put(format!("key-{i}"), vec![0u8; 64], None);
    }

    let mut group = c.benchmark_group("storage_get_hit");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_thread", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let k = format!("key-{}", i % 10_000);
            let _ = black_box(engine.get(&k));
            i = i.wrapping_add(1);
        });
    });
    group.finish();
}

fn bench_storage_get_miss(c: &mut Criterion) {
    let engine = StorageEngine::with_capacity(100_000);
    for i in 0..1_000u64 {
        engine.put(format!("key-{i}"), vec![0u8; 64], None);
    }
    let mut group = c.benchmark_group("storage_get_miss");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_thread", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let k = format!("absent-{i}");
            let _ = black_box(engine.get(&k));
            i = i.wrapping_add(1);
        });
    });
    group.finish();
}

fn bench_ring_lookup(c: &mut Criterion) {
    let mut ring = ConsistentHashRing::new(150);
    for i in 1..=3 {
        ring.add_node(&format!("node-{i}"));
    }

    let mut group = c.benchmark_group("ring_lookup");
    group.throughput(Throughput::Elements(1));
    group.bench_function("get_node_3nodes_150vnodes", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let k = format!("user:{i}");
            let _ = black_box(ring.get_node(&k));
            i = i.wrapping_add(1);
        });
    });
    group.bench_function("get_nodes_replicas_2", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let k = format!("user:{i}");
            let _ = black_box(ring.get_nodes(&k, 2));
            i = i.wrapping_add(1);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_storage_put,
    bench_storage_get_hit,
    bench_storage_get_miss,
    bench_ring_lookup
);
criterion_main!(benches);
