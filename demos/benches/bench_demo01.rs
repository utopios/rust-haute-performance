use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use demos::demo01::{somme_carres_naive, somme_carres_preallouee, somme_carres_rayon};

fn bench_somme_carres(c: &mut Criterion) {
    let mut group = c.benchmark_group("somme_carres");

    for &taille in &[1_000usize, 100_000, 1_000_000] {
        let data: Vec<u64> = (0..taille as u64).map(|i| (i % 1000) + 1).collect();

        group.bench_with_input(
            BenchmarkId::new("naive", taille),
            &data,
            |b, data| b.iter(|| somme_carres_naive(black_box(data))),
        );
        group.bench_with_input(
            BenchmarkId::new("rayon", taille),
            &data,
            |b, data| b.iter(|| somme_carres_rayon(black_box(data))),
        );
        group.bench_with_input(
            BenchmarkId::new("preallouee", taille),
            &data,
            |b, data| b.iter(|| somme_carres_preallouee(black_box(data))),
        );
    }
    group.finish();
}

criterion_group!(benches, bench_somme_carres);
criterion_main!(benches);
