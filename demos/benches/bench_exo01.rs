use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};


mod exo01 {
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    pub struct Stats {
        pub p50: u64,
        pub p99: u64,
        pub max: u64,
    }

    pub fn stats_par_tri(data: &[u64]) -> Stats {
        let mut v = data.to_vec();
        v.sort_unstable();
        let n = v.len();
        Stats {
            p50: v[n / 2],
            p99: v[(n * 99) / 100],
            max: v[n - 1],
        }
    }

    pub fn stats_par_select(data: &[u64]) -> Stats {
        let mut v = data.to_vec();
        let n = v.len();
        let idx_max = n - 1;
        let idx_p99 = (n * 99) / 100;
        let idx_p50 = n / 2;
        if idx_p99 == idx_max {
            v.select_nth_unstable(idx_max);
            let max = v[idx_max];
            v[..idx_max].select_nth_unstable(idx_p50);
            return Stats {
                p50: v[idx_p50],
                p99: max,
                max,
            };
        }
        v.select_nth_unstable(idx_max);
        let max = v[idx_max];
        v[..idx_max].select_nth_unstable(idx_p99);
        let p99 = v[idx_p99];
        v[..idx_p99].select_nth_unstable(idx_p50);
        let p50 = v[idx_p50];
        Stats { p50, p99, max }
    }

    pub fn stats_par_histogramme(data: &[u64], borne_max: u64) -> Stats {
        let mut hist = vec![0u32; (borne_max + 1) as usize];
        let mut vrai_max = 0u64;
        for &v in data {
            hist[v as usize] += 1;
            if v > vrai_max {
                vrai_max = v;
            }
        }
        let n = data.len() as u64;
        let seuil_p50 = n / 2 + 1;
        let seuil_p99 = (n * 99) / 100 + 1;
        let mut cumul = 0u64;
        let mut p50 = 0u64;
        let mut p99 = 0u64;
        let mut p50_trouve = false;
        for (valeur, &count) in hist.iter().enumerate() {
            cumul += count as u64;
            if !p50_trouve && cumul >= seuil_p50 {
                p50 = valeur as u64;
                p50_trouve = true;
            }
            if cumul >= seuil_p99 {
                p99 = valeur as u64;
                break;
            }
        }
        Stats {
            p50,
            p99,
            max: vrai_max,
        }
    }

    pub fn dataset(taille: usize, borne_max: u64) -> Vec<u64> {
        (0..taille as u64)
            .map(|i| {
                let h = i.wrapping_mul(2654435761).wrapping_add(1013904223);
                (h % borne_max) + 1
            })
            .collect()
    }
}

fn bench_percentiles(c: &mut Criterion) {
    let mut group = c.benchmark_group("percentiles");

    let borne_max = 5_000u64;
    for &taille in &[1_000usize, 10_000, 100_000] {
        let data = exo01::dataset(taille, borne_max);

        group.bench_with_input(BenchmarkId::new("tri", taille), &data, |b, data| {
            b.iter(|| exo01::stats_par_tri(black_box(data)))
        });
        group.bench_with_input(BenchmarkId::new("select", taille), &data, |b, data| {
            b.iter(|| exo01::stats_par_select(black_box(data)))
        });
        group.bench_with_input(BenchmarkId::new("histogramme", taille), &data, |b, data| {
            b.iter(|| exo01::stats_par_histogramme(black_box(data), borne_max))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_percentiles);
criterion_main!(benches);
