use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

mod exo02 {
    use smallvec::SmallVec;

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct PointOptimise {
        pub latence_ns: u64,
        pub timestamp_ns: u64,
        pub niveau: u8,
        pub actif: bool,
    }

    pub struct MetriquesAoS {
        pub points: Vec<PointOptimise>,
    }

    pub struct MetriquesSoA {
        pub actifs: Vec<bool>,
        pub niveaux: Vec<u8>,
        pub latences_ns: Vec<u64>,
        pub timestamps_ns: Vec<u64>,
    }

    fn fabriquer_point(i: usize) -> PointOptimise {
        PointOptimise {
            latence_ns: ((i * 37) % 1_000_000) as u64,
            timestamp_ns: 1_700_000_000_000_000_000u64 + i as u64,
            niveau: (i % 4) as u8,
            actif: i % 2 == 0,
        }
    }

    impl MetriquesAoS {
        pub fn nouvelle(n: usize) -> Self {
            Self {
                points: (0..n).map(fabriquer_point).collect(),
            }
        }
        pub fn somme_latences(&self) -> u64 {
            self.points.iter().map(|p| p.latence_ns).sum()
        }
        pub fn somme_filtree(&self) -> u64 {
            self.points
                .iter()
                .filter(|p| p.actif && p.niveau >= 2)
                .map(|p| p.latence_ns)
                .sum()
        }
    }

    impl MetriquesSoA {
        pub fn nouvelle(n: usize) -> Self {
            let mut actifs = Vec::with_capacity(n);
            let mut niveaux = Vec::with_capacity(n);
            let mut latences = Vec::with_capacity(n);
            let mut timestamps = Vec::with_capacity(n);
            for i in 0..n {
                let p = fabriquer_point(i);
                actifs.push(p.actif);
                niveaux.push(p.niveau);
                latences.push(p.latence_ns);
                timestamps.push(p.timestamp_ns);
            }
            Self {
                actifs,
                niveaux,
                latences_ns: latences,
                timestamps_ns: timestamps,
            }
        }
        pub fn somme_latences(&self) -> u64 {
            self.latences_ns.iter().sum()
        }
        pub fn somme_filtree(&self) -> u64 {
            let mut total = 0u64;
            for i in 0..self.latences_ns.len() {
                if self.actifs[i] && self.niveaux[i] >= 2 {
                    total += self.latences_ns[i];
                }
            }
            total
        }
    }

    pub struct PointAvecTagsVec {
        pub latence_ns: u64,
        pub tags: Vec<u32>,
    }
    pub struct PointAvecTagsSmall {
        pub latence_ns: u64,
        pub tags: SmallVec<[u32; 4]>,
    }
    pub fn construire_tags_vec(n: usize) -> Vec<PointAvecTagsVec> {
        (0..n)
            .map(|i| PointAvecTagsVec {
                latence_ns: i as u64,
                tags: vec![i as u32, (i + 1) as u32],
            })
            .collect()
    }
    pub fn construire_tags_small(n: usize) -> Vec<PointAvecTagsSmall> {
        (0..n)
            .map(|i| {
                let mut tags = SmallVec::<[u32; 4]>::new();
                tags.push(i as u32);
                tags.push((i + 1) as u32);
                PointAvecTagsSmall {
                    latence_ns: i as u64,
                    tags,
                }
            })
            .collect()
    }
}

fn bench_aos_soa(c: &mut Criterion) {
    let mut group = c.benchmark_group("aos_vs_soa");
    for &n in &[10_000usize, 100_000, 1_000_000] {
        let aos = exo02::MetriquesAoS::nouvelle(n);
        let soa = exo02::MetriquesSoA::nouvelle(n);
        group.bench_with_input(BenchmarkId::new("aos_somme_latences", n), &aos, |b, x| {
            b.iter(|| black_box(x).somme_latences())
        });
        group.bench_with_input(BenchmarkId::new("soa_somme_latences", n), &soa, |b, x| {
            b.iter(|| black_box(x).somme_latences())
        });
        group.bench_with_input(BenchmarkId::new("aos_somme_filtree", n), &aos, |b, x| {
            b.iter(|| black_box(x).somme_filtree())
        });
        group.bench_with_input(BenchmarkId::new("soa_somme_filtree", n), &soa, |b, x| {
            b.iter(|| black_box(x).somme_filtree())
        });
    }
    group.finish();
}

fn bench_smallvec(c: &mut Criterion) {
    let mut group = c.benchmark_group("smallvec_vs_vec");
    for &n in &[1_000usize, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::new("vec", n), &n, |b, &n| {
            b.iter(|| exo02::construire_tags_vec(black_box(n)))
        });
        group.bench_with_input(BenchmarkId::new("smallvec", n), &n, |b, &n| {
            b.iter(|| exo02::construire_tags_small(black_box(n)))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_aos_soa, bench_smallvec);
criterion_main!(benches);
