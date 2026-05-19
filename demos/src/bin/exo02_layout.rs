use smallvec::SmallVec;
use std::mem::{align_of, size_of};
use std::time::Instant;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PointNaif {
    pub actif: bool,        
    pub latence_ns: u64,    
    pub niveau: u8,         
    pub timestamp_ns: u64,  
}

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
        let points = (0..n).map(fabriquer_point).collect();
        Self { points }
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


fn bench_manuel<F, T>(nom: &str, iterations: usize, mut f: F) -> u128
where
    F: FnMut() -> T,
{
    for _ in 0..5 {
        let _ = std::hint::black_box(f());
    }
    let debut = Instant::now();
    for _ in 0..iterations {
        let _ = std::hint::black_box(f());
    }
    let duree = debut.elapsed().as_micros();
    let par_iter = duree as f64 / iterations as f64;
    println!("  [{nom:<28}] {iterations} itér en {duree} µs (~{par_iter:.1} µs/itér)");
    duree
}


fn main() {
    println!("=== Correction Exercice 2 : Layout mémoire ===\n");

    println!("--- 1. Layout des structs ---");
    println!(
        "PointNaif      : size = {:>2} octets, align = {} octets",
        size_of::<PointNaif>(),
        align_of::<PointNaif>()
    );
    println!(
        "PointOptimise  : size = {:>2} octets, align = {} octets",
        size_of::<PointOptimise>(),
        align_of::<PointOptimise>()
    );
    let economie = 100.0
        - 100.0 * size_of::<PointOptimise>() as f64 / size_of::<PointNaif>() as f64;
    println!("→ Économie en réordonnant : {economie:.1} %\n");

    let n = 1_000_000;
    println!("--- 2. AoS vs SoA sur {n} points ---");
    let aos = MetriquesAoS::nouvelle(n);
    let soa = MetriquesSoA::nouvelle(n);

    assert_eq!(aos.somme_latences(), soa.somme_latences());
    assert_eq!(aos.somme_filtree(), soa.somme_filtree());
    println!(
        "Cohérence OK : somme_latences = {}, somme_filtree = {}",
        aos.somme_latences(),
        aos.somme_filtree()
    );

    let iters = 50;
    let t_aos_sum = bench_manuel("AoS::somme_latences", iters, || aos.somme_latences());
    let t_soa_sum = bench_manuel("SoA::somme_latences", iters, || soa.somme_latences());
    let t_aos_filt = bench_manuel("AoS::somme_filtree", iters, || aos.somme_filtree());
    let t_soa_filt = bench_manuel("SoA::somme_filtree", iters, || soa.somme_filtree());

    println!(
        "\n  → SoA::somme_latences speedup : {:.2}x",
        t_aos_sum as f64 / t_soa_sum as f64
    );
    println!(
        "  → SoA::somme_filtree  speedup : {:.2}x\n",
        t_aos_filt as f64 / t_soa_filt as f64
    );

    println!("--- 3. SmallVec ---");
    println!(
        "PointAvecTagsVec   : size = {} octets",
        size_of::<PointAvecTagsVec>()
    );
    println!(
        "PointAvecTagsSmall : size = {} octets (inclut 4 tags inline = 16 octets)",
        size_of::<PointAvecTagsSmall>()
    );

    let m = 100_000;
    let t_vec = bench_manuel("construction Vec     ", 10, || construire_tags_vec(m));
    let t_small = bench_manuel("construction SmallVec", 10, || construire_tags_small(m));
    println!(
        "\n  → SmallVec speedup à la construction : {:.2}x\n",
        t_vec as f64 / t_small as f64
    );

    println!(
        "Pour des chiffres rigoureux : `cargo bench --bench bench_exo02`"
    );
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_optimise_plus_petit_que_naif() {
        assert_eq!(size_of::<PointNaif>(), 32);
        assert_eq!(size_of::<PointOptimise>(), 24);
        assert!(size_of::<PointOptimise>() < size_of::<PointNaif>());
    }

    #[test]
    fn aos_et_soa_coherentes() {
        let aos = MetriquesAoS::nouvelle(10_000);
        let soa = MetriquesSoA::nouvelle(10_000);
        assert_eq!(aos.somme_latences(), soa.somme_latences());
        assert_eq!(aos.somme_filtree(), soa.somme_filtree());
    }

    #[test]
    fn smallvec_inline_sous_capacite() {
        let p = PointAvecTagsSmall {
            latence_ns: 0,
            tags: SmallVec::from_slice(&[1, 2, 3]),
        };
        assert!(!p.tags.spilled(), "≤ 4 tags doit rester inline");
        assert_eq!(p.tags.len(), 3);
    }

    #[test]
    fn smallvec_spill_au_dela() {
        let p = PointAvecTagsSmall {
            latence_ns: 0,
            tags: SmallVec::from_slice(&[1, 2, 3, 4, 5]),
        };
        assert!(p.tags.spilled(), "5 tags doit avoir débordé sur le heap");
    }
}
