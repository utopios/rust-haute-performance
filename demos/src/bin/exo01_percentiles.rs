use std::time::Instant;


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Stats {
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
}


pub fn stats_par_tri(data: &[u64]) -> Stats {
    assert!(!data.is_empty(), "dataset vide");
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
    assert!(!data.is_empty(), "dataset vide");
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
    assert!(!data.is_empty(), "dataset vide");
    let mut hist = vec![0u32; (borne_max + 1) as usize];

    let mut vrai_max = 0u64;
    for &v in data {
        debug_assert!(v <= borne_max, "valeur {v} hors borne {borne_max}");
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


fn bench_manuel<F>(nom: &str, iterations: usize, mut f: F) -> (u128, Stats)
where
    F: FnMut() -> Stats,
{
    for _ in 0..5 {
        let _ = f();
    }

    let debut = Instant::now();
    let mut dernier = Stats {
        p50: 0,
        p99: 0,
        max: 0,
    };
    for _ in 0..iterations {
        dernier = std::hint::black_box(f());
    }
    let duree_us = debut.elapsed().as_micros();
    let par_iter_us = duree_us / iterations as u128;
    println!("  [{nom:<18}] {iterations} itérations en {duree_us} µs (~{par_iter_us} µs/itér)");
    (duree_us, dernier)
}

// =============================================================================
// Génération de dataset déterministe (sans crate `rand`)
// =============================================================================
//
// On utilise une formule pseudo-aléatoire reproductible pour que tous les runs
// donnent les mêmes nombres (essentiel pour comparer des benchmarks).

pub fn dataset_latences(taille: usize, borne_max: u64) -> Vec<u64> {
    (0..taille as u64)
        .map(|i| {
            // LCG simple — suffisant pour produire des valeurs "réalistes".
            let h = i.wrapping_mul(2654435761).wrapping_add(1013904223);
            (h % borne_max) + 1
        })
        .collect()
}

// =============================================================================
// Démonstration
// =============================================================================

fn main() {
    println!("=== Correction Exercice 1 : Percentiles de latence ===\n");

    let taille = 100_000;
    let borne_max = 5_000u64;
    let data = dataset_latences(taille, borne_max);
    println!("Dataset : {taille} latences sur [1..={borne_max}] µs\n");

    // Vérification de cohérence
    let s1 = stats_par_tri(&data);
    let s2 = stats_par_select(&data);
    let s3 = stats_par_histogramme(&data, borne_max);
    assert_eq!(s1, s2, "tri vs select divergent !");
    assert_eq!(s1, s3, "tri vs histogramme divergent !");
    println!(
        "Stats cohérentes : p50={}, p99={}, max={}\n",
        s1.p50, s1.p99, s1.max
    );

    // Mini-benchmark
    let iterations = 100;
    println!("--- Mini-benchmark manuel ({iterations} itérations) ---");
    let (t_tri, _) = bench_manuel("tri", iterations, || stats_par_tri(&data));
    let (t_sel, _) = bench_manuel("select", iterations, || stats_par_select(&data));
    let (t_hist, _) = bench_manuel("histogramme", iterations, || {
        stats_par_histogramme(&data, borne_max)
    });

    println!("\n--- Speedups (référence = tri) ---");
    let ratio = |t: u128| t_tri as f64 / t as f64;
    println!("  tri          : 1.00x");
    println!("  select       : {:.2}x", ratio(t_sel));
    println!("  histogramme  : {:.2}x", ratio(t_hist));

    println!(
        "\nNote : pour des chiffres rigoureux, utilisez :\n\
         - `cargo bench --bench bench_exo01` (Criterion)\n\
         - `samply record ./target/release/exo01_percentiles` (CPU)"
    );
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset_test() -> Vec<u64> {
        // 100 valeurs de 1 à 100 : p50 attendu = 51 (index 50), p99 = 100, max = 100
        (1..=100u64).collect()
    }

    #[test]
    fn coherence_petite_taille() {
        let data = dataset_test();
        let s1 = stats_par_tri(&data);
        let s2 = stats_par_select(&data);
        let s3 = stats_par_histogramme(&data, 100);
        assert_eq!(s1, s2);
        assert_eq!(s1, s3);
        assert_eq!(s1.max, 100);
    }

    #[test]
    fn coherence_grand_dataset() {
        let data = dataset_latences(10_000, 1_000);
        let s1 = stats_par_tri(&data);
        let s2 = stats_par_select(&data);
        let s3 = stats_par_histogramme(&data, 1_000);
        assert_eq!(s1, s2);
        assert_eq!(s1, s3);
    }

    #[test]
    fn singleton() {
        let data = vec![42u64];
        let s = stats_par_tri(&data);
        assert_eq!(s.p50, 42);
        assert_eq!(s.p99, 42);
        assert_eq!(s.max, 42);
    }
}
