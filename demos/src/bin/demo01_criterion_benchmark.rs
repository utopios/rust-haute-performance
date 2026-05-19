use demos::demo01::{
    somme_carres_alloc_spam, somme_carres_naive, somme_carres_preallouee, somme_carres_rayon,
    somme_carres_sans_prealloc,
};
use std::time::Instant;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Mesure le temps d'exécution d'une fonction sur N itérations.
/// Retourne (temps_total, valeur_calculée).
fn bench_manuel<F>(nom: &str, iterations: usize, mut f: F) -> (u128, u64)
where
    F: FnMut() -> u64,
{
    // Warm-up : 5 exécutions pour stabiliser les caches CPU
    for _ in 0..5 {
        let _ = f();
    }

    let debut = Instant::now();
    let mut dernier_resultat = 0u64;
    for _ in 0..iterations {
        dernier_resultat = std::hint::black_box(f());
    }
    let duree_us = debut.elapsed().as_micros();

    let par_iter_us = duree_us / iterations as u128;
    println!(
        "  [{nom:<20}] {iterations} itérations en {duree_us} µs (~{par_iter_us} µs/itér)"
    );
    (duree_us, dernier_resultat)
}

fn main() {
    // Avec --features dhat-heap, on démarre le profileur dès l'entrée du
    // programme. Le rapport JSON est écrit dans dhat-heap.json à la fin
    // (lors du drop de _profiler), visualisable sur https://nnethercote.github.io/dh_view/dh_view.html
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    println!("=== Demo : Benchmarking de la somme des carrés ===\n");

    // Pour DHAT, on réduit la taille du dataset : tracer chaque alloc d'un
    // run à 1M éléments rendrait le rapport gigantesque. 100k suffit pour
    // que le ratio entre stratégies soit éloquent.
    #[cfg(feature = "dhat-heap")]
    let taille = 100_000;
    #[cfg(not(feature = "dhat-heap"))]
    let taille = 1_000_000;

    let data: Vec<u64> = (0..taille as u64).map(|i| (i % 1000) + 1).collect();
    println!("Dataset : {taille} métriques (latences simulées en µs)\n");

    // Vérification de cohérence : toutes les variantes doivent donner
    // exactement la même valeur (sinon le benchmark n'a pas de sens).
    let r1 = somme_carres_naive(&data);
    let r2 = somme_carres_rayon(&data);
    let r3 = somme_carres_preallouee(&data);
    let r4 = somme_carres_alloc_spam(&data);
    assert_eq!(r1, r2, "naive et rayon divergent !");
    assert_eq!(r1, r3, "naive et préallouée divergent !");
    assert_eq!(r1, r4, "naive et alloc_spam divergent !");
    println!("Toutes les stratégies donnent : {r1}\n");

    // Mini-benchmark manuel (pour avoir un ordre de grandeur sans Criterion)
    let iterations = 50;
    println!("--- Mini-benchmark manuel ({iterations} itérations) ---");
    let (t_naive, _) = bench_manuel("naïve", iterations, || somme_carres_naive(&data));
    let (t_rayon, _) = bench_manuel("rayon", iterations, || somme_carres_rayon(&data));
    let (t_prealloc, _) = bench_manuel("préallouée", iterations, || somme_carres_preallouee(&data));
    let (t_sans, _) = bench_manuel("sans préalloc", iterations, || {
        somme_carres_sans_prealloc(&data)
    });
    let (t_spam, _) = bench_manuel("alloc_spam", iterations, || somme_carres_alloc_spam(&data));

    // Calcul des ratios pour rendre les comparaisons visuelles
    println!("\n--- Speedups relatifs (référence = naïve) ---");
    let ratio = |t: u128| t_naive as f64 / t as f64;
    println!("  naïve          : 1.00x (référence)");
    println!("  rayon          : {:.2}x", ratio(t_rayon));
    println!("  préallouée     : {:.2}x", ratio(t_prealloc));
    println!("  sans préalloc  : {:.2}x", ratio(t_sans));
    println!("  alloc_spam     : {:.2}x  ← anti-pattern, à profiler !", ratio(t_spam));

    println!(
        "\nNote : ces chiffres sont indicatifs. Pour des mesures rigoureuses :\n\
         - `cargo bench --bench bench_demo01`                                        (Criterion)\n\
         - `samply record ./target/release/demo01_criterion_benchmark`               (CPU, mac/Linux)\n\
         - `cargo flamegraph --release --bin demo01_criterion_benchmark`             (CPU, Linux)\n\
         - `cargo run --release --features dhat-heap --bin demo01_criterion_benchmark` (mémoire)"
    );
}



#[cfg(test)]
mod tests {
    use demos::demo01::*;

    #[test]
    fn test_coherence_strategies() {
        let data: Vec<u64> = (1..=100).collect();
        // Somme des carrés de 1 à 100 = 100 * 101 * 201 / 6 = 338350
        let attendu = 338_350u64;
        assert_eq!(somme_carres_naive(&data), attendu);
        assert_eq!(somme_carres_rayon(&data), attendu);
        assert_eq!(somme_carres_preallouee(&data), attendu);
        assert_eq!(somme_carres_sans_prealloc(&data), attendu);
    }

    #[test]
    fn test_vecteur_vide() {
        let data: Vec<u64> = vec![];
        assert_eq!(somme_carres_naive(&data), 0);
        assert_eq!(somme_carres_rayon(&data), 0);
        assert_eq!(somme_carres_preallouee(&data), 0);
    }

    #[test]
    fn test_un_seul_element() {
        let data = vec![7u64];
        assert_eq!(somme_carres_naive(&data), 49);
        assert_eq!(somme_carres_rayon(&data), 49);
        assert_eq!(somme_carres_preallouee(&data), 49);
    }

    #[test]
    fn test_grand_dataset_realiste() {
        // Simule 10 000 métriques OVH
        let data: Vec<u64> = (0..10_000u64).map(|i| (i % 100) + 1).collect();
        let r_naive = somme_carres_naive(&data);
        let r_rayon = somme_carres_rayon(&data);
        let r_prealloc = somme_carres_preallouee(&data);
        assert_eq!(r_naive, r_rayon);
        assert_eq!(r_naive, r_prealloc);
        assert!(r_naive > 0);
    }
}
