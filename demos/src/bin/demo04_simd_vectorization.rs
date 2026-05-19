use std::time::Instant;

/// Produit scalaire idiomatique. Le compilateur peut vectoriser cette boucle
/// en `-C target-cpu=native`, mais ce n'est pas garanti.
pub fn dot_product_naif(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "vecteurs de tailles différentes");
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Produit scalaire avec hints d'auto-vectorisation.
/// On utilise 8 accumulateurs séparés que LLVM consolide en registre SIMD.
pub fn dot_product_hint(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    let n = a.len();
    let lanes = 8;
    let n_aligne = n - n % lanes;

    // 8 accumulateurs partiels → indépendance des dépendances → SIMD friendly
    let mut acc = [0.0f32; 8];
    let mut i = 0;
    while i < n_aligne {
        for k in 0..lanes {
            acc[k] += a[i + k] * b[i + k];
        }
        i += lanes;
    }
    let mut total: f32 = acc.iter().sum();

    // Traiter le reste (queue non multiple de 8) en scalaire
    for j in n_aligne..n {
        total += a[j] * b[j];
    }
    total
}

#[cfg(target_arch = "x86_64")]
pub fn dot_product_simd(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    assert_eq!(a.len(), b.len());
    let n = a.len();
    let lanes = 4;
    let n_aligne = n - n % lanes;

    // SAFETY : SSE2 est disponible sur tout x86_64 (garanti par l'ABI x86_64).
    // Les loadu/storeu acceptent les adresses non-alignées.
    let total_simd = unsafe {
        let mut acc_v = _mm_setzero_ps(); // 4 × f32 = 0.0
        let mut i = 0;
        while i < n_aligne {
            let va = _mm_loadu_ps(a.as_ptr().add(i));
            let vb = _mm_loadu_ps(b.as_ptr().add(i));
            let prod = _mm_mul_ps(va, vb);
            acc_v = _mm_add_ps(acc_v, prod);
            i += lanes;
        }
        // Réduction horizontale du registre : somme des 4 voies
        let mut tmp = [0.0f32; 4];
        _mm_storeu_ps(tmp.as_mut_ptr(), acc_v);
        tmp[0] + tmp[1] + tmp[2] + tmp[3]
    };

    // Queue scalaire
    let mut total = total_simd;
    for j in n_aligne..n {
        total += a[j] * b[j];
    }
    total
}

#[cfg(target_arch = "aarch64")]
pub fn dot_product_simd(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;
    assert_eq!(a.len(), b.len());
    let n = a.len();
    let lanes = 4;
    let n_aligne = n - n % lanes;

    // SAFETY : NEON est garanti par l'ABI AArch64 (toujours disponible).
    let total_simd = unsafe {
        let mut acc_v = vdupq_n_f32(0.0); // [0.0; 4]
        let mut i = 0;
        while i < n_aligne {
            let va = vld1q_f32(a.as_ptr().add(i));
            let vb = vld1q_f32(b.as_ptr().add(i));
            acc_v = vfmaq_f32(acc_v, va, vb); // fused multiply-add
            i += lanes;
        }
        vaddvq_f32(acc_v) // somme horizontale des 4 voies
    };

    let mut total = total_simd;
    for j in n_aligne..n {
        total += a[j] * b[j];
    }
    total
}

// Fallback pour les architectures sans SIMD intégré (WASM, etc.)
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn dot_product_simd(a: &[f32], b: &[f32]) -> f32 {
    // Pas d'intrinsics disponibles : on retombe sur la version "hint"
    dot_product_hint(a, b)
}

fn main() {
    println!("=== Demo : SIMD et vectorisation — dot product ===\n");

    // Indication de l'architecture détectée à la compilation
    #[cfg(target_arch = "x86_64")]
    println!("Architecture : x86_64 → intrinsics SSE2 utilisées");
    #[cfg(target_arch = "aarch64")]
    println!("Architecture : aarch64 → intrinsics NEON utilisées");
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    println!("Architecture : autre → fallback non-SIMD");

    // Dataset : deux vecteurs de 1M f32 (représentent par exemple
    // des coefficients d'une moyenne pondérée sur 1M de métriques OVH)
    let n = 1_000_000;
    let a: Vec<f32> = (0..n).map(|i| (i as f32) * 0.001).collect();
    let b: Vec<f32> = (0..n).map(|i| ((i % 100) as f32) * 0.01 + 1.0).collect();

    // Vérification de cohérence
    let r1 = dot_product_naif(&a, &b);
    let r2 = dot_product_hint(&a, &b);
    let r3 = dot_product_simd(&a, &b);
    println!("\n--- Vérification de cohérence ---");
    println!("naïf     : {r1:.2}");
    println!("hint     : {r2:.2}");
    println!("SIMD     : {r3:.2}");
    // Tolérance flottante : les ordres de sommation diffèrent → erreur d'arrondi
    let tol = (r1.abs() * 1e-3).max(1.0);
    assert!((r1 - r2).abs() < tol, "naïf vs hint diverge trop");
    assert!((r1 - r3).abs() < tol, "naïf vs SIMD diverge trop");

    // Mini-benchmark : 100 itérations de chaque version
    println!("\n--- Mini-benchmark (100 itérations sur n={n}) ---");
    let iterations = 100;

    // Warm-up
    for _ in 0..5 {
        let _ = std::hint::black_box(dot_product_naif(&a, &b));
        let _ = std::hint::black_box(dot_product_hint(&a, &b));
        let _ = std::hint::black_box(dot_product_simd(&a, &b));
    }

    let debut = Instant::now();
    for _ in 0..iterations {
        let _ = std::hint::black_box(dot_product_naif(&a, &b));
    }
    let dur_naif = debut.elapsed();

    let debut = Instant::now();
    for _ in 0..iterations {
        let _ = std::hint::black_box(dot_product_hint(&a, &b));
    }
    let dur_hint = debut.elapsed();

    let debut = Instant::now();
    for _ in 0..iterations {
        let _ = std::hint::black_box(dot_product_simd(&a, &b));
    }
    let dur_simd = debut.elapsed();

    println!("naïf : {dur_naif:?} (~{} µs/itér)", dur_naif.as_micros() / iterations);
    println!("hint : {dur_hint:?} (~{} µs/itér)", dur_hint.as_micros() / iterations);
    println!("SIMD : {dur_simd:?} (~{} µs/itér)", dur_simd.as_micros() / iterations);

    let speedup_hint = dur_naif.as_nanos() as f64 / dur_hint.as_nanos().max(1) as f64;
    let speedup_simd = dur_naif.as_nanos() as f64 / dur_simd.as_nanos().max(1) as f64;
    println!("\nSpeedup hint vs naïf : {speedup_hint:.2}x");
    println!("Speedup SIMD vs naïf : {speedup_simd:.2}x");

    println!("\nAstuce : pour pousser l'auto-vectorisation, compilez avec");
    println!("  RUSTFLAGS=\"-C target-cpu=native\" cargo run --release --bin demo04_simd_vectorization");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f32, b: f32, tol: f32) -> bool {
        (a - b).abs() <= tol
    }

    #[test]
    fn test_dot_product_vecteurs_unitaires() {
        let a = vec![1.0f32; 100];
        let b = vec![1.0f32; 100];
        assert!(approx_eq(dot_product_naif(&a, &b), 100.0, 1e-3));
        assert!(approx_eq(dot_product_hint(&a, &b), 100.0, 1e-3));
        assert!(approx_eq(dot_product_simd(&a, &b), 100.0, 1e-3));
    }

    #[test]
    fn test_dot_product_zeros() {
        let a = vec![0.0f32; 50];
        let b = vec![3.14f32; 50];
        assert_eq!(dot_product_naif(&a, &b), 0.0);
        assert_eq!(dot_product_hint(&a, &b), 0.0);
        assert_eq!(dot_product_simd(&a, &b), 0.0);
    }

    #[test]
    fn test_dot_product_queue_non_alignee() {
        // 13 éléments → 1 chunk de 8 + 5 éléments de queue
        // (et 3 chunks de 4 + 1 de queue pour SSE/NEON)
        let a: Vec<f32> = (1..=13).map(|i| i as f32).collect();
        let b = vec![1.0f32; 13];
        // 1 + 2 + 3 + ... + 13 = 91
        let attendu = 91.0;
        assert!(approx_eq(dot_product_naif(&a, &b), attendu, 1e-3));
        assert!(approx_eq(dot_product_hint(&a, &b), attendu, 1e-3));
        assert!(approx_eq(dot_product_simd(&a, &b), attendu, 1e-3));
    }

    #[test]
    fn test_coherence_grand_dataset() {
        let n = 10_000;
        let a: Vec<f32> = (0..n).map(|i| (i as f32) * 0.01).collect();
        let b: Vec<f32> = (0..n).map(|i| ((i % 10) as f32) * 0.1).collect();
        let r_naif = dot_product_naif(&a, &b);
        let r_hint = dot_product_hint(&a, &b);
        let r_simd = dot_product_simd(&a, &b);
        // Tolérance proportionnelle à la magnitude (cumul d'erreurs d'arrondi)
        let tol = r_naif.abs() * 1e-3;
        assert!(approx_eq(r_naif, r_hint, tol));
        assert!(approx_eq(r_naif, r_simd, tol));
    }

    #[test]
    #[should_panic(expected = "vecteurs de tailles différentes")]
    fn test_panic_tailles_differentes() {
        let a = vec![1.0f32; 5];
        let b = vec![1.0f32; 6];
        let _ = dot_product_naif(&a, &b);
    }
}
