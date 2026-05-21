use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Incrémente un compteur partagé `iterations` fois depuis `threads` threads
/// et retourne la valeur finale + la durée.
///
/// `Ordering::Relaxed` : aucune garantie d'ordre avec les autres variables,
/// uniquement la garantie que chaque `fetch_add` est lui-même atomique.
/// C'est l'ordering idéal pour les compteurs et métriques où on ne se soucie
/// pas de "quand" l'incrémentation est visible, mais juste qu'aucune n'est perdue.
fn compteur_relaxed(threads: usize, iterations: u64) -> (u64, Duration) {
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let counter = Arc::clone(&counter);
            thread::spawn(move || {
                for _ in 0..iterations {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    (counter.load(Ordering::Relaxed), start.elapsed())
}

/// Même chose mais avec `SeqCst` — utile pour comparer la performance.
fn compteur_seqcst(threads: usize, iterations: u64) -> (u64, Duration) {
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let counter = Arc::clone(&counter);
            thread::spawn(move || {
                for _ in 0..iterations {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    (counter.load(Ordering::SeqCst), start.elapsed())
}

/// Démonstration d'un pattern classique : un producteur prépare une donnée
/// puis lève un drapeau. Un consommateur attend le drapeau puis lit la donnée.
///
/// L'ordering `Release` côté producteur + `Acquire` côté consommateur garantit
/// que toutes les écritures faites AVANT le `store(true)` sont visibles APRÈS
/// le `load() == true` côté consommateur — même sans Mutex.
fn demo_release_acquire() {
    let data = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(AtomicBool::new(false));

    let data_p = Arc::clone(&data);
    let ready_p = Arc::clone(&ready);
    let producer = thread::spawn(move || {
        // Préparation des données (peut être long)
        thread::sleep(Duration::from_millis(20));
        data_p.store(0xDEAD_BEEF, Ordering::Relaxed);

        // Le store Release "publie" toutes les écritures précédentes
        ready_p.store(true, Ordering::Release);
        println!("  [producer] données publiées (0xDEAD_BEEF) + flag = true");
    });

    let consumer = thread::spawn(move || {
        // Spin-wait jusqu'à ce que le drapeau soit levé
        // (en prod on utiliserait un Condvar ou un canal, c'est juste pour démo)
        while !ready.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        // Garanti : on voit la valeur écrite avant le Release
        let val = data.load(Ordering::Relaxed);
        println!("  [consumer] flag observé, donnée lue = 0x{val:08X}");
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

/// Met à jour `current` avec `new_val` si et seulement si `new_val > current`.
///
/// L'opération est lock-free : si plusieurs threads tentent de mettre à jour
/// en même temps, ils boucleront jusqu'à ce que leur CAS réussisse ou que la
/// valeur courante soit déjà supérieure (auquel cas on abandonne).
///
/// `compare_exchange_weak` peut échouer "spuriously" (sans raison) sur certaines
/// architectures (ARM notamment) — mais c'est plus rapide dans une boucle CAS.
fn atomic_max(current: &AtomicU64, new_val: u64) {
    let mut old = current.load(Ordering::Relaxed);
    loop {
        if old >= new_val {
            return; // Pas besoin de mettre à jour
        }
        match current.compare_exchange_weak(
            old,
            new_val,
            Ordering::AcqRel,   // Succès : publier la nouvelle valeur
            Ordering::Relaxed,  // Échec : juste relire
        ) {
            Ok(_) => return,
            Err(actual) => old = actual, // Quelqu'un a modifié entre-temps, on retente
        }
    }
}

/// Variante : atomic_min (symétrique).
fn atomic_min(current: &AtomicU64, new_val: u64) {
    let mut old = current.load(Ordering::Relaxed);
    loop {
        if old <= new_val {
            return;
        }
        match current.compare_exchange_weak(
            old,
            new_val,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => old = actual,
        }
    }
}

/// Compteurs de métriques d'un service OVH (simulé).
///
/// Tous les champs sont `AtomicU64` → mise à jour sans aucun lock.
/// Plusieurs threads peuvent incrémenter en parallèle : la performance
/// reste excellente même avec 100+ threads concurrents.
struct ServiceStats {
    /// Nombre total de requêtes traitées
    total_requests: AtomicU64,
    /// Nombre de requêtes en succès (HTTP 2xx)
    success: AtomicU64,
    /// Nombre de requêtes en échec (HTTP 4xx/5xx)
    failed: AtomicU64,
    /// Latence maximale observée (microsecondes)
    max_latency_us: AtomicU64,
    /// Latence minimale observée (microsecondes), init à u64::MAX
    min_latency_us: AtomicU64,
}

impl ServiceStats {
    fn new() -> Self {
        ServiceStats {
            total_requests: AtomicU64::new(0),
            success: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
            min_latency_us: AtomicU64::new(u64::MAX),
        }
    }

    /// Enregistre une requête traitée.
    fn record(&self, latency_us: u64, ok: bool) {
        // Compteurs : Relaxed est largement suffisant (ordre indifférent)
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if ok {
            self.success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }

        // Max/min : nécessite un CAS loop pour rester lock-free
        atomic_max(&self.max_latency_us, latency_us);
        atomic_min(&self.min_latency_us, latency_us);
    }

    fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.total_requests.load(Ordering::Relaxed),
            self.success.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.max_latency_us.load(Ordering::Relaxed),
            self.min_latency_us.load(Ordering::Relaxed),
        )
    }

    fn print(&self) {
        let (total, succ, fail, max_l, min_l) = self.snapshot();
        let min_l_display = if min_l == u64::MAX { 0 } else { min_l };
        println!(
            "  total={total}  success={succ}  failed={fail}  \
             latency: min={min_l_display}µs  max={max_l}µs"
        );
    }
}

fn main() {
    println!("=== Demo : Atomics et Compare-And-Swap ===\n");

    // -----------------------------------------------------------------
    // Partie 1 : Compteur Relaxed vs SeqCst
    // -----------------------------------------------------------------
    println!("--- [1] Compteur atomique : Relaxed vs SeqCst ---");
    let threads = 4;
    let iterations = 100_000_000u64;

    let (val_rx, t_rx) = compteur_relaxed(threads, iterations);
    let (val_sc, t_sc) = compteur_seqcst(threads, iterations);

    println!(
        "  Relaxed : valeur={}  durée={} ms",
        val_rx,
        t_rx.as_millis()
    );
    println!(
        "  SeqCst  : valeur={}  durée={} ms",
        val_sc,
        t_sc.as_millis()
    );
    assert_eq!(val_rx, threads as u64 * iterations);
    assert_eq!(val_sc, threads as u64 * iterations);
    println!("  Les deux donnent la BONNE valeur — Relaxed suffit pour un compteur.\n");

    // -----------------------------------------------------------------
    // Partie 2 : Release/Acquire — synchronisation explicite
    // -----------------------------------------------------------------
    println!("--- [2] Release/Acquire : producteur/consommateur sans lock ---");
    demo_release_acquire();
    println!();

    // -----------------------------------------------------------------
    // Partie 3 : atomic_max avec CAS loop
    // -----------------------------------------------------------------
    println!("--- [3] atomic_max via CAS : recherche du max en parallèle ---");
    let global_max = Arc::new(AtomicU64::new(0));

    // 4 threads, chacun trouve son max local sur un sous-tableau
    // puis tente d'écraser le max global via atomic_max.
    let data: Vec<u64> = (0..10_000).map(|i| (i * 37 + 13) % 1_000_000).collect();
    let chunk_size = data.len() / 4;
    let data = Arc::new(data);

    let mut handles = Vec::new();
    for t in 0..4 {
        let data = Arc::clone(&data);
        let global_max = Arc::clone(&global_max);
        let start = t * chunk_size;
        let end = if t == 3 { data.len() } else { start + chunk_size };
        handles.push(thread::spawn(move || {
            for v in &data[start..end] {
                atomic_max(&global_max, *v);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let true_max = data.iter().copied().max().unwrap();
    let observed_max = global_max.load(Ordering::Relaxed);
    println!("  max calculé  : {observed_max}");
    println!("  max attendu  : {true_max}");
    assert_eq!(observed_max, true_max);
    println!();

    // -----------------------------------------------------------------
    // Partie 4 : Stats lock-free pour un service OVH
    // -----------------------------------------------------------------
    println!("--- [4] Stats lock-free d'un service OVH simulé ---");
    let stats = Arc::new(ServiceStats::new());

    // Simulons 8 workers qui traitent chacun 1000 requêtes
    let mut workers = Vec::new();
    for worker_id in 0..8u64 {
        let stats = Arc::clone(&stats);
        workers.push(thread::spawn(move || {
            for i in 0..1000u64 {
                // Latence pseudo-aléatoire entre 50 et 950 µs
                let latency = 50 + ((worker_id * 31 + i * 17) % 900);
                // Simule ~95 % de succès
                let ok = (worker_id * 7 + i) % 20 != 0;
                stats.record(latency, ok);
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }

    println!("  Métriques finales :");
    stats.print();
    let (total, succ, fail, _, _) = stats.snapshot();
    assert_eq!(total, 8_000);
    assert_eq!(succ + fail, total);
    println!("\nDémo terminée — toutes les assertions passent.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compteur_relaxed_donne_bonne_valeur() {
        let (val, _) = compteur_relaxed(4, 10_000);
        assert_eq!(val, 40_000);
    }

    #[test]
    fn test_compteur_seqcst_donne_bonne_valeur() {
        let (val, _) = compteur_seqcst(4, 10_000);
        assert_eq!(val, 40_000);
    }

    #[test]
    fn test_atomic_max_basique() {
        let m = AtomicU64::new(0);
        atomic_max(&m, 10);
        atomic_max(&m, 5); // ne doit rien changer
        atomic_max(&m, 100);
        atomic_max(&m, 50); // ne doit rien changer
        assert_eq!(m.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_atomic_min_basique() {
        let m = AtomicU64::new(1000);
        atomic_min(&m, 500);
        atomic_min(&m, 800); // ne doit rien changer
        atomic_min(&m, 50);
        atomic_min(&m, 100); // ne doit rien changer
        assert_eq!(m.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_atomic_max_concurrent() {
        let m = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for t in 0u64..8 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for i in 0..1000u64 {
                    atomic_max(&m, t * 1_000_000 + i);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Max attendu : t=7, i=999 → 7 * 1_000_000 + 999 = 7_000_999
        assert_eq!(m.load(Ordering::Relaxed), 7_000_999);
    }

    #[test]
    fn test_service_stats_record() {
        let stats = ServiceStats::new();
        stats.record(100, true);
        stats.record(200, true);
        stats.record(50, false);
        stats.record(500, true);

        let (total, succ, fail, max_l, min_l) = stats.snapshot();
        assert_eq!(total, 4);
        assert_eq!(succ, 3);
        assert_eq!(fail, 1);
        assert_eq!(max_l, 500);
        assert_eq!(min_l, 50);
    }

    #[test]
    fn test_service_stats_concurrent() {
        let stats = Arc::new(ServiceStats::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let stats = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                for i in 0..500u64 {
                    stats.record(100 + i, i % 5 != 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let (total, succ, fail, max_l, min_l) = stats.snapshot();
        assert_eq!(total, 4_000);
        assert_eq!(succ + fail, 4_000);
        assert_eq!(max_l, 599);
        assert_eq!(min_l, 100);
    }
}
