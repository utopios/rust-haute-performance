use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;


pub struct ServiceStats {
    pub total: AtomicU64,
    pub success: AtomicU64,
    pub failed: AtomicU64,
    pub latence_min: AtomicU64,
    pub latence_max: AtomicU64,
    pub latence_sum: AtomicU64,
}

pub struct StatsSnapshot {
    pub total: u64,
    pub success: u64,
    pub failed: u64,
    pub latence_min: u64,
    pub latence_max: u64,
    pub latence_moy: f64,
}

impl ServiceStats {
    pub fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            success: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            latence_min: AtomicU64::new(u64::MAX),
            latence_max: AtomicU64::new(0),
            latence_sum: AtomicU64::new(0),
        }
    }

    pub fn record(&self, success: bool, latence_us: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        if success {
            self.success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.latence_sum.fetch_add(latence_us, Ordering::Relaxed);
        atomic_min(&self.latence_min, latence_us);
        atomic_max(&self.latence_max, latence_us);
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let total = self.total.load(Ordering::Relaxed);
        let sum = self.latence_sum.load(Ordering::Relaxed);
        StatsSnapshot {
            total,
            success: self.success.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            latence_min: self.latence_min.load(Ordering::Relaxed),
            latence_max: self.latence_max.load(Ordering::Relaxed),
            latence_moy: if total > 0 {
                sum as f64 / total as f64
            } else {
                0.0
            },
        }
    }
}

fn atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(
            current,
            value,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn atomic_min(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value < current {
        match target.compare_exchange_weak(
            current,
            value,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}



#[derive(Default)]
struct StatsInner {
    total: u64,
    success: u64,
    failed: u64,
    latence_min: u64,
    latence_max: u64,
    latence_sum: u64,
}

pub struct ServiceStatsMutex {
    inner: Mutex<StatsInner>,
}

impl ServiceStatsMutex {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(StatsInner {
                latence_min: u64::MAX,
                ..Default::default()
            }),
        }
    }

    pub fn record(&self, success: bool, latence_us: u64) {
        let mut g = self.inner.lock().unwrap();
        g.total += 1;
        if success {
            g.success += 1;
        } else {
            g.failed += 1;
        }
        g.latence_sum += latence_us;
        if latence_us < g.latence_min {
            g.latence_min = latence_us;
        }
        if latence_us > g.latence_max {
            g.latence_max = latence_us;
        }
    }
}


trait Record: Send + Sync {
    fn record(&self, success: bool, latence_us: u64);
}

impl Record for ServiceStats {
    fn record(&self, s: bool, l: u64) {
        ServiceStats::record(self, s, l);
    }
}

impl Record for ServiceStatsMutex {
    fn record(&self, s: bool, l: u64) {
        ServiceStatsMutex::record(self, s, l);
    }
}

fn bench<R: Record + 'static>(
    nom: &str,
    n_threads: usize,
    ops_par_thread: usize,
    stats: Arc<R>,
) -> u128 {
    let start = Instant::now();
    let handles: Vec<_> = (0..n_threads)
        .map(|t| {
            let s = Arc::clone(&stats);
            thread::spawn(move || {
                for i in 0..ops_par_thread {
                    let success = (i + t) % 10 != 0;
                    let lat = (((i + t) * 37) % 900 + 50) as u64;
                    s.record(success, lat);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let duree = start.elapsed().as_millis();
    let total_ops = n_threads * ops_par_thread;
    let throughput = total_ops as f64 / start.elapsed().as_secs_f64() / 1e6;
    println!(
        "  [{nom:<12}] {n_threads:>3}T × {ops_par_thread:>5} ops : {duree:>4} ms ({throughput:>5.1} M ops/s)"
    );
    duree
}

fn main() {
    println!("=== Correction Exercice 6 : Métriques lock-free ===\n");

    println!("--- [1] Test fonctionnel : 16 threads × 10 000 records ---");
    let stats = Arc::new(ServiceStats::new());
    let s2 = Arc::clone(&stats);
    let handles: Vec<_> = (0..16)
        .map(|t| {
            let s = Arc::clone(&s2);
            thread::spawn(move || {
                for i in 0..10_000 {
                    let success = (i + t) % 10 != 0;
                    let lat = (((i + t) * 37) % 900 + 50) as u64;
                    s.record(success, lat);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let snap = stats.snapshot();
    println!(
        "  total={} success={} failed={} min={}µs max={}µs moy={:.1}µs",
        snap.total, snap.success, snap.failed, snap.latence_min, snap.latence_max, snap.latence_moy
    );
    assert_eq!(snap.total, 160_000);
    assert_eq!(snap.success + snap.failed, snap.total);
    assert!(snap.latence_min <= 60);
    assert!(snap.latence_max >= 940);
    println!("  ✓ tous les invariants vérifiés\n");

    println!("--- [2A] Bench COMPTEUR SIMPLE (1 variable partagée) ---");
    println!("    Ici les atomiques gagnent : 1 op atomique vs 1 lock complet");
    for &n in &[1usize, 4, 16, 64] {
        let t_atom = bench("atomique", n, 100_000, Arc::new(CompteurAtom::default()));
        let t_mux = bench("mutex   ", n, 100_000, Arc::new(CompteurMux::default()));
        let speedup = t_mux as f64 / t_atom.max(1) as f64;
        println!("  → speedup atomique/mutex à {n} threads : {speedup:.2}×\n");
    }

    println!("--- [2B] Bench MULTI-CHAMPS (ServiceStats vs Mutex<Inner>) ---");
    println!("    Ici Mutex peut gagner : 6 cache lines bouncing vs 1 seule");
    for &n in &[1usize, 4, 16, 64] {
        let t_atom = bench("atomique", n, 10_000, Arc::new(ServiceStats::new()));
        let t_mux = bench("mutex   ", n, 10_000, Arc::new(ServiceStatsMutex::new()));
        let speedup = t_mux as f64 / t_atom.max(1) as f64;
        println!("  → speedup atomique/mutex à {n} threads : {speedup:.2}×\n");
    }

    println!("LEÇON : atomique ≠ toujours plus rapide.");
    println!("  - 1 variable partagée : atomique gagne (souvent largement)");
    println!("  - N variables séparées : false sharing peut faire perdre l'atomique");
    println!("  - Voir Module 2.3 pour des structures lock-free réellement scalables");
}


#[derive(Default)]
pub struct CompteurAtom {
    n: AtomicU64,
}

#[derive(Default)]
pub struct CompteurMux {
    n: Mutex<u64>,
}

impl Record for CompteurAtom {
    fn record(&self, _s: bool, _l: u64) {
        self.n.fetch_add(1, Ordering::Relaxed);
    }
}

impl Record for CompteurMux {
    fn record(&self, _s: bool, _l: u64) {
        *self.n.lock().unwrap() += 1;
    }
}

