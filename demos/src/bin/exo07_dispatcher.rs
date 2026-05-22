
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;


#[derive(Clone, Copy, Debug)]
pub enum TypeTache {
    Parse,
    Metrique,
    Alerte,
}

#[derive(Clone)]
pub struct Tache {
    pub id: u64,
    pub kind: TypeTache,
    pub payload: u64,
}

fn traiter(t: &Tache) -> u64 {
    let mut acc = t.payload;
    for _ in 0..50 {
        acc = acc.wrapping_mul(2654435761).wrapping_add(1);
    }
    acc
}

fn nom_kind(k: TypeTache) -> &'static str {
    match k {
        TypeTache::Parse => "parse",
        TypeTache::Metrique => "metrique",
        TypeTache::Alerte => "alerte",
    }
}


pub struct Dispatcher {
    pub queue: Arc<SegQueue<Tache>>,
    pub stats: Arc<DashMap<&'static str, u64>>,
    pub running: Arc<AtomicBool>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
            stats: Arc::new(DashMap::new()),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn submit(&self, t: Tache) {
        self.queue.push(t);
    }

    pub fn worker_loop(&self) {
        loop {
            if let Some(t) = self.queue.pop() {
                let _ = std::hint::black_box(traiter(&t));
                *self.stats.entry(nom_kind(t.kind)).or_insert(0) += 1;
            } else if !self.running.load(Ordering::Relaxed) {
                break;
            } else {
                thread::yield_now();
            }
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}


pub struct DispatcherMutex {
    pub queue: Arc<Mutex<VecDeque<Tache>>>,
    pub stats: Arc<Mutex<HashMap<&'static str, u64>>>,
    pub running: Arc<AtomicBool>,
}

impl DispatcherMutex {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn submit(&self, t: Tache) {
        self.queue.lock().unwrap().push_back(t);
    }

    pub fn worker_loop(&self) {
        loop {
            let next = self.queue.lock().unwrap().pop_front();
            if let Some(t) = next {
                let _ = std::hint::black_box(traiter(&t));
                *self.stats.lock().unwrap().entry(nom_kind(t.kind)).or_insert(0) += 1;
            } else if !self.running.load(Ordering::Relaxed) {
                break;
            } else {
                thread::yield_now();
            }
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}


fn fabriquer_taches(n: usize) -> Vec<Tache> {
    (0..n as u64)
        .map(|i| {
            let kind = match i % 3 {
                0 => TypeTache::Parse,
                1 => TypeTache::Metrique,
                _ => TypeTache::Alerte,
            };
            Tache {
                id: i,
                kind,
                payload: i.wrapping_mul(37),
            }
        })
        .collect()
}

fn bench_lockfree(n_workers: usize, taches: &[Tache]) -> u128 {
    let disp = Arc::new(Dispatcher::new());
    let start = Instant::now();

    let workers: Vec<_> = (0..n_workers)
        .map(|_| {
            let d = Arc::clone(&disp);
            thread::spawn(move || d.worker_loop())
        })
        .collect();

    for t in taches {
        disp.submit(t.clone());
    }
    disp.stop();
    for w in workers {
        w.join().unwrap();
    }
    start.elapsed().as_millis()
}

fn bench_mutex(n_workers: usize, taches: &[Tache]) -> u128 {
    let disp = Arc::new(DispatcherMutex::new());
    let start = Instant::now();

    let workers: Vec<_> = (0..n_workers)
        .map(|_| {
            let d = Arc::clone(&disp);
            thread::spawn(move || d.worker_loop())
        })
        .collect();

    for t in taches {
        disp.submit(t.clone());
    }
    disp.stop();
    for w in workers {
        w.join().unwrap();
    }
    start.elapsed().as_millis()
}

fn main() {
    println!("=== Correction Exercice 7 : Dispatcher lock-free ===\n");

    println!("--- [1] Test fonctionnel : 8 producers × 5 000 + 8 workers ---");
    let disp = Arc::new(Dispatcher::new());
    let n_prod = 8;
    let n_work = 8;
    let par_prod = 5_000;

    let workers: Vec<_> = (0..n_work)
        .map(|_| {
            let d = Arc::clone(&disp);
            thread::spawn(move || d.worker_loop())
        })
        .collect();

    let producers: Vec<_> = (0..n_prod)
        .map(|p| {
            let d = Arc::clone(&disp);
            thread::spawn(move || {
                for i in 0..par_prod {
                    let id = (p * par_prod + i) as u64;
                    let kind = match id % 3 {
                        0 => TypeTache::Parse,
                        1 => TypeTache::Metrique,
                        _ => TypeTache::Alerte,
                    };
                    d.submit(Tache {
                        id,
                        kind,
                        payload: id.wrapping_mul(37),
                    });
                }
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    disp.stop();
    for w in workers {
        w.join().unwrap();
    }

    let total_parse = disp.stats.get("parse").map(|r| *r).unwrap_or(0);
    let total_metr = disp.stats.get("metrique").map(|r| *r).unwrap_or(0);
    let total_alerte = disp.stats.get("alerte").map(|r| *r).unwrap_or(0);
    let grand_total = total_parse + total_metr + total_alerte;
    println!(
        "  parse={total_parse} metrique={total_metr} alerte={total_alerte} (total={grand_total})"
    );
    assert_eq!(grand_total as usize, n_prod * par_prod);
    println!("  ✓ aucune tâche perdue\n");

    println!("--- [2] Bench lock-free vs Mutex (40 000 tâches) ---");
    let taches = fabriquer_taches(40_000);
    for &n in &[1usize, 4, 16, 32] {
        let t_lf = bench_lockfree(n, &taches);
        let t_mx = bench_mutex(n, &taches);
        let speedup = t_mx as f64 / t_lf.max(1) as f64;
        println!(
            "  {n:>2} workers : lock-free = {t_lf:>4} ms | mutex = {t_mx:>4} ms | speedup = {speedup:.2}×"
        );
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_et_traiter_single_thread() {
        let disp = Dispatcher::new();
        disp.submit(Tache {
            id: 1,
            kind: TypeTache::Parse,
            payload: 42,
        });
        disp.submit(Tache {
            id: 2,
            kind: TypeTache::Alerte,
            payload: 7,
        });
        disp.stop();
        disp.worker_loop();
        assert_eq!(*disp.stats.get("parse").unwrap(), 1);
        assert_eq!(*disp.stats.get("alerte").unwrap(), 1);
        assert!(disp.queue.is_empty());
    }

    #[test]
    fn mpmc_no_loss() {
        let disp = Arc::new(Dispatcher::new());
        let n_prod = 4;
        let n_work = 4;
        let par_prod = 1000;

        let workers: Vec<_> = (0..n_work)
            .map(|_| {
                let d = Arc::clone(&disp);
                thread::spawn(move || d.worker_loop())
            })
            .collect();

        let producers: Vec<_> = (0..n_prod)
            .map(|p| {
                let d = Arc::clone(&disp);
                thread::spawn(move || {
                    for i in 0..par_prod {
                        let id = (p * par_prod + i) as u64;
                        let kind = match id % 3 {
                            0 => TypeTache::Parse,
                            1 => TypeTache::Metrique,
                            _ => TypeTache::Alerte,
                        };
                        d.submit(Tache {
                            id,
                            kind,
                            payload: id,
                        });
                    }
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        disp.stop();
        for w in workers {
            w.join().unwrap();
        }

        let total: u64 = disp.stats.iter().map(|r| *r.value()).sum();
        assert_eq!(total as usize, n_prod * par_prod);
    }

    #[test]
    fn stop_avant_drain_pas_de_perte() {
        // Vérifie que stop() suivi de worker_loop() vide bien la file
        let disp = Dispatcher::new();
        for i in 0..100 {
            disp.submit(Tache {
                id: i,
                kind: TypeTache::Metrique,
                payload: i,
            });
        }
        disp.stop();  // running = false, mais la file a 100 tâches
        disp.worker_loop();
        assert_eq!(*disp.stats.get("metrique").unwrap(), 100);
    }
}
