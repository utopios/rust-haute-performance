use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::Mutex as PlMutex;
use parking_lot::RwLock as PlRwLock;

/// Compteur partagé entre threads — incrémenté sous Mutex.
///
/// On exécute le même bench avec deux implémentations différentes
/// pour mesurer l'overhead du Mutex de la bibliothèque standard
/// par rapport à parking_lot.
fn bench_std_mutex(iterations: u64, threads: usize) -> Duration {
    let counter = Arc::new(Mutex::new(0u64));
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let counter = Arc::clone(&counter);
            thread::spawn(move || {
                for _ in 0..iterations {
                    let mut guard = counter.lock().unwrap();
                    *guard += 1;
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let final_value = *counter.lock().unwrap();
    assert_eq!(final_value, iterations * threads as u64);
    elapsed
}

fn bench_parking_lot_mutex(iterations: u64, threads: usize) -> Duration {
    let counter = Arc::new(PlMutex::new(0u64));
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let counter = Arc::clone(&counter);
            thread::spawn(move || {
                for _ in 0..iterations {
                    // parking_lot::Mutex : pas de Result, pas de poison
                    let mut guard = counter.lock();
                    *guard += 1;
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let final_value = *counter.lock();
    assert_eq!(final_value, iterations * threads as u64);
    elapsed
}

/// Configuration applicative simulée — typique d'un service OVH.
#[derive(Clone, Debug)]
struct AppConfig {
    server_port: u16,
    max_connections: u32,
    region: String,
}

impl AppConfig {
    fn new(port: u16, max_conn: u32, region: &str) -> Self {
        AppConfig {
            server_port: port,
            max_connections: max_conn,
            region: region.to_string(),
        }
    }
}

/// Store de configuration partagé.
///
/// En production OVH, la configuration est lue des milliers de fois par seconde
/// (par chaque requête entrante) et n'est mise à jour qu'occasionnellement
/// (rechargement à chaud, modification d'un opérateur). RwLock est donc le
/// choix naturel : plusieurs lecteurs en parallèle, un seul écrivain à la fois.
struct ConfigStore {
    inner: PlRwLock<AppConfig>,
}

impl ConfigStore {
    fn new(initial: AppConfig) -> Self {
        ConfigStore {
            inner: PlRwLock::new(initial),
        }
    }

    /// Lecture du port — plusieurs threads peuvent appeler en même temps.
    fn get_port(&self) -> u16 {
        self.inner.read().server_port
    }

    /// Clone complet de la configuration pour le caller.
    fn snapshot(&self) -> AppConfig {
        self.inner.read().clone()
    }

    /// Mise à jour atomique : bloque temporairement tous les lecteurs.
    fn update(&self, new_config: AppConfig) {
        let mut guard = self.inner.write();
        *guard = new_config;
    }
}

/// File FIFO bornée, thread-safe, multi-producer/multi-consumer.
///
/// Pattern fondamental en infrastructure : bus d'événements, queue de jobs,
/// pipeline de traitement. Quand la file est pleine, les producteurs attendent.
/// Quand la file est vide, les consommateurs attendent. Les deux conditions
/// sont implémentées via deux Condvar distinctes pour éviter les réveils inutiles.
struct BoundedQueue<T> {
    inner: Mutex<VecDeque<T>>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: usize,
}

impl<T> BoundedQueue<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "La capacité doit être > 0");
        BoundedQueue {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            capacity,
        }
    }

    /// Insère un élément. Bloque si la file est pleine.
    fn push(&self, item: T) {
        let mut guard = self.inner.lock().unwrap();

        // Boucle while (et pas if !) : protège contre les "spurious wakeups"
        // — un thread peut être réveillé sans qu'aucune condition ait changé.
        while guard.len() >= self.capacity {
            guard = self.not_full.wait(guard).unwrap();
        }

        guard.push_back(item);
        // Réveille un consommateur en attente (s'il y en a un)
        self.not_empty.notify_one();
    }

    /// Retire un élément. Bloque si la file est vide.
    fn pop(&self) -> T {
        let mut guard = self.inner.lock().unwrap();

        while guard.is_empty() {
            guard = self.not_empty.wait(guard).unwrap();
        }

        let item = guard.pop_front().expect("File non vide vérifié");
        self.not_full.notify_one();
        item
    }

    /// Taille actuelle (snapshot — peut être obsolète immédiatement).
    fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

fn main() {
    println!("=== Demo : Mutex, RwLock, Condvar ===\n");

    // -----------------------------------------------------------------
    // Partie 1 : Comparatif Mutex
    // -----------------------------------------------------------------
    println!("--- [1] Comparatif std::Mutex vs parking_lot::Mutex ---");
    let iterations = 50_000;
    let threads = 4;

    let t_std = bench_std_mutex(iterations, threads);
    let t_pl = bench_parking_lot_mutex(iterations, threads);

    println!(
        "  std::Mutex        : {:>6} ms  ({} threads × {} incréments)",
        t_std.as_millis(),
        threads,
        iterations
    );
    println!(
        "  parking_lot::Mutex: {:>6} ms  ({} threads × {} incréments)",
        t_pl.as_millis(),
        threads,
        iterations
    );

    let ratio = t_std.as_nanos() as f64 / t_pl.as_nanos().max(1) as f64;
    println!("  parking_lot est ~{:.2}× plus rapide ici\n", ratio);

    // -----------------------------------------------------------------
    // Partie 2 : RwLock — ConfigStore avec lectures massives
    // -----------------------------------------------------------------
    println!("--- [2] RwLock : ConfigStore multi-lecteurs ---");
    let store = Arc::new(ConfigStore::new(AppConfig::new(8080, 1000, "eu-west-1")));

    // Lance 8 lecteurs concurrents
    let mut readers = Vec::new();
    for id in 0..8 {
        let store = Arc::clone(&store);
        readers.push(thread::spawn(move || {
            let mut port_sum: u64 = 0;
            for _ in 0..1000 {
                port_sum += store.get_port() as u64;
            }
            println!("  [reader-{id}] somme des ports lus = {port_sum}");
        }));
    }

    // Pendant ce temps, un seul écrivain met à jour
    let store_writer = Arc::clone(&store);
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        store_writer.update(AppConfig::new(9090, 2000, "eu-west-3"));
        println!("  [writer]    configuration mise à jour");
    });

    for r in readers {
        r.join().unwrap();
    }
    writer.join().unwrap();

    let snap = store.snapshot();
    println!(
        "  configuration finale : port={}, max_conn={}, region={}\n",
        snap.server_port, snap.max_connections, snap.region
    );

    // -----------------------------------------------------------------
    // Partie 3 : Condvar — BoundedQueue producer/consumer
    // -----------------------------------------------------------------
    println!("--- [3] Condvar : BoundedQueue producer/consumer ---");
    let queue: Arc<BoundedQueue<String>> = Arc::new(BoundedQueue::new(4));

    // 2 producteurs
    let mut producers = Vec::new();
    for p_id in 0..2 {
        let q = Arc::clone(&queue);
        producers.push(thread::spawn(move || {
            for i in 0..5 {
                let msg = format!("event-p{p_id}-#{i}");
                q.push(msg.clone());
                println!("  [prod-{p_id}] push  {msg}  (taille={})", q.len());
                thread::sleep(Duration::from_millis(20));
            }
        }));
    }

    // 3 consommateurs
    let mut consumers = Vec::new();
    for c_id in 0..3 {
        let q = Arc::clone(&queue);
        consumers.push(thread::spawn(move || {
            // Chaque consommateur dépile (2*5)/3 ≈ 3-4 éléments
            for _ in 0..3 {
                let item = q.pop();
                println!("  [cons-{c_id}] pop   {item}");
                thread::sleep(Duration::from_millis(35));
            }
        }));
    }

    for p in producers {
        p.join().unwrap();
    }

    // Vidange du dernier élément éventuellement laissé
    let q_drain = Arc::clone(&queue);
    let drain = thread::spawn(move || {
        if q_drain.len() > 0 {
            let item = q_drain.pop();
            println!("  [drain]   pop   {item}");
        }
    });

    for c in consumers {
        c.join().unwrap();
    }
    let _ = drain.join();

    println!("\nDémo terminée. File résiduelle = {} éléments.", queue.len());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bench_std_mutex_correct_sum() {
        // Vérifie simplement que le compteur final est cohérent
        let _ = bench_std_mutex(1_000, 2);
    }

    #[test]
    fn test_bench_parking_lot_mutex_correct_sum() {
        let _ = bench_parking_lot_mutex(1_000, 2);
    }

    #[test]
    fn test_config_store_read_write() {
        let store = ConfigStore::new(AppConfig::new(8080, 100, "eu-west-1"));
        assert_eq!(store.get_port(), 8080);

        store.update(AppConfig::new(9090, 200, "eu-west-3"));
        assert_eq!(store.get_port(), 9090);

        let snap = store.snapshot();
        assert_eq!(snap.max_connections, 200);
        assert_eq!(snap.region, "eu-west-3");
    }

    #[test]
    fn test_config_store_multiple_readers() {
        let store = Arc::new(ConfigStore::new(AppConfig::new(80, 10, "fr")));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for _ in 0..500 {
                    assert_eq!(s.get_port(), 80);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_bounded_queue_push_pop() {
        let q: BoundedQueue<i32> = BoundedQueue::new(3);
        q.push(1);
        q.push(2);
        q.push(3);
        assert_eq!(q.pop(), 1);
        assert_eq!(q.pop(), 2);
        assert_eq!(q.pop(), 3);
    }

    #[test]
    fn test_bounded_queue_blocks_when_full() {
        let q = Arc::new(BoundedQueue::<i32>::new(2));
        q.push(10);
        q.push(20);
        // La file est maintenant pleine. Lance un producteur qui doit bloquer.
        let q_prod = Arc::clone(&q);
        let producer = thread::spawn(move || {
            q_prod.push(30); // bloque jusqu'à ce qu'un pop libère la place
        });

        // Petit délai pour s'assurer que le producteur a tenté son push
        thread::sleep(Duration::from_millis(50));
        assert_eq!(q.len(), 2, "Producteur aurait dû être bloqué");

        // Libère une place
        assert_eq!(q.pop(), 10);
        producer.join().unwrap();

        // Maintenant la file contient [20, 30]
        assert_eq!(q.pop(), 20);
        assert_eq!(q.pop(), 30);
    }

    #[test]
    fn test_bounded_queue_multi_producer_multi_consumer() {
        let q: Arc<BoundedQueue<u32>> = Arc::new(BoundedQueue::new(8));

        let n_prod = 4;
        let n_cons = 4;
        let items_per_prod = 100;

        let producers: Vec<_> = (0..n_prod)
            .map(|p| {
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    for i in 0..items_per_prod {
                        q.push(p * 1000 + i);
                    }
                })
            })
            .collect();

        let total = (n_prod * items_per_prod) as usize;
        let per_cons = total / n_cons as usize;

        let consumers: Vec<_> = (0..n_cons)
            .map(|_| {
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    let mut sum: u64 = 0;
                    for _ in 0..per_cons {
                        sum += q.pop() as u64;
                    }
                    sum
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }

        let mut total_sum: u64 = 0;
        for c in consumers {
            total_sum += c.join().unwrap();
        }

        // Somme attendue : Σ_{p=0..4} Σ_{i=0..100} (p*1000 + i)
        let expected: u64 = (0..n_prod)
            .map(|p| (0..items_per_prod).map(|i| (p * 1000 + i) as u64).sum::<u64>())
            .sum();
        assert_eq!(total_sum, expected);
    }
}
