use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::deque::{Steal, Stealer, Worker};
use crossbeam::queue::{ArrayQueue, SegQueue};

use dashmap::DashMap;

/// Démonstration d'ArrayQueue avec plusieurs producteurs et consommateurs.
///
/// Cas d'usage OVH : queue de jobs bornée. Quand elle est pleine, les
/// producteurs reçoivent un `Err` immédiat — pas d'attente, pas de blocage.
/// Le caller décide alors quoi faire : retry, drop, alerter, etc.
fn demo_array_queue() {
    println!("--- [1] ArrayQueue : file bornée lock-free ---");

    // File de capacité 16, partagée par plusieurs threads
    let queue: Arc<ArrayQueue<String>> = Arc::new(ArrayQueue::new(16));

    let dropped = Arc::new(AtomicU64::new(0));
    let mut producers = Vec::new();

    for p_id in 0..3 {
        let q = Arc::clone(&queue);
        let dropped = Arc::clone(&dropped);
        producers.push(thread::spawn(move || {
            for i in 0..20u32 {
                let item = format!("job-p{p_id}-#{i}");
                match q.push(item.clone()) {
                    Ok(()) => {
                        // Bien poussé
                    }
                    Err(_returned) => {
                        // File pleine — on incrémente un compteur de pertes
                        dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    // 2 consommateurs qui dépilent en boucle
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut consumers = Vec::new();
    for c_id in 0..2 {
        let q = Arc::clone(&queue);
        let stop = Arc::clone(&stop);
        consumers.push(thread::spawn(move || {
            let mut count = 0u32;
            loop {
                match q.pop() {
                    Some(_item) => {
                        count += 1;
                    }
                    None => {
                        // File vide — vérifier si on doit s'arrêter
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        thread::yield_now();
                    }
                }
            }
            println!("  [cons-{c_id}] a traité {count} jobs");
            count
        }));
    }

    for p in producers {
        p.join().unwrap();
    }

    // Laisser un instant aux consommateurs pour finir
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);

    let mut total_consumed = 0u32;
    for c in consumers {
        total_consumed += c.join().unwrap();
    }

    let lost = dropped.load(Ordering::Relaxed);
    println!(
        "  Total produit visé = {}, consommé = {}, rejeté (file pleine) = {}\n",
        60, total_consumed, lost
    );
    assert_eq!(total_consumed as u64 + lost, 60);
}

/// Démonstration de SegQueue.
///
/// Cas d'usage OVH : collecter des métriques/événements émis par
/// de nombreux composants, sans limite hard. La file grandit dynamiquement
/// — pas de risque de blocage côté producteur.
fn demo_seg_queue() {
    println!("--- [2] SegQueue : file non-bornée lock-free ---");

    let queue: Arc<SegQueue<u64>> = Arc::new(SegQueue::new());

    // 4 producteurs qui poussent 10_000 événements chacun
    let mut producers = Vec::new();
    for p_id in 0..4u64 {
        let q = Arc::clone(&queue);
        producers.push(thread::spawn(move || {
            for i in 0..10_000u64 {
                q.push(p_id * 100_000 + i);
            }
        }));
    }

    for p in producers {
        p.join().unwrap();
    }

    // Tout consommer côté principal
    let mut total: u64 = 0;
    let mut count: u64 = 0;
    while let Some(v) = queue.pop() {
        total = total.wrapping_add(v);
        count += 1;
    }
    println!(
        "  {} événements consommés, somme = {}\n",
        count, total
    );
    assert_eq!(count, 40_000);
}

/// Job simulé — un identifiant + un payload.
#[derive(Debug, Clone)]
struct Task {
    id: u64,
}

/// Simule le traitement d'une tâche.
fn process_task(task: &Task, worker_id: usize) {
    // Pas de println! ici pour éviter le bruit dans la démo
    // (on incrémente un compteur partagé à la place)
    let _ = (task.id, worker_id);
}

/// Démonstration d'un scheduler work-stealing simple.
///
/// Chaque worker a sa propre deque (Worker). Quand un worker n'a plus de
/// tâches, il "vole" sur les deques des autres via leurs Stealer.
/// C'est exactement le pattern utilisé par le runtime Tokio multi_thread.
fn demo_work_stealing() {
    println!("--- [3] Work-stealing : Worker + Stealer ---");

    let n_workers = 4;
    let total_tasks = 1_000u64;

    // Crée un worker par thread, et collecte les stealers
    let mut workers: Vec<Worker<Task>> = (0..n_workers).map(|_| Worker::new_fifo()).collect();
    let stealers: Vec<Stealer<Task>> = workers.iter().map(|w| w.stealer()).collect();

    // Initialise les workers de manière déséquilibrée : tout sur le worker 0
    // → cela force les autres workers à voler.
    for i in 0..total_tasks {
        workers[0].push(Task { id: i });
    }
    println!("  Initialisation : {} tâches placées sur worker-0", total_tasks);

    // Compteur global de tâches traitées par chaque worker
    let processed = Arc::new(
        (0..n_workers)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    let stealers = Arc::new(stealers);

    let start = Instant::now();
    let mut handles = Vec::new();

    // On retire les workers du vecteur pour les passer aux threads
    for (worker_id, worker) in workers.into_iter().enumerate() {
        let processed = Arc::clone(&processed);
        let stealers = Arc::clone(&stealers);

        handles.push(thread::spawn(move || {
            loop {
                // 1. D'abord, essayer notre propre deque (rapide, pas de contention)
                if let Some(task) = worker.pop() {
                    process_task(&task, worker_id);
                    processed[worker_id].fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // 2. Notre deque est vide : tenter de voler chez les autres
                let mut stolen = false;
                for (sid, stealer) in stealers.iter().enumerate() {
                    if sid == worker_id {
                        continue; // pas voler chez nous-même
                    }
                    match stealer.steal() {
                        Steal::Success(task) => {
                            process_task(&task, worker_id);
                            processed[worker_id].fetch_add(1, Ordering::Relaxed);
                            stolen = true;
                            break;
                        }
                        Steal::Empty => continue,
                        Steal::Retry => {
                            stolen = true; // contention : on reboucle
                            break;
                        }
                    }
                }

                if !stolen {
                    // Plus rien à voler nulle part → on sort
                    break;
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let mut total: u64 = 0;
    for (i, p) in processed.iter().enumerate() {
        let n = p.load(Ordering::Relaxed);
        println!("  worker-{i} a traité {n:>4} tâches");
        total += n;
    }
    println!(
        "  Total = {total} tâches en {} µs (équilibrage par work-stealing)\n",
        elapsed.as_micros()
    );
    assert_eq!(total, total_tasks);
}

/// Cache OVH simulé : association hostname → liste de timestamps de requêtes.
///
/// DashMap est un HashMap shardé : il découpe l'espace des clés en
/// plusieurs sous-maps, chacune protégée par son propre RwLock. Résultat :
/// la contention est divisée par le nombre de shards, ce qui permet
/// d'utiliser efficacement plusieurs threads sur le même cache.
fn demo_dashmap() {
    println!("--- [4] DashMap : HashMap concurrent shardé ---");

    let cache: Arc<DashMap<String, Vec<u64>>> = Arc::new(DashMap::new());

    // 8 threads qui insèrent et lisent en parallèle
    let mut handles = Vec::new();
    for t in 0..8u64 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..200u64 {
                // Clé : hostname dérivé de l'identifiant — 10 hostnames différents
                let host = format!("server-{:02}", i % 10);
                let timestamp = t * 1_000_000 + i;

                // `entry().or_insert_with()` est atomique au niveau du shard
                cache
                    .entry(host)
                    .or_insert_with(Vec::new)
                    .push(timestamp);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Lecture : itération concurrente (snapshot par shard)
    let mut total_entries: usize = 0;
    let mut total_timestamps: usize = 0;
    for entry in cache.iter() {
        total_entries += 1;
        total_timestamps += entry.value().len();
    }

    println!(
        "  Cache : {} hostnames distincts, {} timestamps stockés au total",
        total_entries, total_timestamps
    );
    assert_eq!(total_entries, 10);
    assert_eq!(total_timestamps, 8 * 200);

    // Lecture ciblée d'un hostname précis
    if let Some(v) = cache.get("server-03") {
        println!(
            "  server-03 contient {} timestamps (premier = {})",
            v.len(),
            v.first().copied().unwrap_or(0)
        );
    }
    println!();
}

fn main() {
    println!("=== Demo : Structures lock-free (crossbeam + DashMap) ===\n");

    demo_array_queue();
    demo_seg_queue();
    demo_work_stealing();
    demo_dashmap();

    println!("Démo terminée — toutes les assertions passent.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_queue_push_pop_basique() {
        let q: ArrayQueue<u32> = ArrayQueue::new(3);
        assert!(q.push(1).is_ok());
        assert!(q.push(2).is_ok());
        assert!(q.push(3).is_ok());
        // File pleine
        assert!(q.push(4).is_err());

        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(3));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn test_array_queue_concurrent() {
        let q: Arc<ArrayQueue<u32>> = Arc::new(ArrayQueue::new(1024));
        let mut handles = Vec::new();

        for t in 0..4u32 {
            let q = Arc::clone(&q);
            handles.push(thread::spawn(move || {
                for i in 0..250u32 {
                    while q.push(t * 1000 + i).is_err() {
                        thread::yield_now();
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut count = 0;
        while q.pop().is_some() {
            count += 1;
        }
        assert_eq!(count, 1000);
    }

    #[test]
    fn test_seg_queue_basique() {
        let q: SegQueue<&'static str> = SegQueue::new();
        q.push("a");
        q.push("b");
        q.push("c");
        assert_eq!(q.pop(), Some("a"));
        assert_eq!(q.pop(), Some("b"));
        assert_eq!(q.pop(), Some("c"));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn test_seg_queue_concurrent_total() {
        let q: Arc<SegQueue<u64>> = Arc::new(SegQueue::new());

        let mut producers = Vec::new();
        for t in 0..4u64 {
            let q = Arc::clone(&q);
            producers.push(thread::spawn(move || {
                for i in 0..500u64 {
                    q.push(t * 10_000 + i);
                }
            }));
        }
        for p in producers {
            p.join().unwrap();
        }

        let mut count = 0;
        while q.pop().is_some() {
            count += 1;
        }
        assert_eq!(count, 2000);
    }

    #[test]
    fn test_worker_stealer_roundtrip() {
        let w: Worker<u32> = Worker::new_fifo();
        let s = w.stealer();

        w.push(1);
        w.push(2);
        w.push(3);

        // Le stealer prend par l'autre extrémité (head)
        match s.steal() {
            Steal::Success(v) => assert!(v == 1 || v == 2 || v == 3),
            _ => panic!("Le steal aurait dû réussir"),
        }
        // Le worker récupère ses tâches restantes
        assert!(w.pop().is_some());
    }

    #[test]
    fn test_dashmap_concurrent_inserts() {
        let m: Arc<DashMap<u32, u32>> = Arc::new(DashMap::new());
        let mut handles = Vec::new();
        for t in 0..8u32 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for i in 0..100u32 {
                    m.insert(t * 100 + i, t);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.len(), 800);
    }

    #[test]
    fn test_dashmap_entry_or_insert() {
        let m: DashMap<String, Vec<u32>> = DashMap::new();
        m.entry("a".into()).or_insert_with(Vec::new).push(1);
        m.entry("a".into()).or_insert_with(Vec::new).push(2);
        m.entry("b".into()).or_insert_with(Vec::new).push(10);

        assert_eq!(m.get("a").unwrap().len(), 2);
        assert_eq!(m.get("b").unwrap().len(), 1);
    }
}
