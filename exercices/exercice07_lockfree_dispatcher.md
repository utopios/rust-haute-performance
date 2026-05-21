# Exercice 7 — Dispatcher de tâches lock-free (crossbeam + DashMap)

**Jour 2 — Module 2.3 : Structures lock-free**
**Durée estimée :** 1h — 1h15
**Prérequis :** Modules 2.1 (Mutex), 2.2 (atomiques), 2.3 (crossbeam, DashMap)
**Démo de référence :** [`demo07_lockfree_crossbeam`](../demos-stagiaires/src/bin/demo07_lockfree_crossbeam.rs)

---

## Contexte

Votre agrégateur OVH évolue : il devient un **dispatcher de tâches**. Plusieurs *producers* (les agents des serveurs OVH) produisent des `Tache` à traiter (parsing log, calcul métrique, alerte). Un pool de *workers* les consomme, calcule un résultat, et met à jour des **stats par type de tâche** dans un cache concurrent.

Aujourd'hui, l'implémentation utilise un `Mutex<VecDeque>` + `Mutex<HashMap>`. À 32 workers, c'est devenu le goulot d'étranglement. Vous devez **remplacer les deux par des structures lock-free** : `crossbeam::SegQueue` pour la file, `DashMap` pour le cache.

> Vous ne devez **pas** écrire votre propre code lock-free (Treiber stack, Michael-Scott queue, etc.). Utilisez les implémentations existantes. C'est le sujet du module : **savoir utiliser** les bons outils.

---

## Objectifs pédagogiques

1. Choisir entre `SegQueue` (non bornée) et `ArrayQueue` (bornée) selon le besoin de **backpressure**.
2. Utiliser `DashMap` comme remplaçant d'un `Mutex<HashMap>`.
3. Mesurer le **vrai speedup** d'une approche lock-free vs Mutex (qu'on a vu décevant à l'exo 5/6).
4. Comprendre comment `DashMap` évite le false sharing par **sharding interne**.

---

## Énoncé

### Partie 1 — Structures

Dans le projet `demos-stagiaires`, créez `src/bin/exo07_dispatcher.rs`.

```rust
#[derive(Clone, Copy)]
pub enum TypeTache { Parse, Metrique, Alerte }

pub struct Tache {
    pub id: u64,
    pub kind: TypeTache,
    pub payload: u64,  // valeur arbitraire à traiter
}

pub struct Dispatcher {
    pub queue: Arc<crossbeam::queue::SegQueue<Tache>>,
    pub stats: Arc<dashmap::DashMap<&'static str, u64>>,
    pub running: Arc<std::sync::atomic::AtomicBool>,
}
```

### Partie 2 — Le traitement

```rust
impl Dispatcher {
    pub fn new() -> Self;

    /// Envoie une tâche dans la file (lock-free).
    pub fn submit(&self, t: Tache);

    /// Boucle worker : pop une tâche, la traite, met à jour les stats.
    /// Sort quand running == false ET la file est vide.
    pub fn worker_loop(&self);

    /// Signale l'arrêt aux workers.
    pub fn stop(&self);
}
```

Le traitement d'une tâche est simulé :
```rust
fn traiter(t: &Tache) -> u64 {
    // calcul artificiel : pour distinguer le throughput de la file
    // d'un workload qui prend du CPU
    let mut acc = t.payload;
    for _ in 0..50 {
        acc = acc.wrapping_mul(2654435761).wrapping_add(1);
    }
    acc
}
```

Après le traitement, incrémenter les stats dans le DashMap :

```rust
let nom = match t.kind {
    TypeTache::Parse => "parse",
    TypeTache::Metrique => "metrique",
    TypeTache::Alerte => "alerte",
};
*self.stats.entry(nom).or_insert(0) += 1;
```

### Partie 3 — Test fonctionnel

1. 8 producers envoient chacun 5 000 tâches (mix des 3 types, déterministe).
2. 8 workers consomment et mettent à jour les stats.
3. Le main appelle `stop()` après que tous les producers ont fini.
4. Vérifier :
   - `stats["parse"] + stats["metrique"] + stats["alerte"] == 40_000`
   - aucun update perdu
   - tous les workers terminent proprement.

### Partie 4 — Bench vs version Mutex

Implémentez une version `DispatcherMutex` équivalente avec :
- `Arc<Mutex<VecDeque<Tache>>>` pour la file
- `Arc<Mutex<HashMap<&'static str, u64>>>` pour les stats

Sur 40 000 tâches, mesurez les deux versions à 1, 4, 16, 32 workers. Comparez les throughputs et les speedups.

> Cette fois, le speedup lock-free vs Mutex sous forte contention **devrait** être impressionnant (5-20×). Si vous n'observez pas ça, il y a un bug.

---

## Questions d'analyse

1. **SegQueue vs ArrayQueue** : pourquoi avoir choisi `SegQueue` (non bornée) ici ? Quand préférerait-on `ArrayQueue` ? Que se passe-t-il si la file enfle sans limite et que les workers ne suivent pas ?

2. **DashMap : comment ça scale ?** Lisez la documentation de `DashMap`. Combien de shards par défaut ? Quelle est la conséquence pour vous : que se passe-t-il si tous vos accès vont sur les **mêmes 2-3 clés** ("parse", "metrique", "alerte" — seulement 3 clés) ?

3. **Speedup lock-free vs Mutex** : combien gagnez-vous à 16 workers ? À 32 ? Comparez avec le speedup atomique vs Mutex de l'exo 6 (scénario A). La leçon est-elle cohérente ?

4. **Stop propre** : votre `worker_loop` doit sortir quand `running == false` ET la file est vide. Pourquoi pas juste "quand `running == false`" ? Que se passerait-il si on coupait brutalement avec encore 1000 tâches en file ?

5. **Cache miss vs lock-free** : sur quelle métrique observe-t-on le plus de cache misses : la version Mutex (où tous les threads se battent pour la même cache line) ou la version DashMap (où chaque shard a sa cache line) ? Mesurez avec samply si possible.

---

## Bonus (facultatif)

- **B1.** Remplacez `SegQueue` par `ArrayQueue` (bornée à 1000) et observez ce qui se passe quand les producers sont plus rapides que les workers. Implémentez un backpressure côté producer (retry avec backoff exponentiel).

- **B2.** Utilisez `crossbeam::deque::Worker` + `Stealer` pour faire du **work-stealing**. Comparez avec SegQueue centralisée. Pour 8 workers, work-stealing devrait être encore plus rapide (les workers volent les tâches localement, moins de contention sur une queue partagée).

- **B3.** Avec `samply`, profilez les deux versions à 32 threads. Identifiez où le temps est passé : sur le Mutex (frames `__pthread_mutex_lock`), ou sur DashMap (frames `dashmap::DashMap::*::get_mut`).

---

