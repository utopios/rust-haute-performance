# Exercice 6 — Métriques lock-free avec atomiques

**Jour 2 — Module 2.2 : Programmation atomique**
**Durée estimée :** 1h
**Prérequis :** Modules 2.1 (Mutex) et 2.2 (atomiques, memory orderings, CAS)
**Démo de référence :** [`demo06_atomics_cas`](../demos-stagiaires/src/bin/demo06_atomics_cas.rs)

---

## Contexte

Votre agrégateur OVH a besoin de collecter en temps réel des **statistiques par service** : nombre de requêtes, succès, échecs, latence min/max/moyenne. Plusieurs centaines de threads d'ingestion enregistrent chacun leurs événements. Lire les stats doit être instantané.

Aujourd'hui c'est un `Arc<Mutex<Stats>>`. Le profileur a montré que 30 % du temps est passé sur le lock à cause de la contention. **Votre mission : remplacer le Mutex par des atomiques**, en choisissant les bons memory orderings.

> Vous ne devez **pas** utiliser de `Mutex` ni `RwLock` ni `parking_lot`. Tout l'état mutable partagé doit reposer sur `std::sync::atomic`.

---

## Objectifs pédagogiques

1. Maîtriser `AtomicU64::fetch_add` avec le bon ordering (Relaxed pour les compteurs).
2. Implémenter `atomic_max` et `atomic_min` via **compare-and-swap (CAS)** en boucle.
3. Choisir entre `Relaxed`, `Release/Acquire`, `SeqCst` — et savoir justifier.
4. Comparer la performance avec une version `Mutex` équivalente.

---

## Énoncé

### Partie 1 — La structure

Dans le projet `demos-stagiaires`, créez `src/bin/exo06_stats.rs`.

```rust
pub struct ServiceStats {
    pub total:        std::sync::atomic::AtomicU64,
    pub success:      std::sync::atomic::AtomicU64,
    pub failed:       std::sync::atomic::AtomicU64,
    pub latence_min:  std::sync::atomic::AtomicU64,  // µs
    pub latence_max:  std::sync::atomic::AtomicU64,  // µs
    pub latence_sum:  std::sync::atomic::AtomicU64,  // µs
}
```

### Partie 2 — Les opérations

#### record (chemin chaud)

```rust
impl ServiceStats {
    pub fn new() -> Self;

    /// Enregistre un événement. Appelé depuis N threads concurrents.
    pub fn record(&self, success: bool, latence_us: u64);

    /// Snapshot des stats. Lu rarement (toutes les secondes).
    pub fn snapshot(&self) -> StatsSnapshot;
}

pub struct StatsSnapshot {
    pub total: u64,
    pub success: u64,
    pub failed: u64,
    pub latence_min: u64,
    pub latence_max: u64,
    pub latence_moy: f64,
}
```

#### Compteurs simples

Pour `total`, `success`, `failed`, `latence_sum` : utiliser `fetch_add(1, Relaxed)` (et `fetch_add(latence_us, Relaxed)`).

**Pourquoi `Relaxed`** : aucune dépendance entre les compteurs. Chaque thread incrémente le sien ; on n'a pas besoin que les threads voient les modifs des autres dans un ordre particulier.

#### atomic_max et atomic_min (via CAS)

Implémenter :

```rust
fn atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(
            current,
            value,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}
```

**Le pattern CAS-loop** :
1. Lire la valeur actuelle.
2. Calculer la nouvelle valeur souhaitée.
3. `compare_exchange_weak(actuelle, nouvelle)`.
4. Si succès → fini.
5. Si échec → on récupère la valeur **réelle**, on relit, on retente.

C'est l'élément de base du lock-free. À mémoriser.

### Partie 3 — Test multi-thread

Lancez 16 threads qui chacun appellent `record()` 10 000 fois avec :
- `success = i % 10 != 0` (1 échec tous les 10)
- `latence_us = ((i * 37) % 900) + 50` (entre 50 et 949 µs)

Vérifiez :
- `total == 160_000`
- `success ≈ 144_000`
- `latence_min ≤ 50`, `latence_max ≥ 949`
- `latence_moy` ≈ 500 µs

### Partie 4 — Bench vs Mutex : deux régimes à comparer

Implémentez une version **Mutex-based** équivalente (`ServiceStatsMutex` avec un `Mutex<StatsInner>`), et **un compteur simple** pour les deux variantes :

```rust
pub struct CompteurAtom { n: AtomicU64 }       // 1 fetch_add par incrément
pub struct CompteurMux  { n: Mutex<u64> }      // 1 lock par incrément
```

Mesurez **deux scénarios** :

**Scénario A — Compteur simple** : un seul `n += 1` par appel. Variez 1, 4, 16, 64 threads.

**Scénario B — Stats complètes** (ServiceStats avec 6 champs vs ServiceStatsMutex). Variez de même.

> Vous allez observer un résultat **surprenant** : sur le scénario A, atomique gagne largement ; sur le scénario B, Mutex peut rattraper voire battre atomique. C'est l'objet de la question 4.

---

## Questions d'analyse

1. **Pourquoi `Relaxed` est correct pour `fetch_add` sur un compteur** ? Que se passerait-il si on utilisait `SeqCst` à la place ? Mesurez la différence de performance.

2. **Pourquoi `Release/Acquire` est nécessaire** pour la "publication" d'une donnée (pattern producer/consumer) mais pas pour un compteur ? Donnez un exemple où Relaxed seul causerait un bug observable.

3. **`compare_exchange` vs `compare_exchange_weak`** : quelle est la différence ? Lequel faut-il utiliser dans une boucle CAS, et pourquoi ?

4. **Atomique vs Mutex — pourquoi le résultat dépend du nombre de variables** :
   - Sur le **scénario A** (1 compteur), à 64 threads, combien atomique est-il plus rapide ? Expliquez avec le **coût d'un syscall futex** (Mutex) vs le coût d'un `lock xadd` (atomique).
   - Sur le **scénario B** (6 champs), à 64 threads, l'avantage atomique disparaît. Pourquoi ? Indices : (a) un Mutex protège **toutes** les variables en une seule cache line ; (b) 6 atomiques séparés produisent du **false sharing** entre cœurs ; (c) le protocole MESI doit invalider toutes les copies à chaque modification.
   - Conclusion : à quel moment doit-on **vraiment** préférer les atomiques ? Quand peut-on rester sur un Mutex sans regret ?

5. **Limites des atomiques** : ils sont rapides mais **moins expressifs**. Donnez 2-3 scénarios où vous garderiez un Mutex malgré le surcoût (indice : structures composites, invariants entre plusieurs champs).

