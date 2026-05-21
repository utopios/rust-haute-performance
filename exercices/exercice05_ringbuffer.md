# Exercice 5 — Ring buffer thread-safe (Mutex + Condvar)

**Jour 2 — Module 2.1 : Primitives de synchronisation**
**Durée estimée :** 1h
**Prérequis :** Module 2.1 (Mutex, RwLock, Condvar) + l'esprit MPSC/MPMC
**Démo de référence :** [`demo05_mutex_rwlock`](../demos/src/bin/demo05_mutex_rwlock.rs)

---

## Contexte

Vous reprenez l'agrégateur de métriques OVH. Plusieurs threads "agents" produisent des `Evenement` (latence + timestamp + flag), et plusieurs threads "agrégateurs" les consomment. Aujourd'hui, on utilise une `VecDeque` protégée par un Mutex, mais il y a deux problèmes :

1. **Pas de borne supérieure** : si les producteurs sont plus rapides que les consommateurs, la file enfle indéfiniment et finit par tuer le process en mémoire.
2. **Pas de signalisation** : les consommateurs bouclent en `try_lock` + `sleep`, gaspillant CPU.

Votre mission : implémenter un **ring buffer FIFO MPMC** (Multi-Producer Multi-Consumer) avec capacité fixe, `push` bloquant si plein, `pop` bloquant si vide, et `close()` pour signaler la fin.

> Vous ne devez **pas** utiliser Crossbeam (vu après) ni les channels Rust (`std::sync::mpsc`). Tout doit reposer sur `Mutex` + `Condvar`.

---

## Objectifs pédagogiques

1. Comprendre le **pattern Mutex + Condvar** et sa boucle `while` (jamais `if`).
2. Savoir gérer **2 Condvar** sur un même Mutex (`not_full` et `not_empty`).
3. Implémenter un **close** propre qui réveille tous les threads en attente.
4. Mesurer le **coût de la contention** : 1 producteur / 1 consommateur vs 8/8.
5. Comparer `std::Mutex` et `parking_lot::Mutex` sur ce cas.

---

## Énoncé

### Partie 1 — L'API à implémenter

Dans le projet `demos`, créez `src/bin/exo05_ringbuffer.rs`.

```rust
pub struct RingBuffer<T> {
    // Hint : un Mutex<État> + deux Condvar.
    // L'État contient un VecDeque<T>, une capacité, et un flag closed.
}

pub enum BufferError {
    Closed,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self;

    /// Pousse `value`. Bloque si le buffer est plein. Erreur si closed.
    pub fn push(&self, value: T) -> Result<(), BufferError>;

    /// Pop la prochaine valeur. Bloque si le buffer est vide.
    /// Retourne Err(Closed) seulement si le buffer est vide ET fermé.
    pub fn pop(&self) -> Result<T, BufferError>;

    /// Variante non bloquante de push.
    pub fn try_push(&self, value: T) -> Result<(), BufferError>;

    /// Variante non bloquante de pop.
    /// Retourne Ok(None) si vide mais ouvert, Err(Closed) si vide et fermé.
    pub fn try_pop(&self) -> Result<Option<T>, BufferError>;

    /// Ferme le buffer. Réveille tous les threads bloqués sur push ou pop.
    /// Les éléments déjà présents peuvent encore être pop.
    pub fn close(&self);

    pub fn len(&self) -> usize;
    pub fn is_closed(&self) -> bool;
}
```

### Partie 2 — Le pattern Mutex + Condvar

Avant de coder, mémorisez le pattern :

```rust
// MAUVAIS — réveil spurieux possible
if condition_pas_satisfaite {
    condvar.wait(&mut guard);
}

// BON — toujours re-vérifier après wake-up
while condition_pas_satisfaite {
    guard = condvar.wait(guard).unwrap();
}
```

**Pourquoi `while` et pas `if` ?** Les condvars peuvent réveiller un thread **sans qu'aucun `notify` n'ait été émis** (spurious wakeup, autorisé par le standard POSIX). Et même sans spurious wakeup, un autre thread peut avoir consommé l'événement entre le `notify` et votre réveil.

### Partie 3 — Le piège du close

`close()` doit :
1. Mettre le flag `closed = true` sous le lock.
2. Faire `notify_all()` sur **les deux** condvars (`not_full` ET `not_empty`).

Pourquoi `notify_all` ? Parce que les threads bloqués sur push ET sur pop doivent tous se réveiller pour vérifier `closed`.

**`pop` doit avoir une logique fine** : si le buffer est vide ET fermé → renvoyer `Closed`. Si le buffer est vide mais **pas encore fermé** → attendre. Si le buffer a des éléments → les retourner, **même si fermé** (drain).

### Partie 4 — Test fonctionnel

Écrivez un test multi-thread :
1. 4 producteurs poussent chacun 1000 entiers.
2. 4 consommateurs poppent et somment ce qu'ils reçoivent.
3. Le main appelle `close()` une fois tous les producteurs joints.
4. Les consommateurs s'arrêtent proprement et leur somme totale = 4 × somme(0..1000).

### Partie 5 — Bench de contention

Sur un dataset de **100 000 messages au total**, mesurez le throughput :
- 1 producteur, 1 consommateur
- 4 producteurs, 4 consommateurs
- 8 producteurs, 8 consommateurs

Avec `std::sync::Mutex`, puis avec `parking_lot::Mutex`. Calculez les ratios.

---

## Questions d'analyse

1. **Pourquoi `while` au lieu de `if`** ? Donnez un scénario concret où `if` causerait un bug (mêmes thread, plusieurs producers).

2. **Pourquoi `notify_all` dans `close()` et pas `notify_one`** ? Que se passerait-il si on faisait `notify_one` ?

3. **Throughput multi-threadé** : votre throughput double-t-il quand vous passez de 4 producers à 8 ? Sinon pourquoi ? Quel est le **goulot d'étranglement** ?

4. **`std::Mutex` vs `parking_lot::Mutex`** : sur quel scénario voyez-vous le plus gros écart ? Pourquoi ?

5. **Bonus** : que se passe-t-il si un producer panic pendant qu'il tient le lock ? Discutez le concept de **mutex empoisonné** (`PoisonError`) et comment `parking_lot` gère ça différemment.


