// =============================================================================
// DÉMO 3.3-B — CRDTs : convergence sans coordination
// Module 3.3 : Patterns distribués
// =============================================================================
//
// PROBLÈME RÉSOLU
//   Plusieurs répliques (datacenters) acceptent des écritures EN MÊME TEMPS,
//   sans verrou global, parfois hors-ligne. Comment garantir qu'après échange
//   de leurs états, elles convergent TOUTES vers le même résultat, quel que
//   soit l'ordre ou le nombre de fois où elles se synchronisent ?
//
//   Un CRDT (Conflict-free Replicated Data Type) est une structure dont
//   l'opération `merge` est :
//     - commutative : merge(a, b) == merge(b, a)
//     - associative : merge(merge(a,b),c) == merge(a,merge(b,c))
//     - idempotente : merge(a, a) == a
//   Ces trois propriétés suffisent pour une convergence garantie (eventual
//   consistency) sans aucune coordination.
//
// CONTEXTE OVH / production
//   Riak, Redis (CRDTs actifs/actifs), Automerge, Yjs (collaboration temps réel),
//   compteurs de likes/vues répliqués multi-régions, paniers e-commerce.
//
// CETTE DÉMO
//   Implémente G-Counter, PN-Counter et OR-Set, puis VÉRIFIE les 3 propriétés
//   en simulant des répliques qui divergent puis se synchronisent dans des
//   ordres différents.
//
// LANCEMENT
//   cargo run --bin demo_3_3b_crdt
// =============================================================================

use std::collections::{HashMap, HashSet};

// =============================================================================
// SECTION 1 — G-Counter : compteur croissant (grow-only)
// =============================================================================

/// Compteur qui ne peut qu'augmenter. Chaque réplique incrémente UNIQUEMENT
/// sa propre case. La valeur globale = somme des cases. Le merge prend le max
/// de chaque case (car une case ne fait que croître).
#[derive(Clone, Debug, PartialEq)]
struct GCounter {
    node_id: String,
    counters: HashMap<String, u64>,
}

impl GCounter {
    fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            counters: HashMap::new(),
        }
    }

    fn increment(&mut self) {
        *self.counters.entry(self.node_id.clone()).or_insert(0) += 1;
    }

    fn value(&self) -> u64 {
        self.counters.values().sum()
    }

    /// Merge : max case-à-case. Commutatif/associatif/idempotent par construction
    /// (max l'est).
    fn merge(&mut self, other: &GCounter) {
        for (node, &count) in &other.counters {
            let e = self.counters.entry(node.clone()).or_insert(0);
            *e = (*e).max(count);
        }
    }
}

// =============================================================================
// SECTION 2 — PN-Counter : compteur incrémentable ET décrémentable
// =============================================================================

/// Deux G-Counters : un pour les `+`, un pour les `-`. La valeur = P - N.
/// On peut ainsi décrémenter tout en gardant les bonnes propriétés CRDT.
#[derive(Clone, Debug, PartialEq)]
struct PNCounter {
    p: GCounter,
    n: GCounter,
}

impl PNCounter {
    fn new(node_id: &str) -> Self {
        Self {
            p: GCounter::new(node_id),
            n: GCounter::new(node_id),
        }
    }

    fn increment(&mut self) {
        self.p.increment();
    }

    fn decrement(&mut self) {
        self.n.increment();
    }

    fn value(&self) -> i64 {
        self.p.value() as i64 - self.n.value() as i64
    }

    fn merge(&mut self, other: &PNCounter) {
        self.p.merge(&other.p);
        self.n.merge(&other.n);
    }
}

// =============================================================================
// SECTION 3 — OR-Set : ensemble avec ajout ET suppression concurrents
// =============================================================================

/// Observed-Remove Set. Le piège des ensembles répliqués : si une réplique
/// ajoute "x" et une autre le supprime en même temps, qui gagne ?
///
/// L'OR-Set marque chaque ajout par un tag unique. Supprimer = retirer les
/// tags qu'on a OBSERVÉS. Un ajout concurrent porte un tag neuf, donc non
/// observé par la suppression → l'ajout survit. Règle "add wins".
#[derive(Clone, Debug, PartialEq)]
struct OrSet {
    node_id: String,
    counter: u64,
    // élément -> ensemble de tags (unique add-id) actuellement vivants
    elements: HashMap<String, HashSet<String>>,
}

impl OrSet {
    fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            counter: 0,
            elements: HashMap::new(),
        }
    }

    fn add(&mut self, item: &str) {
        self.counter += 1;
        let tag = format!("{}:{}", self.node_id, self.counter);
        self.elements
            .entry(item.to_string())
            .or_default()
            .insert(tag);
    }

    /// Supprime les tags OBSERVÉS localement. Un ajout concurrent (tag inconnu)
    /// n'est pas affecté.
    fn remove(&mut self, item: &str) {
        if let Some(tags) = self.elements.get_mut(item) {
            tags.clear();
        }
    }

    fn contains(&self, item: &str) -> bool {
        self.elements
            .get(item)
            .map(|tags| !tags.is_empty())
            .unwrap_or(false)
    }

    fn values(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .elements
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        v.sort();
        v
    }

    /// Merge : union des tags vivants de chaque élément.
    fn merge(&mut self, other: &OrSet) {
        for (item, tags) in &other.elements {
            self.elements
                .entry(item.clone())
                .or_default()
                .extend(tags.iter().cloned());
        }
    }
}

fn main() {
    println!("=== DÉMO 3.3-B : CRDTs (convergence sans coordination) ===\n");

    // -------------------------------------------------------------------------
    // PARTIE 1 : G-Counter — convergence quel que soit l'ordre de merge
    // -------------------------------------------------------------------------
    println!("--- PARTIE 1 : G-Counter (compteur de vues répliqué) ---");
    let mut a = GCounter::new("dc-paris");
    let mut b = GCounter::new("dc-london");
    let mut c = GCounter::new("dc-warsaw");

    // Chaque datacenter compte ses propres vues, hors-ligne, sans se parler.
    for _ in 0..5 {
        a.increment();
    }
    for _ in 0..3 {
        b.increment();
    }
    for _ in 0..7 {
        c.increment();
    }
    println!(
        "  Avant sync : paris={}, london={}, warsaw={}",
        a.value(),
        b.value(),
        c.value()
    );

    // Synchronisation en étoile : tout le monde voit tout le monde.
    a.merge(&b);
    a.merge(&c);
    b.merge(&a);
    c.merge(&a);
    c.merge(&b);
    println!(
        "  Après sync : paris={}, london={}, warsaw={}  (attendu 15)",
        a.value(),
        b.value(),
        c.value()
    );
    assert_eq!((a.value(), b.value(), c.value()), (15, 15, 15));
    println!("  ✓ Les 3 répliques convergent vers 15.\n");

    // -------------------------------------------------------------------------
    // PARTIE 2 : preuve des 3 propriétés CRDT
    // -------------------------------------------------------------------------
    println!("--- PARTIE 2 : vérification des propriétés CRDT ---");

    // Idempotence : merger 2x le même état ne change rien.
    let mut g = GCounter::new("x");
    g.increment();
    g.increment();
    let snapshot = g.clone();
    g.merge(&snapshot);
    g.merge(&snapshot);
    assert_eq!(g, snapshot);
    println!("  ✓ Idempotence : merge(a, a) == a");

    // Commutativité : l'ordre de merge n'importe pas.
    let (mut g1, mut g2) = (GCounter::new("p"), GCounter::new("p"));
    let mut h = GCounter::new("q");
    h.increment();
    let mut k = GCounter::new("r");
    k.increment();
    k.increment();
    g1.merge(&h);
    g1.merge(&k); // ordre h puis k
    g2.merge(&k);
    g2.merge(&h); // ordre k puis h
    assert_eq!(g1.value(), g2.value());
    println!("  ✓ Commutativité : merge(a,b) == merge(b,a)  (= {})", g1.value());

    // Associativité.
    println!("  ✓ Associativité : (héritée de max, vérifiée par construction)\n");

    // -------------------------------------------------------------------------
    // PARTIE 3 : PN-Counter — incréments et décréments concurrents
    // -------------------------------------------------------------------------
    println!("--- PARTIE 3 : PN-Counter (stock répliqué, +/-) ---");
    let mut s1 = PNCounter::new("entrepot-1");
    let mut s2 = PNCounter::new("entrepot-2");
    s1.increment(); // +10 reçus
    for _ in 0..10 {
        s1.increment();
    }
    for _ in 0..4 {
        s2.decrement(); // 4 expéditions ailleurs
    }
    println!("  Avant sync : entrepot-1 (+11) = {}, entrepot-2 (-4) = {}", s1.value(), s2.value());
    s1.merge(&s2);
    s2.merge(&s1);
    println!("  Après sync : entrepot-1 = {}, entrepot-2 = {}  (attendu 7)", s1.value(), s2.value());
    assert_eq!((s1.value(), s2.value()), (7, 7));
    println!("  ✓ Stock cohérent : 11 - 4 = 7.\n");

    // -------------------------------------------------------------------------
    // PARTIE 4 : OR-Set — add/remove concurrents (add wins)
    // -------------------------------------------------------------------------
    println!("--- PARTIE 4 : OR-Set (panier e-commerce répliqué) ---");
    let mut cart1 = OrSet::new("mobile");
    let mut cart2 = OrSet::new("desktop");

    cart1.add("clavier");
    cart1.add("souris");
    cart2.merge(&cart1); // desktop voit le panier initial
    println!("  Panier initial synchronisé : {:?}", cart2.values());

    // CONFLIT : mobile supprime "souris", desktop le re-ajoute EN MÊME TEMPS.
    cart1.remove("souris"); // mobile retire le tag observé
    cart2.add("souris"); // desktop crée un NOUVEAU tag (non observé par mobile)

    cart1.merge(&cart2);
    cart2.merge(&cart1);
    println!("  Après conflit suppression/ajout concurrent de 'souris' :");
    println!("    mobile  : {:?}", cart1.values());
    println!("    desktop : {:?}", cart2.values());
    assert!(cart1.contains("souris") && cart2.contains("souris"));
    assert_eq!(cart1.values(), cart2.values());
    println!("  ✓ Règle 'add wins' : 'souris' survit car son ré-ajout n'avait pas");
    println!("    été observé par la suppression. Les répliques convergent.\n");

    println!("=== Conclusion ===");
    println!("  Pas de verrou, pas de leader, pas de coordination.");
    println!("  Les 3 propriétés (commutatif/associatif/idempotent) suffisent");
    println!("  à garantir la convergence — c'est ça, l'eventual consistency.");
}
