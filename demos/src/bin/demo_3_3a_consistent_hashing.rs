// =============================================================================
// DÉMO 3.3-A — Consistent Hashing pour le sharding
// Module 3.3 : Patterns distribués
// =============================================================================
//
// PROBLÈME RÉSOLU
//   Un cluster KV-store de N noeuds doit router chaque clé vers UN noeud précis.
//   Approche naïve : node = hash(key) % N.
//   Catastrophe : quand N change (ajout/retrait d'un noeud), PRESQUE TOUTES les
//   clés changent de noeud → toutes les données migrent → le cache s'effondre.
//
//   Le consistent hashing garantit qu'en ajoutant/retirant un noeud, seules
//   ~1/N des clés migrent. Cette démo le PROUVE avec des chiffres mesurés.
//
// CONTEXTE OVH / production
//   C'est exactement le mécanisme de Cassandra, DynamoDB, Riak, memcached
//   (ketama), et des CDN pour répartir les clés sur un cluster élastique.
//
// LANCEMENT
//   cargo run --bin demo_3_3a_consistent_hashing
// =============================================================================

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

// =============================================================================
// SECTION 1 — L'approche naïve : modulo. Pour montrer pourquoi elle est mauvaise.
// =============================================================================

/// Route une clé avec `hash(key) % n`. Simple… et désastreux au resize.
fn naive_node(key: &str, n: usize) -> usize {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() % n as u64) as usize
}

// =============================================================================
// SECTION 2 — Le ring de consistent hashing avec virtual nodes
// =============================================================================

/// Anneau de hachage cohérent.
///
/// Chaque noeud physique est placé `virtual_nodes` fois à des positions
/// différentes sur un anneau `[0, u64::MAX]`. Router une clé = hasher la clé,
/// puis avancer dans le sens horaire jusqu'au premier point de noeud rencontré.
///
/// Pourquoi les virtual nodes ? Sans eux, 3 noeuds = 3 points sur l'anneau,
/// donc des arcs très inégaux → répartition déséquilibrée. Avec 150 réplicas
/// par noeud, la loi des grands nombres lisse la distribution.
struct ConsistentHash {
    ring: BTreeMap<u64, String>, // position sur l'anneau -> nom du noeud physique
    virtual_nodes: usize,
}

impl ConsistentHash {
    fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
        }
    }

    fn hash<T: Hash>(value: &T) -> u64 {
        let mut h = DefaultHasher::new();
        value.hash(&mut h);
        h.finish()
    }

    /// Ajoute un noeud physique : insère ses `virtual_nodes` points sur l'anneau.
    fn add_node(&mut self, node: &str) {
        for i in 0..self.virtual_nodes {
            let vkey = format!("{node}#{i}");
            self.ring.insert(Self::hash(&vkey), node.to_string());
        }
    }

    /// Retire un noeud physique : supprime tous ses points virtuels.
    fn remove_node(&mut self, node: &str) {
        self.ring.retain(|_, v| v != node);
    }

    /// Route une clé vers son noeud : premier point >= hash(key), sinon on
    /// "boucle" au début de l'anneau (premier point tout court).
    fn get_node(&self, key: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }
        let h = Self::hash(&key);
        self.ring
            .range(h..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, v)| v.as_str())
    }
}

// =============================================================================
// Outils de mesure
// =============================================================================

/// Génère un jeu de clés réaliste (style "user:1234", "session:abcd").
fn make_keys(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("user:{:06}:session", i.wrapping_mul(2_654_435_761) % 1_000_000))
        .collect()
}

/// Compte combien de clés sont routées vers chaque noeud (mesure d'équilibre).
fn distribution(ch: &ConsistentHash, keys: &[String]) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for k in keys {
        if let Some(node) = ch.get_node(k) {
            *counts.entry(node.to_string()).or_default() += 1;
        }
    }
    counts
}

fn main() {
    println!("=== DÉMO 3.3-A : Consistent Hashing ===\n");

    let keys = make_keys(100_000);
    println!("Jeu de test : {} clés\n", keys.len());

    // -------------------------------------------------------------------------
    // PARTIE 1 : Pourquoi le modulo est catastrophique au resize
    // -------------------------------------------------------------------------
    println!("--- PARTIE 1 : l'approche naïve `hash % N` ---");
    let before: Vec<usize> = keys.iter().map(|k| naive_node(k, 4)).collect();
    let after: Vec<usize> = keys.iter().map(|k| naive_node(k, 5)).collect(); // on ajoute 1 noeud
    let moved = before
        .iter()
        .zip(&after)
        .filter(|(b, a)| b != a)
        .count();
    let pct = 100.0 * moved as f64 / keys.len() as f64;
    println!("  Passage de 4 → 5 noeuds avec `hash % N` :");
    println!("  Clés qui changent de noeud : {moved} / {} = {pct:.1} %", keys.len());
    println!("  => Quasiment TOUT le cache est invalidé. Inacceptable.\n");

    // -------------------------------------------------------------------------
    // PARTIE 2 : Consistent hashing — équilibre de la distribution
    // -------------------------------------------------------------------------
    println!("--- PARTIE 2 : équilibre du consistent hashing ---");
    let mut ch = ConsistentHash::new(150);
    for node in ["node-A", "node-B", "node-C", "node-D"] {
        ch.add_node(node);
    }
    let dist = distribution(&ch, &keys);
    let ideal = keys.len() as f64 / 4.0;
    println!("  4 noeuds, 150 virtual nodes chacun. Idéal = {ideal:.0} clés/noeud :");
    let mut nodes: Vec<_> = dist.iter().collect();
    nodes.sort_by_key(|(n, _)| (*n).clone());
    for (node, count) in &nodes {
        let dev = 100.0 * (**count as f64 - ideal) / ideal;
        println!("    {node} : {count:>6} clés  ({dev:+.1} % vs idéal)");
    }
    println!();

    // -------------------------------------------------------------------------
    // PARTIE 3 : Consistent hashing — migration minimale au resize
    // -------------------------------------------------------------------------
    println!("--- PARTIE 3 : migration au resize (le coeur du sujet) ---");
    // Routage avant ajout
    let routing_before: HashMap<&String, String> = keys
        .iter()
        .map(|k| (k, ch.get_node(k).unwrap().to_string()))
        .collect();

    // On ajoute un 5e noeud
    ch.add_node("node-E");
    let moved_ch = keys
        .iter()
        .filter(|k| ch.get_node(k).unwrap() != routing_before[*k])
        .count();
    let pct_ch = 100.0 * moved_ch as f64 / keys.len() as f64;
    println!("  Ajout de node-E (4 → 5 noeuds) avec consistent hashing :");
    println!("  Clés qui migrent : {moved_ch} / {} = {pct_ch:.1} %", keys.len());
    println!("  Théorie : ~1/5 = 20 %. Et seules les clés du nouvel arc bougent.\n");

    // Retrait d'un noeud (panne simulée)
    let routing_5 = routing_before; // on réutilise pour comparer le retrait
    let mut ch2 = ConsistentHash::new(150);
    for node in ["node-A", "node-B", "node-C", "node-D"] {
        ch2.add_node(node);
    }
    let routing_4: HashMap<&String, String> = keys
        .iter()
        .map(|k| (k, ch2.get_node(k).unwrap().to_string()))
        .collect();
    ch2.remove_node("node-C"); // node-C tombe en panne
    let moved_rm = keys
        .iter()
        .filter(|k| ch2.get_node(k).unwrap() != routing_4[*k])
        .count();
    let pct_rm = 100.0 * moved_rm as f64 / keys.len() as f64;
    let _ = routing_5;
    println!("  Retrait de node-C (panne, 4 → 3 noeuds) :");
    println!("  Clés réaffectées : {moved_rm} / {} = {pct_rm:.1} %", keys.len());
    println!("  => SEULES les clés de node-C sont réparties sur les survivants.");
    println!("     Les clés de A, B, D ne bougent pas.\n");

    println!("=== Conclusion ===");
    println!("  Modulo        : ~80 % des clés migrent au resize  → cache détruit");
    println!("  Consistent H. : ~{pct_ch:.0} % migrent au resize        → cache préservé");
}
