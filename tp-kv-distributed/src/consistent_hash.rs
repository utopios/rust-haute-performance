// =============================================================================
// consistent_hash.rs – Ring de hashing consistant avec noeuds virtuels
// =============================================================================
//
// Le `ConsistentHashRing` permet de router une clé vers un (ou plusieurs)
// noeud(s) du cluster de manière déterministe et avec une redistribution
// minimale lors de l'ajout/suppression d'un noeud.
//
// Algorithme : pour chaque noeud physique, on génère N "vnodes" qu'on place
// sur un anneau ordonné (BTreeMap<u64, NodeId>). Pour router une clé, on
// calcule son hash et on prend le premier vnode >= hash (wrap-around).
//
// Pourquoi 150 vnodes par défaut ? Compromis entre distribution équilibrée
// (plus de vnodes = meilleur équilibre) et mémoire/temps de lookup
// (BTreeMap O(log n) avec n = vnodes × nodes). 150 est la valeur utilisée
// par défaut par DynamoDB et Cassandra.
// =============================================================================

use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};

pub const DEFAULT_VIRTUAL_NODES: usize = 150;

pub struct ConsistentHashRing {
    /// hash(vnode_key) -> node_id physique
    ring: BTreeMap<u64, String>,
    /// Liste des noeuds physiques (pour itération)
    nodes: Vec<String>,
    /// Nombre de vnodes par noeud physique
    virtual_nodes: usize,
}

impl ConsistentHashRing {
    pub fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: Vec::new(),
            virtual_nodes,
        }
    }

    /// Ajoute un noeud physique. Crée `virtual_nodes` entrées dans l'anneau.
    pub fn add_node(&mut self, node_id: &str) {
        if self.nodes.iter().any(|n| n == node_id) {
            return;
        }
        self.nodes.push(node_id.to_string());
        for i in 0..self.virtual_nodes {
            let vnode_key = format!("{node_id}#vnode-{i}");
            let h = Self::hash(&vnode_key);
            self.ring.insert(h, node_id.to_string());
        }
    }

    /// Supprime un noeud physique. Retire tous ses vnodes.
    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.retain(|n| n != node_id);
        self.ring.retain(|_, v| v != node_id);
    }

    /// Retourne le noeud responsable d'une clé.
    pub fn get_node(&self, key: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }
        let h = Self::hash(key);
        // Premier vnode >= hash, sinon premier vnode du ring (wrap-around)
        self.ring
            .range(h..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, n)| n.as_str())
    }

    /// Retourne jusqu'à `count` noeuds **physiques distincts** responsables
    /// d'une clé (pour la réplication).
    pub fn get_nodes(&self, key: &str, count: usize) -> Vec<&str> {
        if self.ring.is_empty() || count == 0 {
            return Vec::new();
        }
        let h = Self::hash(key);

        let mut result = Vec::with_capacity(count);
        let mut seen = HashSet::new();

        // Parcours du ring à partir du hash, avec wrap autour de 0
        let iter = self.ring.range(h..).chain(self.ring.iter());
        for (_, node) in iter {
            if seen.insert(node.as_str()) {
                result.push(node.as_str());
                if result.len() >= count {
                    break;
                }
            }
            // Si on a vu tous les noeuds physiques, inutile de continuer
            if seen.len() == self.nodes.len() {
                break;
            }
        }
        result
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Hash u64 stable (DefaultHasher est suffisant pour la formation, mais
    /// en production on utiliserait xxhash ou ahash).
    fn hash<T: Hash + ?Sized>(item: &T) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        item.hash(&mut h);
        h.finish()
    }
}

// -----------------------------------------------------------------------------
// Helpers : statistiques de distribution (utiles pour les tests)
// -----------------------------------------------------------------------------

pub fn distribution_stats(
    ring: &ConsistentHashRing,
    num_keys: usize,
) -> std::collections::HashMap<String, usize> {
    let mut counts = std::collections::HashMap::new();
    for i in 0..num_keys {
        let k = format!("key-{i}");
        if let Some(n) = ring.get_node(&k) {
            *counts.entry(n.to_string()).or_insert(0) += 1;
        }
    }
    counts
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_node_stability() {
        let mut r = ConsistentHashRing::new(150);
        r.add_node("n1");
        r.add_node("n2");
        r.add_node("n3");

        let n1 = r.get_node("user:42").unwrap().to_string();
        let n2 = r.get_node("user:42").unwrap();
        assert_eq!(n1, n2);
    }

    #[test]
    fn test_get_nodes_distinct() {
        let mut r = ConsistentHashRing::new(150);
        r.add_node("a");
        r.add_node("b");
        r.add_node("c");

        let replicas = r.get_nodes("clef", 2);
        assert_eq!(replicas.len(), 2);
        assert_ne!(replicas[0], replicas[1]);
    }

    #[test]
    fn test_get_nodes_more_than_available() {
        let mut r = ConsistentHashRing::new(50);
        r.add_node("a");
        r.add_node("b");

        let replicas = r.get_nodes("x", 5);
        // Maximum 2 noeuds physiques disponibles
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn test_empty_ring() {
        let r = ConsistentHashRing::new(150);
        assert!(r.get_node("any").is_none());
        assert!(r.get_nodes("any", 3).is_empty());
    }

    #[test]
    fn test_distribution_balanced() {
        let mut r = ConsistentHashRing::new(150);
        r.add_node("n1");
        r.add_node("n2");
        r.add_node("n3");

        let dist = distribution_stats(&r, 10_000);
        for (node, count) in &dist {
            let pct = (*count as f64 / 10_000.0) * 100.0;
            assert!(
                (25.0..=42.0).contains(&pct),
                "noeud {node} reçoit {pct:.1}% des clés (attendu 25-42%)"
            );
        }
    }

    #[test]
    fn test_minimal_redistribution_on_add() {
        let mut r = ConsistentHashRing::new(150);
        r.add_node("a");
        r.add_node("b");
        r.add_node("c");

        let before: Vec<(String, String)> = (0..1000)
            .map(|i| {
                let k = format!("k-{i}");
                let n = r.get_node(&k).unwrap().to_string();
                (k, n)
            })
            .collect();

        r.add_node("d");

        let moved = before
            .iter()
            .filter(|(k, n)| r.get_node(k).unwrap() != n.as_str())
            .count();

        let pct = moved as f64 / 1000.0 * 100.0;
        // Avec consistent hashing, ~1/4 = 25% des clés bougent
        assert!(
            pct < 40.0,
            "trop de redistribution: {pct:.1}% (attendu <40%)"
        );
    }

    #[test]
    fn test_remove_node() {
        let mut r = ConsistentHashRing::new(50);
        r.add_node("a");
        r.add_node("b");
        r.add_node("c");
        assert_eq!(r.node_count(), 3);

        r.remove_node("b");
        assert_eq!(r.node_count(), 2);

        // Les clés sont maintenant routées vers a ou c uniquement
        for i in 0..100 {
            let k = format!("k-{i}");
            let n = r.get_node(&k).unwrap();
            assert!(n == "a" || n == "c");
        }
    }

    #[test]
    fn test_idempotent_add() {
        let mut r = ConsistentHashRing::new(10);
        r.add_node("a");
        r.add_node("a"); // ignoré
        assert_eq!(r.node_count(), 1);
    }
}
