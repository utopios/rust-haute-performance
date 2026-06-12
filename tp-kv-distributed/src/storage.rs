// =============================================================================
// storage.rs – Moteur de stockage local thread-safe (Sprint 1)
// =============================================================================
//
// Le `StorageEngine` est le coeur du KV store. Il utilise DashMap pour fournir
// une HashMap shardée lock-free en lecture et lock-fine en écriture.
//
// Caractéristiques :
//   - Versionnage monotone via AtomicU64
//   - TTL avec expiration lazy (au prochain get)
//   - Compteurs atomiques pour les métriques
//   - Utilisable derrière Arc<StorageEngine> sans Mutex extérieur
//
// Contexte OVH : ce moteur est ce qu'on retrouve dans le tier "shard local"
// d'un object store distribué. Il doit tenir le million d'ops/s par instance.
// =============================================================================

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// -----------------------------------------------------------------------------
// ValueEntry : entrée stockée dans le moteur
// -----------------------------------------------------------------------------

/// Une valeur stockée avec ses métadonnées de version et de TTL.
#[derive(Clone, Debug)]
pub struct ValueEntry {
    /// Contenu binaire opaque
    pub data: Vec<u8>,
    /// Version monotone globale (incrémente à chaque put)
    pub version: u64,
    /// Instant de création (sert au calcul d'expiration)
    pub created_at: Instant,
    /// Durée de vie. `None` = pas d'expiration.
    pub ttl: Option<Duration>,
}

impl ValueEntry {
    /// Indique si l'entrée est expirée à l'instant courant.
    pub fn is_expired(&self) -> bool {
        match self.ttl {
            Some(ttl) => self.created_at.elapsed() > ttl,
            None => false,
        }
    }
}

// -----------------------------------------------------------------------------
// StorageStats : compteurs atomiques exposés pour les métriques
// -----------------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct StorageStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub expired: AtomicU64,
}

impl StorageStats {
    /// Snapshot lisible (utile pour le endpoint /metrics).
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            gets: self.gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub gets: u64,
    pub puts: u64,
    pub deletes: u64,
    pub hits: u64,
    pub misses: u64,
    pub expired: u64,
}

// -----------------------------------------------------------------------------
// StorageEngine : moteur principal
// -----------------------------------------------------------------------------

pub struct StorageEngine {
    /// Stockage principal, shardé en interne par DashMap.
    store: DashMap<String, ValueEntry>,
    /// Compteur monotone pour générer des versions uniques.
    version_counter: AtomicU64,
    /// Métriques exposables.
    pub stats: StorageStats,
}

impl Default for StorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine {
    /// Crée un moteur vide avec capacité par défaut.
    pub fn new() -> Self {
        Self {
            store: DashMap::new(),
            version_counter: AtomicU64::new(0),
            stats: StorageStats::default(),
        }
    }

    /// Crée un moteur avec une capacité initiale.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            store: DashMap::with_capacity(cap),
            version_counter: AtomicU64::new(0),
            stats: StorageStats::default(),
        }
    }

    /// Lit une clé. Retourne `None` si absente OU expirée.
    /// L'expiration lazy supprime l'entrée si elle est trouvée expirée.
    pub fn get(&self, key: &str) -> Option<ValueEntry> {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);

        // On clone l'entrée pour libérer le verrou DashMap immédiatement.
        let entry = self.store.get(key).map(|e| e.clone());

        match entry {
            Some(e) if e.is_expired() => {
                // Expiration lazy : on supprime l'entrée périmée
                self.store.remove(key);
                self.stats.expired.fetch_add(1, Ordering::Relaxed);
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Some(e) => {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                Some(e)
            }
            None => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Écrit une clé. Retourne la version assignée.
    pub fn put(&self, key: String, data: Vec<u8>, ttl: Option<Duration>) -> u64 {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        // SeqCst pour garantir la monotonie globale entre threads.
        let version = self.version_counter.fetch_add(1, Ordering::SeqCst) + 1;

        let entry = ValueEntry {
            data,
            version,
            created_at: Instant::now(),
            ttl,
        };

        self.store.insert(key, entry);
        version
    }

    /// Écrit une clé en respectant la version source (utilisé par la réplication
    /// pour préserver l'ordre causal des écritures).
    /// Retourne `true` si l'écriture a été appliquée, `false` si la version
    /// locale était déjà supérieure ou égale.
    pub fn put_with_version(
        &self,
        key: String,
        data: Vec<u8>,
        version: u64,
        ttl: Option<Duration>,
    ) -> bool {
        // Mise à jour du compteur global si la version reçue est plus haute,
        // pour qu'un futur put local génère bien une version > toutes celles vues.
        let mut cur = self.version_counter.load(Ordering::SeqCst);
        while version > cur {
            match self.version_counter.compare_exchange(
                cur,
                version,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }

        // Vérifier la version locale existante
        if let Some(existing) = self.store.get(&key) {
            if existing.version >= version {
                return false;
            }
        }

        let entry = ValueEntry {
            data,
            version,
            created_at: Instant::now(),
            ttl,
        };
        self.store.insert(key, entry);
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Supprime une clé. Retourne `true` si la clé existait.
    pub fn delete(&self, key: &str) -> bool {
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        self.store.remove(key).is_some()
    }

    /// Parcourt les clés ayant un préfixe donné (limite N résultats).
    /// Ignore les entrées expirées (sans les supprimer pour ne pas bloquer
    /// l'itération).
    pub fn scan(&self, prefix: &str, limit: usize) -> Vec<(String, ValueEntry)> {
        let mut result = Vec::with_capacity(limit.min(64));

        for entry_ref in self.store.iter() {
            if result.len() >= limit {
                break;
            }
            if entry_ref.key().starts_with(prefix) {
                let entry = entry_ref.value();
                if !entry.is_expired() {
                    result.push((entry_ref.key().clone(), entry.clone()));
                }
            }
        }

        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }

    /// Nombre d'entrées actuellement stockées (peut inclure des expirées non
    /// encore purgées).
    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Purge active des entrées expirées. À appeler périodiquement.
    /// Retourne le nombre d'entrées supprimées.
    pub fn purge_expired(&self) -> usize {
        let expired_keys: Vec<String> = self
            .store
            .iter()
            .filter(|e| e.value().is_expired())
            .map(|e| e.key().clone())
            .collect();

        let count = expired_keys.len();
        for k in expired_keys {
            self.store.remove(&k);
        }
        self.stats.expired.fetch_add(count as u64, Ordering::Relaxed);
        count
    }
}

// -----------------------------------------------------------------------------
// Tests unitaires
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_put_get_roundtrip() {
        let s = StorageEngine::new();
        let v = s.put("hello".into(), b"world".to_vec(), None);
        assert_eq!(v, 1);

        let got = s.get("hello").expect("clé doit exister");
        assert_eq!(got.data, b"world");
        assert_eq!(got.version, 1);
    }

    #[test]
    fn test_get_missing() {
        let s = StorageEngine::new();
        assert!(s.get("absent").is_none());
        let snap = s.stats.snapshot();
        assert_eq!(snap.misses, 1);
        assert_eq!(snap.hits, 0);
    }

    #[test]
    fn test_delete_existing() {
        let s = StorageEngine::new();
        s.put("k".into(), b"v".to_vec(), None);
        assert!(s.delete("k"));
        assert!(s.get("k").is_none());
    }

    #[test]
    fn test_delete_missing() {
        let s = StorageEngine::new();
        assert!(!s.delete("nope"));
    }

    #[test]
    fn test_version_monotonic() {
        let s = StorageEngine::new();
        let v1 = s.put("a".into(), b"1".to_vec(), None);
        let v2 = s.put("b".into(), b"2".to_vec(), None);
        let v3 = s.put("a".into(), b"3".to_vec(), None);
        assert!(v2 > v1);
        assert!(v3 > v2);
    }

    #[test]
    fn test_ttl_expiration() {
        let s = StorageEngine::new();
        s.put("temp".into(), b"x".to_vec(), Some(Duration::from_millis(10)));
        assert!(s.get("temp").is_some());
        thread::sleep(Duration::from_millis(20));
        assert!(s.get("temp").is_none(), "clé doit être expirée");
        // L'entrée doit aussi être purgée du store
        assert!(s.get("temp").is_none());
    }

    #[test]
    fn test_scan_prefix() {
        let s = StorageEngine::new();
        s.put("user:1".into(), b"a".to_vec(), None);
        s.put("user:2".into(), b"b".to_vec(), None);
        s.put("post:1".into(), b"c".to_vec(), None);

        let users = s.scan("user:", 10);
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].0, "user:1");
        assert_eq!(users[1].0, "user:2");
    }

    #[test]
    fn test_scan_limit() {
        let s = StorageEngine::new();
        for i in 0..10 {
            s.put(format!("k{i}"), vec![i as u8], None);
        }
        let r = s.scan("k", 3);
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn test_put_with_version_idempotent() {
        let s = StorageEngine::new();
        // Version 5 acceptée
        assert!(s.put_with_version("k".into(), b"a".to_vec(), 5, None));
        // Version 3 (inférieure) rejetée
        assert!(!s.put_with_version("k".into(), b"b".to_vec(), 3, None));
        // Version 7 acceptée
        assert!(s.put_with_version("k".into(), b"c".to_vec(), 7, None));

        let got = s.get("k").unwrap();
        assert_eq!(got.data, b"c");
        assert_eq!(got.version, 7);
    }

    #[test]
    fn test_concurrent_writes() {
        let s = Arc::new(StorageEngine::new());
        let mut handles = Vec::new();

        for t in 0..10 {
            let s = Arc::clone(&s);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    s.put(format!("t{t}-k{i}"), vec![t as u8], None);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(s.len(), 10 * 1000);
        let snap = s.stats.snapshot();
        assert_eq!(snap.puts, 10_000);
    }

    #[test]
    fn test_purge_expired() {
        let s = StorageEngine::new();
        s.put("a".into(), b"x".to_vec(), Some(Duration::from_millis(5)));
        s.put("b".into(), b"y".to_vec(), None);
        thread::sleep(Duration::from_millis(15));

        let purged = s.purge_expired();
        assert_eq!(purged, 1);
        assert!(s.get("a").is_none());
        assert!(s.get("b").is_some());
    }
}
