// =============================================================================
// replication.rs – Réplication quorum W=2 sur consistent hash
// =============================================================================
//
// `Replicator` orchestre la distribution des opérations sur le cluster :
//   1. Calcul des N réplicas (via ConsistentHashRing) pour une clé
//   2. Pour PUT : écriture locale + envoi en parallèle aux peers + attente
//      de W acks
//   3. Pour GET : lecture sur le premier réplica vivant (avec failover)
//
// Stratégies clés :
//   - tokio::try_join! pour paralléliser les writes inter-noeuds
//   - Timeout par réplica pour ne pas bloquer le quorum sur un noeud lent
//   - Versioning : la version générée localement est propagée pour préserver
//     l'ordre causal côté réplicas
// =============================================================================

use crate::client::KvClient;
use crate::consistent_hash::ConsistentHashRing;
use crate::storage::StorageEngine;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, warn};

#[derive(thiserror::Error, Debug)]
pub enum ReplicationError {
    #[error("quorum non atteint : {acks}/{required} acks reçus")]
    QuorumNotMet { acks: usize, required: usize },
    #[error("aucun réplica disponible pour la clé")]
    NoReplicaAvailable,
    #[error("erreur réseau : {0}")]
    Network(String),
}

pub struct Replicator {
    pub node_id: u32,
    /// Identifiant de ce noeud dans le ring (utilisé pour savoir si le local
    /// fait partie des réplicas).
    pub local_node_name: String,
    /// Ring de consistent hashing partagé en lecture.
    pub ring: Arc<RwLock<ConsistentHashRing>>,
    /// Pool de clients vers les autres noeuds (clé = nom du noeud dans le ring).
    pub peers: Arc<RwLock<HashMap<String, KvClient>>>,
    /// Stockage local.
    pub storage: Arc<StorageEngine>,
    /// Nombre total de réplicas par clé (RF).
    pub replication_factor: usize,
    /// Nombre d'acks à attendre pour confirmer une écriture (W).
    pub write_quorum: usize,
    /// Timeout par opération réseau de réplication.
    pub op_timeout: Duration,
}

impl Replicator {
    pub fn new(
        node_id: u32,
        local_node_name: String,
        ring: Arc<RwLock<ConsistentHashRing>>,
        peers: Arc<RwLock<HashMap<String, KvClient>>>,
        storage: Arc<StorageEngine>,
    ) -> Self {
        Self {
            node_id,
            local_node_name,
            ring,
            peers,
            storage,
            replication_factor: 2,
            write_quorum: 2,
            op_timeout: Duration::from_millis(500),
        }
    }

    /// PUT répliqué avec quorum.
    pub async fn put_replicated(
        &self,
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<u64, ReplicationError> {
        // 1. Déterminer les réplicas
        let replicas = {
            let ring = self.ring.read().await;
            ring.get_nodes(&key, self.replication_factor)
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };

        if replicas.is_empty() {
            return Err(ReplicationError::NoReplicaAvailable);
        }

        let local_is_replica = replicas.contains(&self.local_node_name);

        // 2. Si on est réplica : écrire localement et générer la version.
        //    Sinon : forwarder à l'un des réplicas pour qu'il génère.
        let (version, mut acks) = if local_is_replica {
            let ttl = ttl_ms.map(Duration::from_millis);
            let v = self.storage.put(key.clone(), value.clone(), ttl);
            (v, 1usize)
        } else {
            // Forward vers le premier réplica
            let target = &replicas[0];
            let peers = self.peers.read().await;
            let client = peers
                .get(target)
                .ok_or(ReplicationError::NoReplicaAvailable)?
                .clone();
            drop(peers);

            let ver = client
                .put_with_ttl(&key, value.clone(), ttl_ms)
                .await
                .map_err(|e| ReplicationError::Network(e.to_string()))?;
            (ver, 1usize)
        };

        // 3. Envoyer aux autres réplicas en parallèle
        let mut tasks = Vec::new();
        for replica in &replicas {
            if replica == &self.local_node_name {
                continue;
            }
            // Si on a forwardé, le réplica primaire a déjà écrit ; on prévient
            // les autres réplicas via Replicate.
            let peers = self.peers.read().await;
            let client = match peers.get(replica) {
                Some(c) => c.clone(),
                None => {
                    warn!("réplica {replica} absent du pool de peers");
                    continue;
                }
            };
            drop(peers);

            let key_c = key.clone();
            let val_c = value.clone();
            let source = self.node_id;
            let to = self.op_timeout;
            tasks.push(tokio::spawn(async move {
                timeout(
                    to,
                    client.replicate(key_c, val_c, version, ttl_ms, source),
                )
                .await
            }));
        }

        for t in tasks {
            match t.await {
                Ok(Ok(Ok(()))) => {
                    acks += 1;
                }
                Ok(Ok(Err(e))) => {
                    debug!("replicate error: {e}");
                }
                Ok(Err(_)) => {
                    debug!("replicate timeout");
                }
                Err(e) => {
                    debug!("replicate join error: {e}");
                }
            }
        }

        if acks >= self.write_quorum {
            Ok(version)
        } else {
            Err(ReplicationError::QuorumNotMet {
                acks,
                required: self.write_quorum,
            })
        }
    }

    /// GET répliqué : tente chaque réplica dans l'ordre jusqu'à trouver la
    /// valeur ou épuiser la liste.
    pub async fn get_replicated(&self, key: &str) -> Result<Option<Vec<u8>>, ReplicationError> {
        let replicas = {
            let ring = self.ring.read().await;
            ring.get_nodes(key, self.replication_factor)
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };

        if replicas.is_empty() {
            return Err(ReplicationError::NoReplicaAvailable);
        }

        for replica in &replicas {
            if replica == &self.local_node_name {
                // Lecture locale rapide
                if let Some(v) = self.storage.get(key) {
                    return Ok(Some(v.data));
                }
            } else {
                let peers = self.peers.read().await;
                if let Some(client) = peers.get(replica).cloned() {
                    drop(peers);
                    match timeout(self.op_timeout, client.get(key)).await {
                        Ok(Ok(Some(v))) => return Ok(Some(v)),
                        Ok(Ok(None)) => {} // pas trouvé sur ce réplica, essayer le suivant
                        Ok(Err(e)) => {
                            debug!("get sur {replica} a échoué : {e}");
                        }
                        Err(_) => {
                            debug!("get sur {replica} timeout");
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// DELETE répliqué : meilleur effort (best-effort) sur tous les réplicas.
    /// Les appels distants sont parallélisés pour ne pas sommer les timeouts.
    pub async fn delete_replicated(&self, key: &str) -> Result<bool, ReplicationError> {
        let replicas = {
            let ring = self.ring.read().await;
            ring.get_nodes(key, self.replication_factor)
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        };

        let mut existed_anywhere = false;
        let mut tasks = Vec::new();

        for replica in &replicas {
            if replica == &self.local_node_name {
                if self.storage.delete(key) {
                    existed_anywhere = true;
                }
            } else {
                let peers = self.peers.read().await;
                if let Some(client) = peers.get(replica).cloned() {
                    drop(peers);
                    let key_c = key.to_string();
                    let to = self.op_timeout;
                    tasks.push(tokio::spawn(async move {
                        timeout(to, client.delete(&key_c)).await
                    }));
                }
            }
        }

        for t in tasks {
            if let Ok(Ok(Ok(true))) = t.await {
                existed_anywhere = true;
            }
        }
        Ok(existed_anywhere)
    }
}

// -----------------------------------------------------------------------------
// Tests unitaires (sans réseau, on simule un cluster mono-process)
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_local_only_replicator() -> Replicator {
        let mut ring = ConsistentHashRing::new(50);
        ring.add_node("self");
        let ring = Arc::new(RwLock::new(ring));
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let storage = Arc::new(StorageEngine::new());

        let mut r = Replicator::new(1, "self".into(), ring, peers, storage);
        // En solo, le quorum doit être réduit à 1 sinon impossible
        r.replication_factor = 1;
        r.write_quorum = 1;
        r
    }

    #[tokio::test]
    async fn test_put_get_local_only() {
        let r = make_local_only_replicator();
        let v = r
            .put_replicated("hello".into(), b"world".to_vec(), None)
            .await
            .unwrap();
        assert_eq!(v, 1);

        let got = r.get_replicated("hello").await.unwrap();
        assert_eq!(got, Some(b"world".to_vec()));
    }

    #[tokio::test]
    async fn test_quorum_impossible() {
        let r = make_local_only_replicator();
        // Forcer un quorum supérieur au nombre de noeuds physiques
        let mut r = r;
        r.write_quorum = 3;
        let result = r
            .put_replicated("k".into(), b"v".to_vec(), None)
            .await;
        assert!(matches!(result, Err(ReplicationError::QuorumNotMet { .. })));
    }

    #[tokio::test]
    async fn test_delete_local() {
        let r = make_local_only_replicator();
        r.put_replicated("k".into(), b"v".to_vec(), None).await.unwrap();
        let deleted = r.delete_replicated("k").await.unwrap();
        assert!(deleted);
        let got = r.get_replicated("k").await.unwrap();
        assert!(got.is_none());
    }
}
