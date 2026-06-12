// =============================================================================
// server.rs – Serveur TCP du KV store
// =============================================================================
//
// Le serveur accepte les connexions TCP, route chaque requête vers le bon
// composant :
//   - Get / Put / Delete / Scan publics → Replicator (qui orchestre le quorum)
//   - Replicate / Heartbeat (inter-noeuds) → StorageEngine directement
//
// Une tâche Tokio est spawnée par connexion. Le framing est binaire (cf.
// protocol.rs).
// =============================================================================

use crate::metrics::Metrics;
use crate::protocol::{read_message, write_message, Request, Response};
use crate::replication::Replicator;
use crate::storage::StorageEngine;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

pub struct KvServer {
    pub node_id: u32,
    pub storage: Arc<StorageEngine>,
    pub replicator: Arc<Replicator>,
    pub metrics: Arc<Metrics>,
    pub started_at: Instant,
}

impl KvServer {
    pub fn new(
        node_id: u32,
        storage: Arc<StorageEngine>,
        replicator: Arc<Replicator>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            node_id,
            storage,
            replicator,
            metrics,
            started_at: Instant::now(),
        }
    }

    /// Lance la boucle d'acceptation TCP. Boucle infinie tant qu'aucune
    /// erreur fatale ne survient.
    pub async fn run(self: Arc<Self>, addr: SocketAddr) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!(node_id = self.node_id, %addr, "serveur TCP en écoute");

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    self.metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                    let server = Arc::clone(&self);
                    let metrics = Arc::clone(&self.metrics);
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(stream, peer).await {
                            debug!(%peer, error = %e, "connexion terminée avec erreur");
                        }
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                Err(e) => {
                    warn!(error = %e, "accept failed, on continue");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Boucle de traitement d'une connexion : lit requête, traite, écrit
    /// réponse, recommence.
    async fn handle_connection(
        self: Arc<Self>,
        mut stream: TcpStream,
        peer: SocketAddr,
    ) -> io::Result<()> {
        stream.set_nodelay(true)?;
        debug!(%peer, "nouvelle connexion");

        loop {
            // Lecture d'une requête (EOF = fin propre)
            let req: Request = match read_message(&mut stream).await {
                Ok(r) => r,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            };

            let resp = self.handle_request(req).await;

            // Écriture de la réponse
            if let Err(e) = write_message(&mut stream, &resp).await {
                debug!(%peer, error = %e, "write_message échoué");
                return Err(e);
            }
        }
    }

    /// Dispatche une requête vers le bon composant.
    async fn handle_request(&self, req: Request) -> Response {
        self.metrics.ops_total.fetch_add(1, Ordering::Relaxed);

        match req {
            // -------- API publique --------
            Request::Get { key } => {
                self.metrics.get_total.fetch_add(1, Ordering::Relaxed);
                match self.replicator.get_replicated(&key).await {
                    Ok(Some(data)) => Response::Value {
                        data: Some(data),
                        version: 0,
                    },
                    Ok(None) => Response::Value {
                        data: None,
                        version: 0,
                    },
                    Err(e) => {
                        self.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                        Response::Error(e.to_string())
                    }
                }
            }

            Request::Put { key, value, ttl_ms } => {
                self.metrics.put_total.fetch_add(1, Ordering::Relaxed);
                let start = Instant::now();
                let result = self
                    .replicator
                    .put_replicated(key, value, ttl_ms)
                    .await;
                let lat_ms = start.elapsed().as_millis() as u64;
                self.metrics.update_replication_lag(lat_ms);
                match result {
                    Ok(version) => Response::Ack {
                        version,
                        node_id: self.node_id,
                    },
                    Err(e) => {
                        self.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                        Response::Error(e.to_string())
                    }
                }
            }

            Request::Delete { key } => {
                self.metrics.delete_total.fetch_add(1, Ordering::Relaxed);
                match self.replicator.delete_replicated(&key).await {
                    Ok(b) => Response::Deleted(b),
                    Err(e) => {
                        self.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                        Response::Error(e.to_string())
                    }
                }
            }

            Request::Scan { prefix, limit } => {
                let items: Vec<(String, Vec<u8>)> = self
                    .storage
                    .scan(&prefix, limit as usize)
                    .into_iter()
                    .map(|(k, v)| (k, v.data))
                    .collect();
                Response::ScanResult(items)
            }

            // -------- API inter-noeuds --------
            Request::Replicate {
                key,
                value,
                version,
                ttl_ms,
                source_node_id,
            } => {
                debug!(from = source_node_id, key = %key, version, "réplication entrante");
                let ttl = ttl_ms.map(Duration::from_millis);
                self.storage.put_with_version(key, value, version, ttl);
                Response::Ack {
                    version,
                    node_id: self.node_id,
                }
            }

            Request::Heartbeat { node_id, .. } => {
                debug!(from = node_id, "heartbeat reçu");
                Response::Pong {
                    node_id: self.node_id,
                    keys_count: self.storage.len() as u64,
                    uptime_s: self.started_at.elapsed().as_secs(),
                }
            }
        }
    }

    /// Renvoie l'uptime en secondes (utilisé par /health).
    pub fn uptime_s(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::KvClient;
    use crate::consistent_hash::ConsistentHashRing;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    async fn boot_single_node_server() -> (Arc<KvServer>, SocketAddr) {
        let storage = Arc::new(StorageEngine::new());
        let metrics = Arc::new(Metrics::new(1));
        let mut ring = ConsistentHashRing::new(50);
        ring.add_node("self");
        let replicator = Replicator::new(
            1,
            "self".into(),
            Arc::new(RwLock::new(ring)),
            Arc::new(RwLock::new(HashMap::new())),
            Arc::clone(&storage),
        );
        // En solo, on relâche le quorum
        let mut replicator = replicator;
        replicator.replication_factor = 1;
        replicator.write_quorum = 1;
        let replicator = Arc::new(replicator);

        let server = Arc::new(KvServer::new(1, storage, replicator, metrics));

        // Bind sur un port libre
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // pour libérer le port

        let s = Arc::clone(&server);
        tokio::spawn(async move {
            let _ = s.run(addr).await;
        });
        // Laisser le temps au serveur de bind
        tokio::time::sleep(Duration::from_millis(100)).await;

        (server, addr)
    }

    #[tokio::test]
    async fn test_server_put_get() {
        let (_server, addr) = boot_single_node_server().await;
        let client = KvClient::connect(addr).await.unwrap();

        let v = client.put("hello", b"world".to_vec()).await.unwrap();
        assert!(v >= 1);

        let got = client.get("hello").await.unwrap();
        assert_eq!(got, Some(b"world".to_vec()));
    }

    #[tokio::test]
    async fn test_server_delete() {
        let (_server, addr) = boot_single_node_server().await;
        let client = KvClient::connect(addr).await.unwrap();

        client.put("k", b"v".to_vec()).await.unwrap();
        let deleted = client.delete("k").await.unwrap();
        assert!(deleted);
        let got = client.get("k").await.unwrap();
        assert!(got.is_none());
    }
}
