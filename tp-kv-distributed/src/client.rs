// =============================================================================
// client.rs – Client TCP avec connexion réutilisable
// =============================================================================
//
// `KvClient` encapsule une connexion TCP vers un noeud KV. Il sérialise les
// requêtes et désérialise les réponses via `protocol::{Request, Response}`.
//
// Limitations volontaires (pour rester pédagogique) :
//   - Une connexion = un mutex (pas de pipelining)
//   - Pas de retry automatique au niveau client (le `Replicator` gère cela)
// =============================================================================

use crate::protocol::{read_message, write_message, Request, Response};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
pub struct KvClient {
    /// Connexion TCP partagée derrière un mutex (1 requête à la fois).
    /// Pour du pipelining, on ferait plusieurs `KvClient` en pool.
    inner: Arc<Mutex<TcpStream>>,
    pub addr: SocketAddr,
}

impl KvClient {
    /// Établit une connexion TCP vers un noeud KV.
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;
        stream.set_nodelay(true)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(stream)),
            addr,
        })
    }

    /// Envoie une requête et lit la réponse, avec timeout global.
    async fn call(&self, req: Request, dur: Duration) -> io::Result<Response> {
        let mut guard = self.inner.lock().await;
        timeout(dur, async {
            write_message(&mut *guard, &req).await?;
            read_message::<_, Response>(&mut *guard).await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "rpc timeout"))?
    }

    pub async fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        match self.call(
            Request::Get { key: key.into() },
            Duration::from_secs(2),
        )
        .await?
        {
            Response::Value { data, .. } => Ok(data),
            Response::Error(e) => Err(io::Error::other(e)),
            other => Err(io::Error::other(format!("réponse inattendue : {other:?}"))),
        }
    }

    pub async fn put(&self, key: &str, value: Vec<u8>) -> io::Result<u64> {
        self.put_with_ttl(key, value, None).await
    }

    pub async fn put_with_ttl(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> io::Result<u64> {
        match self
            .call(
                Request::Put {
                    key: key.into(),
                    value,
                    ttl_ms,
                },
                Duration::from_secs(2),
            )
            .await?
        {
            Response::Ack { version, .. } => Ok(version),
            Response::Error(e) => Err(io::Error::other(e)),
            other => Err(io::Error::other(format!("réponse inattendue : {other:?}"))),
        }
    }

    pub async fn delete(&self, key: &str) -> io::Result<bool> {
        match self
            .call(
                Request::Delete { key: key.into() },
                Duration::from_secs(2),
            )
            .await?
        {
            Response::Deleted(b) => Ok(b),
            Response::Error(e) => Err(io::Error::other(e)),
            other => Err(io::Error::other(format!("réponse inattendue : {other:?}"))),
        }
    }

    pub async fn scan(&self, prefix: &str, limit: u32) -> io::Result<Vec<(String, Vec<u8>)>> {
        match self
            .call(
                Request::Scan {
                    prefix: prefix.into(),
                    limit,
                },
                Duration::from_millis(2000),
            )
            .await?
        {
            Response::ScanResult(items) => Ok(items),
            Response::Error(e) => Err(io::Error::other(e)),
            other => Err(io::Error::other(format!("réponse inattendue : {other:?}"))),
        }
    }

    /// Envoi inter-noeuds d'une commande de réplication.
    pub async fn replicate(
        &self,
        key: String,
        value: Vec<u8>,
        version: u64,
        ttl_ms: Option<u64>,
        source_node_id: u32,
    ) -> io::Result<()> {
        let req = Request::Replicate {
            key,
            value,
            version,
            ttl_ms,
            source_node_id,
        };
        match self.call(req, Duration::from_millis(500)).await? {
            Response::Ack { .. } => Ok(()),
            Response::Error(e) => Err(io::Error::other(e)),
            other => Err(io::Error::other(format!("réponse inattendue : {other:?}"))),
        }
    }

    pub async fn heartbeat(&self, node_id: u32) -> io::Result<Response> {
        let req = Request::Heartbeat {
            node_id,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.call(req, Duration::from_millis(500)).await
    }
}
