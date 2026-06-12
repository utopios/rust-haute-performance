// =============================================================================
// metrics.rs – Compteurs atomiques + endpoint HTTP Prometheus-like
// =============================================================================
//
// On expose deux choses :
//   - Un struct `Metrics` partagé (Arc<Metrics>) avec compteurs atomiques
//   - Un mini serveur HTTP qui répond sur /metrics (texte Prometheus) et
//     /health (JSON minimal)
//
// On utilise hyper en mode bas-niveau pour rester léger et démontrer la
// composition avec Tokio.
// =============================================================================

use crate::storage::StorageEngine;
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request as HttpRequest, Response as HttpResponse, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{info, warn};

// -----------------------------------------------------------------------------
// Compteurs
// -----------------------------------------------------------------------------

pub struct Metrics {
    pub node_id: u32,
    pub started_at: Instant,
    pub ops_total: AtomicU64,
    pub get_total: AtomicU64,
    pub put_total: AtomicU64,
    pub delete_total: AtomicU64,
    pub errors_total: AtomicU64,
    /// Latence moyenne mobile (EWMA) de la réplication en ms × 1000 (microsecondes)
    pub replication_lag_us: AtomicU64,
    pub active_connections: AtomicU64,
    pub peers_alive: AtomicU64,
    pub peers_total: AtomicU64,
}

impl Metrics {
    pub fn new(node_id: u32) -> Self {
        Self {
            node_id,
            started_at: Instant::now(),
            ops_total: AtomicU64::new(0),
            get_total: AtomicU64::new(0),
            put_total: AtomicU64::new(0),
            delete_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            replication_lag_us: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            peers_alive: AtomicU64::new(0),
            peers_total: AtomicU64::new(0),
        }
    }

    /// Met à jour la latence de réplication (EWMA, alpha = 1/8).
    pub fn update_replication_lag(&self, lat_ms: u64) {
        let lat_us = lat_ms.saturating_mul(1000);
        let prev = self.replication_lag_us.load(Ordering::Relaxed);
        // EWMA simple : new = (7*prev + lat) / 8
        let next = if prev == 0 {
            lat_us
        } else {
            (prev.saturating_mul(7).saturating_add(lat_us)) / 8
        };
        self.replication_lag_us.store(next, Ordering::Relaxed);
    }

    /// Produit la sortie texte Prometheus.
    pub fn format_prometheus(&self, storage: &StorageEngine) -> String {
        let uptime_s = self.started_at.elapsed().as_secs();
        let lag_ms = self.replication_lag_us.load(Ordering::Relaxed) as f64 / 1000.0;
        let snap = storage.stats.snapshot();
        let nid = self.node_id;

        format!(
            "# HELP kv_ops_total Total number of operations\n\
             # TYPE kv_ops_total counter\n\
             kv_ops_total{{node=\"{nid}\"}} {ops}\n\
             # HELP kv_get_total Total GET operations\n\
             # TYPE kv_get_total counter\n\
             kv_get_total{{node=\"{nid}\"}} {gets}\n\
             # HELP kv_put_total Total PUT operations\n\
             # TYPE kv_put_total counter\n\
             kv_put_total{{node=\"{nid}\"}} {puts}\n\
             # HELP kv_delete_total Total DELETE operations\n\
             # TYPE kv_delete_total counter\n\
             kv_delete_total{{node=\"{nid}\"}} {dels}\n\
             # HELP kv_errors_total Total error responses\n\
             # TYPE kv_errors_total counter\n\
             kv_errors_total{{node=\"{nid}\"}} {errs}\n\
             # HELP kv_storage_hits Storage layer hits\n\
             # TYPE kv_storage_hits counter\n\
             kv_storage_hits{{node=\"{nid}\"}} {hits}\n\
             # HELP kv_storage_misses Storage layer misses\n\
             # TYPE kv_storage_misses counter\n\
             kv_storage_misses{{node=\"{nid}\"}} {miss}\n\
             # HELP kv_storage_expired Lazily expired entries\n\
             # TYPE kv_storage_expired counter\n\
             kv_storage_expired{{node=\"{nid}\"}} {exp}\n\
             # HELP kv_keys_count Current number of keys stored\n\
             # TYPE kv_keys_count gauge\n\
             kv_keys_count{{node=\"{nid}\"}} {keys}\n\
             # HELP kv_active_connections Open TCP connections\n\
             # TYPE kv_active_connections gauge\n\
             kv_active_connections{{node=\"{nid}\"}} {conn}\n\
             # HELP kv_replication_lag_ms EWMA of PUT replication latency\n\
             # TYPE kv_replication_lag_ms gauge\n\
             kv_replication_lag_ms{{node=\"{nid}\"}} {lag:.3}\n\
             # HELP kv_peers_alive Peers currently reachable\n\
             # TYPE kv_peers_alive gauge\n\
             kv_peers_alive{{node=\"{nid}\"}} {alive}\n\
             # HELP kv_peers_total Total peers configured\n\
             # TYPE kv_peers_total gauge\n\
             kv_peers_total{{node=\"{nid}\"}} {peers}\n\
             # HELP kv_uptime_seconds Uptime in seconds\n\
             # TYPE kv_uptime_seconds gauge\n\
             kv_uptime_seconds{{node=\"{nid}\"}} {uptime}\n",
            ops = self.ops_total.load(Ordering::Relaxed),
            gets = self.get_total.load(Ordering::Relaxed),
            puts = self.put_total.load(Ordering::Relaxed),
            dels = self.delete_total.load(Ordering::Relaxed),
            errs = self.errors_total.load(Ordering::Relaxed),
            hits = snap.hits,
            miss = snap.misses,
            exp = snap.expired,
            keys = storage.len(),
            conn = self.active_connections.load(Ordering::Relaxed),
            lag = lag_ms,
            alive = self.peers_alive.load(Ordering::Relaxed),
            peers = self.peers_total.load(Ordering::Relaxed),
            uptime = uptime_s,
        )
    }
}

// -----------------------------------------------------------------------------
// Endpoint HTTP (hyper minimal)
// -----------------------------------------------------------------------------

pub struct MetricsServer {
    pub metrics: Arc<Metrics>,
    pub storage: Arc<StorageEngine>,
    /// Quorum minimum sain (utilisé par /health pour décider OK ou degraded).
    pub min_peers_alive: u64,
}

impl MetricsServer {
    pub fn new(metrics: Arc<Metrics>, storage: Arc<StorageEngine>) -> Self {
        Self {
            metrics,
            storage,
            min_peers_alive: 1,
        }
    }

    /// Lance le serveur HTTP.
    pub async fn run(self: Arc<Self>, addr: SocketAddr) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, "endpoint /metrics et /health en écoute");

        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let me = Arc::clone(&self);

            tokio::spawn(async move {
                let service = service_fn(move |req: HttpRequest<Incoming>| {
                    let me = Arc::clone(&me);
                    async move { Ok::<_, Infallible>(me.dispatch(req).await) }
                });

                if let Err(e) = http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    warn!(error = %e, "erreur HTTP");
                }
            });
        }
    }

    async fn dispatch(&self, req: HttpRequest<Incoming>) -> HttpResponse<Full<Bytes>> {
        match req.uri().path() {
            "/metrics" => self.handle_metrics(),
            "/health" => self.handle_health(),
            "/" => HttpResponse::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain; charset=utf-8")
                .body(Full::new(Bytes::from(
                    "kv-distributed – endpoints: /metrics /health",
                )))
                .unwrap(),
            _ => HttpResponse::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("not found")))
                .unwrap(),
        }
    }

    fn handle_metrics(&self) -> HttpResponse<Full<Bytes>> {
        let body = self.metrics.format_prometheus(&self.storage);
        HttpResponse::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; version=0.0.4")
            .body(Full::new(Bytes::from(body)))
            .unwrap()
    }

    fn handle_health(&self) -> HttpResponse<Full<Bytes>> {
        let uptime_s = self.metrics.started_at.elapsed().as_secs();
        let alive = self.metrics.peers_alive.load(Ordering::Relaxed);
        let total = self.metrics.peers_total.load(Ordering::Relaxed);
        let healthy = alive >= self.min_peers_alive || total == 0;
        let status = if healthy { "ok" } else { "degraded" };
        let code = if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };

        let body = format!(
            "{{\"status\":\"{status}\",\"node_id\":{nid},\"uptime_s\":{uptime},\"peers_alive\":{alive},\"peers_total\":{total}}}",
            nid = self.metrics.node_id,
            uptime = uptime_s,
        );

        HttpResponse::builder()
            .status(code)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_prometheus_contains_basics() {
        let m = Metrics::new(7);
        m.ops_total.store(42, Ordering::Relaxed);
        m.get_total.store(30, Ordering::Relaxed);
        let storage = StorageEngine::new();
        storage.put("k".into(), b"v".to_vec(), None);
        let out = m.format_prometheus(&storage);

        assert!(out.contains("kv_ops_total{node=\"7\"} 42"));
        assert!(out.contains("kv_get_total{node=\"7\"} 30"));
        assert!(out.contains("kv_keys_count{node=\"7\"} 1"));
    }

    #[test]
    fn test_ewma_replication_lag() {
        let m = Metrics::new(1);
        m.update_replication_lag(10);
        m.update_replication_lag(20);
        m.update_replication_lag(30);
        let lag_us = m.replication_lag_us.load(Ordering::Relaxed);
        // Doit être quelque part entre 10 et 30 ms
        assert!(lag_us > 10_000 && lag_us < 30_000);
    }
}
