// =============================================================================
// main.rs – Binaire serveur kv-distributed
// =============================================================================
//
// Démarre un noeud KV :
//   - Parse les arguments CLI (clap)
//   - Construit le storage, le ring, les clients vers les peers
//   - Lance le serveur TCP du protocole KV
//   - Lance le serveur HTTP /metrics et /health
//   - Lance une boucle heartbeat pour suivre l'état des peers
// =============================================================================

use clap::Parser;
use kv_distributed::{
    client::KvClient,
    consistent_hash::ConsistentHashRing,
    metrics::{Metrics, MetricsServer},
    replication::Replicator,
    server::KvServer,
    storage::StorageEngine,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "kv-distributed",
    version,
    about = "TP Jour 4 Plan 3 – Noeud KV distribué (Rust OVH)",
    long_about = None
)]
struct Args {
    /// Identifiant numérique unique du noeud
    #[arg(long)]
    node_id: u32,

    /// Adresse d'écoute principale (protocole KV binaire)
    #[arg(long, default_value = "0.0.0.0:50051")]
    listen: SocketAddr,

    /// Port d'écoute (utilisé si --listen n'est pas fourni)
    #[arg(long)]
    port: Option<u16>,

    /// Adresses des peers, séparées par des virgules (ex: 10.0.0.2:50051,10.0.0.3:50051)
    #[arg(long, default_value = "")]
    peers: String,

    /// Adresse du endpoint HTTP /metrics et /health
    #[arg(long)]
    metrics_addr: Option<SocketAddr>,

    /// Port HTTP /metrics (utilisé si --metrics-addr n'est pas fourni)
    #[arg(long, default_value_t = 9090)]
    metrics_port: u16,

    /// Format des logs : "text" ou "json"
    #[arg(long, default_value = "text")]
    log_format: String,

    /// Facteur de réplication
    #[arg(long, default_value_t = 2)]
    replication_factor: usize,

    /// Quorum d'écriture
    #[arg(long, default_value_t = 2)]
    write_quorum: usize,
}

impl Args {
    fn effective_listen(&self) -> SocketAddr {
        if let Some(p) = self.port {
            let mut addr = self.listen;
            addr.set_port(p);
            addr
        } else {
            self.listen
        }
    }

    fn effective_metrics_addr(&self) -> SocketAddr {
        self.metrics_addr.unwrap_or_else(|| {
            SocketAddr::from(([0, 0, 0, 0], self.metrics_port))
        })
    }

    fn parsed_peers(&self) -> Vec<SocketAddr> {
        self.peers
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.parse::<SocketAddr>().ok())
            .collect()
    }
}

fn init_tracing(format: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,kv_distributed=info"));

    if format == "json" {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing(&args.log_format);

    info!(
        node_id = args.node_id,
        listen = %args.effective_listen(),
        metrics = %args.effective_metrics_addr(),
        peers = ?args.parsed_peers(),
        "démarrage du noeud KV"
    );

    // 1. Storage local
    let storage = Arc::new(StorageEngine::new());

    // 2. Métriques
    let metrics = Arc::new(Metrics::new(args.node_id));

    // 3. Construction du ring : self + peers
    let local_node_name = format!("node-{}", args.node_id);
    let peer_addrs = args.parsed_peers();

    let mut ring = ConsistentHashRing::new(150);
    ring.add_node(&local_node_name);
    let peer_node_names: Vec<String> = peer_addrs
        .iter()
        .enumerate()
        .map(|(i, addr)| {
            // Convention : on dérive un nom à partir de l'index dans la liste,
            // décalé pour éviter la collision avec le noeud local.
            let id = if (i as u32 + 1) >= args.node_id {
                i as u32 + 2
            } else {
                i as u32 + 1
            };
            let _ = addr; // l'adresse est utilisée plus bas pour le pool de clients
            format!("node-{id}")
        })
        .collect();
    for n in &peer_node_names {
        ring.add_node(n);
    }
    let ring = Arc::new(RwLock::new(ring));

    // 4. Pool de clients vers les peers (best-effort, on continue si certains
    //    ne sont pas encore disponibles)
    let mut peers_map = HashMap::new();
    for (name, addr) in peer_node_names.iter().zip(peer_addrs.iter()) {
        match KvClient::connect(*addr).await {
            Ok(c) => {
                info!(peer = %addr, name = %name, "connexion peer établie");
                peers_map.insert(name.clone(), c);
            }
            Err(e) => {
                warn!(peer = %addr, error = %e, "peer indisponible au démarrage, on retentera plus tard");
            }
        }
    }
    metrics
        .peers_total
        .store(peer_addrs.len() as u64, Ordering::Relaxed);
    metrics
        .peers_alive
        .store(peers_map.len() as u64, Ordering::Relaxed);
    let peers = Arc::new(RwLock::new(peers_map));

    // 5. Replicator
    let mut replicator = Replicator::new(
        args.node_id,
        local_node_name.clone(),
        Arc::clone(&ring),
        Arc::clone(&peers),
        Arc::clone(&storage),
    );
    replicator.replication_factor = args.replication_factor;
    replicator.write_quorum = args.write_quorum;
    let replicator = Arc::new(replicator);

    // 6. KvServer (TCP binaire)
    let server = Arc::new(KvServer::new(
        args.node_id,
        Arc::clone(&storage),
        Arc::clone(&replicator),
        Arc::clone(&metrics),
    ));
    let server_addr = args.effective_listen();
    {
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            if let Err(e) = server.run(server_addr).await {
                warn!(error = %e, "serveur KV terminé");
            }
        });
    }

    // 7. MetricsServer (HTTP)
    let metrics_server = Arc::new(MetricsServer::new(
        Arc::clone(&metrics),
        Arc::clone(&storage),
    ));
    let metrics_addr = args.effective_metrics_addr();
    {
        let m = Arc::clone(&metrics_server);
        tokio::spawn(async move {
            if let Err(e) = m.run(metrics_addr).await {
                warn!(error = %e, "metrics server terminé");
            }
        });
    }

    // 8. Boucle heartbeat des peers : reconnecte si nécessaire et met à jour
    //    le compteur peers_alive
    {
        let peers = Arc::clone(&peers);
        let metrics = Arc::clone(&metrics);
        let peer_pairs: Vec<(String, SocketAddr)> = peer_node_names
            .iter()
            .cloned()
            .zip(peer_addrs.iter().copied())
            .collect();
        let node_id = args.node_id;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                let mut alive = 0u64;
                for (name, addr) in &peer_pairs {
                    let mut already = false;
                    {
                        let guard = peers.read().await;
                        if let Some(c) = guard.get(name) {
                            if c.heartbeat(node_id).await.is_ok() {
                                already = true;
                                alive += 1;
                            }
                        }
                    }
                    if !already {
                        if let Ok(c) = KvClient::connect(*addr).await {
                            if c.heartbeat(node_id).await.is_ok() {
                                let mut guard = peers.write().await;
                                guard.insert(name.clone(), c);
                                alive += 1;
                            }
                        }
                    }
                }
                metrics.peers_alive.store(alive, Ordering::Relaxed);
            }
        });
    }

    // 9. Boucle de purge périodique des entrées expirées
    {
        let storage = Arc::clone(&storage);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let purged = storage.purge_expired();
                if purged > 0 {
                    tracing::debug!(purged, "purge TTL");
                }
            }
        });
    }

    // 10. Attente Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Ctrl+C reçu, arrêt du noeud");
    Ok(())
}
