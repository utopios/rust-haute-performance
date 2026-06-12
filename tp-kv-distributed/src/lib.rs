// =============================================================================
// lib.rs – Façade publique de la crate kv-distributed
// =============================================================================
//
// Toute la logique est exposée ici pour permettre aux tests d'intégration
// et aux benchmarks Criterion d'importer les modules sans dupliquer le code.
// =============================================================================

pub mod client;
pub mod consistent_hash;
pub mod metrics;
pub mod protocol;
pub mod replication;
pub mod server;
pub mod storage;

pub use client::KvClient;
pub use consistent_hash::ConsistentHashRing;
pub use metrics::{Metrics, MetricsServer};
pub use protocol::{Request, Response};
pub use replication::{ReplicationError, Replicator};
pub use server::KvServer;
pub use storage::{StorageEngine, ValueEntry};
