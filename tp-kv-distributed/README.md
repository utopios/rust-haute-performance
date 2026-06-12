# TP Fil Rouge – KV Store Distribué (Jour 4, Plan 3)

> **Durée totale** : 09:00 – 17:00 (6h30 de travail effectif + pause déjeuner)
> **Pré-requis** : Tokio, Arc/Mutex, DashMap, Criterion (modules des jours 1-3)
> **Objectif** : Intégrer profiling, concurrence et systèmes distribués dans un **KV Store distribué** entièrement fonctionnel, en s'inspirant des architectures internes d'OVH (object store, métriques cluster).

---

## Objectifs pédagogiques

À l'issue de ce TP, vous serez capable de :

1. Concevoir un **moteur de stockage clé-valeur thread-safe** avec gestion fine des verrous (DashMap).
2. Implémenter un **protocole réseau binaire** asynchrone basé sur Tokio + `tokio_util::codec`.
3. Mettre en place du **consistent hashing** avec noeuds virtuels et un facteur de réplication.
4. Gérer un **quorum d'écriture** (W=2) et la résilience aux pannes de noeud.
5. Écrire des **benchmarks Criterion** ciblés et atteindre des cibles de performance mesurables.
6. Exposer un **endpoint de métriques** Prometheus-like pour observer le cluster.
7. Déployer un cluster **3 noeuds via Docker Compose** et démontrer le bon fonctionnement.

---

## Architecture cible

```
                       Client (CLI / TCP)
                              │
                ┌─────────────┼─────────────┐
                ▼             ▼             ▼
           ┌─────────┐   ┌─────────┐   ┌─────────┐
           │  Node 1 │   │  Node 2 │   │  Node 3 │
           │ :50051  │   │ :50052  │   │ :50053  │
           ├─────────┤   ├─────────┤   ├─────────┤
           │ Public  │   │ Public  │   │ Public  │
           │ API TCP │   │ API TCP │   │ API TCP │
           ├─────────┤   ├─────────┤   ├─────────┤
           │ Router  │   │ Router  │   │ Router  │
           │(consist.│   │(consist.│   │(consist.│
           │  hash)  │   │  hash)  │   │  hash)  │
           ├─────────┤   ├─────────┤   ├─────────┤
           │ Storage │   │ Storage │   │ Storage │
           │(DashMap)│   │(DashMap)│   │(DashMap)│
           ├─────────┤   ├─────────┤   ├─────────┤
           │ Metrics │   │ Metrics │   │ Metrics │
           │ HTTP    │   │ HTTP    │   │ HTTP    │
           │ :9091   │   │ :9092   │   │ :9093   │
           └────┬────┘   └────┬────┘   └────┬────┘
                └──── Réplication ────┘
                  (quorum W=2, RF=2)
```

---

## Note technique : protocole binaire vs gRPC

L'énoncé original mentionne **gRPC (tonic + prost)**. Pour rester compilable sans `build.rs` complexe et pour exposer plus clairement les patterns réseau bas niveau, ce TP utilise un **protocole binaire custom** basé sur :

- `tokio::net::TcpListener` / `TcpStream` pour le transport
- `tokio_util::codec` (LengthDelimitedCodec) pour le framing
- `bincode` pour la sérialisation des messages
- Un trait `Message` qui factorise public et inter-noeuds

Les patterns appris (services public + interne, streaming, quorum) sont identiques. Une variante gRPC est mentionnée dans le `GUIDE_FORMATEUR.md` pour les stagiaires qui veulent porter le projet sur tonic en autonomie.

---

## Agenda de la journée

| Horaire        | Sprint                                | Durée | Livrables principaux                    |
|----------------|---------------------------------------|-------|-----------------------------------------|
| 09:00 – 10:30  | **Sprint 1** – Moteur de stockage     | 1h30  | `storage.rs` + bench `storage_bench`    |
| 10:30 – 12:00  | **Sprint 2** – Protocole réseau       | 1h30  | `protocol.rs`, `server.rs`              |
| 13:00 – 14:30  | **Sprint 3** – Distribution + replic. | 1h30  | `consistent_hash.rs`, `replication.rs`  |
| 14:30 – 16:00  | **Sprint 4** – Tests + benchmarks     | 1h30  | `tests/`, cibles perf atteintes         |
| 16:00 – 17:00  | **Sprint 5** – Observabilité + Docker | 1h    | `metrics.rs`, `docker-compose.yml`      |

Chaque sprint a son cadre d'objectifs détaillé dans [`ENONCE.md`](./ENONCE.md).

---

## Stack technique

| Couche             | Crate                                     | Rôle                                     |
|--------------------|-------------------------------------------|------------------------------------------|
| Runtime async      | `tokio` 1.x (features `full`)             | I/O réseau, tâches, timers               |
| Stockage interne   | `dashmap` 5.x                             | HashMap shardée, lock-free en lecture    |
| Framing            | `tokio-util` (codec, `LengthDelimitedCodec`) | Messages préfixés par longueur        |
| Sérialisation      | `bincode` 1.x                             | Encodage binaire compact                 |
| CLI                | `clap` 4.x (derive)                       | Parsing `--node-id`, `--port`, `--peers` |
| Logging structuré  | `tracing` + `tracing-subscriber`          | Logs structurés JSON                     |
| Benchmarks         | `criterion`                               | Mesures throughput + latence             |
| Métriques HTTP     | `hyper` (server, low-level)               | Endpoint `/metrics` Prometheus-like      |

---

## Build & Run

### Compilation locale

```bash
# Compilation release
cargo build --release

# Lancer un noeud
cargo run --release -- --node-id 1 --port 50051 --metrics-port 9091 \
    --peers "127.0.0.1:50052,127.0.0.1:50053"
```

### Tests

```bash
# Tests unitaires
cargo test

# Tests d'intégration (3 noeuds locaux)
cargo test --test integration -- --nocapture

# Benchmarks Criterion
cargo bench
```

### Cluster Docker

```bash
docker compose up --build
# Cluster 3 noeuds, expose :50051, :50052, :50053 + :9091, :9092, :9093

# Test depuis l'hôte (CLI minimal fourni)
cargo run --release --bin kvc -- --addr 127.0.0.1:50051 put hello world
cargo run --release --bin kvc -- --addr 127.0.0.1:50051 get hello
```

### Cibles de performance (Sprint 4)

| Opération       | Cible single-noeud | Cible cluster 3 noeuds (RF=2, W=2) |
|-----------------|--------------------|-----------------------------------|
| GET             | > 100 000 ops/s    | > 60 000 ops/s                    |
| PUT             | > 50 000 ops/s     | > 25 000 ops/s                    |
| Latence P50     | < 200 µs           | < 1 ms                            |
| Latence P99     | < 1 ms             | < 5 ms                            |

---

## Arborescence du projet

```
tp-kv-distributed/
├── README.md              (ce fichier)
├── ENONCE.md              (sprints détaillés + critères)
├── GUIDE_FORMATEUR.md     (déroulé pédagogique)
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
├── proto/
│   └── kvstore.proto      (variante gRPC documentaire)
├── src/
│   ├── main.rs            (entrée binaire + CLI)
│   ├── lib.rs             (façade publique)
│   ├── storage.rs         (StorageEngine + ValueEntry + TTL)
│   ├── consistent_hash.rs (ConsistentHashRing avec vnodes)
│   ├── replication.rs     (Replicator, quorum W=2)
│   ├── protocol.rs        (Message, codec)
│   ├── server.rs          (KvServer Tokio)
│   ├── client.rs          (KvClient + connection pool)
│   ├── metrics.rs         (compteurs atomiques + endpoint HTTP)
│   └── bin/
│       └── kvc.rs         (CLI client minimal)
├── benches/
│   └── storage_bench.rs   (benchmarks Criterion)
└── tests/
    └── integration.rs     (tests cluster 3 noeuds)
```

---

## Comment valider chaque sprint

Voir [`ENONCE.md`](./ENONCE.md) pour la liste exhaustive. En résumé :

- [ ] **Sprint 1** : `cargo test --lib storage::tests` passe + bench affiche > 1M get/s en single-thread.
- [ ] **Sprint 2** : `cargo run` démarre un serveur, `kvc put/get` fonctionne en local.
- [ ] **Sprint 3** : un cluster 3 noeuds répond à un PUT avec ack quorum, GET trouve la valeur depuis n'importe quel noeud.
- [ ] **Sprint 4** : `cargo bench` atteint les cibles, un test "kill node 2" passe (lecture toujours possible).
- [ ] **Sprint 5** : `curl http://localhost:9091/metrics` retourne les compteurs, `docker compose up` démarre 3 noeuds sains.

---

## Pour aller plus loin

- Variante **gRPC tonic** : voir `proto/kvstore.proto` et la section "Bonus" de `GUIDE_FORMATEUR.md`.
- Persistance disque (write-ahead log) : non couvert ici, idée pour un module ultérieur.
- Consensus Raft (au lieu du quorum simple) : crates `raft-rs` ou `openraft`.
