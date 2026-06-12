# Énoncé KV Store Distribué


---

##  Moteur de stockage local

### Objectif
Construire un moteur de stockage clé-valeur **thread-safe**, **versionné**, **avec TTL**, dans le fichier `src/storage.rs`. Mesurer ses performances avec Criterion.

### Tâches

1. **Définir `ValueEntry`** dans `src/storage.rs` :
   ```rust
   #[derive(Clone, Debug)]
   pub struct ValueEntry {
       pub data: Vec<u8>,
       pub version: u64,
       pub created_at: Instant,
       pub ttl: Option<Duration>,
   }
   ```

2. **Définir `StorageEngine`** avec un `DashMap<String, ValueEntry>` et un `AtomicU64` pour les versions. Le moteur doit être `Send + Sync` et utilisable derrière un `Arc`.

3. **Implémenter les opérations** :
   - `pub fn get(&self, key: &str) -> Option<ValueEntry>` (clone l'entrée, vérifie TTL)
   - `pub fn put(&self, key: String, data: Vec<u8>, ttl: Option<Duration>) -> u64` (renvoie la nouvelle version)
   - `pub fn delete(&self, key: &str) -> bool`
   - `pub fn scan(&self, prefix: &str, limit: usize) -> Vec<(String, ValueEntry)>`

4. **Gérer le TTL** : un `get` sur une clé expirée doit retourner `None` ET supprimer l'entrée en arrière-plan (lazy expiration).

5. **Exposer un struct `StorageStats`** avec compteurs atomiques : `gets`, `puts`, `deletes`, `hits`, `misses`.

6. **Tests unitaires** dans le module `tests` :
   - put / get round-trip
   - delete d'une clé existante / inexistante
   - TTL expiration (utiliser `tokio::time::pause()` ou un sleep court)
   - scan avec préfixe
   - concurrence : 100 tâches qui font 1000 put chacune, le store doit contenir 100k entrées au final

7. **Benchmark Criterion** dans `benches/storage_bench.rs` :
   - `bench_put_single_thread` – 1 thread, 100k clés
   - `bench_get_hit` – 1 thread, 100% hits
   - `bench_get_concurrent` – 4 threads en lecture


---

##  Protocole réseau

### Objectif
Exposer le `StorageEngine` via un **serveur TCP asynchrone** avec un protocole binaire framé. Implémenter un **client** pouvant émettre des requêtes.

### Tâches

1. **Définir l'enum `Request`** dans `src/protocol.rs` :
   ```rust
   #[derive(Debug, Serialize, Deserialize)]
   pub enum Request {
       Get { key: String },
       Put { key: String, value: Vec<u8>, ttl_ms: Option<u64> },
       Delete { key: String },
       Scan { prefix: String, limit: u32 },
       // Inter-noeuds
       Replicate { key: String, value: Vec<u8>, version: u64, ttl_ms: Option<u64> },
       Heartbeat { node_id: u32 },
   }
   ```

2. **Définir `Response`** symétrique : `Value(Option<Vec<u8>>)`, `Ack { version: u64 }`, `Deleted(bool)`, `ScanResult(Vec<(String, Vec<u8>)>)`, `Pong`, `Error(String)`.

3. **Implémenter `KvCodec`** basé sur `LengthDelimitedCodec` + `bincode` :
   - `encode<W: AsyncWrite>(stream: &mut W, msg: &Request) -> io::Result<()>`
   - `decode<R: AsyncRead>(stream: &mut R) -> io::Result<Request>`

4. **Implémenter `KvServer`** dans `src/server.rs` :
   - `pub async fn run(addr: SocketAddr, storage: Arc<StorageEngine>) -> io::Result<()>`
   - Une tâche `tokio::spawn` par connexion
   - Boucle : decode request → handle → encode response
   - Logger chaque erreur via `tracing`

5. **Implémenter `KvClient`** dans `src/client.rs` :
   - `pub async fn connect(addr: SocketAddr) -> io::Result<Self>`
   - `pub async fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>>`
   - `pub async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<u64>`
   - Avec un **pool de connexions** réutilisables (`Vec<TcpStream>` + `Mutex`)

6. **Binaire CLI** `src/bin/kvc.rs` : `kvc put|get|delete <addr> <key> [value]`



---

## Distribution et réplication

### Objectif
Transformer le noeud isolé en **cluster distribué** avec consistent hashing et réplication quorum W=2.

### Tâches

1. **Implémenter `ConsistentHashRing`** dans `src/consistent_hash.rs` :
   - 150 noeuds virtuels par noeud physique
   - `add_node`, `remove_node`
   - `get_node(&self, key: &str) -> Option<&str>` (clé → noeud principal)
   - `get_nodes(&self, key: &str, count: usize) -> Vec<&str>` (clé → N réplicas)
   - Distribution attendue : pour 10 000 clés sur 3 noeuds, chaque noeud reçoit entre 28% et 38% des clés.

2. **Implémenter `Replicator`** dans `src/replication.rs` :
   ```rust
   pub struct Replicator {
       node_id: u32,
       ring: Arc<RwLock<ConsistentHashRing>>,
       peers: HashMap<String, KvClient>, // pool inter-noeuds
       storage: Arc<StorageEngine>,
       replication_factor: usize, // 2
       write_quorum: usize,       // 2
   }
   ```
   - `pub async fn put_replicated(&self, key: String, value: Vec<u8>) -> Result<u64>`
     - Calcule les N réplicas avec `ring.get_nodes(&key, 2)`
     - Si le noeud local est dans la liste : write local + envoie `Replicate` aux autres en parallèle (`tokio::try_join!`)
     - Si le noeud local n'est PAS dans la liste : forward complet vers le premier réplica
     - Attend `W=2` acks (avec timeout configurable de 500 ms)
   - `pub async fn get_replicated(&self, key: &str) -> Result<Option<Vec<u8>>>`
     - Lit depuis le premier réplica vivant (fallback si timeout)

3. **Étendre `KvServer`** pour router les requêtes via le `Replicator` (le serveur public utilise le replicator, le service interne va directement au storage).

4. **CLI étendue** : flag `--peers "addr1,addr2,..."` qui construit le ring au démarrage.

5. **Test manuel multi-noeuds** (3 terminaux) :
   ```bash
   # Terminal 1
   cargo run -- --node-id 1 --port 50051 --peers 127.0.0.1:50052,127.0.0.1:50053
   # Terminal 2
   cargo run -- --node-id 2 --port 50052 --peers 127.0.0.1:50051,127.0.0.1:50053
   # Terminal 3
   cargo run -- --node-id 3 --port 50053 --peers 127.0.0.1:50051,127.0.0.1:50052

   # Client
   kvc put --addr 127.0.0.1:50051 user:42 alice  # → write quorum OK
   kvc get --addr 127.0.0.1:50053 user:42        # → "alice" (lecture depuis n'importe quel noeud)
   ```



##  Observabilité et Docker

### Objectif
Rendre le cluster **opérable en production-like** : métriques exposées, health check, déploiement Docker Compose.

### Tâches

1. **Module `src/metrics.rs`** :
   ```rust
   pub struct Metrics {
       pub ops_total: AtomicU64,
       pub get_total: AtomicU64,
       pub put_total: AtomicU64,
       pub delete_total: AtomicU64,
       pub errors_total: AtomicU64,
       pub replication_lag_ms: AtomicU64, // moyenne mobile EWMA
       pub active_connections: AtomicU64,
   }
   ```
   - Méthode `format_prometheus(&self) -> String` qui produit du texte Prometheus.

2. **Endpoint HTTP** `/metrics` exposé sur `--metrics-port` :
   - Utiliser `hyper` (server::conn::http1) pour rester léger
   - Réponse `Content-Type: text/plain; version=0.0.4`
   - Inclure les compteurs **par noeud** et un **timestamp Unix**.

3. **Endpoint `/health`** :
   - Retourne 200 OK avec JSON `{"node_id": 1, "uptime_s": 42, "peers_alive": 2}` si quorum OK
   - Retourne 503 si quorum impossible

4. **Logs structurés JSON** via `tracing_subscriber::fmt().json()` :
   - Activé si `RUST_LOG=info` ou `--log-format=json`

5. **Dockerfile multi-stage** :
   - Stage 1 : `rust:1.82-slim` + `cargo build --release`
   - Stage 2 : `gcr.io/distroless/cc-debian12` + binaire seulement
   - Image finale **< 50 Mo**

6. **`docker-compose.yml`** : 3 services `node1`, `node2`, `node3` qui se découvrent via DNS Docker.

7. **Démonstration finale** :
   ```bash
   docker compose up --build
   # Dans un autre terminal
   docker compose exec node1 kvc put hello world
   curl http://localhost:9091/metrics  # voir les compteurs
   docker compose stop node2
   docker compose exec node1 kvc get hello  # doit fonctionner (RF=2, lecture sur n3)
   curl http://localhost:9091/health  # statut "degraded" mais 200
   ```
