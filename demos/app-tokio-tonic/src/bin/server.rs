// =============================================================================
// SERVEUR gRPC — KV-store en mémoire
//
// Démontre :
//   TONIC : on implémente le trait `KvStore` généré depuis le .proto, on
//           branche le service dans un Server, et on sert sur un port TCP.
//   TOKIO : #[tokio::main] lance le runtime async. Chaque requête est une task.
//           Le store est partagé entre tasks via Arc + Mutex. Le Watch utilise
//           un broadcast channel pour pousser les événements en temps réel.
//
// LANCEMENT : cargo run --bin server
// =============================================================================

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

// --- Code généré par tonic-build à partir de proto/kvstore.proto ---
// `kvstore` est le `package` déclaré dans le .proto.
pub mod kvstore {
    tonic::include_proto!("kvstore");
}
use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse, WatchEvent,
    WatchRequest,
};

// -----------------------------------------------------------------------------
// L'état partagé du serveur.
// -----------------------------------------------------------------------------
// - `store`     : la map clé→valeur. Arc = partage entre tasks, Mutex (Tokio,
//                 async-aware) = un seul writer à la fois sans bloquer le thread.
// - `events_tx` : l'émetteur d'un broadcast channel. Chaque écriture y publie un
//                 WatchEvent ; tous les clients abonnés au Watch le reçoivent.
struct KvStoreService {
    store: Arc<Mutex<HashMap<String, String>>>,
    events_tx: broadcast::Sender<WatchEvent>,
}

impl KvStoreService {
    fn new() -> Self {
        // capacité 128 : tampon d'événements en attente avant qu'un abonné lent
        // ne "rate" des messages (lagging). Suffisant pour la démo.
        let (events_tx, _) = broadcast::channel(128);
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
            events_tx,
        }
    }
}

// -----------------------------------------------------------------------------
// Implémentation du service. `#[tonic::async_trait]` permet d'avoir des
// méthodes `async fn` dans le trait généré.
// -----------------------------------------------------------------------------
#[tonic::async_trait]
impl KvStore for KvStoreService {
    // --- PUT : écrit une clé, et publie un événement aux watchers ---
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let PutRequest { key, value } = request.into_inner();

        // .lock().await : verrou async — on n'attend PAS en bloquant le thread,
        // la task est suspendue puis reprise quand le verrou se libère.
        let mut store = self.store.lock().await;
        let created = store.insert(key.clone(), value.clone()).is_none();
        drop(store); // on relâche le verrou tôt, avant de publier l'événement

        println!("[server] PUT {key} = {value}  (created={created})");

        // On notifie les abonnés Watch. `send` échoue seulement s'il n'y a aucun
        // abonné — ce n'est pas une erreur ici, on l'ignore.
        // `Kind::Put as i32` : dans Protobuf un enum est transporté comme entier.
        let _ = self.events_tx.send(WatchEvent {
            kind: kvstore::watch_event::Kind::Put as i32,
            key,
            value,
        });

        Ok(Response::new(PutResponse { created }))
    }

    // --- GET : lit une clé ---
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        let store = self.store.lock().await;
        let resp = match store.get(&key) {
            Some(v) => GetResponse {
                found: true,
                value: v.clone(),
            },
            None => GetResponse {
                found: false,
                value: String::new(),
            },
        };
        println!("[server] GET {key} -> found={}", resp.found);
        Ok(Response::new(resp))
    }

    // --- DELETE : supprime une clé, et publie un événement ---
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let key = request.into_inner().key;
        let mut store = self.store.lock().await;
        let deleted = store.remove(&key).is_some();
        drop(store);

        println!("[server] DELETE {key}  (deleted={deleted})");

        if deleted {
            let _ = self.events_tx.send(WatchEvent {
                kind: kvstore::watch_event::Kind::Delete as i32,
                key,
                value: String::new(),
            });
        }
        Ok(Response::new(DeleteResponse { deleted }))
    }

    // --- WATCH : server streaming. 1 requête → un FLUX d'événements ---
    // C'est le coeur de la démo Tokio+Tonic.
    type WatchStream = ReceiverStream<Result<WatchEvent, Status>>;

    async fn watch(
        &self,
        _request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        println!("[server] nouveau client WATCH abonné");

        // On s'abonne au broadcast (reçoit tous les events futurs).
        let mut events_rx = self.events_tx.subscribe();

        // mpsc channel : le pont entre notre task et le stream renvoyé à Tonic.
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        // tokio::spawn : on lance une TASK indépendante qui vit tant que le
        // client écoute. Elle relaie chaque event broadcast vers le stream.
        tokio::spawn(async move {
            loop {
                match events_rx.recv().await {
                    Ok(event) => {
                        // si l'envoi échoue, c'est que le client s'est déconnecté
                        if tx.send(Ok(event)).await.is_err() {
                            println!("[server] client WATCH déconnecté");
                            break;
                        }
                    }
                    // l'abonné est trop lent et a raté des messages : on continue
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        println!("[server] WATCH a pris du retard, {n} events sautés");
                    }
                    // l'émetteur n'existe plus : fin
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        // On renvoie immédiatement le stream ; la task ci-dessus l'alimente.
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// -----------------------------------------------------------------------------
// Point d'entrée — TOKIO démarre ici.
// `#[tokio::main]` transforme ce `async fn main` en un vrai `main` qui crée le
// runtime multi-thread et y exécute notre future.
// -----------------------------------------------------------------------------
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let service = KvStoreService::new();

    println!("=== Serveur KV-store gRPC ===");
    println!("[server] écoute sur http://{addr}");
    println!("[server] lancez le client dans un autre terminal : cargo run --bin client\n");

    // TONIC : on assemble le serveur, on y ajoute notre service, et on sert.
    // `.serve(addr).await` tourne jusqu'à Ctrl-C.
    Server::builder()
        .add_service(KvStoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
