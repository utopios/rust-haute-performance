// =============================================================================
// CLIENT gRPC — pilote le KV-store
//
// Démontre :
//   TONIC : le client `KvStoreClient` est généré depuis le .proto. On appelle
//           ses méthodes comme de simples fonctions async, Tonic s'occupe de la
//           sérialisation Protobuf et du transport HTTP/2.
//   TOKIO : on lance une TASK qui écoute le flux Watch EN PARALLÈLE des écritures,
//           pour montrer le push temps réel. `tokio::time::sleep` rythme la démo.
//
// LANCEMENT (le serveur doit déjà tourner) : cargo run --bin client
// =============================================================================

use std::time::Duration;

use tokio_stream::StreamExt; // pour `.next()` sur le stream Watch

pub mod kvstore {
    tonic::include_proto!("kvstore");
}
use kvstore::kv_store_client::KvStoreClient;
use kvstore::{DeleteRequest, GetRequest, PutRequest, WatchEvent, WatchRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Client KV-store gRPC ===\n");

    // TONIC : connexion HTTP/2 au serveur. `connect` rend un client prêt à l'emploi.
    let mut client = KvStoreClient::connect("http://127.0.0.1:50051").await?;

    // -------------------------------------------------------------------------
    // 1) On démarre un WATCH en arrière-plan (task Tokio séparée).
    //    Il va afficher en direct chaque modification faite ensuite.
    // -------------------------------------------------------------------------
    let mut watch_client = client.clone(); // un clone partage la même connexion HTTP/2
    let watcher = tokio::spawn(async move {
        let mut stream = watch_client
            .watch(WatchRequest {})
            .await
            .expect("abonnement watch")
            .into_inner();

        // `.next().await` attend le prochain event poussé par le serveur.
        while let Some(item) = stream.next().await {
            match item {
                Ok(ev) => {
                    println!(
                        "   📡 [watch] {:<6} {} = {:?}",
                        WatchEvent::kind_label(ev.kind),
                        ev.key,
                        ev.value
                    );
                }
                Err(e) => {
                    eprintln!("   [watch] erreur de flux : {e}");
                    break;
                }
            }
        }
    });

    // petit délai pour s'assurer que l'abonnement Watch est actif avant d'écrire
    tokio::time::sleep(Duration::from_millis(200)).await;

    // -------------------------------------------------------------------------
    // 2) Quelques opérations unary : PUT, GET, DELETE.
    //    Chaque PUT/DELETE déclenchera un event affiché par le watcher ci-dessus.
    // -------------------------------------------------------------------------
    println!("--- Écritures (chaque PUT/DELETE est poussé au watcher) ---");

    let r = client
        .put(PutRequest {
            key: "user:1".into(),
            value: "Alice".into(),
        })
        .await?;
    println!("PUT user:1=Alice    -> created={}", r.into_inner().created);
    tokio::time::sleep(Duration::from_millis(150)).await;

    let r = client
        .put(PutRequest {
            key: "user:2".into(),
            value: "Bob".into(),
        })
        .await?;
    println!("PUT user:2=Bob      -> created={}", r.into_inner().created);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // mise à jour d'une clé existante → created=false
    let r = client
        .put(PutRequest {
            key: "user:1".into(),
            value: "Alice Cooper".into(),
        })
        .await?;
    println!("PUT user:1=Alice C. -> created={}", r.into_inner().created);
    tokio::time::sleep(Duration::from_millis(150)).await;

    println!("\n--- Lectures ---");
    let r = client
        .get(GetRequest {
            key: "user:1".into(),
        })
        .await?
        .into_inner();
    println!("GET user:1          -> found={}, value={:?}", r.found, r.value);

    let r = client
        .get(GetRequest {
            key: "user:404".into(),
        })
        .await?
        .into_inner();
    println!("GET user:404        -> found={}, value={:?}", r.found, r.value);

    println!("\n--- Suppression ---");
    let r = client
        .delete(DeleteRequest {
            key: "user:2".into(),
        })
        .await?;
    println!("DELETE user:2       -> deleted={}", r.into_inner().deleted);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // -------------------------------------------------------------------------
    // 3) Fin. On laisse le watcher se terminer proprement.
    // -------------------------------------------------------------------------
    println!("\n[client] terminé. Fermeture du watcher.");
    watcher.abort(); // on coupe la task de watch (sinon elle attendrait à l'infini)

    Ok(())
}

// Helper : convertit le code numérique du Kind en libellé lisible.
impl WatchEvent {
    fn kind_label(kind: i32) -> &'static str {
        match kvstore::watch_event::Kind::try_from(kind) {
            Ok(kvstore::watch_event::Kind::Put) => "PUT",
            Ok(kvstore::watch_event::Kind::Delete) => "DELETE",
            Err(_) => "?",
        }
    }
}
