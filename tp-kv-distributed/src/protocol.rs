// =============================================================================
// protocol.rs – Protocole binaire framé
// =============================================================================
//
// Format wire :
//   [u32 BE : longueur du payload] [payload bincode]
//
// On utilise LengthDelimitedCodec de tokio_util pour gérer le framing,
// puis bincode pour sérialiser/désérialiser les enum Request/Response.
//
// Avantages par rapport à un protocole texte :
//   - Pas de parsing ligne par ligne, robuste aux binaires
//   - Compact, latence faible
//   - Schema explicite via les enum
//
// Limitations vs gRPC :
//   - Pas de streaming bidirectionnel (mais on simule pour Scan via Vec)
//   - Pas de codegen multi-langage
// =============================================================================

use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// -----------------------------------------------------------------------------
// Requêtes (client → serveur public ET inter-noeuds)
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    // --- API publique ---
    Get {
        key: String,
    },
    Put {
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    },
    Delete {
        key: String,
    },
    Scan {
        prefix: String,
        limit: u32,
    },
    // --- API inter-noeuds ---
    Replicate {
        key: String,
        value: Vec<u8>,
        version: u64,
        ttl_ms: Option<u64>,
        source_node_id: u32,
    },
    Heartbeat {
        node_id: u32,
        timestamp_ms: u64,
    },
}

// -----------------------------------------------------------------------------
// Réponses (serveur → client)
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Résultat d'un Get : None = absent ou expiré
    Value {
        data: Option<Vec<u8>>,
        version: u64,
    },
    /// Ack d'un Put avec la version assignée
    Ack {
        version: u64,
        node_id: u32,
    },
    /// Résultat d'un Delete : true si la clé existait
    Deleted(bool),
    /// Résultat d'un Scan : liste (clé, valeur)
    ScanResult(Vec<(String, Vec<u8>)>),
    /// Heartbeat / health check
    Pong {
        node_id: u32,
        keys_count: u64,
        uptime_s: u64,
    },
    /// Erreur applicative (le serveur a répondu mais l'opération a échoué)
    Error(String),
}

// -----------------------------------------------------------------------------
// Constantes de framing
// -----------------------------------------------------------------------------

/// Taille maximale d'un message : 16 Mo (limite anti-DoS)
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

// -----------------------------------------------------------------------------
// Encodage / décodage bas niveau
// -----------------------------------------------------------------------------

/// Encode un message vers un AsyncWrite (préfixe longueur + payload bincode).
pub async fn write_message<W: AsyncWrite + Unpin, M: Serialize>(
    stream: &mut W,
    msg: &M,
) -> io::Result<()> {
    let payload = bincode::serialize(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if payload.len() > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message trop volumineux : {} octets", payload.len()),
        ));
    }

    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    Ok(())
}

/// Décode un message depuis un AsyncRead.
pub async fn read_message<R: AsyncRead + Unpin, M: for<'de> Deserialize<'de>>(
    stream: &mut R,
) -> io::Result<M> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame trop grosse : {len} octets"),
        ));
    }

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;

    bincode::deserialize(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_roundtrip_get_request() {
        let (mut a, mut b) = duplex(4096);
        let req = Request::Get {
            key: "hello".into(),
        };
        write_message(&mut a, &req).await.unwrap();
        drop(a);

        let received: Request = read_message(&mut b).await.unwrap();
        match received {
            Request::Get { key } => assert_eq!(key, "hello"),
            other => panic!("type inattendu : {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_roundtrip_put_request() {
        let (mut a, mut b) = duplex(4096);
        let req = Request::Put {
            key: "k".into(),
            value: b"v".to_vec(),
            ttl_ms: Some(5000),
        };
        write_message(&mut a, &req).await.unwrap();
        drop(a);

        let received: Request = read_message(&mut b).await.unwrap();
        match received {
            Request::Put { key, value, ttl_ms } => {
                assert_eq!(key, "k");
                assert_eq!(value, b"v");
                assert_eq!(ttl_ms, Some(5000));
            }
            other => panic!("type inattendu : {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_roundtrip_response() {
        let (mut a, mut b) = duplex(4096);
        let resp = Response::Ack {
            version: 42,
            node_id: 1,
        };
        write_message(&mut a, &resp).await.unwrap();
        drop(a);

        let received: Response = read_message(&mut b).await.unwrap();
        match received {
            Response::Ack { version, node_id } => {
                assert_eq!(version, 42);
                assert_eq!(node_id, 1);
            }
            other => panic!("type inattendu : {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_oversized_frame_rejected() {
        let (mut a, mut b) = duplex(64);
        // Écrire une longueur > MAX_FRAME_SIZE
        let too_big: u32 = (MAX_FRAME_SIZE + 1) as u32;
        a.write_all(&too_big.to_be_bytes()).await.unwrap();
        drop(a);

        let result: io::Result<Request> = read_message(&mut b).await;
        assert!(result.is_err());
    }
}
