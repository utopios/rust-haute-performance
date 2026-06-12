// =============================================================================
// kvc.rs – CLI client minimal pour le KV store
// =============================================================================
//
// Usage :
//   kvc --addr 127.0.0.1:50051 put hello world
//   kvc --addr 127.0.0.1:50051 get hello
//   kvc --addr 127.0.0.1:50051 delete hello
//   kvc --addr 127.0.0.1:50051 scan user: 10
// =============================================================================

use clap::{Parser, Subcommand};
use kv_distributed::client::KvClient;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(name = "kvc", about = "Client CLI pour le KV store distribué")]
struct Args {
    /// Adresse du noeud KV
    #[arg(long, default_value = "127.0.0.1:50051")]
    addr: SocketAddr,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Lit une clé
    Get { key: String },
    /// Écrit une clé (avec TTL optionnel en millisecondes)
    Put {
        key: String,
        value: String,
        #[arg(long)]
        ttl_ms: Option<u64>,
    },
    /// Supprime une clé
    Delete { key: String },
    /// Parcourt les clés avec un préfixe
    Scan {
        prefix: String,
        #[arg(default_value_t = 10)]
        limit: u32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let client = KvClient::connect(args.addr).await?;

    match args.cmd {
        Cmd::Get { key } => match client.get(&key).await? {
            Some(v) => {
                let s = String::from_utf8_lossy(&v);
                println!("{s}");
            }
            None => {
                println!("(absent)");
                std::process::exit(2);
            }
        },
        Cmd::Put { key, value, ttl_ms } => {
            let version = client
                .put_with_ttl(&key, value.into_bytes(), ttl_ms)
                .await?;
            println!("OK version={version}");
        }
        Cmd::Delete { key } => {
            let existed = client.delete(&key).await?;
            println!("deleted={existed}");
        }
        Cmd::Scan { prefix, limit } => {
            let items = client.scan(&prefix, limit).await?;
            for (k, v) in items {
                let s = String::from_utf8_lossy(&v);
                println!("{k}\t{s}");
            }
        }
    }
    Ok(())
}
