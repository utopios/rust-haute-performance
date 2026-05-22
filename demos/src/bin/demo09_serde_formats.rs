use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Une métrique unitaire (CPU, RAM, IO, ...)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Metric {
    pub name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

/// Un rapport agrégé envoyé par un agent OVH vers le collecteur central.
/// C'est ce type d'objet qui transite à très haut débit dans la flotte.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricReport {
    pub hostname: String,
    pub datacenter: String,
    pub timestamp: i64,
    pub metrics: Vec<Metric>,
}

impl MetricReport {
    /// Construit un rapport réaliste avec `n` métriques.
    pub fn sample(n: usize) -> Self {
        let mut metrics = Vec::with_capacity(n);
        for i in 0..n {
            let mut labels = HashMap::new();
            labels.insert("core".to_string(), format!("{}", i % 16));
            labels.insert("env".to_string(), "prod".to_string());
            labels.insert("zone".to_string(), "gra-az1".to_string());
            metrics.push(Metric {
                name: format!("cpu_usage_core_{}", i),
                value: 0.42 + (i as f64) * 0.01,
                labels,
            });
        }
        MetricReport {
            hostname: "rbx-srv-0142.ovh.net".to_string(),
            datacenter: "rbx7".to_string(),
            timestamp: 1_715_900_000,
            metrics,
        }
    }
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value & 0x7F) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn write_tag(out: &mut Vec<u8>, field: u32, wire_type: u32) {
    write_varint(out, ((field << 3) | wire_type) as u64);
}

fn write_string(out: &mut Vec<u8>, field: u32, s: &str) {
    write_tag(out, field, 2); // length-delimited
    write_varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

fn write_double(out: &mut Vec<u8>, field: u32, v: f64) {
    write_tag(out, field, 1); // 64-bit fixed
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_int64(out: &mut Vec<u8>, field: u32, v: i64) {
    write_tag(out, field, 0);
    write_varint(out, v as u64);
}

/// Encode un MetricReport façon Protobuf.
/// Mapping des champs :
///   MetricReport: 1=hostname, 2=datacenter, 3=timestamp, 4=metrics (repeated)
///   Metric:       1=name, 2=value, 3=labels (repeated key/value sous-messages)
fn protobuf_like_encode(report: &MetricReport) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    write_string(&mut out, 1, &report.hostname);
    write_string(&mut out, 2, &report.datacenter);
    write_int64(&mut out, 3, report.timestamp);
    for m in &report.metrics {
        // Sous-message Metric : on encode dans un buffer puis on préfixe par sa longueur
        let mut sub = Vec::with_capacity(64);
        write_string(&mut sub, 1, &m.name);
        write_double(&mut sub, 2, m.value);
        for (k, v) in &m.labels {
            // entry : sous-sous-message {key=1, value=2}
            let mut kv = Vec::with_capacity(32);
            write_string(&mut kv, 1, k);
            write_string(&mut kv, 2, v);
            write_tag(&mut sub, 3, 2);
            write_varint(&mut sub, kv.len() as u64);
            sub.extend_from_slice(&kv);
        }
        write_tag(&mut out, 4, 2);
        write_varint(&mut out, sub.len() as u64);
        out.extend_from_slice(&sub);
    }
    out
}

/// Résultat d'un benchmark pour un format donné.
#[derive(Debug, Clone)]
pub struct BenchResult {
    pub format: String,
    pub size_bytes: usize,
    pub encode_ns: u128,
    pub decode_ns: u128,
}

/// Mesure encode + decode sur `iterations` répétitions et retourne la moyenne.
fn bench<F, G>(name: &str, iterations: u32, mut encode: F, mut decode: G) -> BenchResult
where
    F: FnMut() -> Vec<u8>,
    G: FnMut(&[u8]),
{
    // Mesure de taille (1 encode)
    let bytes = encode();
    let size = bytes.len();

    // Mesure encode
    let t = Instant::now();
    for _ in 0..iterations {
        let _ = encode();
    }
    let encode_ns = t.elapsed().as_nanos() / iterations as u128;

    // Mesure decode
    let t = Instant::now();
    for _ in 0..iterations {
        decode(&bytes);
    }
    let decode_ns = t.elapsed().as_nanos() / iterations as u128;

    BenchResult {
        format: name.to_string(),
        size_bytes: size,
        encode_ns,
        decode_ns,
    }
}

fn main() {
    println!("=== Demo : Comparatif de formats de sérialisation OVH ===\n");

    // 1. Construction du rapport métier
    let report = MetricReport::sample(50);
    println!(
        "Rapport généré : {} métriques sur {} ({})",
        report.metrics.len(),
        report.hostname,
        report.datacenter
    );

    let iterations = 5_000;
    println!("Benchmark : {} itérations par format\n", iterations);

    let mut results: Vec<BenchResult> = Vec::new();

    // -----------------------------------------------------------------------
    // JSON (serde_json) — texte, lisible humainement, le moins compact
    // -----------------------------------------------------------------------
    {
        let r = report.clone();
        let bytes_ref = serde_json::to_vec(&r).expect("JSON encode");
        let res = bench(
            "JSON",
            iterations,
            || serde_json::to_vec(&r).unwrap(),
            |b| {
                let _decoded: MetricReport = serde_json::from_slice(b).unwrap();
            },
        );
        // Vérification round-trip : JSON peut introduire de légères erreurs
        // d'arrondi sur les flottants (ex : 0.45 → 0.44999...), on compare donc
        // hostname + nombre de métriques uniquement.
        let decoded: MetricReport = serde_json::from_slice(&bytes_ref).unwrap();
        assert_eq!(decoded.hostname, r.hostname);
        assert_eq!(decoded.metrics.len(), r.metrics.len());
        results.push(res);
    }

    // -----------------------------------------------------------------------
    // Bincode — binaire compact orienté Rust, idéal entre services Rust
    // -----------------------------------------------------------------------
    {
        let r = report.clone();
        let bytes_ref = bincode::serialize(&r).expect("Bincode encode");
        let res = bench(
            "Bincode",
            iterations,
            || bincode::serialize(&r).unwrap(),
            |b| {
                let _decoded: MetricReport = bincode::deserialize(b).unwrap();
            },
        );
        let decoded: MetricReport = bincode::deserialize(&bytes_ref).unwrap();
        assert_eq!(decoded, r);
        results.push(res);
    }

    // -----------------------------------------------------------------------
    // MessagePack (rmp-serde) — binaire interopérable, schema-less
    // -----------------------------------------------------------------------
    {
        let r = report.clone();
        let bytes_ref = rmp_serde::to_vec(&r).expect("MsgPack encode");
        let res = bench(
            "MessagePack",
            iterations,
            || rmp_serde::to_vec(&r).unwrap(),
            |b| {
                let _decoded: MetricReport = rmp_serde::from_slice(b).unwrap();
            },
        );
        let decoded: MetricReport = rmp_serde::from_slice(&bytes_ref).unwrap();
        assert_eq!(decoded, r);
        results.push(res);
    }

    // -----------------------------------------------------------------------
    // Protobuf simulé — binaire compact avec schéma (varint + tags)
    // -----------------------------------------------------------------------
    {
        let r = report.clone();
        let res = bench(
            "Protobuf*",
            iterations,
            || protobuf_like_encode(&r),
            |_b| {
                // Pour la démo, on ne décode pas (un vrai decoder ferait l'inverse)
                // En production, prost génère automatiquement encode + decode.
            },
        );
        results.push(res);
    }

    // -----------------------------------------------------------------------
    // Affichage tabulaire
    // -----------------------------------------------------------------------
    println!(
        "{:<14} | {:>10} | {:>14} | {:>14}",
        "Format", "Taille", "Encode (µs)", "Decode (µs)"
    );
    println!("{}", "-".repeat(62));
    for r in &results {
        println!(
            "{:<14} | {:>8} o | {:>12.2} µs | {:>12.2} µs",
            r.format,
            r.size_bytes,
            r.encode_ns as f64 / 1_000.0,
            r.decode_ns as f64 / 1_000.0
        );
    }

    // -----------------------------------------------------------------------
    // Ratio de compression vs JSON
    // -----------------------------------------------------------------------
    let json_size = results[0].size_bytes as f64;
    println!("\nRatio de taille (vs JSON = 100%) :");
    for r in &results {
        let pct = (r.size_bytes as f64 / json_size) * 100.0;
        println!("  {:<14} : {:>6.1}%", r.format, pct);
    }

    println!("\n* Protobuf simulé : encodage maison (varint + tags). En production,");
    println!("  utilisez prost ou tonic-build pour générer du code depuis un .proto.");
    println!("\nConseil OVH :");
    println!("  - Entre services Rust   : Bincode (plus rapide, plus compact)");
    println!("  - Polyglotte (Go/Java)  : Protobuf (gRPC) ou MessagePack");
    println!("  - Debug / API externe   : JSON (lisible)");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_report_has_n_metrics() {
        let r = MetricReport::sample(10);
        assert_eq!(r.metrics.len(), 10);
        assert_eq!(r.hostname, "rbx-srv-0142.ovh.net");
    }

    #[test]
    fn test_roundtrip_json() {
        let r = MetricReport::sample(5);
        let bytes = serde_json::to_vec(&r).unwrap();
        let back: MetricReport = serde_json::from_slice(&bytes).unwrap();
        // Comparaison structurelle hors flottants (JSON peut introduire des
        // erreurs d'arrondi sur les f64 du type 0.45 → 0.44999999...).
        assert_eq!(r.hostname, back.hostname);
        assert_eq!(r.datacenter, back.datacenter);
        assert_eq!(r.timestamp, back.timestamp);
        assert_eq!(r.metrics.len(), back.metrics.len());
        for (a, b) in r.metrics.iter().zip(back.metrics.iter()) {
            assert_eq!(a.name, b.name);
            assert!((a.value - b.value).abs() < 1e-9);
            assert_eq!(a.labels, b.labels);
        }
    }

    #[test]
    fn test_roundtrip_bincode() {
        let r = MetricReport::sample(5);
        let bytes = bincode::serialize(&r).unwrap();
        let back: MetricReport = bincode::deserialize(&bytes).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn test_roundtrip_msgpack() {
        let r = MetricReport::sample(5);
        let bytes = rmp_serde::to_vec(&r).unwrap();
        let back: MetricReport = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn test_msgpack_smaller_than_json() {
        // MessagePack utilise un encodage binaire des clés/valeurs et reste
        // systématiquement plus compact que JSON sur ce type de payload.
        // Note : bincode peut au contraire être plus volumineux sur des structs
        // contenant beaucoup de petites HashMap<String, String> (overhead de
        // longueurs codées sur 8 octets) — c'est l'un des points de la démo.
        let r = MetricReport::sample(20);
        let json = serde_json::to_vec(&r).unwrap();
        let msg = rmp_serde::to_vec(&r).unwrap();
        assert!(
            msg.len() < json.len(),
            "MessagePack ({}) devrait être plus petit que JSON ({})",
            msg.len(),
            json.len()
        );
    }

    #[test]
    fn test_varint_encoding() {
        let mut out = Vec::new();
        write_varint(&mut out, 300);
        // 300 = 0b00000010_00101100 → varint = 0xAC 0x02
        assert_eq!(out, vec![0xAC, 0x02]);
    }

    #[test]
    fn test_protobuf_like_non_empty() {
        let r = MetricReport::sample(3);
        let bytes = protobuf_like_encode(&r);
        assert!(!bytes.is_empty());
        // Le premier octet doit être le tag (1 << 3) | 2 = 0x0A (hostname, length-delimited)
        assert_eq!(bytes[0], 0x0A);
    }
}
