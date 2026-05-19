use rayon::prelude::*;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct EvenementLog {
    pub source_ip: String,
    pub niveau: String, // "INFO", "WARN", "ERROR"
    pub latence_ms: u32,
}

/// Parse une ligne du format `"<ip>;<niveau>;<latence>"`
/// Retourne None si la ligne est malformée.
pub fn parser_ligne(ligne: &str) -> Option<EvenementLog> {
    let mut parts = ligne.split(';');
    let source_ip = parts.next()?.to_string();
    let niveau = parts.next()?.to_string();
    let latence_ms: u32 = parts.next()?.parse().ok()?;
    Some(EvenementLog {
        source_ip,
        niveau,
        latence_ms,
    })
}

/// Génère un jeu de données réaliste de logs OVH simulés.
pub fn generer_logs(n: usize) -> Vec<String> {
    let ips = [
        "10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5",
        "10.0.0.6", "10.0.0.7", "10.0.0.8",
    ];
    let niveaux = ["INFO", "WARN", "ERROR"];
    (0..n)
        .map(|i| {
            let ip = ips[i % ips.len()];
            let niv = niveaux[i % niveaux.len()];
            let lat = ((i * 7) % 500) as u32 + 10;
            format!("{ip};{niv};{lat}")
        })
        .collect()
}

pub fn pipeline_sequentiel(lignes: &[String]) -> HashMap<String, u64> {
    lignes
        .iter()
        .filter_map(|l| parser_ligne(l))
        .filter(|e| e.niveau == "ERROR")
        .fold(HashMap::new(), |mut acc, e| {
            *acc.entry(e.source_ip).or_insert(0u64) += e.latence_ms as u64;
            acc
        })
}

pub fn pipeline_parallele(lignes: &[String]) -> HashMap<String, u64> {
    lignes
        .par_iter()
        // Phase 1 : parsing parallèle (chaque thread parse une part du Vec)
        .filter_map(|l| parser_ligne(l))
        // Phase 2 : filtrage parallèle des erreurs
        .filter(|e| e.niveau == "ERROR")
        // Phase 3 : fold local par thread, puis reduce global
        // Chaque thread Rayon construit son propre HashMap local (fold)
        // puis tous les HashMaps locaux sont fusionnés (reduce).
        .fold(
            HashMap::new,
            |mut acc, e| {
                *acc.entry(e.source_ip).or_insert(0u64) += e.latence_ms as u64;
                acc
            },
        )
        .reduce(HashMap::new, fusion_hashmaps)
}

/// Fonction de fusion utilisée par `reduce`.
/// Combine deux HashMaps en sommant les valeurs des clés communes.
fn fusion_hashmaps(
    mut a: HashMap<String, u64>,
    b: HashMap<String, u64>,
) -> HashMap<String, u64> {
    for (k, v) in b {
        *a.entry(k).or_insert(0) += v;
    }
    a
}

pub fn somme_recursive(data: &[u64]) -> u64 {
    // Seuil en dessous duquel on reste séquentiel (coût du fork > gain)
    const SEUIL: usize = 4_096;

    if data.len() <= SEUIL {
        return data.iter().sum();
    }
    let mid = data.len() / 2;
    let (gauche, droite) = data.split_at(mid);

    // rayon::join exécute les deux closures en parallèle
    // et retourne quand les deux sont terminées.
    let (s_g, s_d) = rayon::join(|| somme_recursive(gauche), || somme_recursive(droite));
    s_g + s_d
}

pub fn max_par_chunks(data: &[u32]) -> u32 {
    data.par_chunks(1_024)
        .map(|chunk| chunk.iter().copied().max().unwrap_or(0))
        .max()
        .unwrap_or(0)
}

fn main() {
    println!("=== Demo : Pipeline parallèle avec Rayon ===\n");

    // Génération de 1 000 000 de lignes de logs simulées
    let n = 1_000_000;
    println!("Génération de {n} lignes de logs...");
    let lignes = generer_logs(n);
    println!("Exemple : {}\n", lignes[0]);

    // -------------------------------------------------------------------------
    // Pipeline séquentiel (référence)
    // -------------------------------------------------------------------------
    let debut = Instant::now();
    let agreg_seq = pipeline_sequentiel(&lignes);
    let dur_seq = debut.elapsed();
    println!("--- Pipeline séquentiel ---");
    println!("Agrégation : {} IPs, durée = {dur_seq:?}", agreg_seq.len());

    // -------------------------------------------------------------------------
    // Pipeline parallèle (Rayon)
    // -------------------------------------------------------------------------
    let debut = Instant::now();
    let agreg_par = pipeline_parallele(&lignes);
    let dur_par = debut.elapsed();
    println!("\n--- Pipeline parallèle (Rayon) ---");
    println!("Agrégation : {} IPs, durée = {dur_par:?}", agreg_par.len());

    // Vérification : les deux pipelines doivent donner le même résultat
    assert_eq!(agreg_seq, agreg_par, "les deux pipelines divergent !");

    let speedup = dur_seq.as_nanos() as f64 / dur_par.as_nanos().max(1) as f64;
    println!("\n→ Speedup parallèle : {speedup:.2}x");
    println!("  (sur une machine à {} cœurs Rayon)", rayon::current_num_threads());

    // Affichage du top 3 des IPs les plus erronées
    println!("\n--- Top 3 des IPs avec le plus de latence cumulée sur les ERREURS ---");
    let mut classement: Vec<_> = agreg_par.iter().collect();
    classement.sort_by(|a, b| b.1.cmp(a.1));
    for (i, (ip, total)) in classement.iter().take(3).enumerate() {
        println!("  {}. {ip} → {total} ms cumulés", i + 1);
    }

    // -------------------------------------------------------------------------
    // Démo rayon::join — somme récursive
    // -------------------------------------------------------------------------
    println!("\n--- rayon::join : somme récursive divide-and-conquer ---");
    let nombres: Vec<u64> = (1..=1_000_000u64).collect();
    let attendu: u64 = nombres.iter().sum();

    let debut = Instant::now();
    let calculee = somme_recursive(&nombres);
    let dur = debut.elapsed();
    println!("Somme 1..1M (récursive) : {calculee} (attendue {attendu}), durée = {dur:?}");
    assert_eq!(calculee, attendu);

    // -------------------------------------------------------------------------
    // Démo par_chunks — max par bloc
    // -------------------------------------------------------------------------
    println!("\n--- par_chunks : max par paquets de 1024 ---");
    let valeurs: Vec<u32> = (0..1_000_000u32).map(|i| (i * 13) % 999_983).collect();
    let debut = Instant::now();
    let m = max_par_chunks(&valeurs);
    let dur = debut.elapsed();
    println!("Max trouvé : {m}, durée = {dur:?}");

    println!("\n[Fin] Le pipeline parallèle a traité {n} événements et fusionné les agrégats par IP.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_ligne_valide() {
        let e = parser_ligne("10.0.0.1;ERROR;42").unwrap();
        assert_eq!(e.source_ip, "10.0.0.1");
        assert_eq!(e.niveau, "ERROR");
        assert_eq!(e.latence_ms, 42);
    }

    #[test]
    fn test_parser_ligne_invalide() {
        assert!(parser_ligne("pas de séparateurs").is_none());
        assert!(parser_ligne("10.0.0.1;INFO;abc").is_none());
        assert!(parser_ligne("10.0.0.1;INFO").is_none());
    }

    #[test]
    fn test_pipelines_equivalents() {
        let lignes = generer_logs(10_000);
        let seq = pipeline_sequentiel(&lignes);
        let par = pipeline_parallele(&lignes);
        assert_eq!(seq, par);
    }

    #[test]
    fn test_pipeline_filtre_uniquement_erreurs() {
        let lignes = vec![
            "10.0.0.1;INFO;10".to_string(),
            "10.0.0.1;WARN;20".to_string(),
            "10.0.0.1;ERROR;30".to_string(),
            "10.0.0.2;ERROR;40".to_string(),
        ];
        let agreg = pipeline_parallele(&lignes);
        assert_eq!(agreg.get("10.0.0.1"), Some(&30));
        assert_eq!(agreg.get("10.0.0.2"), Some(&40));
    }

    #[test]
    fn test_somme_recursive() {
        let data: Vec<u64> = (1..=10_000u64).collect();
        let attendu: u64 = 10_000 * 10_001 / 2;
        assert_eq!(somme_recursive(&data), attendu);
    }

    #[test]
    fn test_max_par_chunks() {
        let data: Vec<u32> = (0..50_000u32).collect();
        assert_eq!(max_par_chunks(&data), 49_999);
    }

    #[test]
    fn test_fusion_hashmaps() {
        let mut a = HashMap::new();
        a.insert("x".to_string(), 10u64);
        a.insert("y".to_string(), 20u64);
        let mut b = HashMap::new();
        b.insert("x".to_string(), 5u64);
        b.insert("z".to_string(), 15u64);
        let fusion = fusion_hashmaps(a, b);
        assert_eq!(fusion.get("x"), Some(&15));
        assert_eq!(fusion.get("y"), Some(&20));
        assert_eq!(fusion.get("z"), Some(&15));
    }
}
