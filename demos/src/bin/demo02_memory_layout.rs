use smallvec::{smallvec, SmallVec};
use std::mem::{align_of, size_of};
use std::time::Instant;

/// Mauvais ordre de champs : padding gaspillé.
/// Note : `#[repr(C)]` est nécessaire pour empêcher le compilateur de réordonner
/// les champs automatiquement (optimisation activée par défaut sur les structs Rust).
#[repr(C)]
#[derive(Default)]
pub struct MetriqueMauvaise {
    actif: bool,       // 1 octet + 7 de padding (pour aligner le u64 qui suit)
    latence_ns: u64,   // 8 octets
    flag: bool,        // 1 octet + 7 de padding final
}

/// Bon ordre : champs triés du plus grand au plus petit.
#[repr(C)]
#[derive(Default)]
pub struct MetriqueBonne {
    latence_ns: u64,   // 8 octets
    actif: bool,       // 1 octet
    flag: bool,        // 1 octet (+ 6 de padding final seulement)
}

/// Variante encore plus compacte avec `#[repr(C, packed)]`
/// Attention : `packed` désactive l'alignement → accès non-alignés possibles
/// (lents sur certaines architectures, interdits sur d'autres).
#[repr(C, packed)]
#[derive(Default, Clone, Copy)]
pub struct MetriquePacked {
    latence_ns: u64,
    actif: bool,
    flag: bool,
}

/// Représentation AoS : une seule liste, chaque entrée contient tous les champs.
#[derive(Clone)]
pub struct PointAoS {
    pub latence_ns: u64,
    pub statut: u32,
    pub source_id: u32,
}

pub struct MesuresAoS {
    pub points: Vec<PointAoS>,
}

impl MesuresAoS {
    pub fn nouvelle(n: usize) -> Self {
        let points = (0..n)
            .map(|i| PointAoS {
                latence_ns: (i as u64 % 10_000) + 1,
                statut: (i as u32) % 4,
                source_id: (i as u32) % 16,
            })
            .collect();
        MesuresAoS { points }
    }

    /// Somme uniquement les latences. AoS doit lire 16 octets par point
    /// pour n'en utiliser que 8 → mauvaise utilisation du cache.
    pub fn somme_latences(&self) -> u64 {
        self.points.iter().map(|p| p.latence_ns).sum()
    }
}

/// Représentation SoA : un Vec par champ. Les latences sont stockées
/// de manière contiguë → idéal pour les boucles qui n'utilisent qu'un champ.
pub struct MesuresSoA {
    pub latences_ns: Vec<u64>,
    pub statuts: Vec<u32>,
    pub source_ids: Vec<u32>,
}

impl MesuresSoA {
    pub fn nouvelle(n: usize) -> Self {
        let mut latences = Vec::with_capacity(n);
        let mut statuts = Vec::with_capacity(n);
        let mut sources = Vec::with_capacity(n);
        for i in 0..n {
            latences.push((i as u64 % 10_000) + 1);
            statuts.push((i as u32) % 4);
            sources.push((i as u32) % 16);
        }
        MesuresSoA {
            latences_ns: latences,
            statuts,
            source_ids: sources,
        }
    }

    /// Somme uniquement les latences depuis le Vec<u64> contigu.
    /// Le CPU prefetch toutes les latences à pleine bande passante.
    pub fn somme_latences(&self) -> u64 {
        self.latences_ns.iter().sum()
    }
}

/// Tag attaché à une métrique OVH. La plupart des métriques ont 0 à 4 tags,
/// d'où le choix de SmallVec<[…; 4]>.
#[derive(Clone, Default)]
pub struct MetriqueAvecTags {
    pub valeur: u64,
    pub tags: SmallVec<[(&'static str, &'static str); 4]>,
}

impl MetriqueAvecTags {
    pub fn ajouter_tag(&mut self, cle: &'static str, val: &'static str) {
        self.tags.push((cle, val));
    }

    pub fn est_inline(&self) -> bool {
        // Si la capacité courante == capacité inline, aucune allocation heap n'a eu lieu
        !self.tags.spilled()
    }
}

fn main() {
    println!("=== Demo : Layout mémoire et optimisations ===\n");

    // -------------------------------------------------------------------------
    // 1. Alignement et padding
    // -------------------------------------------------------------------------
    println!("--- 1. Alignement et padding d'une struct ---");
    println!(
        "MetriqueMauvaise : size = {} octets, align = {} octets",
        size_of::<MetriqueMauvaise>(),
        align_of::<MetriqueMauvaise>()
    );
    println!(
        "MetriqueBonne    : size = {} octets, align = {} octets",
        size_of::<MetriqueBonne>(),
        align_of::<MetriqueBonne>()
    );
    println!(
        "MetriquePacked   : size = {} octets, align = {} octets",
        size_of::<MetriquePacked>(),
        align_of::<MetriquePacked>()
    );
    let ratio = size_of::<MetriqueMauvaise>() as f64 / size_of::<MetriqueBonne>() as f64;
    println!("→ Économie en réordonnant : ratio mauvaise/bonne = {ratio:.2}\n");

    // -------------------------------------------------------------------------
    // 2. AoS vs SoA — benchmark d'agrégation
    // -------------------------------------------------------------------------
    println!("--- 2. AoS vs SoA sur 1 000 000 de points ---");
    let n = 1_000_000;
    let aos = MesuresAoS::nouvelle(n);
    let soa = MesuresSoA::nouvelle(n);

    // Vérification que les deux structures contiennent les mêmes données
    assert_eq!(aos.somme_latences(), soa.somme_latences());

    // Warm-up
    let _ = aos.somme_latences();
    let _ = soa.somme_latences();

    let debut = Instant::now();
    let mut acc_aos = 0u64;
    for _ in 0..20 {
        acc_aos = std::hint::black_box(aos.somme_latences());
    }
    let dur_aos = debut.elapsed();

    let debut = Instant::now();
    let mut acc_soa = 0u64;
    for _ in 0..20 {
        acc_soa = std::hint::black_box(soa.somme_latences());
    }
    let dur_soa = debut.elapsed();

    println!("AoS  : somme = {acc_aos}, durée totale = {dur_aos:?}");
    println!("SoA  : somme = {acc_soa}, durée totale = {dur_soa:?}");
    let speedup = dur_aos.as_nanos() as f64 / dur_soa.as_nanos().max(1) as f64;
    println!("→ Speedup SoA vs AoS : {speedup:.2}x\n");

    // Taille mémoire des deux représentations
    let mem_aos = aos.points.capacity() * size_of::<PointAoS>();
    let mem_soa = soa.latences_ns.capacity() * size_of::<u64>()
        + soa.statuts.capacity() * size_of::<u32>()
        + soa.source_ids.capacity() * size_of::<u32>();
    println!("Mémoire AoS : ~{} Mo", mem_aos / 1_048_576);
    println!("Mémoire SoA : ~{} Mo\n", mem_soa / 1_048_576);

    // -------------------------------------------------------------------------
    // 3. SmallVec
    // -------------------------------------------------------------------------
    println!("--- 3. SmallVec : stockage inline jusqu'à 4 éléments ---");
    let mut m = MetriqueAvecTags::default();
    m.valeur = 42;
    println!(
        "Aucune insertion       : tags inline ? {} (len = {})",
        m.est_inline(),
        m.tags.len()
    );

    m.ajouter_tag("region", "gra");
    m.ajouter_tag("service", "compute");
    println!(
        "2 tags ajoutés          : tags inline ? {} (len = {})",
        m.est_inline(),
        m.tags.len()
    );

    m.ajouter_tag("env", "prod");
    m.ajouter_tag("host", "srv-01");
    println!(
        "4 tags (capacité inline): tags inline ? {} (len = {})",
        m.est_inline(),
        m.tags.len()
    );

    m.ajouter_tag("dc", "rbx7");
    println!(
        "5 tags (débordement)    : tags inline ? {} (len = {})",
        m.est_inline(),
        m.tags.len()
    );

    // Taille de la struct hôte avec SmallVec inline
    println!(
        "\nTaille de MetriqueAvecTags : {} octets (incluant 4 tags inline)",
        size_of::<MetriqueAvecTags>()
    );

    // Comparaison rapide : 100k structures avec SmallVec vs avec Vec
    let n_small = 100_000;
    let debut = Instant::now();
    let _stockage: SmallVec<[u32; 8]> = smallvec![1, 2, 3];
    for _ in 0..n_small {
        let sv: SmallVec<[u32; 8]> = smallvec![1, 2, 3, 4];
        std::hint::black_box(sv);
    }
    let dur_smallvec = debut.elapsed();

    let debut = Instant::now();
    for _ in 0..n_small {
        let v: Vec<u32> = vec![1, 2, 3, 4];
        std::hint::black_box(v);
    }
    let dur_vec = debut.elapsed();

    println!("\n--- Micro-bench création de {n_small} petits conteneurs ---");
    println!("SmallVec<[u32;8]> : {dur_smallvec:?}");
    println!("Vec<u32>          : {dur_vec:?}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_taille_mauvaise_struct() {
        // 24 octets attendus à cause du double padding
        assert_eq!(size_of::<MetriqueMauvaise>(), 24);
    }

    #[test]
    fn test_taille_bonne_struct() {
        // 16 octets : u64 + 2 bool + padding final
        assert_eq!(size_of::<MetriqueBonne>(), 16);
    }

    #[test]
    fn test_taille_packed_struct() {
        // packed retire tout le padding : 8 + 1 + 1 = 10 octets
        assert_eq!(size_of::<MetriquePacked>(), 10);
    }

    #[test]
    fn test_coherence_aos_soa() {
        let aos = MesuresAoS::nouvelle(1_000);
        let soa = MesuresSoA::nouvelle(1_000);
        assert_eq!(aos.somme_latences(), soa.somme_latences());
    }

    #[test]
    fn test_smallvec_inline_puis_spill() {
        let mut m = MetriqueAvecTags::default();
        for _ in 0..4 {
            m.ajouter_tag("k", "v");
        }
        assert!(m.est_inline(), "4 tags doivent rester inline");

        m.ajouter_tag("k", "v");
        assert!(!m.est_inline(), "5 tags doivent provoquer un spill heap");
    }

    #[test]
    fn test_smallvec_contenu() {
        let mut m = MetriqueAvecTags::default();
        m.ajouter_tag("a", "1");
        m.ajouter_tag("b", "2");
        assert_eq!(m.tags.len(), 2);
        assert_eq!(m.tags[0], ("a", "1"));
        assert_eq!(m.tags[1], ("b", "2"));
    }
}
