# Exercice 3 — Pipeline parallèle avec Rayon

**Jour 1 — Module 1.3 : Parallélisme data avec Rayon**
**Durée estimée :** 1h — 1h30
**Prérequis :** Modules 1.1 (mesure), 1.2 (layout), 1.3 (Rayon)
**Démo de référence :** [`demo03_rayon_pipeline`](../demos-stagiaires/src/bin/demo03_rayon_pipeline.rs)

---

## Contexte

Vous reprenez le même service d'agrégation de métriques OVH. Cette fois, le flux d'entrée est constitué de **lignes de logs réseau** au format CSV (générées en mémoire pour les besoins de l'exercice) :

```
timestamp;source_ip;dest_ip;protocol;bytes;duration_ms
1700000000;10.0.0.1;192.168.1.5;TCP;1024;42
1700000001;10.0.0.2;192.168.1.7;UDP;512;15
...
```

Vous devez écrire un pipeline qui, **sur 1 million d'événements** :
1. **Parse** chaque ligne en une struct `NetworkEvent`
2. **Filtre** les événements anormaux (`bytes > 10_000_000` ou `duration_ms > 60_000`)
3. **Agrège** par `source_ip` : total des bytes, durée moyenne, nombre d'événements
4. **Trie** par total des bytes décroissant
5. **Garde** le top 100

Et vous devez le faire **en deux versions** (séquentielle et parallèle avec Rayon) pour mesurer le speedup réel.

---

## Objectifs pédagogiques

1. Maîtriser `par_iter()`, `filter`, `map`, `fold` + `reduce` de Rayon.
2. Comprendre la différence entre **par_iter().collect()** (matérialise) et **par_iter().fold(...).reduce(...)** (agrège en streaming).
3. Mesurer un **speedup réel** et comprendre **pourquoi il n'est jamais égal au nombre de cœurs**.
4. Utiliser `rayon::join` pour du divide-and-conquer hiérarchique.
5. Diagnostiquer les cas où la parallélisation **dégrade** les performances.

---

## Énoncé

### Partie 1 — Modèle de données et parsing

Dans le projet `demos-stagiaires`, créez `src/bin/exo03_pipeline.rs`.

```rust
#[derive(Debug, Clone)]
pub struct NetworkEvent {
    pub timestamp: u64,
    pub source_ip: String,
    pub dest_ip: String,
    pub protocol: String,
    pub bytes: u64,
    pub duration_ms: u64,
}

pub fn parser_ligne(ligne: &str) -> Option<NetworkEvent> {
    // À IMPLÉMENTER : parse "ts;src;dst;proto;bytes;dur"
    // Retourne None si la ligne est malformée.
    todo!()
}
```

**À faire** :
1. Implémenter `parser_ligne` (split par `;`, parse les entiers, gère les lignes malformées sans paniquer).
2. Écrire un générateur déterministe `generer_dataset(n: usize) -> Vec<String>` qui produit n lignes réalistes.

Suggestion pour le générateur (à reprendre tel quel) :

```rust
pub fn generer_dataset(n: usize) -> Vec<String> {
    (0..n).map(|i| {
        let src = format!("10.0.0.{}", (i % 8) + 1);
        let dst = format!("192.168.1.{}", (i % 16) + 1);
        let proto = if i % 3 == 0 { "TCP" } else if i % 3 == 1 { "UDP" } else { "ICMP" };
        let bytes = (i % 5_000_000) as u64 + 100;       // jamais > 5 Mo → tous valides
        let dur = (i % 30_000) as u64 + 1;              // jamais > 30 s
        let ts = 1_700_000_000u64 + i as u64;
        format!("{ts};{src};{dst};{proto};{bytes};{dur}")
    }).collect()
}
```

### Partie 2 — Pipeline séquentiel

```rust
pub struct IpStats {
    pub ip: String,
    pub total_bytes: u64,
    pub total_duration_ms: u64,
    pub event_count: u64,
}

impl IpStats {
    pub fn avg_duration_ms(&self) -> f64 {
        self.total_duration_ms as f64 / self.event_count.max(1) as f64
    }
}

pub fn pipeline_sequentiel(lignes: &[String]) -> Vec<IpStats> {
    // 1. parse (filter_map sur parser_ligne)
    // 2. filtre bytes > 10M ou duration > 60s
    // 3. agrège par source_ip (HashMap<String, IpStats>)
    // 4. trie par total_bytes décroissant
    // 5. garde le top 100
    todo!()
}
```

### Partie 3 — Pipeline parallèle

```rust
use rayon::prelude::*;

pub fn pipeline_parallele(lignes: &[String]) -> Vec<IpStats> {
    // Même résultat que pipeline_sequentiel, mais en utilisant par_iter().
    // Indice : pour l'agrégation, utiliser fold + reduce :
    //   .fold(HashMap::new, |mut acc, ev| { ... ; acc })
    //   .reduce(HashMap::new, |a, b| fusionner(a, b))
    todo!()
}
```

**Difficulté clé** : `par_iter().fold().reduce()` est le pattern fondamental pour les **agrégations parallèles**. Chaque thread accumule dans son propre HashMap, puis on fusionne les HashMaps deux à deux. Sans ce pattern, l'agrégation est sérielle malgré le `par_iter`.

### Partie 4 — Vérification de cohérence

Sur 100 000 événements :
1. Lancer les deux pipelines.
2. Vérifier que les résultats sont **strictement identiques** (mêmes IPs dans le même ordre, mêmes chiffres).
3. Si non, débugger : un comparateur de tri non-stable peut générer un ordre différent quand deux IPs ont le même `total_bytes` — utiliser un tri stable secondaire sur `ip` pour départager.

### Partie 5 — Benchmark

#### 5.1 Mini-bench manuel

Sur 1 million d'événements (uniquement le pipeline lui-même, le parsing ET l'agrégation ; le dataset est généré une seule fois en dehors du chrono) :
- Mesurer le temps moyen sur 20 itérations (warm-up de 3).
- Calculer le speedup `seq / par`.

#### 5.2 Bench Criterion

Créer `benches/bench_exo03.rs` :
- Tester sur 3 tailles : 10 000, 100 000, 1 000 000.
- Quatre fonctions : `pipeline_sequentiel`, `pipeline_parallele`, et **bonus** `parse_only_seq`, `parse_only_par` (juste l'étape parse, pour isoler le gain).

Ajouter dans `Cargo.toml` :
```toml
[[bench]]
name = "bench_exo03"
harness = false
```

---

## Questions d'analyse (à rendre)

1. **Quel speedup avez-vous obtenu ?** Combien de cœurs a votre machine (`std::thread::available_parallelism()`) ? Le speedup est-il égal au nombre de cœurs ? Pourquoi pas ?

2. **Que se passe-t-il à 10 000 événements ?** Lancez le bench Criterion sur cette taille. Le pipeline parallèle est-il toujours plus rapide ? Sinon, pourquoi ?

3. **Pourquoi `fold + reduce` au lieu d'un `Mutex<HashMap>`** partagé entre tous les threads ? Implémentez (juste pour le constater) une version avec Mutex et comparez. Donnez un ordre de grandeur du speedup perdu.

4. **L'étape parse seule** : si vous ne parallélisez QUE le parsing (et que vous gardez l'agrégation séquentielle), combien gagnez-vous ? Est-ce que le gain combiné (parse + agreg paralléliser ensemble) est égal à la somme des deux ? Pourquoi pas exactement ?

5. **`available_parallelism`** retourne souvent un nombre supérieur au nombre de cœurs physiques (à cause du SMT / hyperthreading). Utiliser tous ces threads vous donne-t-il un meilleur speedup que d'utiliser uniquement les cœurs physiques ? (Indice : `rayon::ThreadPoolBuilder::new().num_threads(N).build_global()` pour limiter.)

---

## Bonus (facultatif)

- **B1.** Implémentez l'agrégation **sans HashMap** avec un `Vec<IpStats>` indexé manuellement (les IPs sont sous la forme `10.0.0.X` avec X ∈ [1..8], donc on peut les indexer par X-1). Mesurez : on devrait être beaucoup plus rapide qu'avec HashMap. Pourquoi ?

- **B2.** Profiler avec `samply` le pipeline parallèle pour identifier où passent les threads. Si la majorité du temps est passée dans la fusion des HashMaps, c'est qu'on a un problème de **last reducer** : trop peu de travail pour les threads, trop de coordination.

- **B3.** Utilisez `rayon::join` pour faire la lecture du fichier et le parsing **en parallèle entre eux**. (Dans cet exo le fichier est déjà en mémoire, mais imaginez qu'on lise depuis stdin.)

---

## Critères d'évaluation

| Critère | Poids |
|---|---|
| `parser_ligne` correct (rejette les lignes malformées proprement) | 10% |
| Pipeline séquentiel donne le bon résultat (vérifiable contre un script Python par exemple) | 20% |
| Pipeline parallèle utilise `fold + reduce` et donne le **même** résultat | 25% |
| Mini-bench montre un speedup > 2× sur 1M événements | 15% |
| Bench Criterion fonctionne sur 3 tailles | 10% |
| Réponses chiffrées et raisonnées aux 5 questions | 20% |

---

## Indices utiles

- **Pattern d'agrégation parallèle canonique** :
  ```rust
  data.par_iter()
      .fold(
          || HashMap::new(),                         // identité par thread
          |mut acc, ev| { acc.entry(ev.source_ip.clone()).or_insert(IpStats { ... }).accumulate(ev); acc },
      )
      .reduce(
          || HashMap::new(),                         // identité globale
          |mut a, b| { for (k, v) in b { a.entry(k).or_insert_with(...).merge(v); } a },
      )
  ```
- **Piège du `clone()` excessif** : `ev.source_ip.clone()` à chaque ligne coûte une allocation. Pour optimiser, on peut utiliser `Cow<str>`, des `Arc<str>`, ou interner les IPs. Pas obligatoire pour cet exo mais à noter.
- **Top 100** : `vec.sort_unstable_by(|a, b| b.total_bytes.cmp(&a.total_bytes))` puis `vec.truncate(100)`. Pour gros volume : `select_nth_unstable_by` peut être plus rapide (vu dans l'exo 1).
- **Compter les cœurs** : `std::thread::available_parallelism().unwrap().get()` retourne un `usize`.
- **Forcer un nombre de threads pour Rayon** : `rayon::ThreadPoolBuilder::new().num_threads(N).build_global().unwrap();` à appeler une seule fois au début du programme.
