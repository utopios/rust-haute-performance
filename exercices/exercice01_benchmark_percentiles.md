# Exercice 1 — Benchmark de calcul de percentiles de latence

**Jour 1 — Module 1.1 : Profiling et benchmarking**
**Durée estimée :** 45 min — 1h
**Prérequis :** Module 1.1 (Criterion, mesure du temps, `cargo bench`)


---

## Contexte

Vous travaillez sur la collecte de métriques de latence d'une API. Toutes les minutes, le système agrège un échantillon de 100 000 mesures de latence (en µs) et doit calculer trois statistiques :

- la **médiane** (p50)
- le **99ᵉ percentile** (p99)
- le **maximum**

Le calcul actuel est trop lent : il bloque l'agrégation pendant plusieurs centaines de millisecondes. Votre mission est de **mesurer rigoureusement** trois stratégies de calcul et de choisir la meilleure pour mettre en production.

> Vous ne devez **pas** utiliser de parallélisme (Rayon) ni de SIMD : ces sujets seront vus dans les modules suivants. L'enjeu de cet exercice est uniquement la **méthodologie de benchmark** et le choix algorithmique.

---

## Objectifs pédagogiques

À la fin de l'exercice, vous devez savoir :

1. Implémenter plusieurs variantes algorithmiques d'un même problème.
2. Écrire un **mini-bench manuel** avec `std::time::Instant` (échauffement, `black_box`, mesure sur N itérations).
3. Écrire un **bench Criterion** rigoureux (cf. `benches/bench_demo01.rs` de la démo).
4. **Comparer et interpréter** les résultats (médiane, écart-type, choix optimal en fonction de la taille).
5. Justifier votre conclusion avec des chiffres reproductibles.

---

## Énoncé

### Partie 1 — Les trois stratégies à implémenter

Dans le projet `demos-stagiaires`, ajoutez un fichier `src/bin/exo01_percentiles.rs` exposant les trois fonctions suivantes :

```rust
/// Statistiques calculées
#[derive(Debug, PartialEq)]
pub struct Stats {
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
}
```

#### Stratégie A — Tri complet
Trie tout le vecteur, puis indexe `[len/2]`, `[len*99/100]`, `[len-1]`.

```rust
pub fn stats_par_tri(data: &[u64]) -> Stats {
    // À IMPLÉMENTER
    // Indice : `let mut v = data.to_vec(); v.sort_unstable();`
    todo!()
}
```

#### Stratégie B — Sélection partielle (`select_nth_unstable`)
Utilise la méthode de la bibliothèque standard `slice::select_nth_unstable`, qui effectue un **quickselect** : O(n) en moyenne, sans trier tout le vecteur.

```rust
pub fn stats_par_select(data: &[u64]) -> Stats {
    // À IMPLÉMENTER
    // Indice : `v.select_nth_unstable(idx_p50)` repositionne l'élément à sa
    // place finale ; les éléments à gauche sont ≤, ceux à droite sont ≥.
    // Attention à l'ordre des appels et aux invariants entre eux.
    todo!()
}
```

#### Stratégie C — Histogramme (comptage par classes)
Les latences sont bornées (par exemple entre 0 et 10 000 µs en production). On peut donc construire un **histogramme** à 10 001 cases (`Vec<u32>`) et déduire les percentiles par balayage cumulé.

```rust
pub fn stats_par_histogramme(data: &[u64], borne_max: u64) -> Stats {
    // À IMPLÉMENTER
    // Indice : `let mut hist = vec![0u32; (borne_max + 1) as usize];`
    // Incrémenter hist[lat as usize] pour chaque latence, puis parcourir
    // en cumulant jusqu'à atteindre les seuils.
    todo!()
}
```

### Partie 2 — Vérification de cohérence

Dans `main()`, générez un dataset de 100 000 latences simulées (par exemple : `(0..100_000u64).map(|i| (i * 37) % 5000 + 1).collect()`), puis :

1. Calculez `r1`, `r2`, `r3` avec les trois stratégies.
2. Vérifiez avec `assert_eq!` que les trois donnent **exactement le même résultat**.
3. Si ce n'est pas le cas, corrigez avant de benchmarker (benchmarker un calcul faux n'a aucun sens).

### Partie 3 — Mini-benchmark manuel

Reproduisez la fonction `bench_manuel` de [`demo01_criterion_benchmark.rs`](../demos-stagiaires/src/bin/demo01_criterion_benchmark.rs) et mesurez les trois stratégies sur **50 itérations**, avec **5 itérations d'échauffement** pour stabiliser les caches.

Affichez :
- Temps total, temps moyen par itération
- Speedup relatif à la stratégie A (tri complet)

Lancement attendu :
```bash
cd demos-stagiaires
cargo run --release --bin exo01_percentiles
```

### Partie 4 — Benchmark Criterion

Créez `benches/bench_exo01.rs` sur le modèle de `benches/bench_demo01.rs`. Le bench doit :

- Tester les **trois stratégies**
- Sur **trois tailles** : 1 000, 10 000, 100 000
- Avec un **dataset stable** (même seed pour chaque run, pour la reproductibilité)

N'oubliez pas d'ajouter dans `Cargo.toml` :
```toml
[[bench]]
name = "bench_exo01"
harness = false
```

Lancement :
```bash
cargo bench --bench bench_exo01
```

Le rapport HTML sera dans `target/criterion/report/index.html`.

---

## Questions d'analyse (à rendre)

Répondez à ces questions dans un fichier `exo01_analyse.md` à côté de votre code :

1. **Sur quelle taille les trois stratégies s'égalisent-elles ?** À l'inverse, quelle stratégie écrase les autres sur 100 000 éléments ? De quel ordre de grandeur ?

2. **Pourquoi le tri complet est-il sous-optimal ici ?** Quelle est sa complexité ? Pourquoi `select_nth_unstable` peut faire mieux **en théorie**, et est-ce vérifié en pratique sur vos chiffres ?

3. **L'histogramme est typiquement très rapide, mais a une limite.** Laquelle ? Que se passerait-il si la borne max était `1_000_000_000` au lieu de `10_000` ?

4. **Comparaison mini-bench vs Criterion :** vos deux mesures concordent-elles ? Si non, lequel des deux est le plus fiable et pourquoi ? Donnez un exemple chiffré tiré de **vos** résultats.

5. **En production**, vous devez choisir UNE seule implémentation. Laquelle, et pourquoi ? Y a-t-il un cas où votre choix serait remis en cause (dataset différent, contraintes mémoire, etc.) ?

---

## Bonus (facultatif)

- **B1.** Ajoutez une variante D : tri par insertion sur un buffer trié de taille fixe (par exemple, top-100 pour le p99 sans tout trier). Ne marche que pour les percentiles extrêmes — discutez.
- **B2.** Lancez `cargo flamegraph --bin exo01_percentiles` (si `flamegraph` est installé) et identifiez la fonction qui consomme le plus de CPU dans votre stratégie la plus lente. Joignez le SVG.
- **B3.** Faites varier la taille du dataset (1k, 10k, 100k, 1M, 10M) dans le bench Criterion et tracez visuellement le croisement éventuel entre stratégies.

---

## Critères d'évaluation

| Critère | Poids |
|---|---|
| Les trois implémentations sont correctes (mêmes résultats) | 30% |
| Le mini-bench manuel est en place et fonctionne | 15% |
| Le bench Criterion est configuré et donne des chiffres | 25% |
| Les réponses aux 5 questions d'analyse sont chiffrées et raisonnées | 25% |
| Le code compile sans warnings et reste lisible | 5% |

---

## Indices utiles

- `Vec::sort_unstable` est généralement 2–3× plus rapide que `sort` (ne maintient pas l'ordre des égaux, ce qui n'a aucune importance pour des `u64`).
- `select_nth_unstable` modifie le vecteur en place : tu peux le réutiliser pour calculer les trois percentiles d'affilée si tu fais attention à l'ordre.
- Pour Criterion, `black_box(data)` empêche le compilateur d'optimiser la fonction en constante. Sans lui, certains benchs mesurent 0 ns !
- `cargo bench --bench bench_exo01 -- --quick` lance une version rapide (~30s) pour itérer ; le mode complet (~3–5 min) sert au rendu final.
