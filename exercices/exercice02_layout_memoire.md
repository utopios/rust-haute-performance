# Exercice 2 — Optimisation mémoire : layout, AoS/SoA, SmallVec

**Jour 1 — Module 1.2 : Optimisations mémoire**
**Durée estimée :** 1h — 1h15
**Prérequis :** Module 1.1 (mesure rigoureuse avec mini-bench et Criterion) + Module 1.2 (alignement, padding, SoA, SmallVec)
**Démo de référence :** [`demo02_memory_layout`](../demos-stagiaires/src/bin/demo02_memory_layout.rs)

---

## Contexte

Vous travaillez sur un système d'agrégation de **métriques d'observabilité** OVH. Chaque seconde, le système reçoit ~1 million de **points de mesure** depuis les agents distribués sur les serveurs. Chaque point contient :

- une **latence** en nanosecondes (`u64`)
- un **timestamp** Unix en nanosecondes (`u64`)
- un **flag actif** (`bool`)
- un **niveau de sévérité** (`u8`)
- une **petite liste de tags** (souvent 1 à 4 tags, rarement plus)

Deux opérations chaudes :
1. **Agrégation** : calculer p50 / p99 / max des latences sur les 1M points (utilise uniquement `latence`)
2. **Filtrage** : ne garder que les points actifs avec sévérité ≥ 2 (utilise `actif`, `niveau` et `latence`)

Votre objectif est d'optimiser le **layout mémoire** de ces points pour que les deux opérations chaudes soient les plus rapides possible.

> Vous n'utiliserez ni Rayon (vu après) ni SIMD (vu après) ni de profileur externe. Tout le gain doit venir du **layout** lui-même.

---

## Objectifs pédagogiques

1. Diagnostiquer le **padding gaspillé** d'une struct avec `std::mem::size_of` et `align_of`.
2. Comparer **AoS (Array of Structs)** et **SoA (Struct of Arrays)** sur des cas d'accès partiel.
3. Mesurer l'impact de **SmallVec** sur des collections "presque toujours petites".
4. Comprendre pourquoi le **cache CPU** est le vrai bottleneck moderne et savoir mesurer son effet indirectement.

---

## Énoncé

### Partie 1 — Layout d'une struct : trouver le padding caché

Dans le projet `demos-stagiaires`, créez `src/bin/exo02_layout.rs`.

#### 1.1 — Version naïve

```rust
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PointNaif {
    pub actif: bool,        // 1 octet
    pub latence_ns: u64,    // 8 octets
    pub niveau: u8,         // 1 octet
    pub timestamp_ns: u64,  // 8 octets
}
```

**À faire :**
1. Afficher `std::mem::size_of::<PointNaif>()` et `align_of::<PointNaif>()`.
2. Réfléchir : combien d'octets utiles ? Combien de padding ? **Estimez avant de mesurer.**

#### 1.2 — Version optimisée

Réécrivez la même struct sous le nom `PointOptimise` en **réordonnant les champs** pour minimiser le padding.

**À faire :**
1. Afficher `size_of` et `align_of` de `PointOptimise`.
2. Vérifier qu'on est passé de 24 octets (`PointNaif`) à 16 ou 18 octets.
3. Calculer l'économie en pourcentage.

### Partie 2 — AoS vs SoA sur 1M points

#### 2.1 — Représentations

Implémentez les deux représentations :

```rust
pub struct MetriquesAoS {
    pub points: Vec<PointOptimise>,  // [point0, point1, ..., point_N]
}

pub struct MetriquesSoA {
    pub actifs: Vec<bool>,
    pub niveaux: Vec<u8>,
    pub latences_ns: Vec<u64>,
    pub timestamps_ns: Vec<u64>,
}
```

Implémentez un constructeur déterministe `nouvelle(n: usize) -> Self` pour les deux. Les valeurs peuvent être déterministes :
- `actif = i % 2 == 0`
- `niveau = (i % 4) as u8`
- `latence_ns = ((i * 37) % 1_000_000) as u64`
- `timestamp_ns = 1_700_000_000_000_000_000 + i as u64`

#### 2.2 — Opérations chaudes

Pour chacune des deux représentations, implémentez :

```rust
// Lit uniquement le champ `latence_ns` de chaque point
pub fn somme_latences(&self) -> u64 { /* ... */ }

// Lit `actif`, `niveau` et `latence_ns` — retourne la somme des latences filtrées
pub fn somme_filtree(&self) -> u64 { /* ... */ }
```

#### 2.3 — Mini-benchmark

Sur **1 million de points**, mesurez avec un mini-bench (50 itérations + warm-up) :
- `MetriquesAoS::somme_latences`
- `MetriquesSoA::somme_latences`
- `MetriquesAoS::somme_filtree`
- `MetriquesSoA::somme_filtree`

Calculez les speedups SoA / AoS pour chaque opération.

### Partie 3 — SmallVec sur la liste de tags

#### 3.1 — Mesure du surcoût d'un Vec

Définissez :

```rust
pub struct PointAvecTagsVec {
    pub latence_ns: u64,
    pub tags: Vec<u32>,
}

pub struct PointAvecTagsSmall {
    pub latence_ns: u64,
    pub tags: smallvec::SmallVec<[u32; 4]>,
}
```

Affichez la taille des deux structs et expliquez la différence en commentaire.

#### 3.2 — Construction massive

Pour `n = 100 000` points, construisez deux `Vec` :

```rust
let points_vec: Vec<PointAvecTagsVec> = (0..n).map(|i| PointAvecTagsVec {
    latence_ns: i as u64,
    tags: vec![i as u32, (i + 1) as u32],  // 2 tags par point
}).collect();
```

Et l'équivalent avec `SmallVec`. Mesurez le temps de construction des deux. Lequel est plus rapide ? De combien ?

> Indice : dans le cas `Vec`, chaque construction fait 1 allocation heap. Dans le cas `SmallVec<[u32; 4]>`, tant qu'il y a ≤ 4 tags, **aucune allocation heap**.

### Partie 4 — Bench Criterion

Créez `benches/bench_exo02.rs` avec des benchs Criterion pour :
- `aos_somme_latences` vs `soa_somme_latences`
- `aos_somme_filtree` vs `soa_somme_filtree`
- `construction_vec` vs `construction_smallvec`

Sur les tailles 10 000, 100 000, 1 000 000.

Déclarez le bench dans `Cargo.toml` :
```toml
[[bench]]
name = "bench_exo02"
harness = false
```

---

## Questions d'analyse (à rendre)

1. **Padding de `PointNaif`** : combien d'octets sont du padding pur ? Pourquoi le compilateur les insère-t-il ? Que se passerait-il si on les supprimait à la main avec `#[repr(C, packed)]` (réponse : c'est légal mais déconseillé — pourquoi) ?

2. **SoA gagne sur quelle opération ?** L'opération `somme_latences` (1 champ utilisé) et `somme_filtree` (3 champs sur 4) ne montrent **pas du tout le même résultat**. Sur l'une, SoA est plusieurs fois plus rapide ; sur l'autre, AoS peut reprendre l'avantage. Expliquez ce que vous observez avec le concept de **ligne de cache** (64 octets sur la plupart des x86_64 et ARM) et le **nombre de streams mémoire** que le prefetcher CPU peut suivre en parallèle.

3. **SmallVec : à partir de combien de tags par point l'avantage disparaît-il ?** Expliquez ce qui se passe en mémoire quand on dépasse la capacité inline. Donnez un cas où SmallVec serait **contre-productif**.

4. **`align_of` vs `size_of`** : pourquoi `align_of::<PointNaif>() == 8` alors qu'on n'a aucun champ qui exige naturellement un alignement supérieur à 8 ? Que vaudrait `align_of` si tous les champs étaient des `u8` ?

5. **En production**, vous devez choisir UNE seule représentation pour le service d'agrégation. Quels critères regardez-vous ? (Indice : pensez à ce que fait réellement le service en charge — agrégation pure ou aussi lecture/écriture des points individuels.)

---

## Bonus (facultatif)

- **B1.** Ajoutez une variante `PointPacked` avec `#[repr(C, packed)]` et mesurez si on gagne ou on perd en performance par rapport à `PointOptimise`. Étonnant ?
- **B2.** Utilisez `samply` pour profiler `aos_somme_filtree` sur 1M points. Identifiez si la lenteur vient des `cache misses` (frames `__memcpy` ou les fonctions de lecture mémoire) ou du calcul lui-même.
- **B3.** Implémentez un troisième layout : **AoSoA** (Array of Structs of Arrays) avec des chunks de 64 points. C'est ce qu'utilisent certains moteurs de bases de données (DuckDB, ClickHouse). Mesurez où ça se situe entre AoS et SoA.

---

## Critères d'évaluation

| Critère | Poids |
|---|---|
| `PointOptimise` est correctement réordonné et plus petit que `PointNaif` | 15% |
| AoS et SoA sont implémentés et donnent les mêmes résultats numériques | 25% |
| Mini-bench montre un speedup SoA mesurable sur au moins une opération | 15% |
| SmallVec est mis en œuvre et le bench de construction est en place | 15% |
| Bench Criterion fonctionne et donne des intervalles de confiance | 15% |
| Réponses aux 5 questions d'analyse, chiffrées et raisonnées | 15% |

---

## Indices utiles

- **Règle de pouce pour ordonner les champs** : trier par alignement décroissant. `u64` (align 8) en premier, puis `u32` (align 4), puis `u16` (align 2), puis `bool`/`u8` (align 1) à la fin.
- **`#[repr(C)]`** empêche le compilateur de réordonner les champs automatiquement. Sans cet attribut, Rust est libre d'optimiser l'ordre — utile à savoir mais piège si on veut un layout précis pour FFI.
- **Une ligne de cache** vaut 64 octets sur x86_64 et la plupart des ARM. Si vous itérez sur des structs de 24 octets, vous lisez ~2.7 structs par ligne. Si elles font 16 octets, ~4 structs par ligne. C'est cette différence qui se traduit en speedup.
- **SmallVec** : `SmallVec<[T; N]>` stocke jusqu'à N éléments inline. Si on dépasse, il bascule sur le heap comme un Vec normal. Le `N` est figé à la compilation.
- **Pour le bench Criterion** : pas oublier `black_box`. Sur des fonctions qui font juste une somme, LLVM peut tout évaluer à la compilation si l'entrée est constante.
