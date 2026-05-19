# Exercice 4 — Écrire du code SIMD-friendly (auto-vectorisation)

**Jour 1 — Module 1.4 : SIMD et vectorisation**
**Durée estimée :** 1h
**Prérequis :** Modules 1.1 (mesure), 1.2 (cache), 1.3 (Rayon), 1.4 (auto-vec)
**Démo de référence :** [`demo04_simd_vectorization`](../demos-stagiaires/src/bin/demo04_simd_vectorization.rs)

---

## Contexte

Vous reprenez les flux de métriques OVH. Cette fois on s'intéresse à un cas inspiré du **machine learning d'observabilité** : pour détecter des anomalies, le système calcule régulièrement la **norme euclidienne** d'un vecteur de 1 million de mesures normalisées (`f32`).

```
norme(v) = sqrt(v[0]² + v[1]² + ... + v[n-1]²)
```

Ce calcul est **embarrassingly parallel** — chaque élément est indépendant. C'est exactement le pattern que LLVM peut transformer automatiquement en instructions SIMD si vous écrivez le code correctement.

Votre mission : produire **trois implémentations**, mesurer leur performance, et comprendre **pourquoi** l'une est 4 à 8× plus rapide que les autres.

> Vous n'utiliserez **pas** d'intrinsics manuels (`std::arch`) ni la crate `wide`. Tout le gain doit venir d'une écriture du code qui **invite** LLVM à vectoriser. C'est la compétence du Module 1.4.

---

## Objectifs pédagogiques

1. Identifier les **conditions** pour qu'une boucle soit auto-vectorisable par LLVM.
2. Écrire des **accumulateurs indépendants** pour briser les dépendances séquentielles.
3. Compiler avec `RUSTFLAGS="-C target-cpu=native"` et comprendre la différence.
4. Mesurer le gain et le distinguer du gain qu'apporte Rayon (vu au module précédent).
5. Comprendre pourquoi `f32` n'est **pas associatif** — et ce que ça implique.

---

## Énoncé

### Partie 1 — Les trois implémentations

Dans le projet `demos-stagiaires`, créez `src/bin/exo04_simd.rs`.

#### Version A — Naïve idiomatique

```rust
pub fn norme_naive(v: &[f32]) -> f32 {
    let somme: f32 = v.iter().map(|x| x * x).sum();
    somme.sqrt()
}
```

Une ligne. Élégant. **Probablement pas vectorisé.** Pourquoi ? Parce que `.sum()` impose un ordre d'accumulation séquentiel : `((((0 + v0²) + v1²) + v2²) + ...)`. Chaque addition dépend de la précédente — impossible de paralléliser sur les voies SIMD.

#### Version B — Accumulateurs multiples (SIMD-friendly)

```rust
pub fn norme_accumulateurs(v: &[f32]) -> f32 {
    const LANES: usize = 8;
    let chunks = v.chunks_exact(LANES);
    let reste = chunks.remainder();

    let mut acc = [0.0f32; LANES];
    for chunk in chunks {
        for i in 0..LANES {
            acc[i] += chunk[i] * chunk[i];
        }
    }

    let mut total: f32 = acc.iter().sum();
    for &x in reste {
        total += x * x;
    }
    total.sqrt()
}
```

**Le principe** : 8 accumulateurs indépendants. À chaque tour de boucle, on calcule 8 carrés en parallèle (LLVM les pack dans un registre SIMD de 256 bits), on accumule dans 8 cases différentes (pas de dépendance entre elles), et on fusionne à la fin.

Pour LLVM, c'est un pattern reconnaissable : **les 8 lanes du registre AVX/NEON travaillent en parallèle**.

#### Version C — Accumulateurs avec slices fixes (variante du même thème)

```rust
pub fn norme_chunks_array(v: &[f32]) -> f32 {
    let chunks = v.chunks_exact(8);
    let reste = chunks.remainder();

    let mut acc = [0.0f32; 8];
    for chunk in chunks {
        let c: &[f32; 8] = chunk.try_into().unwrap();    // ← slice → array
        for i in 0..8 {
            acc[i] += c[i] * c[i];
        }
    }

    let mut total: f32 = acc.iter().sum();
    for &x in reste {
        total += x * x;
    }
    total.sqrt()
}
```

La différence avec B : on convertit explicitement `&[f32]` (slice de taille **inconnue à la compile**) en `&[f32; 8]` (array de taille **connue à la compile**). LLVM peut alors générer des **loads alignés directs** sur les 8 lanes, plus efficacement.

> **Sur de gros datasets, B et C sont souvent au coude-à-coude.** Sur des plus petits, C peut être 10-20 % plus rapide. À mesurer.

### Partie 2 — Mini-bench

Sur un vecteur de **1 million de f32** :
1. Vérifier que les trois fonctions donnent des résultats **proches** (mais pas forcément égaux — voir question 4).
2. Mesurer avec 100 itérations + 5 de warm-up.
3. Calculer les speedups par rapport à `norme_naive`.

### Partie 3 — Bench Criterion

Créez `benches/bench_exo04.rs` :
- 3 fonctions
- 3 tailles : 1 000, 10 000, 1 000 000
- Mesurer aussi sans `target-cpu=native` puis avec, pour montrer la différence.

Ajouter dans `Cargo.toml` :
```toml
[[bench]]
name = "bench_exo04"
harness = false
```

### Partie 4 — Inspecter le code généré (bonus mais recommandé)

Compilez avec :
```bash
RUSTFLAGS="-C target-cpu=native --emit=asm" cargo build --release --bin exo04_simd
```

Le fichier asm est dans `target/release/deps/exo04_simd-*.s`. Cherchez :
- Pour la version A : des instructions `fadd s0, s1, s2` (addition scalaire f32).
- Pour la version B/C : des instructions `fadd v0.4s, v1.4s, v2.4s` (NEON ARM) ou `vaddps ymm0, ymm1, ymm2` (AVX x86_64).

Si vous ne voyez **que** des `fadd` scalaires partout, LLVM n'a pas vectorisé.

Astuce plus simple si vous avez `cargo-asm` installé (`cargo install cargo-show-asm`) :
```bash
cargo asm --release --rust exo04_simd::norme_accumulateurs
```

---

## Questions d'analyse (à rendre)

1. **Speedup mesuré** : combien la version B est-elle plus rapide que la version A ? Et la version C ? Est-ce cohérent avec les tailles des registres SIMD de votre machine (4 lanes en NEON 128-bit, 8 lanes en AVX 256-bit, 16 lanes en AVX-512) ?

2. **Compilation avec `target-cpu=native`** : refaites le bench avec et sans ce flag. Sur quelle version le gain est-il le plus important ? Pourquoi le flag ne change-t-il pas grand-chose sur la version A ?

3. **Conditions de l'auto-vectorisation** : énumérez 3-4 conditions que doit remplir une boucle pour que LLVM la vectorise. Pour chacune, donnez un contre-exemple (du code qui **empêche** la vectorisation). Indices : aliasing, branches, appels non-inlinés, longueur inconnue.

4. **Non-associativité des `f32`** : vous avez probablement constaté que `norme_naive` et `norme_accumulateurs` ne donnent **pas exactement** le même résultat. Pourquoi ? À combien estimez-vous l'erreur relative (utilisez `(a - b).abs() / a`) ? Cela vous gêne-t-il dans un contexte d'observabilité (où on agrège des latences) ? Et dans un contexte cryptographique ?

5. **Versus Rayon** : Rayon parallélise **entre cœurs**, SIMD parallélise **dans un cœur**. Les deux sont-ils combinables ? Implémentez (rapidement) une version D qui combine `par_iter().chunks().map(norme_accumulateurs).sum()` et mesurez. Le speedup combiné est-il égal au produit des speedups individuels ?

---

## Bonus (facultatif)

- **B1.** Implémentez `norme_array_chunks` en utilisant `slice::array_chunks` (méthode nightly mais aussi disponible en stable depuis 1.77) au lieu de `chunks_exact + try_into`. Plus lisible — performances identiques ?

- **B2.** Désactivez `target-cpu=native` et **lisez l'assembleur** de la version B. Quelle taille de registre LLVM utilise-t-il par défaut ? Sur quel CPU "baseline" cible-t-il ?

- **B3.** Implémentez `dot_product(a: &[f32], b: &[f32])` — un produit scalaire entre deux vecteurs, en suivant le même pattern d'accumulateurs. Mesurez. Vous devriez voir un gain similaire à celui de la norme.

- **B4.** Combinez SIMD et Rayon. Sur 100M f32 (gros !), quelle est la décomposition optimale : Rayon chunks de N éléments × SIMD ? Tester N = 1 000, 10 000, 100 000.

---

## Critères d'évaluation

| Critère | Poids |
|---|---|
| Les trois versions sont implémentées et compilent | 20% |
| `norme_accumulateurs` est strictement plus rapide que `norme_naive` (au moins 3×) | 25% |
| Bench Criterion fonctionne sur les 3 tailles | 15% |
| Réponses aux 5 questions sont chiffrées et raisonnées | 30% |
| L'inspection asm a été tentée (au moins sur 1 fonction) | 10% |

---

## Indices utiles

- **Pourquoi `.iter().map().sum()` n'est pas auto-vectorisé** : la trait `Sum` impose un ordre d'accumulation gauche-à-droite strict. Pour `f32`, cet ordre **change le résultat** à cause de la non-associativité. LLVM est conservateur : il ne réordonne pas par défaut. **Astuce** : `-C target-cpu=native -C opt-level=3` n'active **pas** la `fast-math` par défaut en Rust. Pour ça, il faut soit `RUSTFLAGS="-Z fast-math"` (nightly), soit utiliser les types `f32` "spéciaux" comme `core::intrinsics::fadd_fast` (instable).

- **Taille des registres SIMD selon l'architecture** :

  | Architecture | Registre | Lanes `f32` |
  |---|---|---|
  | x86_64 SSE | 128 bits | 4 |
  | x86_64 AVX | 256 bits | 8 |
  | x86_64 AVX-512 | 512 bits | 16 |
  | ARM NEON | 128 bits | 4 |
  | ARM SVE | variable | 4 à 64 |

  Choisir 8 accumulateurs est un bon compromis : sur ARM, LLVM les regroupera en 2 vecteurs NEON de 4 ; sur AVX2, en 1 vecteur de 8 ; sur AVX-512, en 1 demi-vecteur.

- **Vérifier rapidement que la vectorisation a eu lieu** : si votre version B est plus de 2× plus rapide que A, elle est vectorisée. Si elle est seulement ~1.2× plus rapide, c'est probablement juste de l'unrolling, pas du SIMD.

- **`chunks_exact` vs `chunks`** : `chunks_exact` garantit que chaque chunk a exactement N éléments — c'est ce qu'on veut pour SIMD. Le reste (éléments < N à la fin) est récupéré par `.remainder()`.

- **L'unsafe à éviter** : aucun code de cet exo ne devrait nécessiter `unsafe`. Si vous y êtes tenté, c'est que vous vous engagez dans des intrinsics — pas notre sujet.
