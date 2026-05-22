# Exercice 9 — Sérialisation comparée (JSON / Bincode / MessagePack)

**Jour 3 — Module 3.1 : Sérialisation haute performance**
**Durée estimée :** 1h
**Prérequis :** Serde (`derive`), bases du module 3.1
**Démo de référence :** [`demo09_serde_formats`](../demos/src/bin/demo09_serde_formats.rs)

---

## Contexte

Un agent OVH installé sur chaque serveur émet en continu des **rapports de métriques** vers un collecteur central : utilisation CPU, RAM, disque, latence des disques, etc. À l'échelle de la flotte (plusieurs dizaines de milliers de nœuds), le **format de sérialisation** choisi a un impact direct sur la bande passante consommée et le CPU des collecteurs.

Vous avez vu en démo qu'il existe plusieurs formats (JSON, Bincode, MessagePack, Protobuf, FlatBuffers). Dans cet exercice, vous allez comparer les **trois premiers** sur un payload réaliste et conclure sur le bon choix pour OVH.

---

## Objectifs pédagogiques

1. Définir une structure métier avec `#[derive(Serialize, Deserialize)]`.
2. Implémenter le round-trip dans 3 formats : JSON, Bincode, MessagePack.
3. Mesurer **taille** + **encode** + **decode** pour chacun.
4. Tirer une conclusion : quel format choisir pour quel usage ?

---

## Énoncé

### Partie 1 — La structure métier

Dans le projet `demos`, créez `src/bin/exo09_serialisation.rs`.

Définissez les types suivants (ils doivent dériver `Serialize`, `Deserialize` et `PartialEq` pour les tests) :

```rust
pub struct ServerReport {
    pub hostname: String,
    pub datacenter: String,
    pub timestamp: i64,
    pub metrics: Vec<Metric>,
}

pub struct Metric {
    pub name: String,
    pub value: f64,
    pub unit: String,
}
```

Ajoutez une fonction `sample(n: usize) -> ServerReport` qui produit un rapport déterministe avec `n` métriques. Cela permettra de tester sur des tailles variables.

### Partie 2 — Encode / Decode

Implémentez trois fonctions par format. Signature attendue :

```rust
pub fn encode_json(r: &ServerReport) -> Vec<u8> { ... }
pub fn decode_json(bytes: &[u8]) -> ServerReport { ... }

pub fn encode_bincode(r: &ServerReport) -> Vec<u8> { ... }
pub fn decode_bincode(bytes: &[u8]) -> ServerReport { ... }

pub fn encode_msgpack(r: &ServerReport) -> Vec<u8> { ... }
pub fn decode_msgpack(bytes: &[u8]) -> ServerReport { ... }
```

Crates à utiliser : `serde_json`, `bincode`, `rmp_serde` (toutes déjà déclarées dans `demos/Cargo.toml`).

### Partie 3 — Benchmark

Implémentez `bench(name, iterations, encode_fn, decode_fn) -> BenchResult` qui mesure :
- la taille du payload sérialisé (1 encode),
- la moyenne de `encode_ns` sur `iterations` répétitions,
- la moyenne de `decode_ns` sur `iterations` répétitions.

Affichez un tableau comparatif sur un rapport de **50 métriques**, **5 000 itérations**. Exemple de sortie :

```
Format       |   Taille | Encode (µs) | Decode (µs)
-------------|----------|-------------|-------------
JSON         |   4500 o |    25.30 µs |    62.10 µs
Bincode      |   1800 o |     1.50 µs |     2.10 µs
MessagePack  |   2900 o |     7.40 µs |    14.80 µs
```

### Partie 4 — Tests

1. **Round-trip Bincode** : encode → decode → `assert_eq!`.
2. **Round-trip MessagePack** : pareil.
3. **Round-trip JSON** : pareil **MAIS** attention aux flottants — JSON peut introduire des erreurs d'arrondi (`0.45` → `0.4499999...`). Comparer champ par champ avec une tolérance `1e-9` sur les `f64`.
4. **MessagePack plus petit que JSON** : sur 20 métriques, vérifier que `msgpack.len() < json.len()`.
5. **Bincode taille déterministe** : `encode(sample(10)).len()` doit être stable d'une exécution à l'autre.

---

## Questions d'analyse

1. **Sur quel format obtenez-vous la plus petite taille ?** Pourquoi ?
2. **Sur quel format obtenez-vous le décodage le plus rapide ?** Cohérent avec la théorie ?
3. **Pourquoi JSON ne peut-il pas être comparé brut avec `assert_eq!` sur des `f64` ?**
4. **Bincode est très rapide et compact. Pourquoi ne pas l'utiliser partout ?** (Indice : interop multi-langages, versioning de schéma.)
5. **Si OVH déploie un nouveau collecteur écrit en Go, quel format choisir ?** Pourquoi pas Bincode ?

