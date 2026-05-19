pub mod demo01 {
    use rayon::prelude::*;

    pub fn somme_carres_naive(data: &[u64]) -> u64 {
        data.iter().map(|x| x * x).sum()
    }

    pub fn somme_carres_rayon(data: &[u64]) -> u64 {
        data.par_iter().map(|x| x * x).sum()
    }

    pub fn somme_carres_preallouee(data: &[u64]) -> u64 {
        let mut carres: Vec<u64> = Vec::with_capacity(data.len());
        for &x in data {
            carres.push(x * x);
        }
        carres.iter().sum()
    }

    pub fn somme_carres_sans_prealloc(data: &[u64]) -> u64 {
        let mut carres: Vec<u64> = Vec::new();
        for &x in data {
            carres.push(x * x);
        }
        carres.iter().sum()
    }

    /// Variante "anti-pattern" : alloue un petit Vec<u64> pour CHAQUE élément.
    /// Pédagogiquement utile : c'est le genre de code qu'un flamegraph CPU
    /// (frames alloc::*) et DHAT (n_allocs énorme) révèlent immédiatement.
    ///
    /// Note : `black_box` est indispensable. Sans lui, LLVM voit que le Vec
    /// temporaire ne sert qu'à un `sum()` immédiat, supprime l'allocation
    /// et la variante devient artificiellement rapide.
    pub fn somme_carres_alloc_spam(data: &[u64]) -> u64 {
        let mut total = 0u64;
        for &x in data {
            let tmp: Vec<u64> = std::hint::black_box(vec![x * x]);
            total += tmp.iter().sum::<u64>();
        }
        total
    }
}
