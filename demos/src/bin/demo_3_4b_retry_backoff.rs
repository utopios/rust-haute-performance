// =============================================================================
// DÉMO 3.4-B — Retry avec backoff exponentiel + jitter
// Module 3.4 : Résilience
// =============================================================================
//
// PROBLÈME RÉSOLU
//   Beaucoup de pannes sont TRANSITOIRES : un timeout réseau, un 503 pendant un
//   déploiement, un lock momentané. Réessayer suffit souvent. Mais réessayer
//   MAL aggrave la panne :
//     - retry immédiat en boucle = on martèle un service déjà à terre (DDoS interne)
//     - retry à intervalle FIXE = tous les clients réessaient EN MÊME TEMPS
//       après une panne → "thundering herd" qui retue le service au redémarrage.
//
//   La bonne recette :
//     1. BACKOFF EXPONENTIEL : on espace de plus en plus (100ms, 200ms, 400ms…).
//        On laisse le service respirer.
//     2. JITTER (aléa) : on désynchronise les clients pour éviter qu'ils
//        retombent tous au même instant.
//     3. PLAFOND (max_delay) + nombre max de tentatives : on n'attend pas
//        l'infini, on abandonne proprement.
//     4. Ne retry QUE les erreurs retryables (un 400 "Bad Request" ne se
//        répare pas en réessayant — c'est notre faute, pas une panne).
//
// CONTEXTE OVH / production
//   Tous les SDK cloud (AWS, GCP, Azure) implémentent ce pattern. AWS recommande
//   explicitement le "exponential backoff with jitter" (article de référence).
//
// LANCEMENT
//   cargo run --bin demo_3_4b_retry_backoff
// =============================================================================

use std::time::{Duration, Instant};

#[derive(Clone)]
struct RetryPolicy {
    max_retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
    jitter: bool,
}

impl RetryPolicy {
    /// Calcule le délai AVANT la tentative `attempt` (0-indexée), sans jitter.
    /// delay = initial * multiplier^attempt, plafonné à max_delay.
    fn base_delay(&self, attempt: u32) -> Duration {
        let raw = self.initial_delay.as_secs_f64() * self.multiplier.powi(attempt as i32);
        Duration::from_secs_f64(raw.min(self.max_delay.as_secs_f64()))
    }

    /// Applique un jitter "full" : delay aléatoire dans [0, base]. C'est la
    /// variante recommandée par AWS (désynchronise le mieux les clients).
    fn delay_with_jitter(&self, attempt: u32, rng: &mut SimpleRng) -> Duration {
        let base = self.base_delay(attempt);
        if self.jitter {
            let factor = rng.next_f64(); // [0,1)
            Duration::from_secs_f64(base.as_secs_f64() * factor)
        } else {
            base
        }
    }
}

/// Distinction cruciale : toutes les erreurs ne se retryent pas.
#[derive(Debug)]
enum ServiceError {
    /// Transitoire : timeout, 503, 429… → on RÉESSAIE.
    Retryable(String),
    /// Définitif : 400, 404, 401… → INUTILE de réessayer, on remonte direct.
    Permanent(String),
}

/// Petit RNG déterministe (xorshift) — pas besoin de la crate `rand` ici, et ça
/// reste reproductible pour la démo. En prod, on utiliserait `rand`.
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self { state: seed | 1 }
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Le coeur : exécute `f`, et en cas d'erreur RETRYABLE, réessaie avec backoff.
async fn retry_with_backoff<F, Fut, T>(
    policy: &RetryPolicy,
    rng: &mut SimpleRng,
    mut f: F,
) -> Result<T, ServiceError>
where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = Result<T, ServiceError>>,
{
    let mut attempt = 0;
    loop {
        match f(attempt).await {
            Ok(v) => return Ok(v),
            Err(ServiceError::Permanent(e)) => {
                println!("    [retry] erreur PERMANENTE → abandon immédiat : {e}");
                return Err(ServiceError::Permanent(e));
            }
            Err(ServiceError::Retryable(e)) => {
                if attempt >= policy.max_retries {
                    println!("    [retry] {} tentatives épuisées → abandon : {e}", attempt + 1);
                    return Err(ServiceError::Retryable(e));
                }
                let delay = policy.delay_with_jitter(attempt, rng);
                let base = policy.base_delay(attempt);
                println!(
                    "    [retry] tentative #{} a échoué ({e}). Attente {:>4}ms (base {:>4}ms) avant retry…",
                    attempt + 1,
                    delay.as_millis(),
                    base.as_millis()
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    println!("=== DÉMO 3.4-B : Retry avec backoff exponentiel + jitter ===\n");

    let policy = RetryPolicy {
        max_retries: 5,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(2000),
        multiplier: 2.0,
        jitter: true,
    };

    // -------------------------------------------------------------------------
    // PARTIE 1 : visualiser la courbe de backoff
    // -------------------------------------------------------------------------
    println!("--- PARTIE 1 : la courbe du backoff exponentiel (base, sans jitter) ---");
    println!("  initial=100ms, multiplier=2.0, plafond=2000ms :");
    for attempt in 0..7 {
        let d = policy.base_delay(attempt);
        let bar = "█".repeat((d.as_millis() / 50) as usize);
        println!("    tentative {} → {:>5}ms  {bar}", attempt + 1, d.as_millis());
    }
    println!("  => On double à chaque fois, puis on plafonne à 2000ms.\n");

    // -------------------------------------------------------------------------
    // PARTIE 2 : effet du jitter (désynchronisation)
    // -------------------------------------------------------------------------
    println!("--- PARTIE 2 : effet du jitter sur 5 clients (tentative #4, base=800ms) ---");
    println!("  Sans jitter, les 5 réessaieraient À 800ms PILE → thundering herd.");
    let mut rng = SimpleRng::new(42);
    for client in 1..=5 {
        let d = policy.delay_with_jitter(3, &mut rng);
        println!("    client {client} → retry à {:>4}ms", d.as_millis());
    }
    println!("  => Avec jitter, ils sont étalés sur [0, 800]ms. Pas de pic synchronisé.\n");

    // -------------------------------------------------------------------------
    // PARTIE 3 : cas réel — succès après quelques échecs transitoires
    // -------------------------------------------------------------------------
    println!("--- PARTIE 3 : appel qui réussit après 3 échecs transitoires ---");
    let start = Instant::now();
    let fail_until = 3u32; // échoue aux tentatives 0,1,2 puis réussit
    let result = retry_with_backoff(&policy, &mut rng, |attempt| async move {
        tokio::time::sleep(Duration::from_millis(5)).await; // latence appel
        if attempt < fail_until {
            Err(ServiceError::Retryable(format!("503 Service Unavailable (essai {})", attempt + 1)))
        } else {
            Ok(format!("200 OK (réussi à l'essai {})", attempt + 1))
        }
    })
    .await;
    match result {
        Ok(msg) => println!("    ✓ RÉSULTAT : {msg}  (durée totale {:?})", start.elapsed()),
        Err(e) => println!("    ✗ ÉCHEC : {e:?}"),
    }
    println!();

    // -------------------------------------------------------------------------
    // PARTIE 4 : erreur PERMANENTE → pas de retry inutile
    // -------------------------------------------------------------------------
    println!("--- PARTIE 4 : erreur permanente (400 Bad Request) ---");
    let start = Instant::now();
    let result: Result<String, _> = retry_with_backoff(&policy, &mut rng, |_attempt| async move {
        Err(ServiceError::Permanent("400 Bad Request (payload invalide)".into()))
    })
    .await;
    println!("    Résultat : {result:?}");
    println!("    => Abandon en {:?}, AUCUNE attente : réessayer ne réparerait rien.\n", start.elapsed());

    // -------------------------------------------------------------------------
    // PARTIE 5 : épuisement des tentatives → abandon propre
    // -------------------------------------------------------------------------
    println!("--- PARTIE 5 : service durablement KO → on abandonne après max_retries ---");
    let fast_policy = RetryPolicy {
        max_retries: 3,
        initial_delay: Duration::from_millis(20),
        max_delay: Duration::from_millis(100),
        multiplier: 2.0,
        jitter: false,
    };
    let result: Result<String, _> = retry_with_backoff(&fast_policy, &mut rng, |attempt| async move {
        Err(ServiceError::Retryable(format!("connexion refusée (essai {})", attempt + 1)))
    })
    .await;
    println!("    Résultat final : {result:?}");
    println!("    => 4 tentatives (1 + 3 retries), puis abandon propre. Pas de boucle infinie.\n");

    println!("=== Conclusion ===");
    println!("  Retry = oui, mais : backoff exponentiel (laisser respirer)");
    println!("  + jitter (désynchroniser) + plafond (borner) + filtrer le retryable.");
    println!("  Combiné au circuit breaker (démo 3.4-A), on a une résilience complète.");
}
