// =============================================================================
// DÉMO 3.4-A — Circuit Breaker (disjoncteur)
// Module 3.4 : Résilience
// =============================================================================
//
// PROBLÈME RÉSOLU
//   Un service B est en panne. Le service A continue de l'appeler. Chaque appel
//   attend le timeout (ex. 30 s), consomme un thread/une connexion, et empile.
//   Résultat : A s'effondre à cause de B → panne en cascade (cascading failure).
//
//   Le circuit breaker coupe le courant : après N échecs, il OUVRE le circuit
//   et fait échouer les appels IMMÉDIATEMENT (fail-fast), sans toucher à B.
//   Après un délai, il teste prudemment si B est revenu (HalfOpen). Si oui,
//   il referme ; sinon, il rouvre.
//
//   Trois états :
//     CLOSED   — tout passe. On compte les échecs. Seuil atteint → OPEN.
//     OPEN     — tout est rejeté immédiatement. Après reset_timeout → HALF-OPEN.
//     HALF-OPEN— on laisse passer quelques appels test. Succès → CLOSED.
//                Échec → OPEN (on re-attend).
//
// CONTEXTE OVH / production
//   Netflix Hystrix l'a popularisé. Présent dans Envoy, Istio, resilience4j,
//   les service meshes. Indispensable dès qu'un service dépend d'un autre.
//
// LANCEMENT
//   cargo run --bin demo_3_4a_circuit_breaker
// =============================================================================

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug)]
enum CallError {
    /// Le circuit est ouvert : rejet immédiat sans appeler le service.
    Open,
    /// Le service a été appelé mais a échoué.
    ServiceFailed(String),
}

struct CircuitBreaker {
    failure_threshold: u32,        // nb d'échecs consécutifs avant ouverture
    reset_timeout: Duration,       // délai avant de tester en HalfOpen
    success_threshold: u32,        // nb de succès en HalfOpen pour refermer
    failure_count: AtomicU32,
    success_count: AtomicU32,
    state: Mutex<CircuitState>,
    opened_at: Mutex<Option<Instant>>,
}

impl CircuitBreaker {
    fn new(failure_threshold: u32, reset_timeout: Duration, success_threshold: u32) -> Self {
        Self {
            failure_threshold,
            reset_timeout,
            success_threshold,
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            state: Mutex::new(CircuitState::Closed),
            opened_at: Mutex::new(None),
        }
    }

    fn state(&self) -> CircuitState {
        *self.state.lock().unwrap()
    }

    /// Avant chaque appel : décide si l'on a le droit de passer, et gère la
    /// transition Open → HalfOpen quand le délai est écoulé.
    fn before_call(&self) -> Result<(), CallError> {
        let mut state = self.state.lock().unwrap();
        match *state {
            CircuitState::Closed | CircuitState::HalfOpen => Ok(()),
            CircuitState::Open => {
                let opened = *self.opened_at.lock().unwrap();
                if let Some(t) = opened {
                    if t.elapsed() >= self.reset_timeout {
                        // Le délai est passé : on tente une demi-ouverture.
                        *state = CircuitState::HalfOpen;
                        self.success_count.store(0, Ordering::SeqCst);
                        println!("      [CB] délai écoulé → passage OPEN → HALF-OPEN");
                        return Ok(());
                    }
                }
                Err(CallError::Open)
            }
        }
    }

    fn on_success(&self) {
        let mut state = self.state.lock().unwrap();
        match *state {
            CircuitState::HalfOpen => {
                let s = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if s >= self.success_threshold {
                    *state = CircuitState::Closed;
                    self.failure_count.store(0, Ordering::SeqCst);
                    println!("      [CB] {s} succès test → HALF-OPEN → CLOSED ✓ (service rétabli)");
                }
            }
            CircuitState::Closed => {
                self.failure_count.store(0, Ordering::SeqCst);
            }
            CircuitState::Open => {}
        }
    }

    fn on_failure(&self) {
        let mut state = self.state.lock().unwrap();
        match *state {
            CircuitState::Closed => {
                let f = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                if f >= self.failure_threshold {
                    *state = CircuitState::Open;
                    *self.opened_at.lock().unwrap() = Some(Instant::now());
                    println!("      [CB] {f} échecs consécutifs → CLOSED → OPEN ✗ (circuit coupé)");
                }
            }
            CircuitState::HalfOpen => {
                // Un échec pendant le test → on rouvre immédiatement.
                *state = CircuitState::Open;
                *self.opened_at.lock().unwrap() = Some(Instant::now());
                println!("      [CB] échec en test → HALF-OPEN → OPEN ✗ (service encore KO)");
            }
            CircuitState::Open => {}
        }
    }

    /// Exécute `f` à travers le disjoncteur.
    async fn call<F, Fut, T>(&self, f: F) -> Result<T, CallError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        self.before_call()?; // peut court-circuiter (Err::Open)
        match f().await {
            Ok(v) => {
                self.on_success();
                Ok(v)
            }
            Err(e) => {
                self.on_failure();
                Err(CallError::ServiceFailed(e))
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Service simulé : tombe en panne pendant une fenêtre de temps, puis revient.
// -----------------------------------------------------------------------------
async fn flaky_service(call_id: u32, down_until: u32, current: u32) -> Result<String, String> {
    // Petit délai pour simuler la latence réseau.
    tokio::time::sleep(Duration::from_millis(5)).await;
    if current < down_until {
        Err(format!("timeout/500 sur l'appel #{call_id}"))
    } else {
        Ok(format!("réponse OK à l'appel #{call_id}"))
    }
}

#[tokio::main]
async fn main() {
    println!("=== DÉMO 3.4-A : Circuit Breaker ===\n");
    println!("Config : ouverture après 3 échecs, reset_timeout 300ms, 2 succès pour refermer.\n");

    let cb = CircuitBreaker::new(3, Duration::from_millis(300), 2);

    // Le service est en panne pour les "instants" 0..5, puis se rétablit.
    let service_down_until = 5u32;

    // -------------------------------------------------------------------------
    // PHASE 1 : le service tombe → le circuit s'ouvre après 3 échecs
    // -------------------------------------------------------------------------
    println!("--- PHASE 1 : le service B est en panne ---");
    for i in 0..6u32 {
        let res = cb.call(|| flaky_service(i, service_down_until, i)).await;
        let etat = cb.state();
        match res {
            Ok(msg) => println!("  appel #{i} [{:?}] : {msg}", etat),
            Err(CallError::Open) => {
                println!("  appel #{i} [{:?}] : REJET IMMÉDIAT (fail-fast, B n'est PAS appelé)", etat)
            }
            Err(CallError::ServiceFailed(e)) => println!("  appel #{i} [{:?}] : échec → {e}", etat),
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // PHASE 2 : pendant l'ouverture, tous les appels sont rejetés sans attente
    // -------------------------------------------------------------------------
    println!("--- PHASE 2 : circuit OUVERT, tout est rejeté instantanément ---");
    println!("  (le service B n'est plus sollicité du tout → il peut récupérer)");
    let start = Instant::now();
    for i in 10..13u32 {
        let res = cb.call(|| flaky_service(i, service_down_until, i)).await;
        match res {
            Err(CallError::Open) => println!("  appel #{i} [{:?}] : rejeté en {:?}", cb.state(), start.elapsed()),
            _ => println!("  appel #{i} : (inattendu)"),
        }
    }
    println!("  => 3 rejets quasi-instantanés. Aucun timeout de 30s subi.\n");

    // -------------------------------------------------------------------------
    // PHASE 3 : on attend le reset_timeout → HalfOpen → service rétabli → Closed
    // -------------------------------------------------------------------------
    println!("--- PHASE 3 : récupération (le service B est revenu) ---");
    println!("  Attente du reset_timeout (300ms)...");
    tokio::time::sleep(Duration::from_millis(320)).await;

    // Maintenant `current >= down_until` → le service répond OK.
    for i in 20..23u32 {
        // current = 20 > 5 → service rétabli
        let res = cb.call(|| flaky_service(i, service_down_until, i)).await;
        let etat = cb.state();
        match res {
            Ok(msg) => println!("  appel #{i} [{:?}] : {msg}", etat),
            Err(CallError::Open) => println!("  appel #{i} [{:?}] : encore rejeté", etat),
            Err(CallError::ServiceFailed(e)) => println!("  appel #{i} [{:?}] : {e}", etat),
        }
    }
    println!();

    assert_eq!(cb.state(), CircuitState::Closed);
    println!("=== Conclusion ===");
    println!("  CLOSED → (3 échecs) → OPEN → (timeout) → HALF-OPEN → (2 succès) → CLOSED");
    println!("  Le disjoncteur a isolé la panne, évité l'effondrement en cascade,");
    println!("  et rétabli le trafic automatiquement dès que B est revenu.");
}
