// =============================================================================
// DÉMO 3.3-C — Raft : consensus (leader election + réplication de log)
// Module 3.3 : Patterns distribués
// =============================================================================
//
// PROBLÈME RÉSOLU
//   Comment N machines se mettent-elles d'accord sur une SÉQUENCE d'opérations
//   (un log) malgré des pannes, sans jamais diverger ? C'est le consensus.
//   Raft est l'algorithme de référence (etcd, Consul, TiKV, CockroachDB).
//
//   Raft découpe le problème en deux :
//     1. LEADER ELECTION : un seul leader par "term" (mandat). Les followers
//        votent. Majorité requise → garantit l'unicité du leader.
//     2. LOG REPLICATION : seul le leader accepte les écritures, les réplique
//        aux followers (AppendEntries), et commit une entrée dès qu'une
//        MAJORITÉ (quorum) l'a acceptée.
//
//   La règle d'or : un quorum (N/2 + 1). Sur 3 noeuds → 2. Sur 5 → 3.
//   Tant qu'une majorité est vivante, le cluster progresse et reste cohérent.
//
// CETTE DÉMO
//   Simule un cluster Raft EN MÉMOIRE (déterministe, pas de vrai réseau) pour
//   rendre VISIBLE chaque mécanisme :
//     - une élection avec votes et quorum,
//     - la réplication d'entrées avec commit au quorum,
//     - une panne de leader → ré-élection sur un nouveau term,
//     - le rejet d'un "split vote" sans majorité.
//
// LANCEMENT
//   cargo run --bin demo_3_3c_raft
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Une entrée de log : le `term` où elle a été créée + la commande applicative.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LogEntry {
    term: u64,
    command: String,
}

/// Un noeud Raft simplifié.
#[derive(Debug)]
struct Node {
    id: usize,
    role: Role,
    current_term: u64,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,
    commit_index: usize, // nb d'entrées committées (répliquées sur une majorité)
    alive: bool,
}

impl Node {
    fn new(id: usize) -> Self {
        Self {
            id,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            alive: true,
        }
    }
}

/// Le cluster orchestre l'échange de messages entre noeuds (en mémoire).
struct Cluster {
    nodes: Vec<Node>,
}

impl Cluster {
    fn new(n: usize) -> Self {
        Self {
            nodes: (0..n).map(Node::new).collect(),
        }
    }

    /// Quorum = majorité stricte. Sur 3 → 2, sur 5 → 3.
    fn quorum(&self) -> usize {
        self.nodes.len() / 2 + 1
    }

    fn alive_ids(&self) -> Vec<usize> {
        self.nodes.iter().filter(|n| n.alive).map(|n| n.id).collect()
    }

    // -------------------------------------------------------------------------
    // LEADER ELECTION
    // -------------------------------------------------------------------------
    /// Le noeud `candidate_id` déclenche une élection : il incrémente son term,
    /// vote pour lui-même, et demande des votes (RequestVote) aux autres.
    /// Renvoie true s'il devient leader (quorum atteint).
    fn run_election(&mut self, candidate_id: usize) -> bool {
        if !self.nodes[candidate_id].alive {
            println!("    [election] noeud {candidate_id} est mort, pas de candidature");
            return false;
        }

        // Le candidat passe au term suivant et vote pour lui-même.
        let new_term;
        {
            let c = &mut self.nodes[candidate_id];
            c.current_term += 1;
            c.role = Role::Candidate;
            c.voted_for = Some(candidate_id);
            new_term = c.current_term;
        }
        println!("    [election] noeud {candidate_id} devient CANDIDAT (term {new_term})");

        let mut votes = 1; // vote pour soi-même
        let last_log_term = self.nodes[candidate_id].log.last().map(|e| e.term).unwrap_or(0);
        let last_log_len = self.nodes[candidate_id].log.len();

        // RequestVote envoyé à tous les autres noeuds vivants.
        let other_ids: Vec<usize> = (0..self.nodes.len()).filter(|&i| i != candidate_id).collect();
        for voter_id in other_ids {
            let voter = &mut self.nodes[voter_id];
            if !voter.alive {
                println!("    [vote]     noeud {voter_id} injoignable (panne)");
                continue;
            }
            // Un voter accorde son vote si : le term du candidat >= le sien,
            // il n'a pas déjà voté pour quelqu'un d'autre ce term, et le log du
            // candidat est au moins aussi à jour que le sien.
            let voter_last_term = voter.log.last().map(|e| e.term).unwrap_or(0);
            let log_ok = last_log_term > voter_last_term
                || (last_log_term == voter_last_term && last_log_len >= voter.log.len());

            if new_term > voter.current_term && log_ok {
                voter.current_term = new_term;
                voter.voted_for = Some(candidate_id);
                voter.role = Role::Follower;
                votes += 1;
                println!("    [vote]     noeud {voter_id} VOTE pour {candidate_id}");
            } else {
                println!("    [vote]     noeud {voter_id} REFUSE (term/log non éligible)");
            }
        }

        let q = self.quorum();
        println!("    [election] {votes} votes obtenus, quorum requis = {q}");
        if votes >= q {
            self.nodes[candidate_id].role = Role::Leader;
            // Les autres deviennent followers de ce term.
            for n in self.nodes.iter_mut() {
                if n.id != candidate_id && n.alive {
                    n.role = Role::Follower;
                }
            }
            println!("    [election] ✓ noeud {candidate_id} est élu LEADER (term {new_term})\n");
            true
        } else {
            self.nodes[candidate_id].role = Role::Follower;
            println!("    [election] ✗ pas de majorité → pas de leader (split vote)\n");
            false
        }
    }

    fn leader(&self) -> Option<usize> {
        self.nodes
            .iter()
            .find(|n| n.role == Role::Leader && n.alive)
            .map(|n| n.id)
    }

    // -------------------------------------------------------------------------
    // LOG REPLICATION
    // -------------------------------------------------------------------------
    /// Le leader reçoit une commande client, l'ajoute à son log, puis la
    /// réplique via AppendEntries. Elle est COMMITTÉE dès qu'une majorité
    /// (quorum) l'a en log.
    fn client_request(&mut self, command: &str) -> bool {
        let Some(leader_id) = self.leader() else {
            println!("    [append]   pas de leader → requête '{command}' rejetée");
            return false;
        };
        let term = self.nodes[leader_id].current_term;
        let entry = LogEntry {
            term,
            command: command.to_string(),
        };
        self.nodes[leader_id].log.push(entry.clone());
        let target_index = self.nodes[leader_id].log.len();
        println!("    [append]   leader {leader_id} ajoute '{command}' (index {target_index}, term {term})");

        // AppendEntries vers chaque follower vivant.
        let mut replicated = 1; // le leader lui-même
        let other_ids: Vec<usize> = (0..self.nodes.len()).filter(|&i| i != leader_id).collect();
        for fid in other_ids {
            let f = &mut self.nodes[fid];
            if !f.alive {
                println!("    [append]   follower {fid} injoignable, réplication différée");
                continue;
            }
            f.log.push(entry.clone());
            replicated += 1;
            println!("    [append]   follower {fid} a répliqué (log len = {})", f.log.len());
        }

        let q = self.quorum();
        if replicated >= q {
            // Commit : avancer commit_index sur tous ceux qui ont l'entrée.
            for n in self.nodes.iter_mut() {
                if n.alive && n.log.len() >= target_index {
                    n.commit_index = n.commit_index.max(target_index);
                }
            }
            println!("    [append]   ✓ COMMIT à l'index {target_index} ({replicated}/{} répliques ≥ quorum {q})\n", self.nodes.len());
            true
        } else {
            println!("    [append]   ✗ {replicated}/{} répliques < quorum {q} → entrée en log mais NON committée\n", self.nodes.len());
            false
        }
    }

    fn print_state(&self) {
        println!("    État du cluster :");
        for n in &self.nodes {
            let status = if n.alive { "vivant" } else { "MORT  " };
            println!(
                "      noeud {} [{}] role={:?}  term={}  log={}  commit={}",
                n.id, status, n.role, n.current_term, n.log.len(), n.commit_index
            );
        }
        println!();
    }
}

fn main() {
    println!("=== DÉMO 3.3-C : Raft (consensus) ===\n");
    println!("Cluster de 3 noeuds. Quorum = 2.\n");

    let mut cluster = Cluster::new(3);

    // -------------------------------------------------------------------------
    // PHASE 1 : première élection
    // -------------------------------------------------------------------------
    println!("--- PHASE 1 : élection du premier leader ---");
    println!("    Noeuds vivants : {:?}", cluster.alive_ids());
    let elected = cluster.run_election(0);
    assert!(elected);
    cluster.print_state();

    // -------------------------------------------------------------------------
    // PHASE 2 : réplication de log avec quorum
    // -------------------------------------------------------------------------
    println!("--- PHASE 2 : réplication de commandes (quorum atteint) ---");
    assert!(cluster.client_request("SET x=1"));
    assert!(cluster.client_request("SET y=2"));
    cluster.print_state();

    // -------------------------------------------------------------------------
    // PHASE 3 : panne d'un follower → on garde le quorum, on progresse
    // -------------------------------------------------------------------------
    println!("--- PHASE 3 : panne d'un follower (noeud 2) ---");
    cluster.nodes[2].alive = false;
    println!("    noeud 2 tombe en panne. 2/3 vivants → quorum (2) encore atteignable.");
    assert!(cluster.client_request("SET z=3")); // 2 répliques (0 et 1) = quorum
    cluster.print_state();

    // -------------------------------------------------------------------------
    // PHASE 4 : panne du LEADER → ré-élection sur un nouveau term
    // -------------------------------------------------------------------------
    println!("--- PHASE 4 : panne du LEADER (noeud 0) → ré-élection ---");
    cluster.nodes[0].alive = false;
    cluster.nodes[2].alive = true; // noeud 2 revient
    println!("    leader (noeud 0) tombe. noeud 2 revient. noeud 1 se porte candidat.");
    let reelected = cluster.run_election(1);
    assert!(reelected);
    cluster.print_state();
    println!("    Le nouveau leader (noeud 1) a un term SUPÉRIEUR à l'ancien :");
    println!("    => l'ancien leader, s'il revient, sera rétrogradé (term périmé).\n");

    // -------------------------------------------------------------------------
    // PHASE 5 : perte du quorum → blocage (sûreté avant disponibilité)
    // -------------------------------------------------------------------------
    println!("--- PHASE 5 : perte de quorum (sûreté > disponibilité) ---");
    cluster.nodes[0].alive = false;
    cluster.nodes[2].alive = false; // 2 noeuds sur 3 morts
    println!("    Seul le noeud 1 est vivant (1/3). Quorum (2) impossible.");
    let blocked = cluster.client_request("SET w=4");
    assert!(!blocked);
    println!("    => Raft REFUSE de committer plutôt que de risquer une divergence.");
    println!("       C'est le théorème CAP : en cas de partition, Raft choisit C+P,");
    println!("       sacrifie A (disponibilité en écriture).\n");

    println!("=== Conclusion ===");
    println!("  Leader unique par term + commit au quorum = consensus sûr.");
    println!("  Tant qu'une MAJORITÉ vit, le cluster progresse et reste cohérent.");
}
