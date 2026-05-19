//! Consistency models for IngoDB, expressed as a `(model, scope)` pair.
//!
//! Vocabulary follows [jepsen.io/consistency/models](https://jepsen.io/consistency/models).
//!
//! ## Design
//!
//! Two orthogonal axes:
//!
//! 1. **[`ConsistencyModel`]** — *what* is guaranteed: a set of properties
//!    (RYW, monotonic reads/writes, causal, total order, real-time order,
//!    durability). Properties compose into well-known named models (Causal,
//!    Sequential, Linearizable, …). Implication is set containment.
//!
//! 2. **[`Scope`]** — *where* it is guaranteed: `SingleNode` (this engine
//!    instance) or `Cluster` (across replicas). Scopes are **incomparable**:
//!    neither implies the other.
//!
//! ### Why scopes are incomparable
//!
//! It is tempting to say "cluster is stronger than single-node" because a
//! cluster *contains* nodes. That intuition is wrong: cluster guarantees and
//! single-node guarantees protect against **different failure modes**, so
//! neither generally implies the other.
//!
//! - A cluster of nodes — each NOT locally fsync-durable — can still be
//!   `DURABLE` against the loss of a minority of nodes (replication suffices).
//!   So `Consistency::cluster(DURABLE)` does **not** imply
//!   `Consistency::single_node(DURABLE)`.
//! - A single fsync-durable node survives a process crash but not the
//!   destruction of the machine; a non-durable cluster member doesn't survive
//!   its own crash but the cluster survives losing it.
//! - Ordering properties have an analogous asymmetry: cluster linearizability
//!   constrains observers across replicas, while node linearizability
//!   constrains observers on one node. Cross-scope, they describe different
//!   observation contexts.
//!
//! Cross-scope `Consistency` values therefore compare as `None`. Same-scope
//! values inherit the model's partial order.
//!
//! Cluster scope is a placeholder today; IngoDB is single-node.
//!
//! ## Future direction: isolation as a second branch in the same lattice
//!
//! IngoDB doesn't have transactions yet, so the current `ConsistencyModel`
//! flags only cover *consistency* properties (RYW, ordering, durability).
//! When transactions land we expect to grow a second branch — *isolation*
//! properties — into the **same** bitflags struct, not a separate type.
//!
//! Jepsen draws consistency and isolation as two branches of one lattice
//! that converge at **Strict Serializable** = Serializable + Linearizable.
//! Our bitflags map onto this directly: every level is just a set, so
//! adding properties from a new branch is mechanical, and the existing
//! `implies` (set containment) handles cross-branch comparisons for free.
//!
//! Sketch of how the type would grow:
//!
//! ```ignore
//! bitflags! {
//!     pub struct ConsistencyModel: u64 {
//!         // ── Consistency branch (today) ──
//!         const READ_YOUR_WRITES     = 1 << 0;
//!         // ... (RYW, MR, MW, WFR, TOTAL_ORDER, REAL_TIME_ORDER, DURABLE)
//!
//!         // ── Isolation branch (future) ──
//!         // Reserved bit range 24-31 keeps the two branches separate.
//!         const READ_COMMITTED        = 1 << 24;
//!         const CURSOR_STABILITY      = (1 << 25) | Self::READ_COMMITTED.bits();
//!         const MONOTONIC_ATOMIC_VIEW = (1 << 26) | Self::READ_COMMITTED.bits();
//!         const REPEATABLE_READ       = (1 << 27) | Self::CURSOR_STABILITY.bits();
//!         const SNAPSHOT_ISOLATION    = (1 << 28) | Self::MONOTONIC_ATOMIC_VIEW.bits();
//!         const SERIALIZABLE          = (1 << 29)
//!                                     | Self::REPEATABLE_READ.bits()
//!                                     | Self::SNAPSHOT_ISOLATION.bits();
//!         // Note: SI and RR are both subsumed by Serializable but neither
//!         // implies the other — RR allows phantoms, SI allows write skew.
//!
//!         // ── Top: both branches converge ──
//!         const STRICT_SERIALIZABLE = Self::SERIALIZABLE.bits()
//!                                   | Self::REAL_TIME_ORDER.bits();
//!
//!         // ── Bespoke hybrids drawn from real systems ──
//!         /// MongoDB-style: causal consistency for sessions + snapshot reads.
//!         /// Below STRICT_SERIALIZABLE, above PRAM and READ_COMMITTED;
//!         /// incomparable to LINEARIZABLE (no real-time across writes) and to
//!         /// SERIALIZABLE (no write-skew protection).
//!         const MONGODB_COMPATIBLE = Self::CAUSAL.bits()
//!                                  | Self::SNAPSHOT_ISOLATION.bits();
//!     }
//! }
//! ```
//!
//! Why this matters:
//!
//! - **Compositionality is free.** Every level is a *set*, so a user
//!   wanting both Snapshot Isolation and Causal Consistency just ORs them:
//!   `SNAPSHOT_ISOLATION | CAUSAL`. No need for the API to accept a list of
//!   levels — the bitflag IS the list.
//! - **Hybrid named models are first-class.** `MONGODB_COMPATIBLE` sits in
//!   the *middle* of the lattice, useful below the top of the Jepsen
//!   pyramid where most real systems live. Adding new bespoke models is
//!   just adding more constants.
//! - **Cross-branch incomparability falls out.** A pure consistency claim
//!   (e.g. `LINEARIZABLE`) and a pure isolation claim (e.g. `SERIALIZABLE`)
//!   neither implies the other — they sit on incomparable branches until
//!   you reach `STRICT_SERIALIZABLE`. The partial-order `partial_cmp`
//!   already returns `None` for them.
//!
//! Caveats for when this lands:
//!
//! - The `Consistency` *type* doesn't need to change — just the flags inside
//!   `ConsistencyModel` and what `LsmEngine` / `Transaction` does with them.
//! - Isolation is a per-*transaction* claim; consistency in our current
//!   design is per-*operation*. The unified `Consistency` value will need
//!   to be interpreted by the caller (a transaction vs a bare put/get).
//!   The `Scope` axis is unaffected — cluster vs single-node still applies.
//! - Some isolation levels in real systems are stronger or weaker than the
//!   strict ANSI definition (PostgreSQL's "Repeatable Read" is actually
//!   Snapshot Isolation, etc.). Document the chosen definition per constant.

use bitflags::bitflags;
use std::cmp::Ordering;

bitflags! {
    /// Set of abstract consistency properties. Their meaning is given by the
    /// [`Scope`] in which they are claimed:
    ///
    /// - At `SingleNode`, `DURABLE` means "survives this node's crash" (fsync).
    /// - At `Cluster`, `DURABLE` means "survives a node failure" (quorum-replicated).
    /// - At `SingleNode`, `REAL_TIME_ORDER` is single-node-linearizable.
    /// - At `Cluster`, `REAL_TIME_ORDER` is cluster-linearizable (requires consensus).
    ///
    /// Implication is set containment: `a.implies(b)` iff `a.contains(b)`.
    /// Two models with neither containing the other are *incomparable*.
    #[derive(Copy, Clone, Eq, PartialEq, Debug, Hash, Default)]
    pub struct ConsistencyModel: u64 {
        // ── Visibility / session ──
        const READ_YOUR_WRITES     = 1 << 0;
        const MONOTONIC_READS      = 1 << 1;
        const MONOTONIC_WRITES     = 1 << 2;
        const WRITES_FOLLOW_READS  = 1 << 3;

        // ── Ordering ──
        /// Single global order of operations observable to all clients.
        const TOTAL_ORDER          = 1 << 8;
        /// The total order respects real-time precedence.
        /// (Implies `TOTAL_ORDER`.)
        const REAL_TIME_ORDER      = (1 << 9) | Self::TOTAL_ORDER.bits();

        // ── Durability ──
        /// Effects survive the relevant failure for the scope (node crash at
        /// SingleNode, node failure at Cluster).
        const DURABLE              = 1 << 16;

        // ── Named models ──
        const PRAM = Self::READ_YOUR_WRITES.bits()
                   | Self::MONOTONIC_READS.bits()
                   | Self::MONOTONIC_WRITES.bits();

        const CAUSAL = Self::PRAM.bits()
                     | Self::WRITES_FOLLOW_READS.bits();

        const SEQUENTIAL = Self::CAUSAL.bits()
                         | Self::TOTAL_ORDER.bits();

        /// Linearizable: causal + sequential + real-time order.
        const LINEARIZABLE = Self::SEQUENTIAL.bits()
                           | Self::REAL_TIME_ORDER.bits();

        /// Strict linearizable: linearizable + durable.
        /// (Aguilera & Frølund 2003.)
        const STRICT_LINEARIZABLE = Self::LINEARIZABLE.bits()
                                  | Self::DURABLE.bits();
    }
}

impl ConsistencyModel {
    /// True iff `self` provides every property in `other`.
    pub const fn implies(self, other: ConsistencyModel) -> bool {
        self.contains(other)
    }
}

impl PartialOrd for ConsistencyModel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.contains(*other), other.contains(*self)) {
            (true, true)   => Some(Ordering::Equal),
            (true, false)  => Some(Ordering::Greater),
            (false, true)  => Some(Ordering::Less),
            (false, false) => None,
        }
    }
}

/// Where a consistency model is guaranteed.
///
/// Scopes are **incomparable**: cluster does not imply single-node, nor vice
/// versa. They describe different failure modes and observation contexts.
/// See the crate-level docs ("Why scopes are incomparable") for the rationale.
///
/// Currently only `SingleNode` is implemented.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash, Default)]
pub enum Scope {
    #[default]
    SingleNode,
    /// Cross-replica scope. Requires replication + consensus. Not implemented.
    Cluster,
}

impl PartialOrd for Scope {
    /// `Equal` for the same scope, `None` for cross-scope. Intentionally
    /// not totally ordered — see crate-level docs.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other { Some(Ordering::Equal) } else { None }
    }
}
// No `Ord` impl — Scope is genuinely a partial order with two incomparable
// elements.

/// A full consistency specification: a [`ConsistencyModel`] claimed at a
/// particular [`Scope`].
///
/// The partial order:
/// - **Same scope:** compare models by set containment.
/// - **Cross scope:** always incomparable. Cluster guarantees and single-node
///   guarantees protect against different failure modes; one does not imply
///   the other. (See crate-level docs.)
///
/// Notable incomparable pairs:
/// - `Consistency::single_node(STRICT_LINEARIZABLE)` vs
///   `Consistency::cluster(STRICT_LINEARIZABLE)` — different failure modes.
/// - `Consistency::single_node(STRICT_LINEARIZABLE)` vs
///   `Consistency::cluster(LINEARIZABLE)` — node-durable-but-not-replicated
///   vs replicated-but-not-locally-durable.
/// - `Consistency::single_node(READ_YOUR_WRITES)` vs
///   `Consistency::single_node(MONOTONIC_READS)` — sibling session properties.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash, Default)]
pub struct Consistency {
    pub model: ConsistencyModel,
    pub scope: Scope,
}

impl Consistency {
    pub const fn new(scope: Scope, model: ConsistencyModel) -> Self {
        Consistency { scope, model }
    }

    pub const fn single_node(model: ConsistencyModel) -> Self {
        Consistency { scope: Scope::SingleNode, model }
    }

    pub const fn cluster(model: ConsistencyModel) -> Self {
        Consistency { scope: Scope::Cluster, model }
    }

    /// True iff `self` provides every guarantee that `other` requires.
    /// Always false across scopes (with the trivial exception of an empty
    /// model on `other`, which any value implies).
    pub fn implies(self, other: Consistency) -> bool {
        if self.scope != other.scope {
            // Cross-scope: only the trivial empty case is implied. Anything
            // non-empty in a different scope describes a different failure
            // mode and is not guaranteed by `self`.
            return other.model.is_empty();
        }
        self.model.implies(other.model)
    }
}

impl PartialOrd for Consistency {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.scope != other.scope {
            return None;
        }
        self.model.partial_cmp(&other.model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ConsistencyModel partial order ──

    #[test]
    fn model_named_chain() {
        assert!(ConsistencyModel::STRICT_LINEARIZABLE > ConsistencyModel::LINEARIZABLE);
        assert!(ConsistencyModel::LINEARIZABLE > ConsistencyModel::SEQUENTIAL);
        assert!(ConsistencyModel::SEQUENTIAL > ConsistencyModel::CAUSAL);
        assert!(ConsistencyModel::CAUSAL > ConsistencyModel::PRAM);
        assert!(ConsistencyModel::PRAM > ConsistencyModel::READ_YOUR_WRITES);
    }

    #[test]
    fn model_real_time_implies_total() {
        assert!(ConsistencyModel::REAL_TIME_ORDER.contains(ConsistencyModel::TOTAL_ORDER));
    }

    #[test]
    fn model_pram_siblings_incomparable() {
        assert_eq!(
            ConsistencyModel::READ_YOUR_WRITES
                .partial_cmp(&ConsistencyModel::MONOTONIC_READS),
            None
        );
        assert_eq!(
            ConsistencyModel::MONOTONIC_READS
                .partial_cmp(&ConsistencyModel::MONOTONIC_WRITES),
            None
        );
    }

    #[test]
    fn model_durable_and_ordering_orthogonal() {
        assert_eq!(
            ConsistencyModel::DURABLE.partial_cmp(&ConsistencyModel::LINEARIZABLE),
            None
        );
    }

    #[test]
    fn model_implies() {
        let s = ConsistencyModel::STRICT_LINEARIZABLE;
        assert!(s.implies(ConsistencyModel::READ_YOUR_WRITES));
        assert!(s.implies(ConsistencyModel::CAUSAL));
        assert!(s.implies(ConsistencyModel::DURABLE));
        assert!(!ConsistencyModel::empty().implies(ConsistencyModel::READ_YOUR_WRITES));
    }

    // ── Scope ──

    #[test]
    fn scopes_are_incomparable() {
        // Scopes describe different failure modes; neither implies the other.
        assert_eq!(
            Scope::SingleNode.partial_cmp(&Scope::Cluster),
            None
        );
        assert_eq!(Scope::Cluster.partial_cmp(&Scope::Cluster), Some(Ordering::Equal));
    }

    // ── Consistency: combined partial order ──

    #[test]
    fn consistency_cross_scope_same_model_incomparable() {
        // Cluster-LINEARIZABLE doesn't imply single-node LINEARIZABLE
        // (and vice versa) — they're different observation contexts.
        let sn = Consistency::single_node(ConsistencyModel::LINEARIZABLE);
        let cl = Consistency::cluster(ConsistencyModel::LINEARIZABLE);
        assert_eq!(sn.partial_cmp(&cl), None);
        assert!(!sn.implies(cl));
        assert!(!cl.implies(sn));
    }

    #[test]
    fn consistency_strict_strongest_within_each_scope() {
        // Within a scope, strict > linearizable. Across scopes: incomparable.
        let sn_strict = Consistency::single_node(ConsistencyModel::STRICT_LINEARIZABLE);
        let sn_lin = Consistency::single_node(ConsistencyModel::LINEARIZABLE);
        let cl_strict = Consistency::cluster(ConsistencyModel::STRICT_LINEARIZABLE);
        let cl_lin = Consistency::cluster(ConsistencyModel::LINEARIZABLE);
        assert!(sn_strict > sn_lin);
        assert!(cl_strict > cl_lin);
        assert_eq!(sn_strict.partial_cmp(&cl_strict), None);
    }

    #[test]
    fn consistency_node_strict_vs_cluster_volatile_incomparable() {
        // A node-fsync'd but non-replicated write vs a replicated but
        // non-fsync'd write — classic engineering tradeoff. Incomparable.
        let sn_strict = Consistency::single_node(ConsistencyModel::STRICT_LINEARIZABLE);
        let cl_volatile = Consistency::cluster(ConsistencyModel::LINEARIZABLE);
        assert_eq!(sn_strict.partial_cmp(&cl_volatile), None);
        assert!(!sn_strict.implies(cl_volatile));
        assert!(!cl_volatile.implies(sn_strict));
    }

    #[test]
    fn consistency_cross_scope_durability_does_not_propagate() {
        // The motivating case: a cluster can be DURABLE (replication survives
        // minority failures) even when none of its nodes are individually
        // fsync-durable. So cluster(DURABLE) does NOT imply single_node(DURABLE).
        let cl_dur = Consistency::cluster(ConsistencyModel::DURABLE);
        let sn_dur = Consistency::single_node(ConsistencyModel::DURABLE);
        assert!(!cl_dur.implies(sn_dur));
        assert!(!sn_dur.implies(cl_dur));
    }

    #[test]
    fn consistency_cross_scope_only_empty_is_implied() {
        // Trivially: any consistency value implies the empty (no-guarantee)
        // claim, even across scopes.
        let sn = Consistency::single_node(ConsistencyModel::LINEARIZABLE);
        let cl_empty = Consistency::cluster(ConsistencyModel::empty());
        assert!(sn.implies(cl_empty));
    }

    #[test]
    fn consistency_default_is_weakest() {
        let d: Consistency = Default::default();
        assert_eq!(d.scope, Scope::SingleNode);
        assert_eq!(d.model, ConsistencyModel::empty());
        // Default doesn't even guarantee RYW.
        assert!(!d.implies(Consistency::single_node(ConsistencyModel::READ_YOUR_WRITES)));
    }
}
