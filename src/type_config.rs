//! Type configuration for the foundation
//!
//! This module provides [`OpenRaftTypes`], the single `RaftTypeConfig` this crate ever runs:
//! one state machine (the KV), so no type parameter.

use openraft::OptionalSend;
use openraft::RaftTypeConfig;
use openraft::Vote;
use openraft::impls::InlineBatch;
use openraft::impls::OneshotResponder;
use openraft::impls::leader_id_std::LeaderId;
use serde::Deserialize;
use serde::Serialize;

use crate::command::Command;
use crate::command::CommandOutcome;

/// The foundation's `RaftTypeConfig`
///
/// A zero-sized marker: every association below is fixed, so there is nothing to store.
#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct OpenRaftTypes;

impl RaftTypeConfig for OpenRaftTypes {
    type D = Command;
    // `None` answers framework-generated entries (blank, membership); every transaction is
    // answered with `Some` by the state machine.
    type R = Option<CommandOutcome>;
    type NodeId = u64;
    type Node = Node;
    type Term = u64;
    type LeaderId = EzLeaderId;
    type Vote = Vote<EzLeaderId>;
    type Entry = crate::entry::EzEntry;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder<X>
        = OneshotResponder<Self, X>
    where X: OptionalSend + 'static;
    type Batch<X>
        = InlineBatch<X>
    where X: OptionalSend + 'static;
    type ErrorSource = openraft::AnyError;
}

/// Leader ID type: standard Raft, at most one leader per term
pub type EzLeaderId = LeaderId<u64, u64>;

/// Type alias for the Raft vote
pub type EzVote = Vote<EzLeaderId>;

/// A cluster member, as membership carries it: its address on the internal port
///
/// Replaces openraft's `BasicNode` (ech0-media#29, DECISIONS.md ech0-media#18 item 2). A node is
/// addressed by id through the current membership - `Transport`, `Rpc::call`, and `EzKv`'s own
/// membership-change handlers all look a target node's `Node` up there rather than being told an
/// address directly - and this is the type that lookup returns. It carries no other metadata: a
/// node's client-facing address and any application-defined metadata belong to the registry
/// ticket #33, not here.
///
/// Plain serde derives (no `tag`/`untagged`/`flatten`) and `Default`: openraft's own storage
/// conformance suite requires `C::Node: Default` even though such a placeholder node is never
/// actually addressed.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Node {
    /// This member's address on the internal port - where its `Transport`, `Rpc`, and
    /// `AdminClient` are all reached
    pub internal_addr: String,
}

impl Node {
    /// A `Node` addressed at `internal_addr`
    pub fn new(internal_addr: impl ToString) -> Self {
        Self {
            internal_addr: internal_addr.to_string(),
        }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.internal_addr)
    }
}
