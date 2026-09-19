//! Type configuration for the foundation
//!
//! This module provides [`OpenRaftTypes`], the single `RaftTypeConfig` this crate ever runs:
//! one state machine (the KV), so no type parameter.

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftTypeConfig;
use openraft::Vote;
use openraft::impls::InlineBatch;
use openraft::impls::OneshotResponder;
use openraft::impls::leader_id_std::LeaderId;

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
    type Node = BasicNode;
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
