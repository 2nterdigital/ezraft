//! The Raft log's request and response types
//!
//! One state machine (the KV) means one command shape, but not only one *kind* of command:
//! [`Command`] wraps whatever the log entry actually does (`body`) together with the two fields
//! every command needs for idempotency, regardless of `body` - a [`RequestId`] and the ledger
//! capacity its proposer was configured with (ech0-media#30). [`CommandBody::Txn`] is a user
//! transaction; [`CommandBody::RegisterNode`] and [`CommandBody::RemoveNode`] (ech0-media#33) are
//! foundation-internal node-registry commands. All three reuse `Command`'s two idempotency fields
//! and the any-node command path (`crate::write`) as-is - forwarding, same-request-id retry, and
//! the ledger - rather than growing a second path per kind.
//!
//! `command` is a private module: [`Command`], [`CommandBody`] and [`CommandOutcome`] are `pub`
//! only so [`crate::entry::EzEntry`] - which must stay public, since [`crate::EzStorage`] hands it
//! to implementors - type-checks without a `private_interfaces` warning. None of the three names
//! is re-exported from the crate root, so nothing outside this crate can name or construct them.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::kv::Conflict;
use crate::kv::RegisterNode;
use crate::kv::RemoveNode;
use crate::kv::RequestId;
use crate::kv::SetStatus;
use crate::kv::Txn;

/// The Raft log's request type: an idempotency-tracked command
///
/// Plain serde derives throughout, so a positional format (postcard) can still round-trip a
/// logged entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    /// The idempotency ledger's key for this command - the same value on every retry of the same
    /// logical write, however many times it ends up proposed.
    pub request_id: RequestId,
    /// The idempotency ledger's FIFO eviction bound, stamped in by whichever node actually
    /// proposed this command from *its own* `EzConfig::ledger_capacity` - never read back out of
    /// this node's own configuration when the command is applied. See
    /// `crate::kv::ledger`'s module docs for why that is what keeps eviction a pure function of
    /// the log.
    pub ledger_capacity: u64,
    /// The term this command is fenced to, if any: it takes effect only if the log entry that
    /// carries it was itself appended in this term
    ///
    /// `None` for every ordinary command. `Some(term)` for a command proposed from a leader-only
    /// context (ech0-media#34): a leader stamps the entries it appends with its own term, so an
    /// entry whose term differs was appended under some other leadership - even one the same node
    /// won later - and `apply` refuses it. Checking the node's role and term before proposing
    /// cannot rule that out by itself, because leadership can change between the check and the
    /// append; comparing against the entry's own term can, and does so identically on every
    /// replica, since it reads nothing but the log.
    pub required_term: Option<u64>,
    /// What this command actually does
    pub body: CommandBody,
}

/// What one [`Command`] does, once its idempotency check has passed
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandBody {
    /// A conditional, all-or-nothing KV transaction
    Txn(Txn),
    /// Register or update a node's registry entry (ech0-media#33) - proposed by that node itself,
    /// at startup (retried until it succeeds) and on every `EzKv::update_node_meta` call
    RegisterNode(RegisterNode),
    /// Remove a node's registry entry (ech0-media#33) - proposed by the leader, once the matching
    /// membership change has committed
    RemoveNode(RemoveNode),
    /// Set one node's liveness status (ech0-media#34) - proposed only by the built-in liveness
    /// leader task, through the leader-only propose path that never forwards
    /// (`crate::write::propose_leader_only`)
    SetStatus(SetStatus),
    /// Remove every registry entry whose node is no longer in the committed membership
    /// (ech0-media#34) - the crash window `EzKv::remove_node` can leave between its own two
    /// steps. Carries no data: `apply` computes which entries are stale from the state machine's
    /// own already-applied membership, identical on every replica by construction.
    ReconcileRegistry,
}

impl fmt::Display for Command {
    /// A short summary that never prints a key, a value, or application-defined `meta` bytes -
    /// openraft logs every request it processes, and both a [`Txn`]'s ops and a
    /// [`CommandBody::RegisterNode`]'s `meta` carry arbitrary caller-controlled bytes. Delegates
    /// to `Txn`'s own `Display`, which already keeps that promise.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.body {
            CommandBody::Txn(txn) => write!(f, "{} (request {})", txn, self.request_id),
            CommandBody::RegisterNode(cmd) => {
                write!(f, "register node {} (request {})", cmd.node_id, self.request_id)
            }
            CommandBody::RemoveNode(cmd) => {
                write!(
                    f,
                    "remove node {} from the registry (request {})",
                    cmd.node_id, self.request_id
                )
            }
            CommandBody::SetStatus(cmd) => {
                write!(
                    f,
                    "set node {} status to {:?} (request {})",
                    cmd.node_id, cmd.status, self.request_id
                )
            }
            CommandBody::ReconcileRegistry => {
                write!(
                    f,
                    "reconcile the node registry against the committed membership (request {})",
                    self.request_id
                )
            }
        }
    }
}

/// What applying one [`Command`] produced, as the writer hears it - one variant per kind of
/// [`CommandBody`], since a transaction and a registry command have nothing in common to report
///
/// The change group a [`CommandBody::Txn`] makes goes to the publisher, not back to the writer - a
/// writer knows what it wrote, and this is also what a forwarded write's answer carries over the
/// network (ech0-media#30). This is the exact shape the idempotency ledger records too
/// (`crate::kv::ledger::LedgerOutcome`); `crate::storage::adapter::state_machine` is the seam that
/// converts between the two, so neither `crate::kv` nor this module needs to know about the other.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandOutcome {
    /// A transaction's own outcome: the revision it committed at, or the conflict it hit
    Txn(Result<u64, Conflict>),
    /// A registry command's outcome - registering, updating, or removing an entry cannot itself
    /// fail once accepted, so there is nothing to carry beyond "this ran"
    Registry,
    /// The command was fenced to a term ([`Command::required_term`]) other than the one its log
    /// entry was appended in, and changed nothing
    StaleTerm,
}
