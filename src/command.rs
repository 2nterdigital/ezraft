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
//! What each command *means* is decided here too ([`Command::apply`]), in one place and from
//! nothing but the command, the log and the replicated state - the rule every replica's agreement
//! rests on. `crate::storage::adapter` only feeds it entries and publishes what it reports.
//!
//! `command` is a private module: [`Command`], [`CommandBody`] and [`CommandOutcome`] are `pub`
//! only so [`crate::entry::EzEntry`] - which must stay public, since [`crate::EzStorage`] hands it
//! to implementors - type-checks without a `private_interfaces` warning. None of the three names
//! is re-exported from the crate root, so nothing outside this crate can name or construct them.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::kv::Changes;
use crate::kv::Conflict;
use crate::kv::KvState;
use crate::kv::NodeInfo;
use crate::kv::NodeStatus;
use crate::kv::RegisterNode;
use crate::kv::Registry;
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

/// What applying one [`Command`] did: its answer, and whatever changed for the state machine
/// adapter to publish
pub(crate) struct AppliedCommand {
    /// What the writer hears
    pub(crate) outcome: CommandOutcome,
    /// The change group a freshly executed transaction made. `None` for everything else - a
    /// conflict, a deduplicated retry, any other kind of command - because nothing in the user's
    /// key space changed.
    pub(crate) changes: Option<Changes>,
    /// The registry as it now stands, if this command changed it
    pub(crate) registry: Option<Registry>,
}

impl AppliedCommand {
    fn registry(registry: Option<Registry>) -> Self {
        Self {
            outcome: CommandOutcome::Registry,
            changes: None,
            registry,
        }
    }
}

impl Command {
    /// Apply this command to `kv`, as the log entry appended in `term`
    ///
    /// Everything this reads is replicated: the command itself, the term of the entry carrying
    /// it, `kv`, and - through `is_member` - the membership the state machine has applied so far.
    /// Never this node's configuration, a clock, or a metric: every replica has to reach the same
    /// state from the same log, so whatever a proposer decided from its own configuration
    /// travels inside the command (`ledger_capacity`, a [`SetStatus`]'s status).
    ///
    /// Every body goes through the same idempotency ledger (ech0-media#30): a retried request id
    /// changes nothing and reports nothing to publish. A registry command never advances the
    /// revision and never produces a change group - the registry is not the user's key space.
    pub(crate) fn apply(self, term: u64, is_member: impl Fn(u64) -> bool, kv: &mut KvState) -> AppliedCommand {
        // A command fenced to a term takes effect only if this very entry was appended in that
        // term (ech0-media#34). Decided from the entry alone, so every replica refuses it
        // identically, and before anything else looks at it: a refused command touches neither
        // the state nor the ledger.
        if self.required_term.is_some_and(|required| required != term) {
            return AppliedCommand {
                outcome: CommandOutcome::StaleTerm,
                changes: None,
                registry: None,
            };
        }

        let request_id = self.request_id;
        let ledger_capacity = self.ledger_capacity as usize;

        match self.body {
            CommandBody::Txn(txn) => {
                let applied = kv.apply_idempotent(request_id, ledger_capacity, &txn);
                AppliedCommand {
                    outcome: CommandOutcome::Txn(applied.outcome),
                    changes: applied.changes,
                    registry: None,
                }
            }
            // A node registering or updating its own entry is, by definition, up right now.
            CommandBody::RegisterNode(entry) => {
                AppliedCommand::registry(kv.apply_registry_command(request_id, ledger_capacity, |registry| {
                    registry.set(entry.node_id, NodeInfo {
                        external_addr: entry.external_addr,
                        meta: entry.meta,
                        status: NodeStatus::Up,
                    });
                }))
            }
            CommandBody::RemoveNode(removal) => {
                AppliedCommand::registry(kv.apply_registry_command(request_id, ledger_capacity, |registry| {
                    registry.remove(removal.node_id);
                }))
            }
            // An entry that no longer exists (concurrently reconciled away) is a silent no-op, not
            // an error: the leader's decision can be stale by the time it is applied, and there
            // is nothing left to correct.
            CommandBody::SetStatus(change) => {
                AppliedCommand::registry(kv.apply_registry_command(request_id, ledger_capacity, |registry| {
                    if let Some(mut info) = registry.get(change.node_id) {
                        info.status = change.status;
                        registry.set(change.node_id, info);
                    }
                }))
            }
            // Which entries are stale is decided against the membership this state machine has
            // itself applied - every replica applies the same membership entries in the same order
            // before reaching this one - never against what the proposing leader believed.
            CommandBody::ReconcileRegistry => {
                AppliedCommand::registry(kv.apply_registry_command(request_id, ledger_capacity, |registry| {
                    let stale: Vec<u64> = registry.iter().map(|(id, _)| id).filter(|id| !is_member(*id)).collect();
                    for node_id in stale {
                        registry.remove(node_id);
                    }
                }))
            }
        }
    }
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
/// network (ech0-media#30). The idempotency ledger records the same two shapes
/// (`crate::kv::ledger::LedgerOutcome`); [`Command::apply`] is where one becomes the other, so
/// `crate::kv` never needs to know about commands.
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn command(required_term: Option<u64>, body: CommandBody) -> Command {
        Command {
            request_id: RequestId::new(),
            ledger_capacity: 100,
            required_term,
            body,
        }
    }

    fn register(node_id: u64) -> CommandBody {
        CommandBody::RegisterNode(RegisterNode {
            node_id,
            external_addr: format!("node-{node_id}.example:1"),
            meta: Bytes::from_static(b"meta"),
        })
    }

    fn set_status(node_id: u64, status: NodeStatus) -> CommandBody {
        CommandBody::SetStatus(SetStatus { node_id, status })
    }

    /// Everyone is a member, unless a test says otherwise
    fn everyone(_: u64) -> bool {
        true
    }

    #[test]
    fn a_command_fenced_to_another_term_touches_neither_the_state_nor_the_ledger() {
        let mut kv = KvState::new();
        let fenced = command(Some(3), CommandBody::Txn(Txn::new().put("k", "v")));

        let refused = fenced.clone().apply(4, everyone, &mut kv);
        assert_eq!(CommandOutcome::StaleTerm, refused.outcome);
        assert!(refused.changes.is_none() && refused.registry.is_none());
        assert_eq!(0, kv.view().revision());

        // The ledger did not record the refusal: the same request id still runs in its own term.
        let applied = fenced.apply(3, everyone, &mut kv);
        assert_eq!(CommandOutcome::Txn(Ok(1)), applied.outcome);
        assert_eq!(Some(1), applied.changes.map(|changes| changes.revision));
    }

    #[test]
    fn a_retried_request_id_answers_the_same_and_publishes_nothing() {
        let mut kv = KvState::new();
        let write = command(None, CommandBody::Txn(Txn::new().put("k", "v")));

        assert_eq!(
            CommandOutcome::Txn(Ok(1)),
            write.clone().apply(1, everyone, &mut kv).outcome
        );
        let retried = write.apply(1, everyone, &mut kv);
        assert_eq!(CommandOutcome::Txn(Ok(1)), retried.outcome);
        assert!(retried.changes.is_none(), "nothing changed the second time");
        assert_eq!(1, kv.view().revision());
    }

    #[test]
    fn registering_publishes_the_registry_without_touching_the_key_space() {
        let mut kv = KvState::new();

        let applied = command(None, register(7)).apply(1, everyone, &mut kv);

        assert_eq!(CommandOutcome::Registry, applied.outcome);
        assert!(applied.changes.is_none());
        let registry = applied.registry.expect("a new entry is a change");
        assert_eq!(Some(NodeStatus::Up), registry.get(7).map(|info| info.status));
        assert_eq!(
            0,
            kv.view().revision(),
            "a registry command never advances the revision"
        );
    }

    #[test]
    fn a_node_registering_again_is_up_whatever_it_was_judged_before() {
        let mut kv = KvState::new();
        command(None, register(7)).apply(1, everyone, &mut kv);
        command(None, set_status(7, NodeStatus::Down)).apply(1, everyone, &mut kv);
        assert_eq!(Some(NodeStatus::Down), kv.registry().get(7).map(|info| info.status));

        let applied = command(None, register(7)).apply(1, everyone, &mut kv);

        let registry = applied.registry.expect("the status changed back");
        assert_eq!(Some(NodeStatus::Up), registry.get(7).map(|info| info.status));
    }

    /// The leader's judgement can be stale by the time it is applied; and a command that found
    /// nothing to change gives the registry's subscribers nothing to hear about.
    #[test]
    fn a_status_for_an_entry_that_is_gone_or_already_has_it_changes_nothing() {
        let mut kv = KvState::new();
        command(None, register(7)).apply(1, everyone, &mut kv);

        let gone = command(None, set_status(8, NodeStatus::Down)).apply(1, everyone, &mut kv);
        assert_eq!(CommandOutcome::Registry, gone.outcome);
        assert!(gone.registry.is_none());

        let unchanged = command(None, set_status(7, NodeStatus::Up)).apply(1, everyone, &mut kv);
        assert!(unchanged.registry.is_none());
    }

    #[test]
    fn reconciling_removes_exactly_the_entries_outside_the_applied_membership() {
        let mut kv = KvState::new();
        for node_id in [1, 2, 3] {
            command(None, register(node_id)).apply(1, everyone, &mut kv);
        }

        let applied = command(None, CommandBody::ReconcileRegistry).apply(1, |id| id != 2, &mut kv);
        let registry = applied.registry.expect("an entry was removed");
        assert_eq!(vec![1, 3], registry.iter().map(|(id, _)| id).collect::<Vec<_>>());

        let nothing_stale = command(None, CommandBody::ReconcileRegistry).apply(1, |id| id != 2, &mut kv);
        assert!(nothing_stale.registry.is_none());
    }
}
