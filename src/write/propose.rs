//! Proposing a command on this node's own Raft: the one place a command is validated, stamped
//! and handed to openraft - for the any-node path when this node leads, for the `ez.write`
//! handler when another node forwarded to it, and, fenced to a term, for leader-only tasks

use std::fmt;
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;
use openraft::errors::ClientWriteError;
use openraft::errors::RaftError;

use super::LeaderWriteError;
use crate::command::Command;
use crate::command::CommandBody;
use crate::command::CommandOutcome;
use crate::kv::KvLimits;
use crate::kv::LimitError;
use crate::kv::RequestId;
use crate::raft::EzKv;
use crate::raft::ORRaft;

/// One attempt's result: either a definite outcome to stop on, or a reason to retry
///
/// Shared by every kind of command the any-node command path carries: [`EzKv::write_with_id`]'s
/// own loop proposes [`CommandBody::Txn`] and unwraps [`CommandOutcome::Txn`] out of `Done`;
/// `crate::registry` proposes [`CommandBody::RegisterNode`]/`RemoveNode` and expects
/// [`CommandOutcome::Registry`] instead. Neither caller can ever see the other's outcome variant
/// from a node of this version - each proposes only its own kind of body, and
/// [`crate::command::Command::apply`] answers a command with the outcome shape that matches its
/// body. A forwarded command's answer is a peer's to give, though, so each caller still treats
/// the wrong shape as no answer at all: logged, and retried.
pub(crate) enum Attempt {
    /// Stop retrying: `Ok` is whatever the command produced, `Err` is a limit rejection that
    /// never reached the log (a [`CommandBody::Txn`] only concern - `propose` never validates any
    /// other body, so this is never produced for one)
    Done(Result<CommandOutcome, LimitError>),
    /// Not definite: worth retrying, after the loop's usual wait
    Retry(RetryReason),
}

/// Why one attempt did not produce a definite outcome - purely for the structured tracing event
/// every any-node command's retry loop emits; never itself part of [`WriteError`]
#[derive(Debug)]
pub(crate) enum RetryReason {
    /// This node knows of no leader right now
    NoKnownLeader,
    /// This node believes it leads, but openraft's writable-leader check says its lease is not
    /// valid yet (a fresh election, or a membership change, before the first quorum
    /// acknowledgement) - the write was never appended, so retrying is safe
    LeaseGap,
    /// The local Raft engine did not answer with a proposal outcome or a not-leader redirect -
    /// for instance a fatal/shutdown condition mid-write. Unknown, not a success this node can
    /// vouch for.
    LocalEngineUnavailable,
    /// This node proposed the entry itself, but no commit came back before the deadline - a
    /// leader that lost its quorum mid-write. The entry is in its log and may yet commit.
    LocalCommitTimeout,
    /// The node this attempt forwarded to does not (or no longer) lead
    RemoteNotLeader {
        asked: u64,
        /// The leader that node hinted at instead, if it named one - for the tracing event: the
        /// next attempt asks whoever *this* node's own metrics name by then.
        hint: Option<u64>,
    },
    /// The forward RPC to the believed leader timed out
    RpcTimeout { asked: u64 },
    /// The believed leader could not be reached
    ConnectionFailure { asked: u64 },
    /// The believed leader is not in this node's current membership view
    StaleMembership { asked: u64 },
    /// The forward RPC reached the believed leader but did not produce a usable answer (an
    /// authorization mismatch, or a response this node's own RPC layer could not decode) - this
    /// node cannot tell whether the write ran
    LostResponse { asked: u64 },
}

impl RetryReason {
    /// The node this attempt was addressed to, if any - `None` for the local-propose path and for
    /// `NoKnownLeader`, where there was nobody to ask
    pub(super) fn target(&self) -> Option<u64> {
        match self {
            RetryReason::NoKnownLeader
            | RetryReason::LeaseGap
            | RetryReason::LocalEngineUnavailable
            | RetryReason::LocalCommitTimeout => None,
            RetryReason::RemoteNotLeader { asked, .. }
            | RetryReason::RpcTimeout { asked }
            | RetryReason::ConnectionFailure { asked }
            | RetryReason::StaleMembership { asked }
            | RetryReason::LostResponse { asked } => Some(*asked),
        }
    }
}

impl fmt::Display for RetryReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetryReason::NoKnownLeader => write!(f, "no leader is currently known"),
            RetryReason::LeaseGap => write!(f, "this node leads but its lease is not valid yet"),
            RetryReason::LocalEngineUnavailable => {
                write!(f, "the local Raft engine did not answer definitively")
            }
            RetryReason::LocalCommitTimeout => {
                write!(
                    f,
                    "this node proposed the entry but no commit came back before the deadline"
                )
            }
            RetryReason::RemoteNotLeader { asked, hint } => {
                write!(f, "node {asked} does not currently lead (hinted leader: {hint:?})")
            }
            RetryReason::RpcTimeout { asked } => write!(f, "the forward RPC to node {asked} timed out"),
            RetryReason::ConnectionFailure { asked } => write!(f, "node {asked} was unreachable"),
            RetryReason::StaleMembership { asked } => {
                write!(f, "node {asked} is not in this node's current membership view")
            }
            RetryReason::LostResponse { asked } => {
                write!(f, "the forward RPC to node {asked} did not produce a usable answer")
            }
        }
    }
}

/// What one actual `raft.client_write` attempt produced, before this module's own retry loop
/// interprets it
pub(super) enum ProposeOutcome {
    /// A definite outcome: the command was applied (or deduplicated by the ledger).
    Definite(CommandOutcome),
    /// This node - whichever one `raft` belongs to - does not durably lead right now
    NotLeader { leader_id: Option<u64> },
    /// Neither of the above: an error this milestone does not otherwise model for a plain KV
    /// write (for instance the engine shutting down mid-write). Never treated as a success.
    Unknown,
}

/// Validate `body` (a [`CommandBody::Txn`] only concern), then propose it on `raft` as a fresh
/// idempotency-tracked [`Command`]
///
/// Free function, not a method: called both from the local-propose path (`raft` is this node's
/// own) and from the `ez.write` RPC handler (`raft` is whichever node the caller forwarded to) -
/// the exact same "validate, stamp this node's own ledger capacity, propose" logic either way,
/// whatever kind of command `body` is. `ledger_capacity` is *this* call's own configured value:
/// whichever node ends up calling this is "the proposing leader" `crate::kv::ledger`'s module
/// docs describe.
pub(super) async fn propose(
    raft: &ORRaft,
    kv_limits: KvLimits,
    ledger_capacity: usize,
    request_id: RequestId,
    required_term: Option<u64>,
    body: CommandBody,
) -> Result<ProposeOutcome, LimitError> {
    // Every size a caller controls is checked here, before anything enters the log - never in
    // `apply`. The remaining command bodies carry node ids and a status, nothing of any size.
    match &body {
        CommandBody::Txn(txn) => txn.validate(&kv_limits)?,
        CommandBody::RegisterNode(entry) => entry.validate(&kv_limits)?,
        CommandBody::RemoveNode(_) | CommandBody::SetStatus(_) | CommandBody::ReconcileRegistry => {}
    }

    let command = Command {
        request_id,
        ledger_capacity: ledger_capacity as u64,
        required_term,
        body,
    };

    match raft.client_write(command).await {
        Ok(resp) => Ok(match resp.data {
            Some(outcome) => ProposeOutcome::Definite(outcome),
            // Never actually produced by `crate::storage::adapter::state_machine` (the adapter
            // always answers `Some` for a `Normal` entry) - kept as `Unknown` rather than a panic,
            // since a writer's future must never treat an unexpected shape as a success it cannot
            // vouch for.
            None => ProposeOutcome::Unknown,
        }),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => Ok(ProposeOutcome::NotLeader {
            leader_id: fwd.leader_id,
        }),
        Err(_other) => Ok(ProposeOutcome::Unknown),
    }
}

/// The local-propose half of [`attempt_command`]: `node` believes it leads, so this proposes
/// directly rather than forwarding
pub(super) async fn propose_command_locally(node: &EzKv, request_id: RequestId, body: CommandBody) -> Attempt {
    match propose(
        node.inner(),
        node.kv_limits(),
        node.ledger_capacity(),
        request_id,
        None,
        body,
    )
    .await
    {
        Err(limit_err) => Attempt::Done(Err(limit_err)),
        Ok(ProposeOutcome::Definite(outcome)) => Attempt::Done(Ok(outcome)),
        Ok(ProposeOutcome::NotLeader { .. }) => Attempt::Retry(RetryReason::LeaseGap),
        Ok(ProposeOutcome::Unknown) => Attempt::Retry(RetryReason::LocalEngineUnavailable),
    }
}

/// The leader-only propose path [`crate::leader_task::LeaderCtx::write`] and the built-in
/// liveness task's own internal commands share (ech0-media#34): never forwards, and checks that
/// `node` is leading *in `term`* immediately before every propose attempt, so a context captured
/// from an earlier leadership can never quietly commit under a later one just because the same
/// physical node happens to be re-elected.
///
/// # Fencing
///
/// Two checks, because neither is enough alone. Immediately before each attempt this reads
/// `node`'s own current metrics (a plain, synchronous watch read - no round trip) and returns
/// [`LeaderWriteError::NotLeader`] at once unless `node`'s role is
/// [`openraft::ServerState::Leader`] and its current term equals `term`: cheap, and it spares the
/// log an entry that could not take effect. But leadership can change between that read and the
/// append, so the command is also fenced to `term` ([`Command::required_term`]), and `apply`
/// refuses it unless the entry itself was appended in that term - which holds even if this same
/// node lost leadership and won it back in between. Because this function never forwards, `body`
/// can only ever be proposed on `node` itself: there is no path by which a write issued through a
/// stale context takes effect, through this node or any other.
///
/// Running out of time while still leading is [`LeaderWriteError::Indeterminate`], not
/// `NotLeader`: the entry may have been appended and may yet commit.
///
/// A [`ProposeOutcome::NotLeader`] from the propose attempt itself is retried, waiting a short
/// backoff, up to [`crate::EzConfig::write_deadline`] (the same bound [`EzKv::write`] retries a
/// whole forwarded write across, reused here for one node's own bounded local wait instead of a
/// second configured value): openraft's writable-leader check reports the alpha.33 no-lease gap
/// the exact same way it reports a genuine loss of leadership, and the fencing check at the top of
/// the next loop iteration is what actually tells the two apart, since a genuine loss of
/// leadership fails that check and returns immediately rather than retrying to no purpose.
pub(crate) async fn propose_leader_only(
    node: &EzKv,
    term: u64,
    body: CommandBody,
) -> Result<CommandOutcome, LeaderWriteError> {
    let deadline = tokio::time::Instant::now() + node.write_deadline();
    let backoff = Duration::from_millis(node.inner().config().heartbeat_interval);

    loop {
        let metrics = node.inner().metrics().borrow_watched().clone();
        if metrics.state != openraft::ServerState::Leader || metrics.current_term != term {
            return Err(LeaderWriteError::NotLeader { term });
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(LeaderWriteError::Indeterminate { term });
        }

        // Bounded by `remaining`, the same way `attempt_command`'s own local-propose path is:
        // openraft's `client_write` never times out on its own, and a leader that has lost its
        // quorum (isolated, say) but still holds a valid lease would otherwise hang this call past
        // `term`'s own bounded wait. A fresh `RequestId` every attempt is safe here - unlike
        // `EzKv::write_with_id`'s ledger-backed exactly-once retry, nothing this function proposes
        // is guaranteed to have never reached the log on a timeout, and this crate never claims
        // otherwise: duplicate actions across attempts are the task author's to fence with CAS
        // conditions, exactly as `LeaderCtx::write`'s own docs say.
        let attempt = tokio::time::timeout(
            remaining,
            propose(
                node.inner(),
                node.kv_limits(),
                node.ledger_capacity(),
                RequestId::new(),
                // The check above cannot by itself rule out that leadership changes before the
                // append: the entry is fenced to `term`, and `apply` refuses it unless the entry
                // itself was appended in that term.
                Some(term),
                body.clone(),
            ),
        )
        .await;

        match attempt {
            // Appended, perhaps, but not known to be committed: not a refusal.
            Err(_elapsed) => return Err(LeaderWriteError::Indeterminate { term }),
            Ok(Err(limit_err)) => return Err(LeaderWriteError::LimitExceeded(limit_err)),
            // The entry was appended under another leadership - even one this same node won
            // later - and changed nothing, on any replica.
            Ok(Ok(ProposeOutcome::Definite(CommandOutcome::StaleTerm))) => {
                return Err(LeaderWriteError::NotLeader { term });
            }
            Ok(Ok(ProposeOutcome::Definite(outcome))) => return Ok(outcome),
            Ok(Ok(ProposeOutcome::NotLeader { .. } | ProposeOutcome::Unknown)) => {
                tracing::debug!(
                    term,
                    "leader-only propose found no definite outcome yet (lease gap or transient \
                     engine state); retrying within the same term",
                );
                tokio::time::sleep(backoff.min(remaining)).await;
            }
        }
    }
}
