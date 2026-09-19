//! The write path: any node, forwarding, same-request-id retry, read-your-writes (ech0-media#30)
//!
//! [`EzKv::write`] and [`EzKv::write_with_id`] work on every node: a leader proposes locally, a
//! follower or learner forwards `{request_id, txn}` to whoever it believes leads through the
//! internal `ez.write` RPC (`write::forward`) and retries - with the *same* request id - on
//! anything short of a definite answer, until [`crate::EzConfig::write_deadline`] runs out. Only
//! the idempotency ledger (`crate::kv::ledger`, consulted inside `apply` before a command ever
//! executes) makes a retried command safe to propose again: this module never tries to avoid
//! proposing twice, only to make doing so harmless.
//!
//! - **Limits are checked twice, deliberately** (ech0-media#12): once here, on the calling node,
//!   before anything is sent - a rejection this way costs nothing and involves no other node - and
//!   again by [`propose`] on whichever node actually ends up proposing, since that may be a
//!   differently-configured leader. [`crate::LimitError`] therefore has plain serde derives, so it
//!   can travel back over the forward RPC.
//! - **Read-your-writes by default** (ech0-media#16): once an outcome is definite, this node waits
//!   for its own published revision to reach it before returning - a success only once
//!   [`EzKv::view`] would already show it, a conflict only once the caller's upcoming re-read could
//!   actually see what it collided with - within the same overall deadline. A wait that runs out
//!   turns a success into [`WriteError::LocalLag`], never a lie about a conflict.
//! - **Indeterminate, never a false success**: once the deadline passes with no definite answer -
//!   including a local propose failing because this node's own Raft engine is mid-shutdown -
//!   [`WriteError::Indeterminate`] is what comes back. [`EzKv::write_with_id`] with the same id
//!   later resolves it, within the ledger's window.

mod forward;

use std::fmt;
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;
use openraft::errors::ClientWriteError;
use openraft::errors::RaftError;

use crate::command::Command;
use crate::command::CommandBody;
use crate::command::CommandOutcome;
use crate::kv::Conflict;
use crate::kv::KvLimits;
use crate::kv::LimitError;
use crate::kv::RequestId;
use crate::kv::Txn;
use crate::raft::EzKv;
use crate::raft::ORRaft;
use crate::rpc::Rpc;

/// Why [`EzKv::write`] or [`EzKv::write_with_id`] did not produce a new revision
///
/// [`Self::LimitExceeded`] and [`Self::Conflict`] mean nothing was proposed anywhere, or - if a
/// forward happened - that whichever node actually proposed found nothing to apply: every
/// replica is untouched. There is no `NotLeader` here: any node answers `write`, forwarding to the
/// leader on its own.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// `txn` exceeded a configured limit - either this node's own (checked first, nothing sent),
    /// or the proposing node's (checked again before entering the log)
    #[error(transparent)]
    LimitExceeded(#[from] LimitError),

    /// `txn`'s conditions were not all satisfied
    #[error(transparent)]
    Conflict(#[from] Conflict),

    /// The deadline passed with no definite outcome: the write may or may not have committed.
    /// [`EzKv::write_with_id`] with this same `request_id` resolves it later, within the
    /// idempotency ledger's window.
    #[error("write outcome unknown after the deadline (request {request_id})")]
    Indeterminate {
        /// The id a later [`EzKv::write_with_id`] call must reuse to find out what really
        /// happened
        request_id: RequestId,
    },

    /// The write committed, but this node's own applied state did not catch up to it within the
    /// deadline
    #[error(transparent)]
    LocalLag(#[from] LocalLag),
}

/// The write committed at `revision`, but this node's own applied state has not caught up to it
/// (yet)
///
/// Not a failure: the data is durably committed and already visible on whichever node's log it
/// reached first. [`EzKv::wait_revision`] (or another [`EzKv::write`]/[`EzKv::view`] later on this
/// same node) resolves it once ordinary replication catches this node up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("write committed at revision {revision}, but this node has not caught up to it yet")]
pub struct LocalLag {
    /// The revision this node has not yet applied
    pub revision: u64,
}

/// One attempt's result: either a definite outcome to stop on, or a reason to retry
///
/// Shared by every kind of command the any-node command path carries: [`EzKv::write_with_id`]'s
/// own loop proposes [`CommandBody::Txn`] and unwraps [`CommandOutcome::Txn`] out of `Done`;
/// `crate::registry` proposes [`CommandBody::RegisterNode`]/`RemoveNode` and expects
/// [`CommandOutcome::Registry`] instead. Neither caller can ever see the other's outcome variant
/// in practice - each proposes only its own kind of body, and [`crate::storage::adapter`]'s apply
/// loop answers a command with the outcome shape that matches its own `CommandBody` variant - so
/// unwrapping the wrong one is an invariant violation, not a state either caller has to handle.
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
        /// The leader that node hinted at instead, if it named one. Not acted on directly today -
        /// the next attempt still waits for *this* node's own metrics to reflect a leadership
        /// change - kept for the tracing event and a future attempt that could retry against the
        /// hint directly.
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
    fn target(&self) -> Option<u64> {
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
enum ProposeOutcome {
    /// A definite outcome: the command was applied (or deduplicated by the ledger).
    Definite(CommandOutcome),
    /// This node - whichever one `raft` belongs to - does not durably lead right now
    NotLeader {
        leader_id: Option<u64>,
        leader_addr: Option<String>,
    },
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
async fn propose(
    raft: &ORRaft,
    kv_limits: KvLimits,
    ledger_capacity: usize,
    request_id: RequestId,
    body: CommandBody,
) -> Result<ProposeOutcome, LimitError> {
    if let CommandBody::Txn(txn) = &body {
        txn.validate(&kv_limits)?;
    }

    let command = Command {
        request_id,
        ledger_capacity: ledger_capacity as u64,
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
            leader_addr: fwd.leader_node.map(|node| node.internal_addr),
        }),
        Err(_other) => Ok(ProposeOutcome::Unknown),
    }
}

/// Register this node's `ez.write` handler - see `write::forward`
///
/// Called once, from [`EzKv::new`](crate::raft::EzKv::new), for every node: a node that never
/// calls its own `write` must still answer another node's forwarded one - and, since this same
/// RPC now carries every kind of internal command (ech0-media#33), a node that never proposes a
/// registry command of its own must still answer one forwarded to it.
pub(crate) fn register_forward_handler(rpc: &Rpc, raft: ORRaft, kv_limits: KvLimits, ledger_capacity: usize) {
    forward::register(rpc, raft, kv_limits, ledger_capacity);
}

/// One attempt at committing `body` as a fresh idempotency-tracked command: locally if `node`
/// currently leads, forwarded to whoever it believes does otherwise
///
/// The any-node command path in full, minus the retry loop around it: [`EzKv::write_with_id`]'s
/// own loop is one caller, and `crate::registry`'s startup registration and
/// [`EzKv::update_node_meta`](crate::EzKv::update_node_meta) are others - reusing this rather than
/// growing a second forwarding path per kind of command (ech0-media#33).
///
/// `remaining` bounds this one attempt: a local propose is wrapped in a timeout of exactly
/// `remaining` (openraft's `client_write` never times out on its own, and a leader cut off from
/// its quorum mid-write would otherwise hang this call past the caller's own deadline); a forward
/// bounds the actual RPC round to `remaining` capped at one election timeout, so one unresponsive
/// peer cannot itself consume a caller's whole budget before it gets to try elsewhere - see
/// `forward::attempt`.
pub(crate) async fn attempt_command(
    node: &EzKv,
    request_id: RequestId,
    body: CommandBody,
    remaining: Duration,
) -> Attempt {
    if node.is_leader() {
        match tokio::time::timeout(remaining, propose_command_locally(node, request_id, body)).await {
            Ok(attempt) => attempt,
            Err(_elapsed) => Attempt::Retry(RetryReason::LocalCommitTimeout),
        }
    } else {
        forward::attempt(node, request_id, body, remaining).await
    }
}

/// The local-propose half of [`attempt_command`]: `node` believes it leads, so this proposes
/// directly rather than forwarding
async fn propose_command_locally(node: &EzKv, request_id: RequestId, body: CommandBody) -> Attempt {
    match propose(node.inner(), node.kv_limits(), node.ledger_capacity(), request_id, body).await {
        Err(limit_err) => Attempt::Done(Err(limit_err)),
        Ok(ProposeOutcome::Definite(outcome)) => Attempt::Done(Ok(outcome)),
        Ok(ProposeOutcome::NotLeader { .. }) => Attempt::Retry(RetryReason::LeaseGap),
        Ok(ProposeOutcome::Unknown) => Attempt::Retry(RetryReason::LocalEngineUnavailable),
    }
}

impl EzKv {
    /// Write `txn`, on any node
    ///
    /// Assigns a fresh [`RequestId`] and calls [`Self::write_with_id`] with it. Use
    /// [`Self::write_with_id`] directly only to retry a specific earlier attempt (see its own
    /// docs).
    ///
    /// # Errors
    ///
    /// See [`WriteError`].
    pub async fn write(&self, txn: Txn) -> Result<u64, WriteError> {
        self.write_with_id(RequestId::new(), txn).await
    }

    /// Write `txn` under a caller-supplied `request_id`, on any node
    ///
    /// Validates `txn` against this node's own [`crate::EzConfig::kv_limits`] before sending
    /// anything. If this node leads, it proposes locally; otherwise it forwards
    /// `{request_id, txn}` to whichever node it believes leads, through the internal `ez.write`
    /// RPC. On anything short of a definite outcome - `ForwardToLeader` (with or without a leader
    /// named), a remote "not leader", a connection failure, an RPC timeout, a lost response, or no
    /// leader known right now - it retries with the *same* `request_id`, waiting for a leadership
    /// change or a short backoff between attempts, never spinning, until
    /// [`crate::EzConfig::write_deadline`] elapses.
    ///
    /// Once an outcome is definite, this node waits (within the same overall deadline) for its own
    /// published revision to reach whatever the outcome depends on: a success only returns once
    /// [`Self::view`] would already show it (otherwise [`WriteError::LocalLag`]); a conflict waits,
    /// best-effort, for the local view to reach the conflict's `current_mod_rev` first, so the
    /// caller's upcoming re-read can actually see what it collided with.
    ///
    /// **Idempotency window**: within the ledger's window (`crate::EzConfig::ledger_capacity`
    /// entries, evicted first-in-first-out), a repeated call with the same `request_id` returns
    /// the *first* real outcome without executing `txn` again - **the caller must pass the same
    /// transaction it passed the first time**; the ledger stores outcomes, not transactions, so a
    /// different `txn` under a reused id is not detected and simply never runs. Outside the
    /// window, the same id executes as if new.
    ///
    /// # Errors
    ///
    /// See [`WriteError`].
    pub async fn write_with_id(&self, request_id: RequestId, txn: Txn) -> Result<u64, WriteError> {
        txn.validate(&self.kv_limits())?;

        let deadline = tokio::time::Instant::now() + self.write_deadline();
        let backoff = Duration::from_millis(self.inner().config().heartbeat_interval);

        let mut attempts: u32 = 0;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                tracing::warn!(
                    request_id = %request_id,
                    attempts,
                    "write forwarding gave up: no definite outcome within the deadline",
                );
                return Err(WriteError::Indeterminate { request_id });
            }
            attempts += 1;

            let believed_leader = self.inner().metrics().borrow_watched().current_leader;

            let attempt = attempt_command(self, request_id, CommandBody::Txn(txn.clone()), remaining).await;

            match attempt {
                Attempt::Done(Ok(CommandOutcome::Txn(Ok(revision)))) => {
                    return self.after_success(request_id, revision, deadline).await;
                }
                Attempt::Done(Ok(CommandOutcome::Txn(Err(conflict)))) => {
                    self.wait_for_conflict_visibility(&conflict, deadline).await;
                    return Err(WriteError::Conflict(conflict));
                }
                Attempt::Done(Ok(CommandOutcome::Registry)) => unreachable!(
                    "write_with_id only ever proposes CommandBody::Txn, whose outcome is always CommandOutcome::Txn"
                ),
                Attempt::Done(Err(err)) => return Err(WriteError::LimitExceeded(err)),
                Attempt::Retry(reason) => {
                    tracing::debug!(
                        request_id = %request_id,
                        attempt = attempts,
                        reason = %reason,
                        target_leader = ?reason.target(),
                        "write-forward retry",
                    );
                    self.wait_before_next_attempt(believed_leader, backoff, remaining).await;
                }
            }
        }
    }

    /// Wait for this node's own published [`Self::view`] revision to reach `revision`
    ///
    /// Exposes the same catch-up wait [`Self::write`] does after a success, for a receipt
    /// obtained elsewhere (another node's answer to the same logical write, or a revision read
    /// off a [`crate::Changes`] group).
    ///
    /// # Errors
    ///
    /// [`LocalLag`] if `timeout` elapses first - the revision may already be committed elsewhere,
    /// just not yet applied here.
    pub async fn wait_revision(&self, revision: u64, timeout: Duration) -> Result<(), LocalLag> {
        let deadline = tokio::time::Instant::now() + timeout;
        if self.await_local_revision(revision, deadline).await {
            Ok(())
        } else {
            Err(LocalLag { revision })
        }
    }

    /// Wait for a leadership change away from `believed_leader`, or `backoff`, whichever comes
    /// first - never longer than `remaining`, and never a busy loop either way
    ///
    /// `pub(crate)`, not private: every any-node command's retry loop waits the same way between
    /// attempts, `crate::registry`'s included (ech0-media#33).
    pub(crate) async fn wait_before_next_attempt(
        &self,
        believed_leader: Option<u64>,
        backoff: Duration,
        remaining: Duration,
    ) {
        let budget = backoff.min(remaining);
        let _ = self
            .wait_metrics(
                Some(budget),
                |m| m.current_leader != believed_leader,
                "waiting for a leadership change before retrying a forwarded write",
            )
            .await;
    }

    /// After a success at `revision`: wait for this node's own view to catch up, within the
    /// overall deadline, or report [`WriteError::LocalLag`]
    async fn after_success(
        &self,
        request_id: RequestId,
        revision: u64,
        deadline: tokio::time::Instant,
    ) -> Result<u64, WriteError> {
        if self.await_local_revision(revision, deadline).await {
            Ok(revision)
        } else {
            tracing::info!(
                request_id = %request_id,
                revision,
                "write committed, but this node's local view has not caught up to it yet",
            );
            Err(WriteError::LocalLag(LocalLag { revision }))
        }
    }

    /// Best-effort: wait for this node's view to reach `conflict.current_mod_rev` before the
    /// caller sees the conflict, so its upcoming re-read can actually observe what it collided
    /// with. `write` still returns the conflict either way - a local view still behind after the
    /// deadline is not itself a failure here, only something the caller's re-read may briefly
    /// still miss.
    async fn wait_for_conflict_visibility(&self, conflict: &Conflict, deadline: tokio::time::Instant) {
        let _ = self.await_local_revision(conflict.current_mod_rev, deadline).await;
    }

    /// Wait until this node's own [`Self::view`] has reached `revision`, or `deadline` passes first
    ///
    /// Woken by the publish that makes it true, not by polling for it.
    async fn await_local_revision(&self, revision: u64, deadline: tokio::time::Instant) -> bool {
        self.view_receiver().wait_for_revision(revision, deadline).await
    }
}
