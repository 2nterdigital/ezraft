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

mod error;
mod forward;
mod propose;

use std::time::Duration;

use openraft::async_runtime::WatchReceiver;

pub use self::error::LeaderWriteError;
pub use self::error::LocalLag;
pub use self::error::WriteError;
pub(crate) use self::propose::Attempt;
use self::propose::RetryReason;
use self::propose::propose_command_locally;
pub(crate) use self::propose::propose_leader_only;
use crate::command::CommandBody;
use crate::command::CommandOutcome;
use crate::kv::Conflict;
use crate::kv::KvLimits;
use crate::kv::RequestId;
use crate::kv::Txn;
use crate::raft::EzKv;
use crate::raft::ORRaft;
use crate::rpc::Rpc;

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
                // A transaction is answered as a transaction. Anything else came from a node that
                // does not speak this one's protocol - the answer to a forwarded write is a peer's
                // to give - so it is no outcome to vouch for, and certainly no reason to panic.
                Attempt::Done(Ok(unexpected @ (CommandOutcome::Registry | CommandOutcome::StaleTerm))) => {
                    tracing::error!(
                        request_id = %request_id,
                        outcome = ?unexpected,
                        "a transaction was answered with another kind of command's outcome",
                    );
                    self.wait_before_next_attempt(believed_leader, backoff, Some(deadline)).await;
                }
                Attempt::Done(Err(err)) => return Err(WriteError::LimitExceeded(err)),
                Attempt::Retry(reason) => {
                    tracing::debug!(
                        request_id = %request_id,
                        attempt = attempts,
                        reason = %reason,
                        target_leader = ?reason.target(),
                        "write-forward retry",
                    );
                    self.wait_before_next_attempt(believed_leader, backoff, Some(deadline)).await;
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
    /// first - never past `deadline`, and never a busy loop either way
    ///
    /// `deadline` rather than a remaining budget: the attempt that just failed spent some of it,
    /// so only the deadline itself still says how long this may wait. `None` is a loop that
    /// retries for as long as it takes.
    ///
    /// `pub(crate)`, not private: every any-node command's retry loop waits the same way between
    /// attempts, `crate::registry`'s included (ech0-media#33).
    pub(crate) async fn wait_before_next_attempt(
        &self,
        believed_leader: Option<u64>,
        backoff: Duration,
        deadline: Option<tokio::time::Instant>,
    ) {
        let budget = match deadline {
            Some(deadline) => backoff.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
            None => backoff,
        };
        let started = tokio::time::Instant::now();
        let leader_changed = self
            .wait_metrics(
                Some(budget),
                |m| m.current_leader != believed_leader,
                "waiting for a leadership change before retrying a forwarded write",
            )
            .await
            .is_ok();

        // Only a leadership change is a reason to come back early. The wait also ends early, with
        // an error, once this node has been shut down - its metrics are gone for good - and a
        // caller's retry loop must not become a busy loop for the rest of its deadline on that.
        if !leader_changed {
            tokio::time::sleep(budget.saturating_sub(started.elapsed())).await;
        }
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
