//! The node registry's handle-level half: reads, the change subscription, and the commands that
//! keep an entry in step with this node (ech0-media#33)
//!
//! `crate::kv::registry` owns the replicated data ([`NodeInfo`](crate::kv::NodeInfo),
//! [`Registry`](crate::kv::Registry)) and how one command mutates it; this module owns getting a
//! command there and back, the same way `crate::write` does for a transaction:
//!
//! - [`spawn_self_registration`] is started once, from [`crate::raft::EzKv::new`], for every node:
//!   it proposes a single `RegisterNode` command under one request id, retrying - through the same
//!   any-node command path `crate::write` builds (forwarding, backoff, the idempotency ledger) -
//!   until it succeeds, however long that takes (this node may not even be a member yet, or the
//!   cluster may have no leader). [`crate::raft::EzKv::shutdown`] aborts it if it is still
//!   retrying.
//! - [`EzKv::update_node_meta`] runs the same kind of command later, bounded by the ordinary
//!   [`crate::EzConfig::write_deadline`] like any other write.
//! - [`remove_entry`] is what [`crate::raft::EzKv::remove_node`] calls after its membership change
//!   commits: best-effort, within one `write_deadline`, never turning a successful membership
//!   change into a reported failure if it cannot also clean up the registry in time. A leader that
//!   crashes in the (necessarily two-step) window between the two leaves a stale entry for the
//!   built-in leader task of ech0-media#34 to reconcile.
//! - [`EzKv::registry`] and [`EzKv::watch_registry`] are the read side: both go straight to the
//!   registry watch channel [`crate::storage::adapter::StateMachineStore`] publishes into
//!   (`crate::kv::Publisher::publish_registry`), never through Raft - a local read never blocks on
//!   consensus, the same way [`crate::EzKv::view`] does not.

use std::time::Duration;

use bytes::Bytes;
use openraft::async_runtime::WatchReceiver;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::command::CommandBody;
use crate::command::CommandOutcome;
use crate::kv::LimitError;
use crate::kv::RegisterNode;
use crate::kv::Registry;
use crate::kv::RemoveNode;
use crate::kv::RequestId;
use crate::raft::EzKv;
use crate::write::Attempt;

/// Why [`EzKv::update_node_meta`] did not confirm its update
///
/// The same two ways a write without conditions can end short of success: refused for its size
/// before anything was proposed, or still unconfirmed at the deadline.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryWriteError {
    /// The entry - this node's `external_addr` plus the new `meta` - is larger than a single
    /// value may be ([`LimitError::NodeEntryTooLarge`]). Nothing was proposed.
    #[error(transparent)]
    LimitExceeded(#[from] LimitError),

    /// The deadline passed with no confirmed result
    ///
    /// The update may still take effect. Calling [`EzKv::update_node_meta`] again is always safe:
    /// the entry is overwritten whole, so the last call to be applied wins.
    #[error("registry write outcome unknown after the deadline (request {request_id})")]
    Indeterminate {
        /// The id the command was proposed under - useful to correlate with the structured
        /// `tracing` events this module emits.
        request_id: RequestId,
    },
}

/// Start this node's self-registration task: propose `RegisterNode { node_id, external_addr,
/// meta }` once, retrying under one request id until it succeeds
///
/// Called exactly once, from [`crate::raft::EzKv::new`], right after this node has an id.
/// `crate::raft::EzKv::shutdown` aborts the returned handle if it is still running - "retries
/// until it succeeds" must not mean "forever, even after this node has stopped".
pub(crate) fn spawn_self_registration(node: EzKv, external_addr: String, meta: Bytes) -> JoinHandle<()> {
    tokio::spawn(async move {
        let node_id = node.node_id();
        let request_id = RequestId::new();
        let backoff = Duration::from_millis(node.inner().config().heartbeat_interval);
        // One attempt's own round timeout: unlike `update_node_meta` or a bounded write, this
        // loop has no overall deadline (it retries "until it succeeds"), so each attempt is
        // bounded only by one election timeout rather than a shrinking `remaining` budget.
        let round_timeout = Duration::from_millis(node.inner().config().election_timeout_max);

        loop {
            let body = CommandBody::RegisterNode(RegisterNode {
                node_id,
                external_addr: external_addr.clone(),
                meta: meta.clone(),
            });
            let believed_leader = node.inner().metrics().borrow_watched().current_leader;

            match crate::write::attempt_command(&node, request_id, body, round_timeout).await {
                Attempt::Done(Ok(_)) => {
                    tracing::info!(node_id, external_addr, "node registered itself in the registry");
                    return;
                }
                // `EzKv::new` already held this entry to this node's own limits, so only a leader
                // configured with stricter ones can answer this - and will again, every time.
                Attempt::Done(Err(refused)) => {
                    tracing::error!(node_id, error = %refused, "the leader refused this node's registry entry; not retrying");
                    return;
                }
                Attempt::Retry(reason) => {
                    tracing::debug!(node_id, %reason, "self-registration retry");
                    node.wait_before_next_attempt(believed_leader, backoff, None).await;
                }
            }
        }
    })
}

/// Remove `node_id`'s registry entry, best-effort, within one `write_deadline`
///
/// Called by [`crate::raft::EzKv::remove_node`] once its membership change has committed. Failing
/// to remove the entry in time is logged, not returned as an error: the membership change already
/// succeeded, which is the part `remove_node`'s caller actually asked for, and a leader crash
/// exactly here is the very window ech0-media#34's built-in leader task exists to reconcile.
pub(crate) async fn remove_entry(node: &EzKv, node_id: u64) {
    let request_id = RequestId::new();
    let deadline = tokio::time::Instant::now() + node.write_deadline();
    let backoff = Duration::from_millis(node.inner().config().heartbeat_interval);
    let body = CommandBody::RemoveNode(RemoveNode { node_id });

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            tracing::warn!(
                node_id,
                "could not confirm the node's registry entry was removed within the write deadline \
                 after its membership change committed - left for a future reconciling leader task",
            );
            return;
        }

        let believed_leader = node.inner().metrics().borrow_watched().current_leader;
        match crate::write::attempt_command(node, request_id, body.clone(), remaining).await {
            Attempt::Done(_) => return,
            Attempt::Retry(reason) => {
                tracing::debug!(node_id, %reason, "registry removal retry");
                node.wait_before_next_attempt(believed_leader, backoff, Some(deadline)).await;
            }
        }
    }
}

impl EzKv {
    /// This node's local, immediate read of the whole node registry
    ///
    /// Never blocks on consensus and never fails, the same way [`Self::view`] does not: it may lag
    /// the leader by however long replication takes. Cheap to call as often as needed (O(1) clone).
    pub fn registry(&self) -> Registry {
        self.registry_receiver().borrow().clone()
    }

    /// Subscribe to every future change to the whole node registry
    ///
    /// The registry is small (one entry per cluster member), so a subscriber is simply handed the
    /// whole current value on every change - no ring buffer, no `Reset`, unlike `EzKv::watch`.
    /// [`Self::registry`] and this both read the exact same published value.
    pub fn watch_registry(&self) -> watch::Receiver<Registry> {
        self.registry_receiver().clone()
    }

    /// Update this node's own registry entry with new `meta`, keeping its already-registered
    /// `external_addr`
    ///
    /// Bounded by [`crate::EzConfig::write_deadline`], like [`Self::write`] - proposes locally if
    /// this node leads, forwards and retries under one request id otherwise.
    ///
    /// # Errors
    ///
    /// [`RegistryWriteError::LimitExceeded`] if the entry would be larger than a single value may
    /// be - checked on this node before anything is sent, and again by the leader against its own
    /// limits; [`RegistryWriteError::Indeterminate`] if the deadline passes with no confirmed
    /// result.
    pub async fn update_node_meta(&self, meta: impl Into<Bytes>) -> Result<(), RegistryWriteError> {
        let entry = RegisterNode {
            node_id: self.node_id(),
            external_addr: self.external_addr().to_string(),
            meta: meta.into(),
        };
        entry.validate(&self.kv_limits())?;
        let body = CommandBody::RegisterNode(entry);

        let request_id = RequestId::new();
        let deadline = tokio::time::Instant::now() + self.write_deadline();
        let backoff = Duration::from_millis(self.inner().config().heartbeat_interval);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(RegistryWriteError::Indeterminate { request_id });
            }

            let believed_leader = self.inner().metrics().borrow_watched().current_leader;
            match crate::write::attempt_command(self, request_id, body.clone(), remaining).await {
                Attempt::Done(Ok(CommandOutcome::Registry)) => return Ok(()),
                // The leader's own limits are stricter than this node's.
                Attempt::Done(Err(refused)) => return Err(refused.into()),
                // A registry command is answered as one. Anything else came from a node that does
                // not speak this one's protocol - a forwarded command's answer is a peer's to give
                // - so it is not an outcome, and not worth a panic.
                Attempt::Done(unexpected) => {
                    tracing::error!(request_id = %request_id, outcome = ?unexpected, "unexpected answer to a registry command");
                    self.wait_before_next_attempt(believed_leader, backoff, Some(deadline)).await;
                }
                Attempt::Retry(reason) => {
                    tracing::debug!(request_id = %request_id, %reason, "update_node_meta retry");
                    self.wait_before_next_attempt(believed_leader, backoff, Some(deadline)).await;
                }
            }
        }
    }
}
