//! The write path: validate limits, then propose on this node only
//!
//! Forwarding a write to the leader, request-id retries and `Indeterminate`/`LocalLag` are
//! ech0-media#30. This ticket's `EzKv::write` proposes on the node it is called on and nothing
//! else: a follower answers [`WriteError::NotLeader`] without proposing anything.

use std::time::Duration;

use openraft::errors::ClientWriteError;
use openraft::errors::RaftError;

use crate::command::Command;
use crate::kv::Conflict;
use crate::kv::LimitError;
use crate::kv::Txn;
use crate::raft::EzKv;

/// Why [`EzKv::write`] did not produce a new revision
///
/// [`Self::LimitExceeded`] and [`Self::Conflict`] mean nothing was proposed - the transaction
/// never entered the log - and every replica is untouched. [`Self::NotLeader`] means the same:
/// this node did not propose anything, because it does not (yet, durably) lead.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// `txn` exceeded a configured limit, checked before anything was proposed
    #[error(transparent)]
    LimitExceeded(#[from] LimitError),

    /// `txn`'s conditions were not all satisfied
    #[error(transparent)]
    Conflict(#[from] Conflict),

    /// This node does not lead, and proposed nothing
    ///
    /// Forwarding to the node that does is ech0-media#30.
    #[error(transparent)]
    NotLeader(#[from] NotLeader),

    /// The Raft engine reported an error this milestone does not otherwise expect for a plain KV
    /// write (for instance a fatal shutdown mid-write)
    #[error("the Raft engine reported an unexpected error: {0}")]
    Fatal(String),
}

/// This node does not lead, and could not propose the write
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not the leader (leader: {leader_id:?} at {leader_addr:?})")]
pub struct NotLeader {
    /// The current leader's node id, if this node knows of one
    pub leader_id: Option<u64>,
    /// The current leader's address, if this node knows of one
    pub leader_addr: Option<String>,
}

impl EzKv {
    /// Validate `txn` against this node's [`crate::EzConfig::kv_limits`], then propose it **on
    /// this node only**
    ///
    /// A limit violation or a failed condition is reported without anything having been
    /// proposed. Success carries the transaction's new revision; by the time this returns,
    /// [`EzKv::view`] on this node already reflects it - the state-machine adapter publishes the
    /// new view before answering the writer (DESIGN.md §5), and that publish runs inside the
    /// same `apply` call that unblocks this future.
    ///
    /// alpha.33 requires the leader to hold a quorum-acknowledged lease before it accepts a
    /// write (OPENRAFT-A33.md §3): right after an election or a membership change, this node may
    /// lead by its own metrics yet have openraft answer an empty `ForwardToLeader` for a moment.
    /// The write was not appended in that case, so retrying is safe; this method does so, within
    /// a budget on the order of the election timeout, before giving up with
    /// [`WriteError::NotLeader`].
    ///
    /// # Errors
    ///
    /// See [`WriteError`].
    pub async fn write(&self, txn: Txn) -> Result<u64, WriteError> {
        txn.validate(&self.kv_limits())?;

        let config = self.inner().config();
        let retry_budget = Duration::from_millis(config.election_timeout_max);
        let retry_interval = Duration::from_millis(config.heartbeat_interval);
        let deadline = tokio::time::Instant::now() + retry_budget;

        loop {
            match self.inner().client_write(Command::Txn(txn.clone())).await {
                Ok(resp) => {
                    let outcome =
                        resp.data.ok_or_else(|| WriteError::Fatal("write produced no response".to_string()))?;
                    return outcome.map_err(WriteError::from);
                }
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(to_leader))) => {
                    // The lease gap OPENRAFT-A33.md §3 describes: this node's own metrics say it
                    // leads, yet openraft names no leader at all because the lease is not valid
                    // yet. Retrying is safe (nothing was appended) and worthwhile (the gap is
                    // brief) only while both hold and the budget is not spent.
                    let lease_gap = to_leader.leader_id.is_none() && self.is_leader();
                    if lease_gap && tokio::time::Instant::now() < deadline {
                        tokio::time::sleep(retry_interval).await;
                        continue;
                    }

                    return Err(WriteError::NotLeader(NotLeader {
                        leader_id: to_leader.leader_id,
                        leader_addr: to_leader.leader_node.map(|node| node.addr),
                    }));
                }
                Err(e) => return Err(WriteError::Fatal(e.to_string())),
            }
        }
    }
}
