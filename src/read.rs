//! Linearizable reads on any node: ReadIndex, coalesced on the follower (ech0-media#31)
//!
//! [`EzKv::linearizable_view`] answers "what has definitely been committed", not merely "what
//! this node has applied so far" ([`EzKv::view`](crate::EzKv::view), which may lag). ReadIndex
//! only - there is no LeaseRead path anywhere in this crate, so correctness never depends on
//! clocks agreeing across nodes.
//!
//! - **On the leader**: [`openraft::Raft::get_read_linearizer`] with [`ReadPolicy::ReadIndex`]
//!   confirms leadership with a quorum of heartbeats, without waiting for this node's own apply;
//!   this node then waits locally until its applied index reaches the confirmed position and
//!   returns its own [`View`].
//! - **On a follower or a learner**: the exact same confirmation happens on the leader, reached
//!   through the internal `ez.read_index` RPC (a handler registered under the crate's reserved
//!   `ez.` name prefix, ech0-media#29). The leader's handler runs the same
//!   [`Raft::get_read_linearizer`] call and returns the read position over the wire, again without
//!   waiting for its own apply. This node rebuilds an
//!   [`openraft::raft::linearizable_read::Linearizer`] from that position with
//!   [`ReadLogId::from_log_id`], waits locally the same way the leader does, and returns *its own*
//!   view. Concurrent callers on one follower or learner share the RPC round through
//!   [`coalesce::ReadIndexCoalescer`] - see that module for the coalescing rule and why it bounds a
//!   concurrent burst to two RPC rounds. Leader-side requests are never coalesced: doing that would
//!   need changes inside openraft itself.
//!
//! # Why the returned view is never behind what `try_await_ready` reports as applied
//!
//! [`Linearizer::try_await_ready`] waits on openraft's own `RaftMetrics::last_applied`, which
//! only advances once the state-machine adapter's whole `apply` call for a batch of log entries
//! has returned (`openraft`'s apply worker reports a batch's last log id back to `RaftCore` only
//! after that call completes - see `openraft`'s `core/sm/worker.rs`). The adapter
//! (`crate::storage::adapter::state_machine`) publishes a transaction's new view *before*
//! answering that transaction's own writer, and it does so synchronously, inside the very same
//! `apply` call, before that call's stream of entries is fully drained and before it returns
//! (ech0-media#16's publish-before-reply rule). So by the time `RaftMetrics::last_applied` ever
//! reaches a given log index, on this node or on the leader, every transaction up to and
//! including it has *already* been published - there is no window in which "applied" is visible
//! to a metrics watcher before the view that transaction produced is visible to [`EzKv::view`].
//! [`await_local_apply`] relies on exactly this: once `try_await_ready` reports the read position
//! reached, `EzKv::view()` already reflects it.

mod coalesce;

use std::time::Duration;

pub(crate) use coalesce::ReadIndexCoalescer;
use coalesce::RoundError;
use openraft::LogId;
use openraft::ReadPolicy;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::LinearizableReadError;
use openraft::errors::RaftError;
use openraft::raft::linearizable_read::Linearizer;
use openraft::raft::linearizable_read::ReadLogId;
use serde::Deserialize;
use serde::Serialize;

use crate::entry::EzLogId;
use crate::kv::View;
use crate::raft::EzKv;
use crate::raft::ORRaft;
use crate::rpc::Rpc;
use crate::type_config::OpenRaftTypes;

/// Why [`EzKv::linearizable_view`] did not return a view
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// No leader could confirm leadership within the timeout: no leader was known, the known
    /// leader could not be reached, or a quorum could not be confirmed - every one of these is
    /// retried inside the budget (leadership can change, or an answer can be a redirect) rather
    /// than failing on the first miss.
    #[error("no leader could confirm leadership within the timeout")]
    NoLeader,

    /// This node (`.0`) is not a member of the current cluster
    #[error("node {0} is not a member of the current cluster")]
    NotMember(u64),

    /// The read position was confirmed, but this node's own apply did not reach it before the
    /// timeout: the write is committed, just not yet visible on this node
    #[error(
        "local apply (at {applied_index:?}) did not reach the confirmed read position \
         (index {read_index}) within the timeout"
    )]
    NotCaughtUp {
        /// The log index this node needed to have applied to answer the read
        read_index: u64,
        /// This node's own last-applied log index when the timeout elapsed, if it has applied
        /// anything at all
        applied_index: Option<u64>,
    },
}

/// The RPC name reserved for follower-initiated ReadIndex requests: `POST /rpc/ez.read_index`
const READ_INDEX_RPC_NAME: &str = "ez.read_index";

/// The wire reply for `ez.read_index`
///
/// The request is the caller's own node id (see [`register_read_index_handler`]): this handler
/// runs on whichever node the caller *believes* leads, and that belief can be stale, so a plain
/// RPC-level error would not tell a retrying caller anything it did not already know.
/// [`Self::NotLeader`] carries this node's own idea of who actually leads, letting the caller's
/// next round address the real leader sooner; [`Self::NotMember`] tells a caller the leader's
/// own, authoritative membership no longer counts it - a follower's own local membership view
/// can otherwise never learn this on its own once the leader stops replicating to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ReadIndexReply {
    /// This node confirmed leadership with a quorum and returns the read position, without
    /// waiting for its own apply to reach it - the caller waits for that locally.
    Granted {
        /// (term, index) - see [`EzLogId`]
        log_id: EzLogId,
    },
    /// This node does not currently lead, or could not confirm a quorum
    NotLeader {
        /// The leader this node knows of instead, if any
        leader_id: Option<u64>,
    },
    /// The caller is not a member of this node's current membership
    NotMember,
}

/// Register this node's `ez.read_index` handler
///
/// Called once, from [`EzKv::new`](crate::raft::EzKv::new), for every node: a node that never
/// leads is simply never asked, and one that does must already have this registered by the time
/// a follower's first request reaches it - registering it lazily, only when this node itself
/// calls [`EzKv::linearizable_view`], would leave a leader that never calls its own method
/// unable to ever answer one.
///
/// The request is the calling node's own id, not `()`: it is what lets this handler answer
/// [`ReadIndexReply::NotMember`] authoritatively (see [`leader_read_index`]) rather than relying
/// on the caller's own, possibly permanently-stale, view of whether it is still a member.
pub(crate) fn register_read_index_handler(rpc: &Rpc, raft: ORRaft) {
    rpc.register_internal::<u64, ReadIndexReply, std::convert::Infallible, _, _>(READ_INDEX_RPC_NAME, move |caller| {
        let raft = raft.clone();
        async move { Ok(leader_read_index(&raft, caller).await) }
    })
    .expect("'ez.read_index' is registered exactly once, right after this node's Rpc facade is built");
}

/// The leader side of `ez.read_index`: confirm leadership and hand back the read position,
/// without waiting for this node's own apply - the caller (a follower or learner) does that
/// locally, against its own applied index, once it has the position
///
/// Leadership is confirmed *first*, and only then is `caller` checked against this node's
/// membership. A node can be removed from the cluster and, once the leader stops replicating to
/// it, never again receive anything that would update its own local membership view - this RPC is
/// the one place a removed node reliably learns that it no longer counts. But only a node that
/// has just confirmed it leads may say so: its membership is the cluster's, where a follower's
/// may simply not have caught up with the very change that added the caller.
async fn leader_read_index(raft: &ORRaft, caller: u64) -> ReadIndexReply {
    let linearizer = match raft.get_read_linearizer(ReadPolicy::ReadIndex).await {
        Ok(linearizer) => linearizer,
        Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(fwd))) => {
            return ReadIndexReply::NotLeader {
                leader_id: fwd.leader_id,
            };
        }
        // No quorum could be confirmed, or the engine itself has stopped: either way this node
        // cannot vouch for a read position right now, and it knows of no better leader to name.
        Err(RaftError::APIError(LinearizableReadError::QuorumNotEnough(_))) | Err(RaftError::Fatal(_)) => {
            return ReadIndexReply::NotLeader { leader_id: None };
        }
    };

    let is_member = raft.metrics().borrow_watched().membership_config.membership().get_node(&caller).is_some();
    if !is_member {
        return ReadIndexReply::NotMember;
    }

    ReadIndexReply::Granted {
        log_id: to_ez_log_id(linearizer.read_log_id()),
    }
}

/// [`ReadLogId::committed_leader_id`]'s term, plus [`ReadLogId::index`] - the wire shape every
/// `ez.read_index` reply and every internal retry loop here carries a read position as
fn to_ez_log_id(read_log_id: &ReadLogId<OpenRaftTypes>) -> EzLogId {
    (read_log_id.committed_leader_id().term, read_log_id.index())
}

/// What one leader-side or follower-side attempt at confirming a read position produced
enum Attempt {
    /// A read position was confirmed; wait for local apply next.
    Ready(ReadPosition),
    /// This attempt did not succeed, but is worth trying again within the timeout.
    Retry,
    /// This attempt cannot succeed no matter how many more are tried; fail with this error now.
    Fail(ReadError),
}

/// A confirmed read position, in whichever shape this node obtained it
enum ReadPosition {
    /// The leader's own [`Linearizer`], already carrying its own `applied` hint from the same
    /// call that confirmed leadership
    Linearizer(Linearizer<OpenRaftTypes>),
    /// A raw (term, index) received from the leader over `ez.read_index`, to rebuild a
    /// [`Linearizer`] from locally - see [`ReadLogId::from_log_id`]'s own caveat that it does not
    /// verify its argument came from a leader; the RPC boundary here is what guarantees that.
    LogId(EzLogId),
}

impl EzKv {
    /// A linearizable read: a [`View`] that reflects every write acknowledged before this call
    /// started, on any node - leader, follower, or learner. ReadIndex only; there is no
    /// LeaseRead path, so correctness never depends on clocks agreeing across nodes.
    ///
    /// `timeout` bounds the whole call: confirming a read position and this node's own catch-up
    /// to it, together.
    ///
    /// This node's own membership is never checked against its own (possibly stale) local view:
    /// on the leader path, confirming leadership already implies membership, and on the follower
    /// path [`ReadError::NotMember`] comes from the leader's own authoritative answer to the
    /// `ez.read_index` RPC instead - a node's own view can never reliably self-detect its removal
    /// once the leader stops replicating to it.
    ///
    /// # Errors
    ///
    /// See [`ReadError`].
    pub async fn linearizable_view(&self, timeout: Duration) -> Result<View, ReadError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let retry_interval = Duration::from_millis(self.inner().config().heartbeat_interval);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(ReadError::NoLeader);
            }

            let attempt = if self.is_leader() {
                // A quorum round of its own, which a leader cut off from its quorum never
                // finishes: bounded here, or it would answer late by however long that takes.
                match tokio::time::timeout(remaining, self.leader_attempt()).await {
                    Ok(attempt) => attempt,
                    Err(_elapsed) => return Err(ReadError::NoLeader),
                }
            } else {
                self.follower_attempt(remaining).await
            };

            match attempt {
                Attempt::Ready(position) => {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    return self.await_local_apply(position, remaining).await;
                }
                Attempt::Retry => tokio::time::sleep(retry_interval.min(remaining)).await,
                Attempt::Fail(err) => return Err(err),
            }
        }
    }

    /// One leader-side attempt: confirm leadership locally with a quorum, without waiting for
    /// this node's own apply
    async fn leader_attempt(&self) -> Attempt {
        match self.inner().get_read_linearizer(ReadPolicy::ReadIndex).await {
            Ok(linearizer) => Attempt::Ready(ReadPosition::Linearizer(linearizer)),
            // A brand-new leader without a quorum-acknowledged lease yet, or one that just lost
            // touch with a quorum: worth retrying inside the budget. `is_leader()` is re-checked
            // on the next loop iteration too, so a node that genuinely stepped down switches to
            // the follower path - and a real leader elsewhere - on its own.
            Err(RaftError::APIError(_)) => Attempt::Retry,
            // The Raft engine itself has stopped: nothing left to retry against.
            Err(RaftError::Fatal(_)) => Attempt::Fail(ReadError::NoLeader),
        }
    }

    /// One follower- or learner-side attempt: join whichever ReadIndex round this call is
    /// allowed to ride (see the `coalesce` module), bounded by `remaining`
    async fn follower_attempt(&self, remaining: Duration) -> Attempt {
        match tokio::time::timeout(remaining, self.read_index().request()).await {
            Err(_elapsed) => Attempt::Fail(ReadError::NoLeader),
            Ok(Ok(log_id)) => Attempt::Ready(ReadPosition::LogId(log_id)),
            // The leader's own authoritative answer that this node no longer counts: retrying
            // cannot change that on its own, so this fails immediately rather than spending the
            // rest of the budget on rounds that would only repeat it.
            Ok(Err(round_error)) if matches!(*round_error, RoundError::NotMember) => {
                Attempt::Fail(ReadError::NotMember(self.node_id()))
            }
            Ok(Err(_round_error)) => Attempt::Retry,
        }
    }

    /// Wait for this node's own apply to reach `position`, then return the view it produced
    ///
    /// See the module doc comment for why that view is never behind what `try_await_ready`
    /// reports as applied.
    async fn await_local_apply(&self, position: ReadPosition, timeout: Duration) -> Result<View, ReadError> {
        let linearizer = match position {
            ReadPosition::Linearizer(linearizer) => linearizer,
            ReadPosition::LogId((term, index)) => {
                let read_log_id = ReadLogId::from_log_id(LogId::new_term_index(term, index));
                Linearizer::new(self.node_id(), read_log_id, None)
            }
        };

        match linearizer.try_await_ready(self.inner(), Some(timeout)).await {
            Ok(Ok(_ready)) => Ok(self.view()),
            Ok(Err(not_ready)) => Err(ReadError::NotCaughtUp {
                read_index: not_ready.read_log_id().index(),
                applied_index: not_ready.applied().map(|log_id| log_id.index()),
            }),
            // This node's own Raft engine stopped while waiting: nothing further to report.
            Err(_fatal) => Err(ReadError::NoLeader),
        }
    }
}
