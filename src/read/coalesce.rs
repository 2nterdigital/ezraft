//! Follower-side coalescing for the `ez.read_index` RPC: at most one round in flight per node
//!
//! A "round" is one attempt at getting a read position from this node's believed leader: at
//! most one RPC call - or none at all, if no leader is currently known.
//! [`ReadIndexCoalescer`] is a single background task (spawned once, in
//! [`crate::raft::EzKv::new`]) that owns all of its state as plain local variables in
//! [`run`] - no lock is ever held, none is even needed - and callers reach it only through a
//! channel:
//!
//! - a call that arrives while no round is running starts one immediately, and is joined by every
//!   other call that manages to arrive before the task gets around to actually sending the RPC (an
//!   `mpsc` channel buffers exactly that, at essentially no cost, since nothing between receiving a
//!   message and issuing the RPC ever awaits anything that can pause for long);
//! - a call that arrives once a round's RPC is already in flight cannot join it - that round's read
//!   position may predate the request - so it waits behind the channel until the round finishes,
//!   and is then served, together with everyone else who arrived in the meantime, by the very next
//!   round.
//!
//! However many callers arrive during one round's flight, they all land in that single next
//! round: a burst of N concurrent callers therefore costs at most two RPC rounds - the one
//! already running when the burst starts (if any), and the one that drains everyone who arrived
//! while it was running (ech0-media#31).
//!
//! A caller that drops its half of the channel - its own timeout elapsed, or it was cancelled -
//! never affects the round or any other waiter: [`run`] does not care whether anyone is still
//! listening by the time a round's result is ready, and a round it already started keeps running
//! to completion regardless (nothing here ever aborts one).

use std::sync::Arc;
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::READ_INDEX_RPC_NAME;
use super::ReadIndexReply;
use crate::entry::EzLogId;
use crate::raft::ORRaft;
use crate::rpc::Rpc;
use crate::rpc::RpcError;

/// What one round produced: the confirmed read position, or why it could not be obtained.
///
/// Wrapped in `Arc` so every waiter of a round can hold a clone of the same failure without
/// [`RoundError`] itself needing to implement `Clone` - it wraps [`RpcError`], which does not.
pub(crate) type RoundOutcome = Result<EzLogId, Arc<RoundError>>;

/// Why one `ez.read_index` round did not produce a read position
#[derive(Debug, thiserror::Error)]
pub(crate) enum RoundError {
    /// This node knows of no leader right now (`RaftMetrics::current_leader` is `None`); no RPC
    /// was even attempted
    #[error("no leader is currently known")]
    NoLeaderKnown,

    /// The node this round asked does not (or no longer) lead
    #[error("node {asked} does not currently lead")]
    NotLeader {
        /// The node id this round addressed, believing it led
        asked: u64,
        /// The leader that node hinted at instead, if it named one
        leader_id: Option<u64>,
        /// That hinted leader's address, if any
        ///
        /// Not read anywhere yet: today's retry just waits for this node's own `current_leader`
        /// metric to catch up on the next round rather than addressing the hint directly, since
        /// every acceptance scenario this ticket covers converges either way. A future round
        /// could act on it to shave off a wasted round-trip on a stale-leader-hint chain.
        #[allow(dead_code, reason = "kept for a future round that retries against the hint directly")]
        leader_addr: Option<String>,
    },

    /// The leader's own, authoritative membership no longer counts this node
    #[error("this node is not a member of the leader's current membership")]
    NotMember,

    /// The RPC to the believed leader did not produce a typed result
    #[error(transparent)]
    Rpc(#[from] RpcError),

    /// This node's coalescing task has stopped
    ///
    /// Never actually observed through a live [`crate::EzKv`]: a call into
    /// [`ReadIndexCoalescer::request`] holds a clone of the very sender whose existence keeps
    /// [`run`] alive, so the channel this reports as closed cannot yet be closed. Kept as an
    /// explicit, typed fallback rather than a `panic!`/`unwrap` on that channel.
    #[error("the ReadIndex coalescer has stopped")]
    Stopped,
}

/// One request into the coalescer: answered with whichever round actually serves it
type Waiter = oneshot::Sender<RoundOutcome>;

/// The follower-side ReadIndex coalescer behind [`crate::raft::EzKv::linearizable_view`]
///
/// Cheap to `Clone`: every clone is a sender onto the same background loop, spawned once per
/// node by [`Self::spawn`] and shared by every [`crate::EzKv`] clone of that node.
#[derive(Clone)]
pub(crate) struct ReadIndexCoalescer {
    tx: mpsc::UnboundedSender<Waiter>,
}

impl ReadIndexCoalescer {
    /// Spawn the coalescing loop for one node, and return the handle every clone of that node's
    /// [`crate::EzKv`] shares
    ///
    /// `node_id` is this node's own id, sent with every round's RPC request so the leader can
    /// check it against its own membership (see [`super::leader_read_index`]).
    ///
    /// The loop runs for as long as any clone of the returned handle is alive; the last one
    /// dropped closes the channel, and [`run`] exits on its own rather than being told to stop -
    /// the same lifetime every other per-node background handle in this crate (`Rpc`,
    /// `Transport`) already has.
    pub(crate) fn spawn(node_id: u64, raft: ORRaft, rpc: Rpc) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(node_id, raft, rpc, rx));
        Self { tx }
    }

    /// Ask for a read position, coalesced with whatever else is asking right now
    ///
    /// Returns the outcome of whichever round actually serves this call. This future carries no
    /// timeout of its own - bounding it is the caller's job (`EzKv::linearizable_view` wraps it
    /// in `tokio::time::timeout`); dropping it, whether through cancellation or that wrapper's
    /// own timeout, only ever cancels this one wait, never the round itself or any other waiter
    /// (see the module doc comment).
    pub(crate) async fn request(&self) -> RoundOutcome {
        let (resp_tx, resp_rx) = oneshot::channel();

        // The receiving end of `self.tx` only closes once every sender clone - every `EzKv`
        // handle sharing this node's coalescer - has already been dropped. This call holds one
        // such clone (`&self`), so `send` failing here would mean the loop task itself panicked
        // and dropped its receiver while a sender was still alive; `RoundError::Stopped` is this
        // node's typed answer to that, not a case any correctness argument elsewhere depends on.
        if self.tx.send(resp_tx).is_err() {
            return Err(Arc::new(RoundError::Stopped));
        }

        resp_rx.await.unwrap_or_else(|_recv_error| Err(Arc::new(RoundError::Stopped)))
    }
}

/// The coalescing loop: one round at a time, batching every waiter that arrived since the
/// previous round finished
///
/// No lock, because there is nothing to share: every piece of state a round needs (`raft`,
/// `rpc`) is owned right here, and the only cross-task communication is the channel itself.
async fn run(node_id: u64, raft: ORRaft, rpc: Rpc, mut rx: mpsc::UnboundedReceiver<Waiter>) {
    while let Some(first) = rx.recv().await {
        let mut waiters = vec![first];
        // Drain whatever else is already sitting in the channel without waiting for more: this
        // is the "joins the round about to run" case - the round below has not started yet, so
        // this is still before it counts as "in flight". Anything that arrives *after* this
        // drain finds the channel empty and this loop already inside `round(..).await`, so it
        // waits for the next iteration instead, per the module doc comment's coalescing rule.
        while let Ok(more) = rx.try_recv() {
            waiters.push(more);
        }

        let outcome = round(node_id, &raft, &rpc).await;

        for waiter in waiters {
            // A waiter that already gave up (its own timeout fired, or it was cancelled) is not
            // this loop's problem: the round still ran, and everyone still listening gets it.
            let _ = waiter.send(outcome.clone());
        }
    }
}

/// One round: look up the currently believed leader and ask it for a read position with exactly
/// one RPC call - or, if this node knows of no leader at all, fail without sending anything
async fn round(node_id: u64, raft: &ORRaft, rpc: &Rpc) -> RoundOutcome {
    let Some(leader_id) = raft.metrics().borrow_watched().current_leader else {
        return Err(Arc::new(RoundError::NoLeaderKnown));
    };

    // Generous but bounded: long enough that a healthy leader always answers well within it,
    // short enough that a leader that genuinely cannot confirm a quorum does not hold this round
    // open indefinitely. `EzKv::linearizable_view`'s own retry loop is what actually enforces a
    // caller's timeout; this only bounds one round's own RPC.
    let round_timeout = Duration::from_millis(raft.config().election_timeout_max);

    match rpc.call::<u64, ReadIndexReply>(leader_id, READ_INDEX_RPC_NAME, node_id, round_timeout).await {
        Ok(ReadIndexReply::Granted { log_id }) => Ok(log_id),
        Ok(ReadIndexReply::NotLeader {
            leader_id: hint,
            leader_addr,
        }) => Err(Arc::new(RoundError::NotLeader {
            asked: leader_id,
            leader_id: hint,
            leader_addr,
        })),
        Ok(ReadIndexReply::NotMember) => Err(Arc::new(RoundError::NotMember)),
        Err(e) => Err(Arc::new(RoundError::Rpc(e))),
    }
}
