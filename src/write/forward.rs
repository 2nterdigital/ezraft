//! The `ez.write` RPC: forward a command to whichever node this one believes leads
//!
//! [`register`] installs the leader-side handler, on every node, the same way
//! `crate::read`'s `ez.read_index` is: a node that never proposes a command of its own must still
//! answer one forwarded to it. [`attempt`] is the follower/learner side: one RPC round, addressed
//! by this node's own believed leader, turned into a [`super::Attempt`] the retry loop in
//! `write.rs` (and, since ech0-media#33, `crate::registry`'s) interprets the same way it
//! interprets a local propose. Despite the name, this carries every [`CommandBody`] the crate
//! defines, not only transactions - a transaction was simply the first, and the retry loop and
//! the ledger it leans on are exactly what the node registry's commands reuse rather than
//! growing a second forwarding path.
//!
//! No retry happens in this module itself, matching `crate::rpc`'s own contract - the caller's
//! own loop owns retrying with the same request id, across as many leadership changes as its
//! overall deadline allows.

use std::convert::Infallible;
use std::time::Duration;

use openraft::async_runtime::WatchReceiver;
use serde::Deserialize;
use serde::Serialize;

use super::Attempt;
use super::RetryReason;
use super::propose;
use crate::command::CommandBody;
use crate::command::CommandOutcome;
use crate::kv::KvLimits;
use crate::kv::LimitError;
use crate::kv::RequestId;
use crate::raft::EzKv;
use crate::raft::ORRaft;
use crate::rpc::Rpc;
use crate::rpc::RpcError;

/// The RPC name reserved for forwarded commands: `POST /rpc/ez.write`
const FORWARD_WRITE_RPC_NAME: &str = "ez.write";

/// The wire request for `ez.write`: the same `{request_id, body}` pair every attempt - local or
/// forwarded, whatever kind of command `body` is - carries
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ForwardRequest {
    request_id: RequestId,
    body: CommandBody,
}

/// The wire reply for `ez.write`
///
/// [`Self::NotLeader`] mirrors `crate::read`'s `ez.read_index` reply: this handler runs on
/// whichever node the caller *believes* leads, and that belief can be stale, so a plain RPC-level
/// error would not tell a retrying caller anything it did not already know.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ForwardReply {
    /// This node proposed the command (whether freshly executed or deduplicated by the ledger)
    /// and can report a definite outcome.
    Outcome(CommandOutcome),
    /// This node's own configured limits rejected the command's transaction before it was ever
    /// proposed (never produced for any other kind of command) - a definite, non-retryable
    /// outcome, not a reason to look for a different leader.
    LimitExceeded(LimitError),
    /// This node does not currently lead (or could not confirm it durably does)
    NotLeader {
        /// The leader this node knows of instead, if any
        leader_id: Option<u64>,
        /// That leader's address, if any
        leader_addr: Option<String>,
    },
}

/// Register this node's `ez.write` handler
///
/// `kv_limits` and `ledger_capacity` are captured once, from this node's own [`crate::EzConfig`],
/// for every call this handler ever answers - the same values [`crate::EzKv::write`]'s local
/// path already uses when *this* node proposes.
pub(super) fn register(rpc: &Rpc, raft: ORRaft, kv_limits: KvLimits, ledger_capacity: usize) {
    rpc.register_internal::<ForwardRequest, ForwardReply, Infallible, _, _>(FORWARD_WRITE_RPC_NAME, move |req| {
        let raft = raft.clone();
        async move { Ok(handle(&raft, kv_limits, ledger_capacity, req).await) }
    })
    .expect("'ez.write' is registered exactly once, right after this node's Rpc facade is built");
}

/// The leader side of `ez.write`: propose `req.body` under `req.request_id`, answering with
/// whatever [`propose`] produced
async fn handle(raft: &ORRaft, kv_limits: KvLimits, ledger_capacity: usize, req: ForwardRequest) -> ForwardReply {
    match propose(raft, kv_limits, ledger_capacity, req.request_id, req.body).await {
        Err(limit_err) => ForwardReply::LimitExceeded(limit_err),
        Ok(super::ProposeOutcome::Definite(outcome)) => ForwardReply::Outcome(outcome),
        Ok(super::ProposeOutcome::NotLeader { leader_id, leader_addr }) => {
            ForwardReply::NotLeader { leader_id, leader_addr }
        }
        // This node cannot vouch for anything right now (for instance its own engine shutting
        // down mid-propose): the honest answer is "ask someone else", exactly like naming no
        // leader at all.
        Ok(super::ProposeOutcome::Unknown) => ForwardReply::NotLeader {
            leader_id: None,
            leader_addr: None,
        },
    }
}

/// One follower/learner-side attempt: ask whichever node this one currently believes leads to
/// commit `body`
///
/// `remaining` bounds this one round; the caller's own loop enforces the overall deadline (if
/// any) across every attempt.
pub(super) async fn attempt(node: &EzKv, request_id: RequestId, body: CommandBody, remaining: Duration) -> Attempt {
    let Some(leader_id) = node.inner().metrics().borrow_watched().current_leader else {
        return Attempt::Retry(RetryReason::NoKnownLeader);
    };

    // Bounded well under `remaining` when it is generous, so one attempt against an unresponsive
    // peer cannot itself eat the whole deadline before the loop gets a chance to try elsewhere.
    let round_timeout = remaining.min(Duration::from_millis(node.inner().config().election_timeout_max));
    let request = ForwardRequest { request_id, body };

    match node
        .rpc()
        .call::<ForwardRequest, ForwardReply>(leader_id, FORWARD_WRITE_RPC_NAME, request, round_timeout)
        .await
    {
        Ok(ForwardReply::Outcome(outcome)) => Attempt::Done(Ok(outcome)),
        Ok(ForwardReply::LimitExceeded(err)) => Attempt::Done(Err(err)),
        Ok(ForwardReply::NotLeader { leader_id: hint, .. }) => {
            Attempt::Retry(RetryReason::RemoteNotLeader { asked: leader_id, hint })
        }
        Err(RpcError::Timeout { .. }) => Attempt::Retry(RetryReason::RpcTimeout { asked: leader_id }),
        Err(RpcError::Unreachable { .. }) => Attempt::Retry(RetryReason::ConnectionFailure { asked: leader_id }),
        Err(RpcError::NotMember(_)) => Attempt::Retry(RetryReason::StaleMembership { asked: leader_id }),
        Err(RpcError::Unauthorized(_) | RpcError::Remote(_)) => {
            Attempt::Retry(RetryReason::LostResponse { asked: leader_id })
        }
    }
}
