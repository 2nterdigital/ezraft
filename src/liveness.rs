//! The built-in liveness task: leader-side peer health, and registry reconciliation
//! (ech0-media#34)
//!
//! [`install`] registers this crate's own [`crate::EzKv::spawn_leader_task`] factory, for every
//! node, at construction (`crate::raft::EzKv::new`) - the same mechanism any other leader-only
//! task uses, nothing privileged about it beyond running first.
//!
//! # Reading openraft's heartbeat metrics
//!
//! `RaftMetrics::heartbeat` exists only on the leader (`None` on a follower, candidate or learner)
//! and, once `Some`, is a map from *every* member's node id - voters and learners and the leader's
//! own id alike - to the instant of the last *acknowledged* replication or heartbeat RPC the
//! leader sent it, or `None` if none has been acknowledged yet this leadership. Both facts matter
//! here: a fresh leader's map starts with every other member at `None`, not "a long time ago", and
//! the map's own presence (`Some` vs `None`) is itself this task's signal for "is this node
//! currently the leader" - checked every tick rather than assumed for the run's whole lifetime,
//! since the supervisor's own cancellation is cooperative and this task could in principle be
//! polled once more before it lands.
//!
//! # The new-leader grace period
//!
//! A freshly elected leader's heartbeat map starts every peer at `None` - indistinguishable, by
//! that field alone, from a peer that has been genuinely silent forever. Treating `None` as
//! "silent since this leadership began" (`LeaderCtx::leader_since`, not some earlier epoch this
//! leader has no visibility into) and additionally requiring the *leadership itself* to be older
//! than `EzConfig::liveness_grace` before judging anyone `Down` is what stops a change of leader
//! from ever looking, on its own, like every peer going silent at once.
//!
//! # Determinism
//!
//! `EzConfig::liveness_threshold`/`liveness_grace` are read only here, by the leader task that
//! decides *which* status a node should have; `apply` (`crate::storage::adapter::state_machine`)
//! only ever assigns the status a [`crate::command::CommandBody::SetStatus`] command already
//! names, and reconciles against the state machine's own already-applied membership - never a
//! value this node's own configuration could disagree with another replica's about.

use std::collections::BTreeSet;
use std::time::Duration;

use openraft::Instant as _;
use openraft::RaftMetrics;
use openraft::async_runtime::WatchReceiver;

use crate::command::CommandBody;
use crate::command::CommandOutcome;
use crate::kv::NodeStatus;
use crate::kv::Registry;
use crate::kv::SetStatus;
use crate::leader_task::LeaderCtx;
use crate::raft::EzKv;
use crate::type_config::OpenRaftTypes;
use crate::write::LeaderWriteError;

/// Register the built-in liveness task on `node`
///
/// Called once, from [`crate::raft::EzKv::new`], for every node - see this module's own docs.
pub(crate) fn install(node: &EzKv) {
    node.spawn_leader_task("ez.liveness", |ctx: LeaderCtx| async move { run(ctx).await });
}

/// The task body: re-check every peer, and the registry against membership, on every tick, for as
/// long as this run is not cancelled
async fn run(ctx: LeaderCtx) {
    let threshold = ctx.node().liveness_threshold();
    let grace = ctx.node().liveness_grace();

    let mut ticker = tokio::time::interval(ctx.node().liveness_check_interval());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        check_once(&ctx, threshold, grace).await;
    }
}

/// One tick: judge every registered peer's status, then reconcile the registry against membership
async fn check_once(ctx: &LeaderCtx, threshold: Duration, grace: Duration) {
    let node = ctx.node();
    let metrics = node.inner().metrics().borrow_watched().clone();

    // `Some` only on the leader (see this module's own docs) - `None` means this node's own
    // metrics have not (or no longer) reflect leading; nothing to judge this tick, and the
    // supervisor will cancel this run once `cluster()` catches up.
    let Some(heartbeat) = metrics.heartbeat.clone() else {
        return;
    };

    let registry = node.registry();
    let self_id = metrics.id;
    let leadership_age = ctx.leader_since().elapsed();

    for (peer_id, info) in registry.iter() {
        let candidate_status = if peer_id == self_id {
            // The leader is always `Up` - corrects a `Down` a *previous* leadership may have left
            // behind before this node was (re-)elected.
            NodeStatus::Up
        } else {
            let last_ack = heartbeat.get(&peer_id).copied().flatten();
            let silence = match last_ack {
                Some(instant) => instant.elapsed(),
                // Never acknowledged in this leadership: silent since it began, not since some
                // earlier leadership this one has no visibility into.
                None => leadership_age,
            };
            if silence > threshold && leadership_age > grace {
                NodeStatus::Down
            } else {
                NodeStatus::Up
            }
        };

        if candidate_status != info.status {
            set_status(ctx, peer_id, info.status, candidate_status, leadership_age).await;
        }
    }

    reconcile_membership(ctx, &registry, &metrics).await;
}

/// Propose the one status change `check_once` just decided on, and log it once it lands
async fn set_status(ctx: &LeaderCtx, node_id: u64, old_status: NodeStatus, new_status: NodeStatus, silence: Duration) {
    let body = CommandBody::SetStatus(SetStatus {
        node_id,
        status: new_status,
    });

    match crate::write::propose_leader_only(ctx.node(), ctx.term, body).await {
        Ok(CommandOutcome::Registry) => {
            tracing::info!(
                node_id,
                old_status = ?old_status,
                new_status = ?new_status,
                silence_ms = silence.as_millis() as u64,
                term = ctx.term,
                "node liveness status changed",
            );
        }
        Err(LeaderWriteError::NotLeader { .. } | LeaderWriteError::Indeterminate { .. }) => {
            // Either this leadership is ending - the supervisor will cancel this run shortly - or
            // the leader could not get an answer in time. The status is judged afresh on the next
            // tick, by whoever leads then.
        }
        Ok(other) => tracing::error!(node_id, term = ctx.term, outcome = ?other, "unexpected liveness write outcome"),
        // `SetStatus` carries no size limit and no condition to fail - reaching this would mean
        // the leader-only propose path grew a new way to reject one that this task does not yet
        // expect. Logged rather than panicking: a missed liveness tick is not worth a node's
        // availability, and the next tick tries again.
        Err(other) => tracing::error!(node_id, term = ctx.term, error = %other, "unexpected liveness write failure"),
    }
}

/// Remove every registry entry for a node no longer in the committed membership, if any exist
async fn reconcile_membership(ctx: &LeaderCtx, registry: &Registry, metrics: &RaftMetrics<OpenRaftTypes>) {
    let membership = metrics.committed_membership_config.membership();
    let member_ids: BTreeSet<u64> = membership.voter_ids().chain(membership.learner_ids()).collect();

    let stale: Vec<u64> = registry.iter().map(|(node_id, _)| node_id).filter(|id| !member_ids.contains(id)).collect();
    if stale.is_empty() {
        return;
    }

    match crate::write::propose_leader_only(ctx.node(), ctx.term, CommandBody::ReconcileRegistry).await {
        Ok(CommandOutcome::Registry) => {
            tracing::info!(
                term = ctx.term,
                removed = ?stale,
                "reconciled registry entries for nodes no longer in the committed membership",
            );
        }
        Err(LeaderWriteError::NotLeader { .. } | LeaderWriteError::Indeterminate { .. }) => {}
        Ok(other) => tracing::error!(term = ctx.term, outcome = ?other, "unexpected reconciliation write outcome"),
        // See `set_status`'s own comment on this same match arm: `ReconcileRegistry` carries no
        // limit or condition either.
        Err(other) => tracing::error!(term = ctx.term, error = %other, "unexpected reconciliation write failure"),
    }
}
