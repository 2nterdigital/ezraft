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
//! # The new-leader grace period, and "no evidence"
//!
//! A freshly elected leader's heartbeat map starts every peer at `None` - indistinguishable, by
//! that field alone, from a peer that has been genuinely silent forever. So a check has three
//! possible conclusions about a peer, not two ([`judge`]): `Up` takes an acknowledgement this
//! leadership actually received, recently; `Down` takes silence longer than
//! `EzConfig::liveness_threshold` *and* a leadership older than `EzConfig::liveness_grace`; and
//! anything in between is no evidence either way, which leaves the status the peer already has.
//! That last case is what keeps a change of leader from looking like every peer going silent at
//! once - and equally from looking like every `Down` peer recovering at once: a node the previous
//! leadership left `Down` stays `Down` until the new one has heard from it.
//!
//! A peer that has never acknowledged anything is silent since this leadership started watching
//! it - since the leadership began, or since the peer first appeared in the registry if that was
//! later. Measuring a node that joins an established leadership from the leadership's own start
//! would find it silent for longer than any threshold before its first heartbeat was even due.
//!
//! # Determinism
//!
//! `EzConfig::liveness_threshold`/`liveness_grace` are read only here, by the leader task that
//! decides *which* status a node should have; `apply` (`crate::storage::adapter::state_machine`)
//! only ever assigns the status a [`crate::command::CommandBody::SetStatus`] command already
//! names, and reconciles against the state machine's own already-applied membership - never a
//! value this node's own configuration could disagree with another replica's about.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::time::Duration;

use openraft::Instant as _;
use openraft::RaftMetrics;
use openraft::async_runtime::WatchReceiver;
use tokio::time::Instant;

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
    let thresholds = Thresholds {
        threshold: ctx.node().liveness_threshold(),
        grace: ctx.node().liveness_grace(),
    };
    // When this leadership first saw each registered peer: what a peer that has never
    // acknowledged anything is silent since (see this module's own docs).
    let mut watched_since = HashMap::new();

    let mut ticker = tokio::time::interval(ctx.node().liveness_check_interval());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        check_once(&ctx, thresholds, &mut watched_since).await;
    }
}

/// `EzConfig::liveness_threshold` and `EzConfig::liveness_grace`, which only ever travel together
#[derive(Clone, Copy)]
struct Thresholds {
    threshold: Duration,
    grace: Duration,
}

/// What one check knows about one peer
#[derive(Clone, Copy)]
struct Evidence {
    /// How long ago this leadership last had a heartbeat acknowledged by the peer, if it ever has
    last_ack_age: Option<Duration>,
    /// How long this leadership has been watching the peer
    watched_for: Duration,
    /// How long this leadership has existed
    leadership_age: Duration,
}

impl Evidence {
    /// How long the peer has been silent, as far as this leadership can tell
    fn silence(&self) -> Duration {
        self.last_ack_age.unwrap_or(self.watched_for)
    }
}

/// What one check concludes about one peer: `None` is no evidence either way, which leaves the
/// status the peer already has - see this module's own docs for why there are three answers.
fn judge(evidence: Evidence, thresholds: Thresholds) -> Option<NodeStatus> {
    if evidence.last_ack_age.is_some_and(|age| age <= thresholds.threshold) {
        Some(NodeStatus::Up)
    } else if evidence.silence() > thresholds.threshold && evidence.leadership_age > thresholds.grace {
        Some(NodeStatus::Down)
    } else {
        None
    }
}

/// One tick: judge every registered peer's status, then reconcile the registry against membership
async fn check_once(ctx: &LeaderCtx, thresholds: Thresholds, watched_since: &mut HashMap<u64, Instant>) {
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

    // A node that leaves the registry and later registers again is watched afresh.
    watched_since.retain(|node_id, _| registry.get(*node_id).is_some());

    for (peer_id, info) in registry.iter() {
        let watched_for = watched_since.entry(peer_id).or_insert_with(Instant::now).elapsed();
        let evidence = Evidence {
            last_ack_age: heartbeat.get(&peer_id).copied().flatten().map(|instant| instant.elapsed()),
            // The first check runs as the leadership starts, so it is watching every peer already
            // registered from the start; the cap only keeps the two clocks from disagreeing.
            watched_for: watched_for.min(leadership_age),
            leadership_age,
        };

        let judged = if peer_id == self_id {
            // The leader is always `Up` - corrects a `Down` a *previous* leadership may have left
            // behind before this node was (re-)elected.
            Some(NodeStatus::Up)
        } else {
            judge(evidence, thresholds)
        };

        if let Some(status) = judged
            && status != info.status
        {
            set_status(ctx, peer_id, info.status, status, evidence.silence()).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    const THRESHOLDS: Thresholds = Thresholds {
        threshold: Duration::from_secs(5),
        grace: Duration::from_secs(10),
    };

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_recent_acknowledgement_means_up_even_inside_the_grace_period() {
        let evidence = Evidence {
            last_ack_age: Some(secs(1)),
            watched_for: secs(2),
            leadership_age: secs(2),
        };
        assert_eq!(Some(NodeStatus::Up), judge(evidence, THRESHOLDS));
    }

    #[test]
    fn silence_past_the_threshold_means_down_once_the_grace_period_is_over() {
        let evidence = Evidence {
            last_ack_age: Some(secs(6)),
            watched_for: secs(60),
            leadership_age: secs(60),
        };
        assert_eq!(Some(NodeStatus::Down), judge(evidence, THRESHOLDS));
    }

    #[test]
    fn silence_past_the_threshold_is_no_evidence_inside_the_grace_period() {
        let evidence = Evidence {
            last_ack_age: Some(secs(6)),
            watched_for: secs(8),
            leadership_age: secs(8),
        };
        assert_eq!(None, judge(evidence, THRESHOLDS));
    }

    /// A new leader has heard from nobody yet. That is not evidence that a node the previous
    /// leadership left `Down` has recovered, any more than it is evidence that an `Up` one died.
    #[test]
    fn a_peer_never_heard_from_is_no_evidence_inside_the_grace_period() {
        let evidence = Evidence {
            last_ack_age: None,
            watched_for: secs(1),
            leadership_age: secs(1),
        };
        assert_eq!(None, judge(evidence, THRESHOLDS));
    }

    #[test]
    fn a_peer_never_heard_from_is_down_once_both_the_grace_period_and_the_threshold_have_passed() {
        let evidence = Evidence {
            last_ack_age: None,
            watched_for: secs(11),
            leadership_age: secs(11),
        };
        assert_eq!(Some(NodeStatus::Down), judge(evidence, THRESHOLDS));
    }

    /// A node that joins a leadership older than the grace period is silent since it appeared,
    /// not since the leadership began: its first heartbeat is simply not due yet.
    #[test]
    fn a_peer_that_only_just_appeared_is_not_down_however_old_the_leadership() {
        let evidence = Evidence {
            last_ack_age: None,
            watched_for: secs(0),
            leadership_age: secs(3600),
        };
        assert_eq!(None, judge(evidence, THRESHOLDS));
    }
}
