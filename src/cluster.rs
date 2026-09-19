//! Cluster observation: `EzKv::cluster()` and `EzKv::last_leader_contact()` (ech0-media#33)
//!
//! Two independent freshness signals, both about *this* node's own standing in the cluster, never
//! about another node's (that is what the registry's `status`, ech0-media#34, is for):
//!
//! - [`ClusterState`] is this node's id, role, term, current leader, voter and learner sets, and
//!   whether it is still a member - derived from openraft's own `server_metrics` channel by one
//!   background task per node ([`spawn`]), and pushed to [`crate::EzKv::cluster`]'s
//!   `watch::Receiver` only when one of those fields actually changed
//!   ([`tokio::sync::watch::Sender::send_if_modified`]). The task ends on its own once that channel
//!   closes, which is exactly when the node's Raft engine has shut down - no separate shutdown
//!   signal is needed.
//! - [`LeaderContact`] is how long ago this node last accepted an append-entries request (a
//!   heartbeat is an empty one) from whichever node it recognized as the current leader at the
//!   time. openraft alpha.33 has no follower-side metric for this - only the leader's own view of
//!   its followers - so the foundation records it itself, at the one place it can:
//!   `crate::server`'s append route, right after `openraft::Raft::append_entries` answers with
//!   anything other than [`openraft::raft::AppendEntriesResponse::HigherVote`] (the one outcome
//!   that means openraft did *not* recognize the request's sender as this node's leader). A
//!   monotonic clock behind a small mutex - not a metrics pipeline - because this is the one piece
//!   of freshness information nothing openraft already reports even indirectly.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use openraft::ServerState;
use openraft::async_runtime::WatchReceiver;
use openraft::metrics::RaftServerMetrics;
use tokio::sync::watch;

use crate::raft::EzKv;
use crate::raft::ORRaft;
use crate::type_config::OpenRaftTypes;

/// This node's own view of the cluster: id, role, term, current leader, voter and learner sets,
/// and whether it is still a member
///
/// `role` reuses openraft's own [`ServerState`] (already part of this crate's public surface
/// through [`crate::EzKv::metrics`]) rather than a second enum naming the same four states this
/// ticket asks for plus the [`ServerState::Shutdown`] this node can genuinely be in for the brief
/// window between asking to stop and the cluster watch task noticing - reporting that truthfully
/// costs nothing extra and needs no separate type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterState {
    /// This node's own id
    pub node_id: u64,
    /// This node's current role
    pub role: ServerState,
    /// This node's current term
    pub term: u64,
    /// The node this one currently believes leads, if any
    pub current_leader: Option<u64>,
    /// Every voter in the current membership (the union of both configs mid joint-consensus)
    pub voters: BTreeSet<u64>,
    /// Every learner in the current membership
    pub learners: BTreeSet<u64>,
    /// Whether this node is still part of the current membership at all
    pub is_member: bool,
}

/// Build the [`ClusterState`] `server_metrics` currently describes
fn from_server_metrics(metrics: &RaftServerMetrics<OpenRaftTypes>) -> ClusterState {
    let membership = metrics.membership_config.membership();
    ClusterState {
        node_id: metrics.id,
        role: metrics.state,
        term: metrics.vote.leader_id.term,
        current_leader: metrics.current_leader,
        voters: membership.voter_ids().collect(),
        learners: membership.learner_ids().collect(),
        is_member: membership.get_node(&metrics.id).is_some(),
    }
}

/// Start the one background task that feeds [`crate::EzKv::cluster`], and return the receiver end
///
/// One task per node, spawned once from [`crate::raft::EzKv::new`]. It never polls: it awaits
/// openraft's own `server_metrics` watch channel and reacts only when that channel actually
/// changes, translating every such change into a fresh [`ClusterState`] and forwarding it only if
/// that in turn changed one of `ClusterState`'s own fields. It ends - dropping the sender, which is
/// what turns every [`crate::EzKv::cluster`] stream into a normal end rather than a hang - the
/// moment `server_metrics` itself closes, which openraft does only when this node's Raft engine
/// has shut down for good.
pub(crate) fn spawn(raft: ORRaft) -> watch::Receiver<ClusterState> {
    let mut server_metrics = raft.server_metrics();
    let initial = from_server_metrics(&server_metrics.borrow_watched());
    let (tx, rx) = watch::channel(initial);

    tokio::spawn(async move {
        while server_metrics.changed().await.is_ok() {
            let next = from_server_metrics(&server_metrics.borrow_watched());
            tx.send_if_modified(|current| {
                let changed = *current != next;
                if changed {
                    *current = next.clone();
                }
                changed
            });
        }
    });

    rx
}

/// The instant this node last accepted an append-entries request from a leader it recognized,
/// behind a small mutex - see this module's own doc comment for why the foundation has to record
/// this itself
///
/// `Clone` is cheap (an `Arc`): every [`crate::EzKv`] clone shares the same tracked instant, the
/// same discipline every other per-node handle in [`crate::EzKv`] follows.
#[derive(Clone, Default)]
pub(crate) struct LeaderContact(Arc<Mutex<Option<Instant>>>);

impl LeaderContact {
    /// Record "now" as the last time this node heard from a leader it recognized
    pub(crate) fn record_now(&self) {
        *self.0.lock().unwrap() = Some(Instant::now());
    }

    /// How long ago that was, or `None` if [`Self::record_now`] has never run
    pub(crate) fn elapsed(&self) -> Option<Duration> {
        self.0.lock().unwrap().map(|instant| instant.elapsed())
    }
}

impl EzKv {
    /// This node's own view of the cluster, and every future change to it
    ///
    /// Sent only when this node's id, role, term, current leader, voter set, learner set, or
    /// membership status actually changes - never on every metrics tick. The stream ends once this
    /// node's Raft engine shuts down.
    pub fn cluster(&self) -> watch::Receiver<ClusterState> {
        self.cluster_receiver().clone()
    }

    /// How long ago this node last accepted an append-entries request (heartbeats included) from
    /// whoever it recognized, at the time, as the current leader
    ///
    /// Always `Some(Duration::ZERO)` on the leader itself - a leader has no need to hear from
    /// another leader to know it is current. `None` if this node has never accepted one (a brand
    /// new node that has not yet heard from anyone). Recorded by `crate::server`'s append route;
    /// see this module's own doc comment for why openraft alpha.33 cannot report this on its own.
    pub fn last_leader_contact(&self) -> Option<Duration> {
        if self.is_leader() {
            return Some(Duration::ZERO);
        }
        self.leader_contact().elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_leader_contact_has_never_heard_from_anyone() {
        let contact = LeaderContact::default();
        assert_eq!(None, contact.elapsed());
    }

    #[test]
    fn recording_now_makes_elapsed_report_a_short_duration() {
        let contact = LeaderContact::default();
        contact.record_now();
        let elapsed = contact.elapsed().expect("just recorded");
        assert!(elapsed < Duration::from_secs(1), "elapsed was {elapsed:?}");
    }

    #[test]
    fn a_clone_shares_the_same_recorded_instant() {
        let contact = LeaderContact::default();
        let clone = contact.clone();
        contact.record_now();
        assert!(clone.elapsed().is_some(), "a clone observes the same recording");
    }
}
