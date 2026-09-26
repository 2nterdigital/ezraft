//! Who is in the cluster: admitting a node, deciding whether it votes, and removing it
//!
//! Every change here is the leader's to make and is applied on the node it is called on: nothing
//! is retried and nothing is forwarded, so on a follower openraft answers `ForwardToLeader` and
//! that comes straight back. Reaching the leader belongs to whoever speaks the network - the admin
//! endpoints answer with the leader's address, and [`crate::admin`] describes that protocol.

use std::collections::BTreeSet;
use std::io;

use openraft::ChangeMembers;

use crate::raft::EzKv;
use crate::type_config::Node;

impl EzKv {
    /// Add a learner node to the cluster
    ///
    /// A learner receives log replication but does not vote. Every node is admitted this way,
    /// and stays a learner until [`Self::promote`] makes it a voter: a node cannot be added
    /// straight to the voter set, because the new configuration's quorum would count a node that
    /// has not received the response telling it its own id, and so cannot answer anything yet.
    ///
    /// Returns as soon as replication to the new node is set up; the node catches up in the
    /// background. Waiting here would deadlock the join handler, whose caller cannot answer any
    /// Raft RPC until it gets its node id back.
    ///
    /// # Arguments
    ///
    /// * `node_id` - ID of the new learner node
    /// * `addr` - Address of the new learner node
    pub(crate) async fn add_learner(&self, node_id: u64, addr: String) -> Result<(), io::Error> {
        let node = Node::new(addr);
        self.inner().add_learner(node_id, node, false).await.map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }

    /// Make a learner a voter
    ///
    /// Only voters count towards a quorum, so a cluster tolerates a node failure only once the
    /// nodes that joined it have been promoted: three voters tolerate one failure, five tolerate
    /// two. Joining replicates the log; it does not change who decides. Until this is called the
    /// new node is a read replica, which is also what to leave it as if that is all you want.
    ///
    /// Returns once OpenRaft's membership change completes. EzRaft does not add a separate
    /// readiness or catch-up check, so the target should be serving before it is promoted.
    ///
    /// Fails if the node is not a learner of this cluster, and if another membership change is
    /// still in flight, since a cluster admits one at a time. Both are the caller's to retry -
    /// on the joining path that is [`Self::join`]'s promotion request, which retries like every
    /// other request it makes. Leader only: on a follower this fails rather than looking for one.
    pub async fn promote(&self, node_id: u64) -> Result<(), io::Error> {
        self.change_members(ChangeMembers::AddVoterIds(BTreeSet::from([node_id])), false).await
    }

    /// Make a voter a learner
    ///
    /// The node stays in the cluster and keeps receiving the log; it stops being counted in the
    /// quorum a write must reach. Use it to take a node out of the decision-making without taking
    /// it out of the cluster - before shutting it down, or to leave it as a read replica.
    ///
    /// Demoting the last voter is refused: a cluster with no voter can never commit again, not
    /// even the change that would give it one back. Demoting below a quorum is *not* refused,
    /// because it is a legitimate thing to intend, and Raft's answer to it is honest - the
    /// cluster stops committing until enough voters return.
    ///
    /// Demoting the current leader is allowed, and it keeps leading. A Raft leader does not have to
    /// be a voter: it commits the change and carries on committing, on a quorum of the voter set it
    /// is no longer part of. The voters elect one of their own only once it stops. To stop leading,
    /// hand leadership over - `raft.inner().trigger().transfer_leader(node_id)` - or shut the node
    /// down; demoting it is not that.
    ///
    /// Leader only: on a follower this fails rather than looking for one.
    pub async fn demote(&self, node_id: u64) -> Result<(), io::Error> {
        // `retain` is what makes this a demotion rather than a removal: the node stays in the
        // membership, as a learner.
        self.change_members(ChangeMembers::RemoveVoters(BTreeSet::from([node_id])), true).await
    }

    /// Take a node out of the cluster
    ///
    /// The cluster stops replicating to it and forgets it. Works on a voter and on a learner
    /// alike: a voter is removed from the quorum and the membership in one change, so there is no
    /// window in which a node the caller asked to remove is still being waited for.
    ///
    /// This does not stop the node's own process - that is the operator's to do. Doing it first is
    /// what leaves the cluster nothing to talk to; a node removed while still running keeps
    /// serving stale reads to anyone who asks it.
    ///
    /// Leader only: on a follower this fails rather than looking for one.
    ///
    /// Once the membership change itself commits, this also removes `node_id`'s node-registry
    /// entry (ech0-media#33), best-effort within one `EzConfig::write_deadline` -
    /// `crate::registry::remove_entry`'s own docs cover why that is never reported back as this
    /// call's own error, and the window a leader crash between the two steps leaves (reconciled by
    /// the built-in liveness task, `crate::liveness`).
    pub async fn remove_node(&self, node_id: u64) -> Result<(), io::Error> {
        let metrics = self.metrics().await;
        let node = BTreeSet::from([node_id]);

        // A voter and a learner are removed differently, because a voter is in two places at
        // once. Taking it out of the voter set with `retain` off drops its node entry too, but
        // only once the joint config that still counts it has resolved - dropping that entry any
        // sooner would leave the joint config holding a voter with no address to reach it at. A
        // learner is in no config, so its entry is all there is to drop.
        //
        // Deciding from this node's membership is safe even when this node is not the leader: it
        // decides only which change to attempt locally, and that attempt is what discovers there
        // is a leader elsewhere. What crosses the wire is the request above, which the leader
        // resolves against its own membership.
        let change = if metrics.membership_config.membership().voter_ids().any(|id| id == node_id) {
            ChangeMembers::RemoveVoters(node)
        } else {
            ChangeMembers::RemoveNodes(node)
        };

        self.change_members(change, false).await?;
        crate::registry::remove_entry(self, node_id).await;
        Ok(())
    }

    /// Apply a membership change on this node
    ///
    /// Nothing is retried and nothing is forwarded: on a follower openraft answers
    /// `ForwardToLeader` and that comes straight back. Reaching the leader belongs to whoever
    /// speaks the network - the admin endpoints answer with the leader's address, and
    /// [`crate::admin`] describes that protocol.
    async fn change_members(&self, change: ChangeMembers<u64, Node>, retain: bool) -> Result<(), io::Error> {
        self.inner().change_membership(change, retain).await.map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }
}
