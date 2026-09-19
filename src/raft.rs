//! The node handle: `EzKv`
//!
//! This module provides the primary [`EzKv`] struct users interact with: cluster lifecycle
//! (`create`/`join`/`join_as_learner`/`serve`/`shutdown`), membership, metrics and the local
//! view. The write path lives in `crate::write` (a second `impl EzKv` block), kept separate so
//! this file stays about lifecycle and membership as it grows.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actix_web::dev::ServerHandle;
use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::ReadPolicy;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::InitializeError;
use openraft::errors::RaftError;
use tokio::task::JoinHandle;

use crate::admin::AdminClient;
use crate::admin::MembershipChange;
use crate::config::EzConfig;
use crate::entry::EzLogId;
use crate::kv::KvLimits;
use crate::kv::View;
use crate::kv::ViewReceiver;
use crate::network::EzNetworkFactory;
use crate::node_role::NodeRole;
use crate::storage::EzStorage;
use crate::storage::adapter::StateMachineStore;
use crate::storage::adapter::open;
use crate::type_config::OpenRaftTypes;

/// The internal OpenRaft instance, with the KV state machine behind it
pub type ORRaft = Raft<OpenRaftTypes, StateMachineStore>;

/// A promotion in flight, handed from [`EzKv::join`] to [`EzKv::serve`]
type Promotion = Arc<Mutex<Option<JoinHandle<Result<(), io::Error>>>>>;

/// The HTTP server handle [`EzKv::serve_with`] hands to [`EzKv::shutdown`]
///
/// Set once `serve`/`serve_with` has bound the node's address, and taken by `shutdown` so a node
/// stopped that way releases the address again (wave S1 left this gap: see `FORK.md`).
type ServeHandle = Arc<Mutex<Option<ServerHandle>>>;

/// The node handle: create or join a cluster, serve it, and read and write its replicated KV
///
/// `Clone` is cheap: every clone shares the same underlying Raft node.
pub struct EzKv {
    /// Node ID
    node_id: u64,

    /// HTTP bind address
    addr: String,

    /// Internal OpenRaft instance
    raft: ORRaft,

    /// The promotion [`Self::join`] started, for [`Self::serve`] to finish
    ///
    /// Shared and taken rather than owned, because [`EzKv`] is cloned freely and only the one
    /// call to `serve` may await it. `None` once taken, and for a node that never asked to be
    /// promoted.
    promotion: Promotion,

    /// A lock-free reader onto the KV's latest published view (DESIGN.md §5)
    view_rx: ViewReceiver,

    /// The limits [`crate::write`] checks a transaction against before proposing it
    kv_limits: KvLimits,

    /// `EzConfig::json_body_limit`, for [`crate::server::bind`] (ech0-media#26)
    json_body_limit: usize,

    /// `EzConfig::snapshot_max_bytes`, for [`crate::server::bind`] (ech0-media#26)
    snapshot_max_bytes: usize,

    /// The actix `ServerHandle` for the address `serve`/`serve_with` bound, once bound
    serve_handle: ServeHandle,
}

impl Clone for EzKv {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            addr: self.addr.clone(),
            raft: self.raft.clone(),
            promotion: self.promotion.clone(),
            view_rx: self.view_rx.clone(),
            kv_limits: self.kv_limits,
            json_body_limit: self.json_body_limit,
            snapshot_max_bytes: self.snapshot_max_bytes,
            serve_handle: self.serve_handle.clone(),
        }
    }
}

impl EzKv {
    /// Start a new cluster with this node as its only member
    ///
    /// Exactly one node of a cluster is created this way; every other node uses [`Self::join`].
    /// Creating two nodes separately gives two one-node clusters that will never merge.
    ///
    /// # Arguments
    ///
    /// * `http_addr` - Address to bind HTTP server (e.g., "127.0.0.1:8080")
    /// * `storage` - This node's storage
    /// * `config` - configuration (use `EzConfig::default()` for sensible defaults)
    pub async fn create(
        http_addr: impl ToString,
        storage: impl EzStorage,
        config: EzConfig,
    ) -> Result<Self, io::Error> {
        Self::new(http_addr, storage, config, None).await
    }

    /// Join the cluster that `seed_addr` belongs to, and stay a learner
    ///
    /// Same as [`Self::join`] without the promotion: this node replicates the log and answers
    /// reads, but never votes, so it is a read replica and costs the cluster no write latency.
    /// [`Self::promote`], called on the leader, makes it a voter later.
    pub async fn join_as_learner(
        http_addr: impl ToString,
        seed_addr: impl ToString,
        storage: impl EzStorage,
        config: EzConfig,
    ) -> Result<Self, io::Error> {
        Self::new(
            http_addr,
            storage,
            config,
            Some((seed_addr.to_string(), NodeRole::Learner)),
        )
        .await
    }

    /// Join the cluster that `seed_addr` belongs to, and become a voter
    ///
    /// Three steps, of which this call finishes the first two: the seed hands out a node id, adds
    /// this node to the cluster as a *learner*, and then the promotion to *voter* is started in
    /// the background and left for [`Self::serve`] to finish. It has to be left, because a node
    /// is promoted only once it has caught up, and it cannot receive anything until its own HTTP
    /// server is running - which is what `serve` starts. So **a joining node must call `serve`**,
    /// and that is where a failed promotion surfaces.
    ///
    /// The seed does not have to be the leader.
    ///
    /// On restart the persisted id is reused, the seed is not contacted, and no promotion is
    /// started: a node that has already joined is whatever the cluster's membership says it is,
    /// so restarting a node that was deliberately left a learner does not quietly make it a
    /// voter. Passing an address that has since left the cluster is therefore harmless too.
    ///
    /// A promotion that failed therefore stays failed, leaving this node the learner it already
    /// became: entering the membership and being promoted are two steps, and restarting repeats
    /// neither. Finish it with [`Self::promote`] on the leader, which is the same call an operator
    /// uses to promote any learner.
    ///
    /// # Arguments
    ///
    /// * `http_addr` - Address to bind HTTP server (e.g., "127.0.0.1:8081")
    /// * `seed_addr` - Address of any node already in the cluster
    /// * `storage` - This node's storage
    /// * `config` - configuration (use `EzConfig::default()` for sensible defaults)
    pub async fn join(
        http_addr: impl ToString,
        seed_addr: impl ToString,
        storage: impl EzStorage,
        config: EzConfig,
    ) -> Result<Self, io::Error> {
        Self::new(
            http_addr,
            storage,
            config,
            Some((seed_addr.to_string(), NodeRole::Voter)),
        )
        .await
    }

    async fn new(
        http_addr: impl ToString,
        storage: impl EzStorage,
        config: EzConfig,
        seed: Option<(String, NodeRole)>,
    ) -> Result<Self, io::Error> {
        let http_addr = http_addr.to_string();

        // Converted first, because it is the only step that can fail on nothing but its
        // arguments, and everything below it either takes an id from the cluster or changes the
        // cluster's membership. A configuration this node cannot run with must not first cost the
        // cluster a learner that then has to be removed by hand.
        let raft_config = Arc::new(config.to_raft_config()?);
        let kv_limits = config.kv_limits;
        let json_body_limit = config.json_body_limit;
        let snapshot_max_bytes = config.snapshot_max_bytes;

        // Open user storage as the two stores openraft asks for
        let (log, sm) = open(storage).await?;
        let view_rx = sm.subscribe_view();

        // Determine node_id, and whether a promotion has to follow. A node that already has an id
        // has joined before, so neither the seed nor the promotion applies to it again: what it
        // is now, the membership decides.
        let mut promote_via = None;

        let node_id = if let Some(id) = log.node_id().await {
            // Use persisted node_id (restart case)
            id
        } else if let Some((seed_addr, role)) = &seed {
            // Join existing cluster via seed node: take an id, then enter the membership as a
            // learner. Always a learner, whichever role was asked for - a node cannot be added
            // straight to the voter set, because the new configuration's quorum would count a
            // node that is not answering yet, and the change would wait forever on its own ack.
            let admin = AdminClient::new(seed_addr);

            let id = admin.node_id().await?;
            let enter = MembershipChange::Add {
                node_id: id,
                addr: http_addr.clone(),
            };
            admin.membership(enter).await?;
            log.save_meta(|m| m.node_id = Some(id)).await?;

            if *role == NodeRole::Voter {
                promote_via = Some(admin);
            }
            id
        } else {
            // First node in cluster
            let id = 0;
            log.save_meta(|m| m.node_id = Some(id)).await?;
            id
        };

        // The state machine only learns this node's id now: `open` (and so `sm`'s own
        // construction) ran before it was known. Tracing is the only thing that needs it
        // (DESIGN.md §8, ech0-media#26's snapshot build/install events).
        sm.set_node_id(node_id);

        // Create network factory. `with_faults` only exists under the `fault-injection`
        // feature; shadowing rather than a `mut` binding keeps a non-feature build from
        // warning about a binding that is never mutated.
        let network = EzNetworkFactory::new(node_id, snapshot_max_bytes)?;
        #[cfg(feature = "fault-injection")]
        let network = network.with_faults(config.faults.clone());

        // Create OpenRaft instance
        let raft = Raft::new(node_id, raft_config, network, log, sm)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;

        // The created node starts the cluster with itself as its only member. On restart it loads
        // id 0 from storage and comes back here, where initializing is rightly refused; every
        // other refusal means the node cannot run.
        if node_id == 0 {
            let nodes = BTreeMap::from_iter([(node_id, BasicNode::new(http_addr.clone()))]);
            match raft.initialize(nodes).await {
                Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {}
                Err(e) => return Err(io::Error::other(e.to_string())),
            }
        }

        // Asking now, answered later. The leader holds the request until this node has caught up,
        // which cannot happen until `serve` is running, so the wait belongs there and not here.
        let promotion = promote_via.map(|admin| {
            let promote = MembershipChange::SetRole {
                node_id,
                role: NodeRole::Voter,
            };

            tokio::spawn(async move { admin.membership(promote).await })
        });

        Ok(Self {
            node_id,
            addr: http_addr,
            raft,
            promotion: Arc::new(Mutex::new(promotion)),
            view_rx,
            kv_limits,
            json_body_limit,
            snapshot_max_bytes,
            serve_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// The limits `crate::write` checks a transaction against before proposing it
    pub(crate) fn kv_limits(&self) -> KvLimits {
        self.kv_limits
    }

    /// `EzConfig::json_body_limit`, for [`crate::server::bind`]
    pub(crate) fn json_body_limit(&self) -> usize {
        self.json_body_limit
    }

    /// `EzConfig::snapshot_max_bytes`, for [`crate::server::bind`]
    pub(crate) fn snapshot_max_bytes(&self) -> usize {
        self.snapshot_max_bytes
    }

    /// This node's latest published view of the KV
    ///
    /// Never awaits, never takes the state machine's lock, and never delays `apply` by more than
    /// a pointer swap: it is a plain read of the value `crate::kv::Publisher` (crate-internal)
    /// last published.
    /// May lag the leader; on the leader itself, a [`Self::write`] that just returned is always
    /// already reflected here (DESIGN.md §5's publish-before-reply).
    pub fn view(&self) -> View {
        self.view_rx.current()
    }

    /// Wait until this node's metrics satisfy a linearizable-read barrier, returning the log id
    /// it is anchored to
    ///
    /// Confirms this node still leads, then waits until the local state machine has applied
    /// everything committed up to that point. Only the leader can serve this: on a follower it
    /// returns an error instead of forwarding, since forwarding cannot make this node's local
    /// state current.
    ///
    /// `policy` chooses how leadership is confirmed. [`ReadPolicy::ReadIndex`] asks a quorum and
    /// is correct whatever the clocks do; [`ReadPolicy::LeaseRead`] answers from the leader lease
    /// with no round trip, and trades that cost for the assumption that clock drift between nodes
    /// stays inside the lease.
    ///
    /// The returned log id's term identifies the leadership that granted the read, which is what
    /// a caller comparing two reads across a leader change needs.
    pub async fn linearizable(&self, policy: ReadPolicy) -> Result<EzLogId, io::Error> {
        let read_log_id = self.raft.ensure_linearizable(policy).await.map_err(|e| io::Error::other(e.to_string()))?;

        let term = read_log_id.committed_leader_id().term;
        let index = read_log_id.index();
        Ok((term, index))
    }

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
        let node = BasicNode::new(addr);
        self.raft.add_learner(node_id, node, false).await.map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }

    /// Make a learner a voter
    ///
    /// Only voters count towards a quorum, so a cluster tolerates a node failure only once the
    /// nodes that joined it have been promoted: three voters tolerate one failure, five tolerate
    /// two. Joining replicates the log; it does not change who decides. Until this is called the
    /// new node is a read replica, which is also what to leave it as if that is all you want.
    ///
    /// Returns once the node is a voter. Bringing it up to date is part of the change, so this
    /// lasts as long as that catch-up takes - for a large state, a whole snapshot transfer.
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

        self.change_members(change, false).await
    }

    /// Apply a membership change on this node
    ///
    /// Nothing is retried and nothing is forwarded: on a follower openraft answers
    /// `ForwardToLeader` and that comes straight back. Reaching the leader belongs to whoever
    /// speaks the network - the admin endpoints answer with the leader's address, and
    /// [`crate::admin`] describes that protocol.
    async fn change_members(&self, change: ChangeMembers<u64, BasicNode>, retain: bool) -> Result<(), io::Error> {
        self.raft.change_membership(change, retain).await.map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    /// Check if this node is the leader
    ///
    /// Reports this node's own state, which is what a caller asking "am I the leader" wants. It
    /// is not a guarantee that the answer is still true elsewhere: a deposed leader does not
    /// find out until it hears from the new one. Nothing here needs that guarantee -- writing
    /// reports its own `NotLeader` when it matters, and code that does want a linearizable read
    /// calls [`Self::linearizable`] first.
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    /// Get the current cluster metrics
    ///
    /// Returns information about the Raft cluster state.
    pub async fn metrics(&self) -> RaftMetrics<OpenRaftTypes> {
        self.raft.metrics().borrow_watched().clone()
    }

    /// Wait until this node's metrics satisfy `predicate`, and return the metrics that did
    ///
    /// The condition is checked against every metrics update, so it must describe a state to wait
    /// for rather than an event to catch. `timeout` bounds the wait, and `None` waits without one.
    /// `reason` names what is being waited for, and is what the error and openraft's own logs
    /// report.
    pub async fn wait_metrics<P>(
        &self,
        timeout: Option<Duration>,
        predicate: P,
        reason: &str,
    ) -> Result<RaftMetrics<OpenRaftTypes>, io::Error>
    where
        P: Fn(&RaftMetrics<OpenRaftTypes>) -> bool + Send,
    {
        self.raft.wait(timeout).metrics(predicate, reason).await.map_err(io::Error::other)
    }

    /// Take a snapshot of the state machine now, rather than at the next configured threshold
    ///
    /// Returns once the snapshot is scheduled, not once it exists: wait on
    /// [`Self::wait_metrics`] for `snapshot` to reach the log id in question.
    pub async fn trigger_snapshot(&self) -> Result<(), io::Error> {
        self.raft.trigger().snapshot().await.map_err(io::Error::other)
    }

    /// Delete log entries up to and including `upto_index`
    ///
    /// Only entries a snapshot already covers may be purged; openraft holds the request until
    /// that is true rather than refusing it. Purging is what bounds the log on disk, since a
    /// snapshot alone only makes the entries below it redundant.
    pub async fn purge_log(&self, upto_index: u64) -> Result<(), io::Error> {
        self.raft.trigger().purge_log(upto_index).await.map_err(io::Error::other)
    }

    /// Ask this leader to hand leadership to `node_id`
    ///
    /// Returns once the handover is under way. The target has to be caught up enough to win the
    /// election that follows, so leadership moves some time after this returns, and a caller that
    /// depends on it waits for the new leader through [`Self::wait_metrics`].
    pub async fn transfer_leader(&self, node_id: u64) -> Result<(), io::Error> {
        self.raft.trigger().transfer_leader(node_id).await.map_err(io::Error::other)
    }

    /// Stop this node's Raft engine and HTTP server, and wait for both to finish
    ///
    /// Releases the address `serve`/`serve_with` bound: a node restarted **in the same
    /// process** - exactly what the in-process cluster tests, and any real restart, need - can
    /// bind it again immediately after this returns. A node with no address to answer on has no
    /// business serving anyway. A node that never called `serve` simply has nothing to stop here.
    pub async fn shutdown(&self) -> Result<(), io::Error> {
        self.raft.shutdown().await.map_err(io::Error::other)?;

        let handle = self.serve_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.stop(true).await;
        }

        Ok(())
    }

    /// Start the HTTP server
    ///
    /// This starts the HTTP server that handles:
    /// - Internal Raft RPC (append entries, vote, install snapshot)
    /// - Admin API (join, add learner, change membership, metrics)
    ///
    /// This method blocks until the server is stopped, so a caller with other work to do spawns
    /// it: `tokio::spawn(node.clone().serve())`. Start it as early as possible. Peers reach a
    /// node only through this server, so a node that has joined a cluster but is not serving yet
    /// cannot be replicated to, and holds up every quorum it is counted in.
    ///
    /// This is also where the promotion started by [`Self::join`] is collected. The server goes
    /// up first, because the promotion completes only once this node has caught up and it can
    /// only catch up through that server; a promotion that fails is returned here, and stops the
    /// server with it. A node that joined as a learner, or was created, has nothing to collect
    /// and simply serves.
    ///
    /// Whichever of the two fails first is what this returns. A server that cannot bind its
    /// address says so immediately, instead of waiting out a promotion that was never going to
    /// arrive without it.
    pub async fn serve(self) -> Result<(), io::Error> {
        self.serve_with(|_| {}).await
    }

    /// Start the HTTP server with application-specific routes
    ///
    /// The closure extends the bundled server with routes backed by the same node handle.
    /// Standard Raft and admin routes are installed first and cannot be replaced.
    pub async fn serve_with<F>(self, configure: F) -> Result<(), io::Error>
    where F: Fn(&mut actix_web::web::ServiceConfig) + Clone + Send + 'static {
        let promotion = self.promotion.lock().unwrap().take();

        let server = crate::server::bind(self.clone(), configure)?;
        *self.serve_handle.lock().unwrap() = Some(server.handle());
        let mut server_task = tokio::spawn(server);

        if let Some(mut promotion) = promotion {
            // The two are raced rather than collected in turn. A promotion completes only once
            // this node has caught up, which a server that never came up will never let it do -
            // waiting for the promotion first would sit on the bind error for the twenty attempts
            // and ninety seconds each that the promotion still has to spend.
            tokio::select! {
                finished = &mut server_task => {
                    // The promotion has nothing left to wait for, and would keep asking the
                    // leader for half an hour if it were merely dropped.
                    promotion.abort();
                    return finished.map_err(io::Error::other)?;
                }
                promoted = &mut promotion => {
                    if let Err(e) = promoted.map_err(io::Error::other)? {
                        // Nothing else will stop it: the caller is holding this call, not the
                        // server.
                        server_task.abort();
                        return Err(e);
                    }
                }
            }
        }

        server_task.await.map_err(io::Error::other)?
    }

    /// Get the node ID
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get the HTTP address
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Get a reference to the internal OpenRaft instance
    ///
    /// This provides access to advanced OpenRaft APIs if needed.
    pub fn inner(&self) -> &ORRaft {
        &self.raft
    }
}
