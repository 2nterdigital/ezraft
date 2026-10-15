//! The node handle: `EzKv`
//!
//! This module defines the primary [`EzKv`] handle, its constructors, metrics, membership and
//! local view. HTTP serving and listener/promotion ownership live in the adjacent
//! `raft/lifecycle.rs`. The write path, cluster observation, node registry and leader tasks each
//! keep their own `impl EzKv` block in their feature modules. This module starts those background
//! tasks from the constructor and owns the membership operation that also touches the registry
//! ([`EzKv::remove_node`]).

mod lifecycle;

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::Raft;
use openraft::RaftMetrics;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::InitializeError;
use openraft::errors::RaftError;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use self::lifecycle::NodeLifecycle;
use self::lifecycle::PromotionRequest;
use crate::admin::AdminClient;
use crate::admin::MembershipChange;
use crate::cluster::ClusterState;
use crate::cluster::LeaderContact;
use crate::config::EzConfig;
use crate::kv::KvLimits;
use crate::kv::Publisher;
use crate::kv::Registry;
use crate::kv::View;
use crate::kv::ViewReceiver;
use crate::leader_task::LeaderTasks;
use crate::network::EzNetworkFactory;
use crate::node_role::NodeRole;
use crate::read::ReadIndexCoalescer;
use crate::rpc::Rpc;
use crate::storage::EzStorage;
use crate::storage::adapter::StateMachineStore;
use crate::storage::adapter::open;
use crate::transport::Transport;
use crate::type_config::Node;
use crate::type_config::OpenRaftTypes;

/// The internal OpenRaft instance, with the KV state machine behind it
pub type ORRaft = Raft<OpenRaftTypes, StateMachineStore>;

/// The node handle: create or join a cluster, serve it, and read and write its replicated KV
///
/// `Clone` is cheap: every clone shares the same underlying Raft node.
#[derive(Clone)]
pub struct EzKv {
    /// Node ID
    node_id: u64,

    /// Address peers use to reach this node, separate from the local listener override
    addr: String,

    /// Internal OpenRaft instance
    raft: ORRaft,

    /// The internal listener and serving/promotion lifecycle shared by every handle clone
    lifecycle: NodeLifecycle,

    /// A lock-free reader onto the KV's latest published view
    view_rx: ViewReceiver,

    /// The configuration this node was started with, shared by every clone of this handle:
    /// whatever another module needs from it is read through one of the accessors below, never
    /// copied into a field of its own.
    config: Arc<EzConfig>,

    /// The typed inter-node RPC facade this node hands out through [`Self::rpc`]
    rpc: Rpc,

    /// The follower-side ReadIndex coalescer behind [`crate::read`]'s
    /// `EzKv::linearizable_view` (ech0-media#31)
    read_index: ReadIndexCoalescer,

    /// The single publish source's watch side, for `crate::watch::EzKv::watch`/`view_and_watch`
    /// (ech0-media#32) to register subscriptions through - the very instance [`Self::view`] reads
    /// from, so a subscription and the view it starts at are always consistent.
    watch: Arc<Publisher>,

    /// A lock-free reader onto the registry's latest published value (ech0-media#33), the
    /// registry's counterpart to `view_rx`
    registry_rx: watch::Receiver<Registry>,

    /// This node's own view of the cluster, kept current by one background task
    /// (`crate::cluster::spawn`, ech0-media#33)
    cluster_rx: watch::Receiver<ClusterState>,

    /// When this node last accepted an append-entries request from a leader it recognized
    /// (ech0-media#33) - written by `crate::server`'s append route, read by
    /// [`Self::last_leader_contact`]
    leader_contact: LeaderContact,

    /// The self-registration task [`crate::registry::spawn_self_registration`] started at
    /// construction, if it has not finished yet - [`Self::shutdown`] aborts it, so "retries until
    /// it succeeds" does not mean "forever, even after this node has stopped" (ech0-media#33)
    self_registration: Arc<Mutex<Option<JoinHandle<()>>>>,

    /// This node's leader-task supervisor (ech0-media#34): the mechanism behind
    /// [`Self::spawn_leader_task`](crate::leader_task), holding every registered factory and
    /// whichever leadership epoch's runs are currently active
    leader_tasks: Arc<LeaderTasks>,
}

impl EzKv {
    /// Start a new cluster with this node as its only member
    ///
    /// Exactly one node of a cluster is created this way; every other node uses [`Self::join`].
    /// Creating two nodes separately gives two one-node clusters that will never merge.
    ///
    /// # Arguments
    ///
    /// * `http_addr` - Address peers use to reach this node (e.g., "127.0.0.1:8080");
    ///   [`EzConfig::internal_bind_addr`] can override the local listener address
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
    /// The seed hands out a node id and adds this node as a *learner*. The voter change is only
    /// requested after this node's Actix workers have initialized in [`Self::serve`], which is
    /// also where a failed promotion surfaces. The learner needs its internal server running to
    /// receive replication; ezraft does not add a separate catch-up/readiness check to OpenRaft's
    /// membership protocol. So **a joining node must call `serve`**.
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
    /// * `http_addr` - Address peers use to reach this node (e.g., "node-1.internal:8081"); set
    ///   [`EzConfig::internal_bind_addr`] when the local listener uses a different address
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

        // Fail closed before anything else happens: like `to_raft_config` below, this can fail
        // on nothing but the caller's own arguments, and every route on the internal port
        // (`crate::auth`) is unauthenticated until this passes, so a node must never come up far
        // enough to answer one with an empty token.
        if config.cluster_token.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "EzConfig::cluster_token must not be empty: refusing to start a node with an unauthenticated internal port",
            ));
        }

        // Same "fail on nothing but the caller's own arguments" discipline as the token check
        // above and `to_raft_config` below (ech0-media#34): a shorter grace than threshold could
        // mark a peer `Down` before a freshly elected leader could possibly have heard from it.
        config.validate_liveness()?;

        // This node's own registry entry is proposed in the background, retried until it lands;
        // an entry too large to ever be accepted has to fail here, where the caller can see it.
        let entry_bytes = config.external_addr.len() + config.node_meta.len();
        if entry_bytes > config.kv_limits.max_value_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "EzConfig::external_addr plus EzConfig::node_meta is {entry_bytes} bytes, exceeding \
                     kv_limits.max_value_bytes ({} bytes): a node's registry entry is held to the size of a single value",
                    config.kv_limits.max_value_bytes
                ),
            ));
        }

        // Converted first, because it is the only step that can fail on nothing but its
        // arguments, and everything below it either takes an id from the cluster or changes the
        // cluster's membership. A configuration this node cannot run with must not first cost the
        // cluster a learner that then has to be removed by hand.
        let raft_config = Arc::new(config.to_raft_config()?);

        // Hold the internal socket before asking another node for an id or changing membership.
        // When configured separately, only this local endpoint is resolved here; `http_addr`
        // remains the address stored in membership and returned from `EzKv::addr`.
        let internal_listener =
            std::net::TcpListener::bind(config.internal_bind_addr.as_deref().unwrap_or(&http_addr))?;
        let config = Arc::new(config);

        // Open user storage as the two stores openraft asks for
        let (log, sm) = open(storage, config.watch_limits).await?;
        let view_rx = sm.subscribe_view();
        let registry_rx = sm.subscribe_registry();
        let watch = sm.publisher();

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
            //
            // The seed is asked before this node has an id, so `AdminClient` carries the token
            // itself rather than going through the per-node `Transport` built below.
            let admin = AdminClient::new(seed_addr, &config.cluster_token);

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
            // Refuse a fresh bootstrap before assigning id 0 or initializing membership.
            // Persisted restarts above remain valid even without a seed.
            if !config.leader_eligible {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "leader-ineligible nodes cannot create a fresh cluster; join an existing cluster instead",
                ));
            }
            // First node in cluster
            let id = 0;
            log.save_meta(|m| m.node_id = Some(id)).await?;
            id
        };

        // The state machine only learns this node's id now: `open` (and so `sm`'s own
        // construction) ran before it was known. Tracing is the only thing that needs it
        // (ech0-media#26's snapshot build/install events).
        sm.set_node_id(node_id);

        // The single outbound Transport: Raft networking below and this node's own `Rpc` facade
        // both send through it, so both carry this cluster token and, in test builds, the same
        // fault-injection rules. `with_faults` only exists under the `fault-injection` feature;
        // shadowing rather than a `mut` binding keeps a non-feature build from warning about a
        // binding that is never mutated.
        let transport = Transport::new(node_id, config.cluster_token.as_str())?;
        #[cfg(feature = "fault-injection")]
        let transport = transport.with_faults(config.faults.clone());

        let network = EzNetworkFactory::new(transport.clone(), config.snapshot_max_bytes);

        // Create OpenRaft instance
        let raft = Raft::new(node_id, raft_config, network, log, sm)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Persisting id 0 precedes initialize: a crash between them leaves an identity without
        // a cluster. initialize itself campaigns, even with automatic elections disabled, so an
        // ineligible restart must never call it. Reject that unfinished bootstrap and stop the
        // core before returning; fully initialized persisted nodes still restart without a seed.
        if node_id == 0 && !config.leader_eligible && !raft.is_initialized().await.map_err(io::Error::other)? {
            raft.shutdown().await.map_err(io::Error::other)?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "leader-ineligible nodes cannot finish an uninitialized cluster bootstrap",
            ));
        }

        // Eligible node 0 may finish an interrupted bootstrap. For an initialized restart,
        // OpenRaft refuses reinitialization, which is the expected outcome.
        if node_id == 0 && config.leader_eligible {
            let nodes = BTreeMap::from_iter([(node_id, Node::new(http_addr.clone()))]);
            match raft.initialize(nodes).await {
                Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {}
                Err(e) => return Err(io::Error::other(e.to_string())),
            }
        }

        let rpc = Rpc::new(node_id, raft.clone(), transport);

        // One background task per node, fed from openraft's own `server_metrics` channel
        // (ech0-media#33); it ends on its own once that channel closes, at shutdown.
        let cluster_rx = crate::cluster::spawn(raft.clone());

        // The `ez.read_index` handler (ech0-media#31) is registered proactively, for every node,
        // rather than lazily the first time this node itself calls `linearizable_view`: a leader
        // that never calls its own read path must still be able to answer a follower's request.
        crate::read::register_read_index_handler(&rpc, raft.clone());
        let read_index = ReadIndexCoalescer::spawn(node_id, raft.clone(), rpc.clone());

        // The `ez.write` handler (ech0-media#30) is registered the same way, for the same
        // reason: any node forwarding a write here must be answered, whether or not this node
        // ever calls its own `write`.
        crate::write::register_forward_handler(&rpc, raft.clone(), config.kv_limits, config.ledger_capacity);

        let ez = Self {
            node_id,
            addr: http_addr,
            raft,
            lifecycle: NodeLifecycle::new(
                internal_listener,
                promote_via.map(|admin| PromotionRequest::new(admin, node_id)),
            ),
            view_rx,
            config,
            rpc,
            read_index,
            watch,
            registry_rx,
            cluster_rx,
            leader_contact: LeaderContact::default(),
            self_registration: Arc::new(Mutex::new(None)),
            leader_tasks: LeaderTasks::new(),
        };

        // Started last, once `ez` itself exists to hand the task a working handle: this node
        // registers or updates its own entry, retrying under one request id until it succeeds
        // (ech0-media#33). `Self::shutdown` aborts it if it is still retrying.
        let handle = crate::registry::spawn_self_registration(
            ez.clone(),
            ez.config.external_addr.clone(),
            ez.config.node_meta.clone(),
        );
        *ez.self_registration.lock().unwrap() = Some(handle);

        // Also started last, for the same reason: this node's leader-task supervisor needs a
        // working `ez` to hand every run (ech0-media#34). The built-in liveness task is
        // registered through the very same public mechanism any other leader task would use -
        // nothing privileged about it beyond running first, and beyond a fresh single-node
        // cluster already leading by the time this call happens, which is exactly the case
        // `crate::leader_task::LeaderTasks::register`'s own "start at once" path exists for.
        ez.leader_tasks.spawn_supervisor(ez.clone());
        crate::liveness::install(&ez);

        Ok(ez)
    }

    /// The limits `crate::write` checks a transaction against before proposing it
    pub(crate) fn kv_limits(&self) -> KvLimits {
        self.config.kv_limits
    }

    /// `EzConfig::ledger_capacity`, for `crate::write` to stamp into every command this node
    /// proposes
    pub(crate) fn ledger_capacity(&self) -> usize {
        self.config.ledger_capacity
    }

    /// `EzConfig::write_deadline`, the total budget `crate::write`'s retry loop is bounded by
    pub(crate) fn write_deadline(&self) -> Duration {
        self.config.write_deadline
    }

    /// `EzConfig::json_body_limit`, for [`crate::server::bind`]
    pub(crate) fn json_body_limit(&self) -> usize {
        self.config.json_body_limit
    }

    /// `EzConfig::snapshot_max_bytes`, for [`crate::server::bind`]
    pub(crate) fn snapshot_max_bytes(&self) -> usize {
        self.config.snapshot_max_bytes
    }

    /// This node's leader-task supervisor, for [`Self::spawn_leader_task`] (defined in
    /// `crate::leader_task`, its own `impl EzKv` block)
    pub(crate) fn leader_tasks(&self) -> &Arc<LeaderTasks> {
        &self.leader_tasks
    }

    /// `EzConfig::liveness_threshold`, for the built-in liveness task (`crate::liveness`)
    pub(crate) fn liveness_threshold(&self) -> Duration {
        self.config.liveness_threshold
    }

    /// `EzConfig::liveness_grace`, for the built-in liveness task (`crate::liveness`)
    pub(crate) fn liveness_grace(&self) -> Duration {
        self.config.liveness_grace
    }

    /// `EzConfig::liveness_check_interval`, for the built-in liveness task (`crate::liveness`)
    pub(crate) fn liveness_check_interval(&self) -> Duration {
        self.config.liveness_check_interval
    }

    /// This node's latest published view of the KV
    ///
    /// Never awaits, never takes the state machine's lock, and never delays `apply` by more than
    /// a pointer swap: it is a plain read of the value `crate::kv::Publisher` (crate-internal)
    /// last published.
    /// May lag the leader; on the leader itself, a [`Self::write`] that just returned is always
    /// already reflected here: the state machine publishes before it answers the writer.
    pub fn view(&self) -> View {
        self.view_rx.current()
    }

    /// The reader behind [`Self::view`], for the write path's wait on a revision
    pub(crate) fn view_receiver(&self) -> &ViewReceiver {
        &self.view_rx
    }

    /// The reader behind `crate::registry::EzKv::registry`/`watch_registry` (ech0-media#33)
    pub(crate) fn registry_receiver(&self) -> &watch::Receiver<Registry> {
        &self.registry_rx
    }

    /// The reader behind [`crate::cluster::EzKv::cluster`] (ech0-media#33)
    pub(crate) fn cluster_receiver(&self) -> &watch::Receiver<ClusterState> {
        &self.cluster_rx
    }

    /// The tracker behind [`crate::cluster::EzKv::last_leader_contact`] (ech0-media#33)
    pub(crate) fn leader_contact(&self) -> &LeaderContact {
        &self.leader_contact
    }

    /// Record "now" as the last time this node accepted an append-entries request from a leader
    /// it recognized - called by `crate::server`'s append route (ech0-media#33)
    pub(crate) fn record_leader_contact(&self) {
        self.leader_contact.record_now();
    }

    /// This node's own external-port address, as registered in the node registry
    /// (`EzConfig::external_addr`, ech0-media#33)
    pub(crate) fn external_addr(&self) -> &str {
        &self.config.external_addr
    }

    /// Check if this node is the leader
    ///
    /// Reports this node's own state, which is what a caller asking "am I the leader" wants. It
    /// is not a guarantee that the answer is still true elsewhere: a deposed leader does not
    /// find out until it hears from the new one. Nothing here needs that guarantee -- a write
    /// finds the real leader (or reports `Indeterminate`) by itself, and a linearizable read
    /// ([`crate::EzKv::linearizable_view`]) confirms leadership itself, on every call.
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

    /// Get the node ID
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// The local startup policy used by the incoming leadership-transfer handler.
    pub(crate) fn leader_eligible(&self) -> bool {
        self.config.leader_eligible
    }

    /// Get the internal address peers use to reach this node
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Get a reference to the internal OpenRaft instance
    ///
    /// This provides access to advanced OpenRaft APIs if needed. Direct manual election,
    /// transfer handling, or runtime configuration overrides through this escape hatch are
    /// outside the supported [`EzConfig::leader_eligible`] policy.
    pub fn inner(&self) -> &ORRaft {
        &self.raft
    }

    /// The typed inter-node RPC facade: register a handler here, or call one on another node by
    /// id
    ///
    /// Cheap to clone (it is one internally); calling this repeatedly costs nothing beyond that
    /// clone.
    pub fn rpc(&self) -> Rpc {
        self.rpc.clone()
    }

    /// This node's cluster token, for [`crate::server::bind`]'s auth middleware
    pub(crate) fn cluster_token(&self) -> &str {
        &self.config.cluster_token
    }

    /// The follower-side ReadIndex coalescer, for `crate::read::EzKv::linearizable_view`
    pub(crate) fn read_index(&self) -> &ReadIndexCoalescer {
        &self.read_index
    }

    /// The single publish source's watch side, for `crate::watch::EzKv::watch`/`view_and_watch`
    pub(crate) fn watch_publisher(&self) -> &Publisher {
        &self.watch
    }
}
