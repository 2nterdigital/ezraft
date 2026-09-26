//! Configuration for EzRaft
//!
//! EzRaft provides sensible defaults for all Raft timing parameters.
//! Users can optionally customize a few key parameters.

use std::fmt;
use std::io;
use std::time::Duration;

use bytes::Bytes;
use openraft::Config;
use openraft::SnapshotPolicy;

use crate::kv::KvLimits;
use crate::watch::WatchLimits;

/// Configuration for EzRaft
///
/// Provides sensible defaults for all Raft timing parameters.
/// Most users can use `EzConfig::default()` directly.
///
/// Election timeout is automatically calculated as 3-6x the heartbeat interval.
#[derive(Clone)]
pub struct EzConfig {
    /// Local address the internal HTTP server binds, if it differs from the address declared to
    /// the cluster
    ///
    /// `EzKv::create`/`join`/`join_as_learner` always use their `http_addr` argument as this node's
    /// member communication address and as [`crate::EzKv::addr`]. When this is `Some`, only the
    /// local listener uses this value; it is bound before the constructor requests an id or
    /// changes cluster membership. This lets a container listen on a local or wildcard interface
    /// while advertising a stable DNS name. When `None` (the default), the constructor argument
    /// continues to serve as the local bind address too.
    pub internal_bind_addr: Option<String>,

    /// Heartbeat interval
    ///
    /// Leader sends heartbeats to followers at this interval.
    /// Election timeout is derived from this value (3x to 6x).
    /// Default: 500ms
    pub heartbeat_interval: Duration,

    /// Number of new log entries between automatic snapshots
    ///
    /// After this many entries since the last snapshot, the framework builds a new one -
    /// [`crate::EzStorage::persist`] receives [`crate::Persist::Snapshot`] - and the entries it
    /// covers become eligible for deletion via [`crate::Persist::DeleteLogs`]. The distance kept
    /// before deletion is [`Self::snapshot_log_to_keep`], a separate, independently configurable
    /// value.
    ///
    /// The default is low enough that a first demo session exercises the whole persist
    /// lifecycle, snapshot and log deletion included, instead of leaving those paths untested.
    /// Default: 500
    pub snapshot_interval: u64,

    /// Log entries kept on disk even after a snapshot already covers them
    ///
    /// Maps to openraft's `max_in_snapshot_log_to_keep`: entries below `last_snapshot_index -
    /// snapshot_log_to_keep` become eligible for deletion via [`crate::Persist::DeleteLogs`];
    /// entries not yet covered by any snapshot are never purged regardless of this value. Keeping
    /// a margin lets a follower that only lagged slightly catch up by replaying log entries
    /// instead of installing a whole snapshot.
    ///
    /// `None` (the default) derives it from [`Self::snapshot_interval`] as `snapshot_interval / 5`,
    /// openraft's own 1:5 ratio between its defaults (5000 logs since the last snapshot, 1000
    /// kept), so changing only the interval keeps the two consistent. Provisional until
    /// benchmarked (ech0-media#36).
    pub snapshot_log_to_keep: Option<u64>,

    /// How far behind in log replication a follower must fall before the leader switches to
    /// sending it a whole snapshot instead of individual log entries
    ///
    /// Maps directly to openraft's `Config::replication_lag_threshold`. openraft's own docs note
    /// this should be at least `snapshot_interval` (which `to_raft_config`, crate-internal,
    /// derives `Config::snapshot_policy` from) - `EzKv::create`/`join` refuse to start a node
    /// whose value is smaller, since a snapshot could then never catch a lagging follower up.
    ///
    /// `None` (the default) derives it from [`Self::snapshot_interval`] as the interval itself,
    /// its documented minimum, so changing only the interval can never produce an inconsistent
    /// pair. ech0-media#36's follower catch-up measurement ran against this default and gave no
    /// reason to change it (`FORK.md`, 参数定值).
    pub replication_lag_threshold: Option<u64>,

    /// The shared secret every request on the internal port must present as
    /// `Authorization: Bearer <cluster_token>` - `/raft/*`, `/api/*`, the RPC prefix, and any
    /// route an embedder adds through [`crate::EzKv::serve_with`] alike - compared in constant
    /// time (ech0-media#15). Exactly one token is supported at a time:
    /// rotating it means restarting the whole cluster with the new value.
    ///
    /// **Mandatory, with no sensible default**: [`EzKv::create`](crate::EzKv::create) and its
    /// siblings refuse to start a node whose token is empty, so there is no way to accidentally
    /// run an unauthenticated internal port. `EzConfig::default()` leaves this empty on purpose -
    /// set a real value before starting a node with it.
    pub cluster_token: String,

    /// Size and count limits every transaction is checked against before it is proposed - on the
    /// node that accepts the call, and again on the leader before it enters the log - so that a
    /// node with different limits than the rest can never make replicas diverge.
    ///
    /// Defaults (1 KiB key, 64 KiB value, 128 ops, 1 MiB total) are ech0-media#27's. They are
    /// limits on what callers may ask for rather than tuning values: ech0-media#36 measured what
    /// depends on them (the request body limit, snapshot size and timing) and left them as they
    /// are.
    pub kv_limits: KvLimits,

    /// Size, count and timing limits for `watch`/`view_and_watch` subscriptions (ech0-media#32):
    /// the retained window's group count and byte size, each subscriber's queue length, and the
    /// `Progress` rate limit.
    ///
    /// Defaults (10,000 groups, 64 MiB, a queue of 1024, a 1-second `Progress` interval) kept as
    /// measured (`FORK.md`'s "参数定值" § watch 环形缓冲与订阅队列): for typical small KV
    /// changes the group-count cap binds well before the byte cap, and fan-out delivery latency
    /// showed no rising cost per subscriber. `WatchLimits::default()` itself lives in
    /// `src/watch.rs`, outside this measurement's own file - see that type's own doc comment for
    /// its definitive default values.
    pub watch_limits: WatchLimits,

    /// Maximum size of a JSON request body for the Raft and admin/RPC routes
    /// (`actix_web::web::JsonConfig`, ech0-media#26) - every route except `/raft/snapshot`, which
    /// carries the snapshot as a raw body instead (see [`Self::snapshot_max_bytes`]).
    ///
    /// Must comfortably exceed the largest legal single transaction once JSON-encoded: values
    /// travel as base64 (roughly 4/3 their raw bytes) inside a `Vec<Op>`/`Vec<Condition>`
    /// structure, so a transaction at `kv_limits.max_txn_bytes` (default 1 MiB) can reach close to
    /// double that once serialized. `EzConfig::to_raft_config` (crate-internal) derives openraft's
    /// `max_payload_entries` (how many log entries one `/raft/append` request may batch) from this
    /// limit and `kv_limits.max_txn_bytes`, rather than leaving openraft's own default of 300 in
    /// place - 300 entries at the 1 MiB transaction default would be up to ~300 MiB in a single
    /// request, which no sane body limit should admit. Raising this value only changes how many
    /// transactions the leader batches per replication round trip; it never changes the
    /// oversized-request failure mode itself. actix's own default is 2 MiB.
    ///
    /// Default: 8 MiB - several times a single maximally-sized transaction, with headroom to
    /// batch a handful more per request. Measured (`FORK.md`'s "参数定值" § 请求体上限):
    /// against the derived `max_payload_entries` of 4 (openraft's own default is 300), a
    /// benchmark of small entries under concurrency and of a follower a few hundred entries
    /// behind found no meaningful difference in write throughput or catch-up time on the
    /// measured machine - both were dominated by this crate's per-entry fsync cost (see
    /// [`Self::ledger_capacity`]'s own doc comment), not by how many entries one append batches.
    /// That measurement does not generalize to a deployment where round trips are expensive
    /// (real network RTT) or storage is fast enough for batch size to matter; if the derived cap
    /// of 4 turns out to cost throughput in such a deployment, splitting an oversized append by
    /// *bytes* inside `Network::append_entries` (crate-internal) and answering openraft's
    /// `AppendEntriesResponse::PartialSuccess` would let small entries batch by the hundreds
    /// while this limit continues to bound the worst case - a proposal recorded in `FORK.md`,
    /// not implemented.
    pub json_body_limit: usize,

    /// Maximum size of the raw snapshot body `/raft/snapshot` accepts, and the largest snapshot
    /// this node will ever attempt to *send* (ech0-media#26).
    ///
    /// Exceeding it is an explicit error on both sides: the sending node refuses to start a
    /// transfer over its own configured maximum (logged, and reported to openraft as an
    /// unreachable peer, so replication backs off and retries rather than wedging - raising the
    /// limit is what lets a later retry succeed); the receiving node's `/raft/snapshot` route
    /// answers a body over its own configured maximum with `413`
    /// (`actix_web::web::PayloadConfig`) before ever attempting to decode it. A cluster where
    /// nodes disagree on this value can have a leader whose snapshots are too small to trip its
    /// own check but too large for a stricter follower's
    /// - configure it the same on every node.
    ///
    /// Default: 64 MiB. The snapshot design (ech0-media#14) accepts that both ends hold a whole
    /// snapshot in memory at once, so this is sized as a generous ceiling on that memory cost -
    /// many times larger than anything this milestone's own acceptance tests exercise (a few
    /// MiB) - rather than as a realistic expectation of actual snapshot size. actix's own
    /// `PayloadConfig` default is 256 KiB. Measured (`FORK.md`'s "参数定值" §
    /// 快照超时与体积上限): a real-process build/transfer/install benchmark up to ~51 MiB (a
    /// three-voter cluster, one shared disk) stayed well inside this ceiling with margin to
    /// spare; kept at 64 MiB rather than lowered, since a localhost benchmark cannot speak to how
    /// large a legitimate production snapshot gets.
    pub snapshot_max_bytes: usize,

    /// Budget for one whole `/raft/snapshot` POST, mapped to openraft's
    /// `install_snapshot_timeout` (ech0-media#26).
    ///
    /// This is the field that actually governs the snapshot request's effective timeout: the
    /// network layer (crate-internal) gives every request `RPCOption::soft_ttl()` (75% of whatever
    /// hard timeout openraft configured for it) as its `reqwest` timeout, and openraft computes
    /// the snapshot RPC's hard timeout from exactly this `Config` field. Before this field existed,
    /// ezraft left `install_snapshot_timeout` at openraft's own default (200ms), so the whole
    /// snapshot POST - including the receiving node's time to decode and install it - had to
    /// finish inside a 150ms soft budget (ech0-media#6): enough for a trivial demo state,
    /// nowhere near enough for the >2 MiB states this ticket's acceptance tests exercise.
    ///
    /// Default: 30 seconds. Measured (`FORK.md`'s "参数定值" § 快照超时与体积上限): on a
    /// real three-voter process cluster (one shared disk), build+transfer+install for a ~51 MiB
    /// snapshot - sent to a brand-new joiner, which always needs the full snapshot - completed
    /// within roughly 100 ms, comfortably inside this budget. Kept generous rather than tightened
    /// to the measured figure: localhost has no real network latency and shares one physical disk
    /// across all three voters, so a real deployment's transfer time (and thus the safe timeout)
    /// could be substantially higher; see the same section for the full caveat.
    pub snapshot_timeout: Duration,

    /// How many entries the idempotency ledger (ech0-media#30) keeps before evicting the oldest,
    /// first-in-first-out.
    ///
    /// Read only when a write is *proposed* - the proposing node stamps its own configured value
    /// into the Raft log command - never inside `apply` itself: eviction must be a pure function
    /// of the log, or two differently-configured replicas would diverge the moment one
    /// deduplicated a retry the other re-executed (see `crate::kv::ledger`'s module docs).
    ///
    /// Default: 100,000, matching ech0-media#12's spec. Measured (`FORK.md`'s "参数定值" §
    /// 幂等账本容量): this crate's own writes serialize regardless of client concurrency (one
    /// `Durability::Immediate` persist per log entry, no group commit, ech0-media#11's
    /// resolution) - on the measured machine, roughly 55-75 writes/second sustained. Covering
    /// [`Self::write_deadline`]'s default (10s) with a 3x margin at that rate needs on the order
    /// of 2,000 entries, comfortably under the default; kept at 100,000 rather than lowered,
    /// since a production write rate this benchmark cannot reproduce could be far higher.
    pub ledger_capacity: usize,

    /// The total time budget [`crate::EzKv::write`] and [`crate::EzKv::write_with_id`] retry a
    /// write across leadership changes before giving up with `Indeterminate`.
    ///
    /// Bounds the *whole* call: forwarding to the leader, retrying with the same request id
    /// through any number of leadership changes, and - once a success or conflict is definite -
    /// this node's own catch-up wait before returning it (or `LocalLag` if that wait itself runs
    /// out the budget). Also bounds a single node's own leader-only retry of the alpha.33 no-lease
    /// gap (ech0-media#34): `crate::leader_task::LeaderCtx::write` and the built-in liveness
    /// task's internal commands never forward, so there is no second, longer budget to reuse for
    /// them - this is the one configured value both share.
    ///
    /// Default: 10 seconds (ech0-media#15). ech0-media#36 measured a leader failover at about
    /// 5.3-6 seconds with the default heartbeat, so one deadline covers one failover with room to
    /// spare - which is what lets a write issued during an election succeed rather than come back
    /// `Indeterminate` (`FORK.md`, 参数定值).
    pub write_deadline: Duration,

    /// This node's client-facing signaling address, registered in the node registry
    /// (ech0-media#33) at startup and readable by every node through
    /// [`crate::EzKv::registry`]/[`crate::EzKv::watch_registry`] - distinct from `http_addr`
    /// (`EzKv::create`/`join`'s argument), which is the *internal* port Raft traffic and inter-node
    /// RPC use and is never meant for a client.
    ///
    /// Opaque to the foundation: never parsed, never dialed. Default: an empty string - a node
    /// with nothing configured here still registers, with an empty `external_addr`, rather than
    /// skipping registration.
    pub external_addr: String,

    /// This node's initial application-defined registry metadata (ech0-media#33): opaque bytes,
    /// stored and handed back verbatim, never interpreted by the foundation. Registered alongside
    /// [`Self::external_addr`] at startup; [`crate::EzKv::update_node_meta`] changes it later.
    ///
    /// Default: empty.
    pub node_meta: Bytes,

    /// How long a peer's last-acknowledged heartbeat may go unrenewed before the built-in
    /// liveness task (ech0-media#34) marks its registry entry `Down`
    ///
    /// Read only by the leader task that makes this judgment, from its own view of openraft's
    /// per-peer heartbeat metrics - never by `apply`, which only ever assigns the status a
    /// `SetStatus` command already names (the determinism rule: thresholds shape which command
    /// the leader proposes, never how a replica applies one). A peer this leadership has never
    /// heard from counts as silent since the leadership began, not since some earlier leadership
    /// this one has no visibility into - see [`Self::liveness_grace`] for why that matters.
    ///
    /// Default: 5 seconds. Measured (`FORK.md`'s "参数定值" § 存活判定阈值与宽限期): a
    /// `SIGKILL`ed follower's registry entry read "Down" on a surviving node within threshold
    /// plus one check tick on the measured machine, and no healthy node was ever falsely marked
    /// "Down" under write load or during a leader change - including a tighter 1.5-second
    /// candidate tried side by side. Kept at the generous default rather than tightened to that
    /// candidate: a real deployment's slower elections (real network round trips, more voters,
    /// real per-node disk contention) are exactly the case a shorter threshold could
    /// false-positive on, and a localhost benchmark with no real network latency cannot
    /// reproduce that slowness to rule it out.
    pub liveness_threshold: Duration,

    /// How long a freshly won leadership must have lasted before the built-in liveness task marks
    /// *any* peer `Down`
    ///
    /// openraft alpha.33's per-peer heartbeat metrics start every member other than the leader
    /// itself at `None` on a brand new leadership (see `crate::liveness`'s own module docs), so
    /// without a grace period every peer would briefly look silent right after an election, well
    /// before the new leader could possibly have heard from any of them even once.
    ///
    /// Must be at least [`Self::liveness_threshold`] - `EzKv::create`/`join` refuse to start a
    /// node configured with a shorter one, since that could still mark a peer `Down` before this
    /// leadership could possibly have heard from it.
    ///
    /// Default: equal to [`Self::liveness_threshold`]'s own default (5 seconds). Measured
    /// (`FORK.md`'s "参数定值" § 存活判定阈值与宽限期): a fresh election took roughly
    /// 5.3-6 seconds end to end on the measured machine - a small multiple of
    /// `election_timeout_max` (`6 × heartbeat_interval` at this crate's own defaults), which is
    /// the scale a leader change should take - and no surviving healthy follower was ever falsely
    /// marked "Down" during one, even with a tighter 1.5-second grace tried side by side. Kept at
    /// the generous default for the same reason as [`Self::liveness_threshold`]'s own doc
    /// comment: a real deployment's elections can run slower than this benchmark's idle localhost
    /// cluster can reproduce.
    pub liveness_grace: Duration,

    /// How often the built-in liveness task re-evaluates every peer's heartbeat, and the registry
    /// against the committed membership
    ///
    /// Default: 1 second - comfortably below the default threshold, so a peer going down is
    /// detected promptly once past it. Measured (`FORK.md`'s "参数定值" § 存活判定阈值与宽限期):
    /// this is the dominant source of slack in observed detection time (threshold plus up to one
    /// tick, matching this value); no evidence favored a different value over the round number
    /// already this small relative to the default threshold.
    pub liveness_check_interval: Duration,

    /// The test cluster's shared network fault-injection handle, if any
    ///
    /// Only present in builds compiled with the `fault-injection` cargo feature (off by
    /// default): a normal build has no such field, and pays nothing for it. Every node of one
    /// in-process test cluster is given the same `Faults` handle, which is how a test drops,
    /// delays, or partitions requests between chosen node pairs and heals them again - see
    /// `ezraft::fault`. Default: `None`.
    #[cfg(feature = "fault-injection")]
    pub faults: Option<crate::transport::fault::Faults>,
}

/// Written out rather than derived, for one field: [`EzConfig::cluster_token`] is a secret, and a
/// configuration is exactly the kind of value that gets logged whole at startup.
impl fmt::Debug for EzConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Destructured in full, so that a field added later cannot be left out of this by
        // accident - or, were it another secret, shown by default.
        let Self {
            internal_bind_addr,
            heartbeat_interval,
            snapshot_interval,
            snapshot_log_to_keep,
            replication_lag_threshold,
            cluster_token: _,
            kv_limits,
            watch_limits,
            json_body_limit,
            snapshot_max_bytes,
            snapshot_timeout,
            ledger_capacity,
            write_deadline,
            external_addr,
            node_meta,
            liveness_threshold,
            liveness_grace,
            liveness_check_interval,
            #[cfg(feature = "fault-injection")]
            faults,
        } = self;

        let mut shown = f.debug_struct("EzConfig");
        shown
            .field("internal_bind_addr", internal_bind_addr)
            .field("heartbeat_interval", heartbeat_interval)
            .field("snapshot_interval", snapshot_interval)
            .field("snapshot_log_to_keep", snapshot_log_to_keep)
            .field("replication_lag_threshold", replication_lag_threshold)
            .field("cluster_token", &"<redacted>")
            .field("kv_limits", kv_limits)
            .field("watch_limits", watch_limits)
            .field("json_body_limit", json_body_limit)
            .field("snapshot_max_bytes", snapshot_max_bytes)
            .field("snapshot_timeout", snapshot_timeout)
            .field("ledger_capacity", ledger_capacity)
            .field("write_deadline", write_deadline)
            .field("external_addr", external_addr)
            .field("node_meta", node_meta)
            .field("liveness_threshold", liveness_threshold)
            .field("liveness_grace", liveness_grace)
            .field("liveness_check_interval", liveness_check_interval);
        #[cfg(feature = "fault-injection")]
        shown.field("faults", faults);
        shown.finish()
    }
}

impl Default for EzConfig {
    fn default() -> Self {
        Self {
            internal_bind_addr: None,
            heartbeat_interval: Duration::from_millis(500),
            cluster_token: String::new(),
            snapshot_interval: 500,
            // Both derived from `snapshot_interval` unless set: see the fields' own docs.
            snapshot_log_to_keep: None,
            replication_lag_threshold: None,
            kv_limits: KvLimits::default(),
            watch_limits: WatchLimits::default(),
            json_body_limit: 8 * 1024 * 1024,
            snapshot_max_bytes: 64 * 1024 * 1024,
            snapshot_timeout: Duration::from_secs(30),
            ledger_capacity: 100_000,
            write_deadline: Duration::from_secs(10),
            external_addr: String::new(),
            node_meta: Bytes::new(),
            liveness_threshold: Duration::from_secs(5),
            liveness_grace: Duration::from_secs(5),
            liveness_check_interval: Duration::from_secs(1),
            #[cfg(feature = "fault-injection")]
            faults: None,
        }
    }
}

impl EzConfig {
    /// Validate the liveness settings
    ///
    /// Called by `EzKv::new`, the same way the replication-lag/snapshot-interval pair validates
    /// itself inside [`Self::to_raft_config`] - a check on nothing but this configuration's own
    /// arguments, so it belongs alongside every other "fail before this node starts" check rather
    /// than surfacing however the first liveness tick would discover it.
    pub(crate) fn validate_liveness(&self) -> Result<(), io::Error> {
        if self.liveness_grace < self.liveness_threshold {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "liveness_grace ({:?}) must be at least liveness_threshold ({:?}): a shorter grace period \
                     could mark a peer Down before a freshly elected leader could possibly have heard from it \
                     even once",
                    self.liveness_grace, self.liveness_threshold
                ),
            ));
        }
        Ok(())
    }

    /// Convert to openraft Config
    ///
    /// Validates and creates the internal Raft configuration.
    /// Election timeout is calculated as 3x to 6x the heartbeat interval.
    pub(crate) fn to_raft_config(&self) -> Result<Config, io::Error> {
        let heartbeat_ms = self.heartbeat_interval.as_millis() as u64;

        // openraft's own docs on `replication_lag_threshold` (which `snapshot_policy` below is
        // built from `snapshot_interval` to match): "This value should be greater than
        // snapshot_policy.SnapshotPolicy.LogsSinceLast, otherwise transmitting a snapshot may not
        // fix the lagging." Checked here, with a message naming both configured values, rather
        // than left to surface however openraft's own `validate()` (below) would report it - or,
        // worse, not report it at all, since openraft's `validate()` does not actually check this
        // combination itself.
        let snapshot_log_to_keep = self.snapshot_log_to_keep.unwrap_or(self.snapshot_interval / 5);
        let replication_lag_threshold = self.replication_lag_threshold.unwrap_or(self.snapshot_interval);

        if replication_lag_threshold < self.snapshot_interval {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "replication_lag_threshold ({}) must be at least snapshot_interval ({}): a value \
                     smaller than the snapshot policy's own threshold means a snapshot may never catch a \
                     lagging follower up",
                    replication_lag_threshold, self.snapshot_interval
                ),
            ));
        }

        // Bound how many log entries openraft may batch into one `/raft/append` request so the
        // worst case never exceeds `json_body_limit` - see that field's own doc comment for why
        // openraft's own default (300) cannot be left as-is. `* 2` leaves headroom for a
        // transaction's base64 inflation (~4/3) plus the JSON structure around it (`Command::Txn`,
        // `EzEntry`'s own fields, one object per op/condition).
        let max_txn_bytes = (self.kv_limits.max_txn_bytes as u64).max(1);
        let max_payload_entries = self.json_body_limit as u64 / (max_txn_bytes * 2);

        // A body limit that cannot hold even one maximal transaction would not fail at the write:
        // the entry commits to the leader's log, and every `/raft/append` carrying it is then
        // refused by the follower, forever. Refuse the configuration instead.
        if max_payload_entries == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "json_body_limit ({} bytes) must be at least twice kv_limits.max_txn_bytes ({} bytes): \
                     one maximal transaction, JSON-encoded, has to fit in a single replication request",
                    self.json_body_limit, self.kv_limits.max_txn_bytes
                ),
            ));
        }

        let config = Config {
            heartbeat_interval: heartbeat_ms,
            election_timeout_min: heartbeat_ms * 3,
            election_timeout_max: heartbeat_ms * 6,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(self.snapshot_interval),
            max_in_snapshot_log_to_keep: snapshot_log_to_keep,
            replication_lag_threshold,
            install_snapshot_timeout: self.snapshot_timeout.as_millis() as u64,
            max_payload_entries,
            // Pre-Vote stays off, matching openraft's own default (`enable_pre_vote: None`): a
            // deliberate choice (ech0-media#10's resolution), not an oversight - reconnecting
            // nodes causing disruptive elections is already partly mitigated by the leader lease
            // rejecting votes while it holds.
            ..Default::default()
        };

        config.validate().map_err(io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defaults batch a handful of maximal transactions per replication request.
    #[test]
    fn the_default_body_limit_holds_several_maximal_transactions() {
        let config = EzConfig::default().to_raft_config().unwrap();

        assert_eq!(4, config.max_payload_entries);
    }

    /// A transaction the write path accepts must be replicable. A body limit below one maximal
    /// transaction would let it commit on the leader and then refuse every append carrying it.
    #[test]
    fn a_body_limit_below_one_maximal_transaction_is_refused() {
        let config = EzConfig {
            json_body_limit: 1024 * 1024,
            ..EzConfig::default()
        };

        let err = config.to_raft_config().unwrap_err();

        assert_eq!(io::ErrorKind::InvalidInput, err.kind());
        assert!(err.to_string().contains("json_body_limit"), "unexpected error: {err}");
    }

    /// The three snapshot parameters take effect: each configured value reaches openraft's
    /// `Config` unchanged, not silently re-derived from `snapshot_interval` the way the previous,
    /// non-configurable defaults were.
    #[test]
    fn the_three_snapshot_parameters_reach_the_raft_config_unchanged() {
        let config = EzConfig {
            snapshot_interval: 50,
            snapshot_log_to_keep: Some(7),
            replication_lag_threshold: Some(90),
            ..EzConfig::default()
        };

        let raft_config = config.to_raft_config().unwrap();

        assert_eq!(SnapshotPolicy::LogsSinceLast(50), raft_config.snapshot_policy);
        assert_eq!(7, raft_config.max_in_snapshot_log_to_keep);
        assert_eq!(90, raft_config.replication_lag_threshold);
    }

    /// Left unset, the two derived parameters follow the interval (`snapshot_interval / 5`, and the
    /// lag threshold equal to the interval - its documented minimum), so nodes that never touch
    /// them behave exactly as before this ticket.
    #[test]
    fn unset_snapshot_parameters_are_derived_from_the_interval() {
        let raft_config = EzConfig::default().to_raft_config().unwrap();

        assert_eq!(SnapshotPolicy::LogsSinceLast(500), raft_config.snapshot_policy);
        assert_eq!(100, raft_config.max_in_snapshot_log_to_keep);
        assert_eq!(500, raft_config.replication_lag_threshold);
    }

    /// Changing only the interval must not produce an inconsistent pair: the derived lag threshold
    /// follows it instead of staying at the old interval and failing validation.
    #[test]
    fn changing_only_the_interval_keeps_the_derived_parameters_consistent() {
        let config = EzConfig {
            snapshot_interval: 2000,
            ..EzConfig::default()
        };

        let raft_config = config.to_raft_config().unwrap();

        assert_eq!(400, raft_config.max_in_snapshot_log_to_keep);
        assert_eq!(2000, raft_config.replication_lag_threshold);
    }

    /// An inconsistent combination - a lag threshold below the snapshot interval, which openraft's
    /// own docs warn may leave a snapshot unable to fix a lagging follower - is refused with a
    /// message naming both configured values, not left to fail obscurely later.
    #[test]
    fn a_replication_lag_threshold_below_the_snapshot_interval_is_refused() {
        let config = EzConfig {
            snapshot_interval: 500,
            replication_lag_threshold: Some(100),
            ..EzConfig::default()
        };

        let err = config.to_raft_config().unwrap_err();

        assert_eq!(io::ErrorKind::InvalidInput, err.kind());
        assert!(
            err.to_string().contains("replication_lag_threshold") && err.to_string().contains("snapshot_interval"),
            "unexpected error: {err}"
        );
    }

    /// A lag threshold exactly equal to the snapshot interval is openraft's documented minimum,
    /// not an off-by-one over it - it must be accepted, matching the defaults themselves.
    #[test]
    fn a_replication_lag_threshold_equal_to_the_snapshot_interval_is_accepted() {
        let config = EzConfig {
            snapshot_interval: 500,
            replication_lag_threshold: Some(500),
            ..EzConfig::default()
        };

        assert!(config.to_raft_config().is_ok());
    }

    /// The defaults keep the grace period at least as long as the threshold, matching the
    /// documented minimum.
    /// A configuration gets logged whole; the cluster token must not go with it.
    #[test]
    fn debug_output_never_contains_the_cluster_token() {
        let config = EzConfig {
            cluster_token: "s3cret-cluster-token".to_string(),
            ..EzConfig::default()
        };
        let shown = format!("{config:?} {config:#?}");
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(
            shown.contains("cluster_token") && shown.contains("<redacted>"),
            "{shown}"
        );
        assert!(
            shown.contains("heartbeat_interval"),
            "every other field is still shown: {shown}"
        );
    }

    #[test]
    fn the_default_liveness_grace_is_at_least_the_default_threshold() {
        assert!(EzConfig::default().validate_liveness().is_ok());
        assert_eq!(
            EzConfig::default().liveness_threshold,
            EzConfig::default().liveness_grace
        );
    }

    /// A grace period shorter than the threshold is refused with a message naming both configured
    /// values - it could mark a peer `Down` before a freshly elected leader could possibly have
    /// heard from it even once.
    #[test]
    fn a_liveness_grace_shorter_than_the_threshold_is_refused() {
        let config = EzConfig {
            liveness_threshold: Duration::from_secs(10),
            liveness_grace: Duration::from_secs(5),
            ..EzConfig::default()
        };

        let err = config.validate_liveness().unwrap_err();

        assert_eq!(io::ErrorKind::InvalidInput, err.kind());
        assert!(
            err.to_string().contains("liveness_grace") && err.to_string().contains("liveness_threshold"),
            "unexpected error: {err}"
        );
    }

    /// A grace period exactly equal to the threshold is the documented minimum, not an off-by-one
    /// over it - it must be accepted.
    #[test]
    fn a_liveness_grace_equal_to_the_threshold_is_accepted() {
        let config = EzConfig {
            liveness_threshold: Duration::from_secs(10),
            liveness_grace: Duration::from_secs(10),
            ..EzConfig::default()
        };

        assert!(config.validate_liveness().is_ok());
    }
}
