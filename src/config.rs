//! Configuration for EzRaft
//!
//! EzRaft provides sensible defaults for all Raft timing parameters.
//! Users can optionally customize a few key parameters.

use std::io;
use std::time::Duration;

use openraft::Config;
use openraft::SnapshotPolicy;

use crate::kv::KvLimits;

/// Configuration for EzRaft
///
/// Provides sensible defaults for all Raft timing parameters.
/// Most users can use `EzConfig::default()` directly.
///
/// Election timeout is automatically calculated as 3-6x the heartbeat interval.
#[derive(Clone, Debug)]
pub struct EzConfig {
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
    /// before deletion is derived from this value.
    ///
    /// The default is low enough that a first demo session exercises the whole persist
    /// lifecycle, snapshot and log deletion included, instead of leaving those paths untested.
    /// Default: 500
    pub snapshot_interval: u64,

    /// Size and count limits every transaction is checked against before it is proposed - on the
    /// node that accepts the call, and again on the leader before it enters the log - so that a
    /// node with different limits than the rest can never make replicas diverge.
    ///
    /// Defaults (1 KiB key, 64 KiB value, 128 ops, 1 MiB total) match ech0-media#27's spec and
    /// are provisional until benchmarked (ech0-media#36).
    pub kv_limits: KvLimits,

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
    /// Default: provisional until benchmarked (ech0-media#36); 8 MiB - several times a single
    /// maximally-sized transaction, with headroom to batch a handful more per request.
    pub json_body_limit: usize,

    /// Maximum size of the raw snapshot body `/raft/snapshot` accepts, and the largest snapshot
    /// this node will ever attempt to *send* (ech0-media#26).
    ///
    /// Exceeding it is an explicit error on both sides: the sending node refuses to start a
    /// transfer over its own configured maximum (logged, and reported to openraft as a network
    /// error, so replication backs off and retries rather than wedging - raising the limit is what
    /// lets a later retry succeed); the receiving node's `/raft/snapshot` route answers a body over
    /// its own configured maximum with `413` (`actix_web::web::PayloadConfig`) before ever
    /// attempting to decode it. A cluster where nodes disagree on this value can have a leader
    /// whose snapshots are too small to trip its own check but too large for a stricter follower's
    /// - configure it the same on every node.
    ///
    /// Default: provisional until benchmarked (ech0-media#36); 64 MiB. DESIGN.md §6 already
    /// accepts that both ends hold a whole snapshot in memory at once, so this is sized as a
    /// generous ceiling on that memory cost - many times larger than anything this milestone's own
    /// acceptance tests exercise (a few MiB) - rather than as a realistic expectation of actual
    /// snapshot size. actix's own `PayloadConfig` default is 256 KiB.
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
    /// finish inside a 150ms soft budget (OPENRAFT-A33.md §4): enough for a trivial demo state,
    /// nowhere near enough for the >2 MiB states this ticket's acceptance tests exercise.
    ///
    /// Default: provisional until benchmarked (ech0-media#36); 30 seconds - a round, generous
    /// number chosen ahead of real measurements.
    pub snapshot_timeout: Duration,

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

impl Default for EzConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(500),
            snapshot_interval: 500,
            kv_limits: KvLimits::default(),
            json_body_limit: 8 * 1024 * 1024,
            snapshot_max_bytes: 64 * 1024 * 1024,
            snapshot_timeout: Duration::from_secs(30),
            #[cfg(feature = "fault-injection")]
            faults: None,
        }
    }
}

impl EzConfig {
    /// Convert to openraft Config
    ///
    /// Validates and creates the internal Raft configuration.
    /// Election timeout is calculated as 3x to 6x the heartbeat interval.
    pub(crate) fn to_raft_config(&self) -> Result<Config, io::Error> {
        let heartbeat_ms = self.heartbeat_interval.as_millis() as u64;

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
            // Keep openraft's default proportions: retain 1/5 of the interval for
            // lagging followers, and openraft requires the lag threshold to be at
            // least the snapshot interval or a snapshot may not fix the lag.
            max_in_snapshot_log_to_keep: self.snapshot_interval / 5,
            replication_lag_threshold: self.snapshot_interval,
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
}
