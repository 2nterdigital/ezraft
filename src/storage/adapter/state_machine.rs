//! The state machine half of the storage adapter: the KV, and its snapshot
//!
//! Owns the single [`Publisher`] - views and watches must share one source: every applied
//! transaction publishes its new view before answering the writer ([`RaftStateMachine::apply`]),
//! and installing a snapshot or finishing startup recovery publishes again ([`Self::new`],
//! [`RaftStateMachine::install_snapshot`]).

use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use futures::StreamExt;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use openraft::alias::LogIdOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use tokio::sync::Mutex;

use crate::command::Command;
use crate::command::CommandBody;
use crate::kv::KvState;
use crate::kv::Publisher;
use crate::kv::ViewReceiver;
use crate::kv::decode_snapshot;
use crate::kv::encode_snapshot;
use crate::snapshot::EzSnapshot;
use crate::snapshot::EzSnapshotData;
use crate::snapshot::EzSnapshotMeta;
use crate::storage::Persist;
use crate::storage::adapter::SharedStorage;
use crate::type_config::OpenRaftTypes;

/// Internal state machine wrapper that tracks Raft metadata alongside the KV
struct StateMachineState {
    /// The replicated KV: the crate's only state machine
    kv: KvState,

    /// Last log ID applied to the state machine
    last_applied: Option<LogIdOf<OpenRaftTypes>>,

    /// Last membership applied to the state machine
    membership: StoredMembershipOf<OpenRaftTypes>,
}

/// The state machine: the KV, what it has applied, and its snapshot
pub struct StateMachineStore {
    storage: SharedStorage,

    sm_state: Arc<Mutex<StateMachineState>>,

    /// The snapshot last written or loaded, kept so that serving one to a lagging follower does
    /// not re-run the startup-only [`EzStorage::load`](crate::storage::EzStorage::load).
    snapshot: Arc<Mutex<Option<EzSnapshot>>>,

    /// The single publisher: every applied transaction, and installing a
    /// snapshot, publish through this one instance.
    publisher: Arc<Publisher>,

    /// A `view()` reader clone, handed out again by [`Self::subscribe_view`]; lock-free and
    /// never touches `sm_state`.
    view_rx: ViewReceiver,

    /// This node's own id, attached to the snapshot build/install tracing events
    /// (ech0-media#26) - `crate::raft::EzKv::new` sets it exactly once via [`Self::set_node_id`],
    /// right after this store is constructed, because `crate::storage::adapter::open` (and so
    /// this constructor) runs *before* the node id is known: a restart reads it back from the
    /// very log store `open` returns, and a join asks a seed for one. A plain constructor
    /// argument would need that ordering reversed, which is more than this ticket's tracing-only
    /// mandate for this file calls for.
    node_id: Arc<OnceLock<u64>>,
}

impl Clone for StateMachineStore {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sm_state: self.sm_state.clone(),
            snapshot: self.snapshot.clone(),
            publisher: self.publisher.clone(),
            view_rx: self.view_rx.clone(),
            node_id: self.node_id.clone(),
        }
    }
}

impl StateMachineStore {
    /// Restore the state machine from the snapshot the storage loaded, if there is one
    ///
    /// The snapshot data is restored here, not just its position: reporting `last_applied` at
    /// the snapshot makes openraft re-apply only the log tail after it, and skip installing this
    /// snapshot itself. The publisher this constructs already holds that restored view - the
    /// "publish after startup recovery" the read path needs (ech0-media#16) falls out of the log
    /// tail's own applies, or, if there is no tail, out of this initial value.
    ///
    /// An unknown format version - or any other way the stored bytes fail to decode - is
    /// reported here as a plain `Err`, which is exactly how `crate::raft::EzKv::new` (via
    /// `crate::storage::adapter::open`) already propagates every other startup failure: it makes
    /// `create`/`join` fail outright rather than starting a node on a snapshot it cannot read.
    pub(super) fn new(storage: SharedStorage, snapshot: Option<EzSnapshot>) -> Result<Self, std::io::Error> {
        let (last_applied, membership, kv) = match &snapshot {
            Some(snap) => {
                let decoded = decode_snapshot(snap.snapshot.get_ref())
                    .inspect_err(|e| {
                        tracing::error!(
                            last_log_id = ?snap.meta.last_log_id,
                            size_bytes = snap.snapshot.get_ref().len(),
                            error = %e,
                            outcome = "failed",
                            "failed to load the stored snapshot at startup: invalid or unsupported snapshot data",
                        );
                    })
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                (
                    snap.meta.last_log_id,
                    snap.meta.last_membership.clone(),
                    KvState::restore(decoded.revision, decoded.entries, decoded.internal),
                )
            }
            None => (None, StoredMembership::new(None, Membership::default()), KvState::new()),
        };

        let (publisher, view_rx) = Publisher::new(kv.view());

        let sm_state = StateMachineState {
            kv,
            last_applied,
            membership,
        };

        Ok(Self {
            storage,
            sm_state: Arc::new(Mutex::new(sm_state)),
            snapshot: Arc::new(Mutex::new(snapshot)),
            publisher: Arc::new(publisher),
            view_rx,
            node_id: Arc::new(OnceLock::new()),
        })
    }

    /// A clone of the reader every `view()` call reads from: lock-free, and never delayed by
    /// `apply` by more than a pointer swap.
    pub(crate) fn subscribe_view(&self) -> ViewReceiver {
        self.view_rx.clone()
    }

    /// A clone of the single publisher, for `crate::watch::EzKv::watch`/`view_and_watch`
    /// (ech0-media#32) to register subscriptions through - the same instance
    /// [`RaftStateMachine::apply`] and [`RaftStateMachine::install_snapshot`] publish through, so
    /// a subscription and the view it starts at are always consistent.
    pub(crate) fn publisher(&self) -> Arc<Publisher> {
        self.publisher.clone()
    }

    /// Record this node's own id, once, for the snapshot build/install tracing events
    ///
    /// Called exactly once, by `crate::raft::EzKv::new`, right after this store is constructed -
    /// see the field's own doc comment for why the node id cannot be a constructor argument here.
    /// A second call (which should never happen) is silently ignored rather than panicking: a
    /// tracing field is not worth a node's availability.
    pub(crate) fn set_node_id(&self, node_id: u64) {
        let _ = self.node_id.set(node_id);
    }

    /// This node's own id, if [`Self::set_node_id`] has run yet
    fn node_id(&self) -> Option<u64> {
        self.node_id.get().copied()
    }
}

impl RaftStateMachine<OpenRaftTypes> for StateMachineStore {
    type SnapshotData = EzSnapshotData;

    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<OpenRaftTypes>>, StoredMembershipOf<OpenRaftTypes>), std::io::Error> {
        let sm = self.sm_state.lock().await;
        Ok((sm.last_applied, sm.membership.clone()))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), std::io::Error>
    where Strm: futures::Stream<Item = Result<EntryResponder<OpenRaftTypes>, std::io::Error>> + OptionalSend + Unpin
    {
        let mut sm = self.sm_state.lock().await;

        let mut entries = entries;
        while let Some(res) = entries.next().await {
            let (entry, responder) = res.map_err(std::io::Error::other)?;

            // Update last_applied for every entry
            let (term, index) = entry.log_id;
            let log_id = LogId::new_term_index(term, index);
            sm.last_applied = Some(log_id);

            let resp = match entry.payload {
                EntryPayload::Normal(Command {
                    request_id,
                    ledger_capacity,
                    body: CommandBody::Txn(txn),
                }) => {
                    // The idempotency ledger (ech0-media#30) is consulted *inside*
                    // `apply_idempotent`, before `txn` ever runs: a retried request id comes back
                    // as the outcome already recorded for it, with `changes: None`. Publish before
                    // answering the writer (ech0-media#16) only applies to a freshly executed
                    // command - a deduplicated one changed nothing, so there is nothing new to
                    // publish.
                    let applied = sm.kv.apply_idempotent(request_id, ledger_capacity as usize, &txn);
                    if let Some(changes) = applied.changes {
                        self.publisher.publish(sm.kv.view(), changes);
                    }
                    Some(applied.outcome)
                }
                EntryPayload::Membership(membership) => {
                    sm.membership = StoredMembership::new(Some(log_id), membership);
                    None
                }
                EntryPayload::Blank => None,
            };

            if let Some(responder) = responder {
                responder.send(resp);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        snapshot_meta: &EzSnapshotMeta,
        snapshot_data: EzSnapshotData,
    ) -> Result<(), std::io::Error> {
        let start = Instant::now();

        // Extract snapshot data
        let mut cursor = snapshot_data;
        cursor.seek(SeekFrom::Start(0))?;
        let mut data = Vec::new();
        cursor.read_to_end(&mut data)?;
        let size_bytes = data.len();

        // Decoded and validated - magic, format version, then the body - before anything keeps
        // it. Bytes that do not decode - a corrupt transfer, an unknown format version from a
        // peer running an incompatible build - are this transfer's problem, and persisting them
        // would make them every later start's problem too: [`Self::new`] reads the stored
        // snapshot back and would fail on the same bytes, with the state they replaced already
        // gone. openraft treats an `Err` from `install_snapshot` as fatal to this node, which is
        // exactly the "stop" an unknown version calls for - logged at `error` before it
        // propagates, not merely `warn`.
        let decoded = decode_snapshot(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            .inspect_err(|e| {
                tracing::error!(
                    node_id = ?self.node_id(),
                    last_log_id = ?snapshot_meta.last_log_id,
                    size_bytes,
                    elapsed_ms = start.elapsed().as_millis(),
                    error = %e,
                    outcome = "failed",
                    "failed to install a snapshot: invalid or unsupported snapshot data",
                );
            })?;
        let kv = KvState::restore(decoded.revision, decoded.entries, decoded.internal);

        // Update storage state
        {
            let mut cached = self.snapshot.lock().await;
            let mut storage = self.storage.lock().await;
            storage.persist(Persist::Snapshot(new_snapshot(snapshot_meta, &data))).await?;
            *cached = Some(new_snapshot(snapshot_meta, &data));
        }

        // The log positions are not touched here. openraft purges the log up to the snapshot
        // right after this, and that purge records both of them: `last_purged` directly, and
        // `last_log_id` through the invariant [`Self::save_meta`] keeps.

        // Update state machine state and restore the KV from the snapshot; then publish the
        // restored view.
        {
            let mut sm = self.sm_state.lock().await;
            sm.last_applied = snapshot_meta.last_log_id;
            sm.membership = snapshot_meta.last_membership.clone();
            let view = kv.view();
            sm.kv = kv;
            self.publisher.reset(view);
        }

        tracing::info!(
            node_id = ?self.node_id(),
            last_log_id = ?snapshot_meta.last_log_id,
            size_bytes,
            elapsed_ms = start.elapsed().as_millis(),
            outcome = "ok",
            "installed a snapshot",
        );

        Ok(())
    }

    /// Serving a snapshot never touches the storage, so a lagging follower being caught up does
    /// not queue behind an append.
    async fn get_current_snapshot(&mut self) -> Result<Option<EzSnapshot>, std::io::Error> {
        let cached = self.snapshot.lock().await;
        Ok(cached.as_ref().map(|snap| new_snapshot(&snap.meta, snap.snapshot.get_ref())))
    }
}

impl RaftSnapshotBuilder<OpenRaftTypes> for StateMachineStore {
    type SnapshotData = EzSnapshotData;

    async fn build_snapshot(&mut self) -> Result<EzSnapshot, std::io::Error> {
        let start = Instant::now();

        // Hold the lock only long enough to clone O(1) state: the persistent map's root
        // (`KvState::view`), the (today empty) internal state block, `last_applied` and the
        // membership. Encoding - O(n) over every key, and CPU work once the state is large -
        // happens after the lock is released, off this async worker thread: `apply` and `view()`
        // proceed at full speed while a snapshot is being encoded.
        let (view, internal, last_applied, last_membership) = {
            let sm = self.sm_state.lock().await;
            (
                sm.kv.view(),
                sm.kv.internal_state(),
                sm.last_applied,
                sm.membership.clone(),
            )
        };

        let snapshot_data = tokio::task::spawn_blocking(move || encode_snapshot(&view, &internal))
            .await
            .map_err(|e| std::io::Error::other(format!("snapshot encoding task panicked: {e}")))?
            .map_err(std::io::Error::other)?;

        let snapshot_meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
        };

        // Persist before returning: openraft purges logs covered by this snapshot right after,
        // and a durable purge point with no durable snapshot is an unrecoverable state.
        {
            let mut cached = self.snapshot.lock().await;
            let mut storage = self.storage.lock().await;
            storage.persist(Persist::Snapshot(new_snapshot(&snapshot_meta, &snapshot_data))).await?;
            *cached = Some(new_snapshot(&snapshot_meta, &snapshot_data));
        }

        tracing::info!(
            node_id = ?self.node_id(),
            last_log_id = ?last_applied,
            size_bytes = snapshot_data.len(),
            elapsed_ms = start.elapsed().as_millis(),
            outcome = "ok",
            "built a snapshot",
        );

        Ok(new_snapshot(&snapshot_meta, &snapshot_data))
    }
}

/// Build a [`EzSnapshot`] from its parts
fn new_snapshot(meta: &EzSnapshotMeta, data: &[u8]) -> EzSnapshot {
    Snapshot {
        meta: meta.clone(),
        snapshot: Cursor::new(data.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use bytes::Bytes;
    use openraft::LogId;
    use openraft::Membership;

    use super::*;
    use crate::kv::KeyValue;
    use crate::kv::Txn;
    use crate::storage::EzStorage;
    use crate::storage::Loaded;
    use crate::storage::adapter::open;

    /// A snapshot as it sits in storage: its metadata, and the bytes of the state
    type StoredSnapshot = Option<(EzSnapshotMeta, Vec<u8>)>;

    /// Storage that keeps the snapshot it was last asked to persist, and hands it back from
    /// `load()` - so a test can drive either `install_snapshot` directly or a fresh `open()` that
    /// restores from what was persisted, the same as a real restart would.
    #[derive(Clone, Default)]
    struct SnapshotStore {
        snapshot: Arc<StdMutex<StoredSnapshot>>,
    }

    #[async_trait::async_trait]
    impl EzStorage for SnapshotStore {
        async fn load(&mut self) -> Result<Loaded, std::io::Error> {
            let snapshot = self.snapshot.lock().unwrap().clone().map(|(meta, data)| new_snapshot(&meta, &data));
            Ok(Loaded {
                meta: crate::meta::EzMeta::default(),
                snapshot,
            })
        }

        async fn persist(&mut self, op: Persist) -> Result<(), std::io::Error> {
            if let Persist::Snapshot(snapshot) = op {
                *self.snapshot.lock().unwrap() = Some((snapshot.meta, snapshot.snapshot.into_inner()));
            }
            Ok(())
        }

        async fn read_logs(&mut self, _start: u64, _end: u64) -> Result<Vec<crate::entry::EzEntry>, std::io::Error> {
            unreachable!("these tests only install snapshots")
        }
    }

    fn snapshot_meta(index: u64) -> EzSnapshotMeta {
        let last_log_id = LogId::new_term_index(1, index);

        EzSnapshotMeta {
            last_log_id: Some(last_log_id),
            last_membership: StoredMembership::new(Some(last_log_id), Membership::default()),
        }
    }

    /// A valid v1 snapshot's bytes for `entries` at `revision`, built through the real
    /// [`encode_snapshot`] - the same function [`RaftSnapshotBuilder::build_snapshot`] calls -
    /// never hand-assembled, so these tests exercise the actual wire format.
    fn valid_snapshot_bytes(revision: u64, entries: &[(&str, &[u8], u64, u64)]) -> Vec<u8> {
        let entries = entries
            .iter()
            .map(|(key, value, create_rev, mod_rev)| KeyValue {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
                create_rev: *create_rev,
                mod_rev: *mod_rev,
            })
            .collect();
        let state = KvState::restore(revision, entries, Default::default());
        encode_snapshot(&state.view(), &state.internal_state()).unwrap()
    }

    /// Patch a valid snapshot's header to name a version this build does not understand -
    /// exactly the header layout `crate::kv::snapshot_codec` documents (4-byte magic, then this
    /// 2-byte little-endian version), never a private field of storage.
    fn with_unsupported_version(mut bytes: Vec<u8>) -> Vec<u8> {
        bytes[4..6].copy_from_slice(&0xFFFFu16.to_le_bytes());
        bytes
    }

    /// A snapshot whose bytes do not decode must be refused whole. Keeping it would outlive the
    /// failed transfer: startup restores from the stored snapshot, so the next start would fail
    /// on the same bytes - with the state they replaced already gone.
    #[tokio::test]
    async fn an_invalid_snapshot_leaves_the_last_valid_one_in_place() {
        let store = SnapshotStore::default();
        let persisted = store.snapshot.clone();
        let (_log, mut sm) = open(store).await.unwrap();

        let valid = valid_snapshot_bytes(1, &[("a", b"1", 1, 1)]);
        sm.install_snapshot(&snapshot_meta(10), Cursor::new(valid.clone())).await.unwrap();

        let err = sm
            .install_snapshot(&snapshot_meta(20), Cursor::new(b"not a valid ezraft snapshot".to_vec()))
            .await
            .unwrap_err();
        assert_eq!(std::io::ErrorKind::InvalidData, err.kind(), "unexpected error: {}", err);

        // Nothing of it is kept: not in storage, not in the cache that serves lagging followers,
        // not in the state machine.
        assert_eq!(Some((snapshot_meta(10), valid.clone())), *persisted.lock().unwrap());

        let cached = sm.get_current_snapshot().await.unwrap().map(|snap| (snap.meta, snap.snapshot.into_inner()));
        assert_eq!(Some((snapshot_meta(10), valid)), cached);

        assert_eq!(
            Some("1".as_bytes().to_vec()),
            sm.subscribe_view().current().get("a").map(|kv| kv.value.to_vec())
        );
    }

    /// An installed snapshot whose header names a version this build does not understand is
    /// rejected outright - the same invariant
    /// `an_invalid_snapshot_leaves_the_last_valid_one_in_place` proves for corrupt bytes, now
    /// for an explicitly unsupported version: the last good state stays in place, and the error
    /// names the version problem rather than misreporting it as generic corruption.
    #[tokio::test]
    async fn an_unknown_version_at_install_time_leaves_the_last_valid_one_in_place() {
        let store = SnapshotStore::default();
        let persisted = store.snapshot.clone();
        let (_log, mut sm) = open(store).await.unwrap();

        let valid = valid_snapshot_bytes(1, &[("a", b"1", 1, 1)]);
        sm.install_snapshot(&snapshot_meta(10), Cursor::new(valid.clone())).await.unwrap();

        let future_version = with_unsupported_version(valid_snapshot_bytes(2, &[("a", b"2", 1, 2)]));
        let err = sm.install_snapshot(&snapshot_meta(20), Cursor::new(future_version)).await.unwrap_err();
        assert_eq!(std::io::ErrorKind::InvalidData, err.kind(), "unexpected error: {}", err);
        assert!(
            err.to_string().contains("version"),
            "error must name the version problem explicitly, got: {err}"
        );

        assert_eq!(Some((snapshot_meta(10), valid.clone())), *persisted.lock().unwrap());
        let cached = sm.get_current_snapshot().await.unwrap().map(|snap| (snap.meta, snap.snapshot.into_inner()));
        assert_eq!(Some((snapshot_meta(10), valid)), cached);
        assert_eq!(
            Some("1".as_bytes().to_vec()),
            sm.subscribe_view().current().get("a").map(|kv| kv.value.to_vec())
        );
    }

    /// A node whose *stored* snapshot names a version this build does not understand must fail
    /// to start rather than silently misparse it - `open` is exactly the function
    /// `crate::raft::EzKv::new` calls before a node does anything else, so failing here is what
    /// makes `create`/`join` fail outright.
    #[tokio::test]
    async fn opening_storage_with_an_unknown_snapshot_version_fails_to_start() {
        let mut store = SnapshotStore::default();
        let future_version = with_unsupported_version(valid_snapshot_bytes(1, &[("a", b"1", 1, 1)]));
        store.persist(Persist::Snapshot(new_snapshot(&snapshot_meta(10), &future_version))).await.unwrap();

        let err = open(store).await.map(|_| ()).expect_err("an unknown snapshot version must refuse to open");
        assert_eq!(std::io::ErrorKind::InvalidData, err.kind(), "unexpected error: {}", err);
        assert!(
            err.to_string().contains("version"),
            "error must name the version problem explicitly, got: {err}"
        );
    }

    /// A snapshot round-trips the KV's revision and per-key revisions, not just its values -
    /// exactly what ech0-media#27b's restart and lagging-joiner paths depend on.
    #[tokio::test]
    async fn build_snapshot_then_install_it_reproduces_the_same_view() {
        let store = SnapshotStore::default();
        let (_log, mut sm) = open(store).await.unwrap();

        {
            let mut state = sm.sm_state.lock().await;
            state.kv.apply(&Txn::new().put("a", Bytes::from_static(b"1"))).unwrap();
            state.kv.apply(&Txn::new().put("b", Bytes::from_static(b"2"))).unwrap();
        }

        let built = sm.build_snapshot().await.unwrap();

        let mut installer = sm.clone();
        installer.install_snapshot(&built.meta, Cursor::new(built.snapshot.into_inner())).await.unwrap();

        let view = installer.subscribe_view().current();
        assert_eq!(2, view.revision());
        assert_eq!(Bytes::from_static(b"1"), view.get("a").unwrap().value);
        assert_eq!(Bytes::from_static(b"2"), view.get("b").unwrap().value);
    }
}
