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

use crate::kv::InternalState;
use crate::kv::KvState;
use crate::kv::Publisher;
use crate::kv::Registry;
use crate::kv::View;
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

    /// A registry watch receiver clone, handed out again by [`Self::subscribe_registry`]
    /// (ech0-media#33); lock-free and never touches `sm_state`.
    registry_rx: tokio::sync::watch::Receiver<Registry>,

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
            registry_rx: self.registry_rx.clone(),
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

        let (publisher, view_rx, registry_rx) = Publisher::new(kv.view(), kv.registry());

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
            registry_rx,
            node_id: Arc::new(OnceLock::new()),
        })
    }

    /// A clone of the reader every `view()` call reads from: lock-free, and never delayed by
    /// `apply` by more than a pointer swap.
    pub(crate) fn subscribe_view(&self) -> ViewReceiver {
        self.view_rx.clone()
    }

    /// A clone of the watch receiver every `EzKv::watch_registry()` call reads from
    /// (ech0-media#33): lock-free, and never delayed by `apply` by more than a pointer swap.
    pub(crate) fn subscribe_registry(&self) -> tokio::sync::watch::Receiver<Registry> {
        self.registry_rx.clone()
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
                // What a command means is `Command::apply`'s to decide, from the log and the
                // replicated state alone; this only publishes what it reports changed - before
                // answering the writer (ech0-media#16), so a write is already visible on this
                // node's own view and watches by the time its caller hears of it.
                EntryPayload::Normal(command) => {
                    let sm = &mut *sm;
                    let membership = sm.membership.membership();
                    let applied = command.apply(term, |id| membership.get_node(&id).is_some(), &mut sm.kv);

                    if let Some(changes) = applied.changes {
                        self.publisher.publish(sm.kv.view(), changes);
                    }
                    if let Some(registry) = applied.registry {
                        self.publisher.publish_registry(registry);
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
        // restored view and registry alike (ech0-media#33): an installed snapshot can change
        // either wholesale, just as it can the user's KV.
        {
            let mut sm = self.sm_state.lock().await;
            sm.last_applied = snapshot_meta.last_log_id;
            sm.membership = snapshot_meta.last_membership.clone();
            let view = kv.view();
            let registry = kv.registry();
            sm.kv = kv;
            self.publisher.reset(view);
            self.publisher.publish_registry(registry);
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

        // Encoding - O(n) over every key, and CPU work once the state is large - happens with no
        // lock held, off this async worker thread: `apply` and `view()` proceed at full speed
        // while a snapshot is being encoded.
        let SnapshotSource { view, internal, meta } = self.capture_snapshot_source().await;
        let data = tokio::task::spawn_blocking(move || encode_snapshot(&view, &internal))
            .await
            .map_err(|e| std::io::Error::other(format!("snapshot encoding task panicked: {e}")))?
            .map_err(std::io::Error::other)?;
        let size_bytes = data.len();

        let stored = self.store_built_snapshot(meta, data).await?;

        tracing::info!(
            node_id = ?self.node_id(),
            last_log_id = ?stored.meta.last_log_id,
            size_bytes,
            elapsed_ms = start.elapsed().as_millis(),
            outcome = "ok",
            "built a snapshot",
        );

        Ok(stored)
    }
}

/// What a snapshot is built from: the state machine as of one applied log position
struct SnapshotSource {
    view: View,
    internal: InternalState,
    meta: EzSnapshotMeta,
}

impl StateMachineStore {
    /// Capture what a snapshot is built from, holding the state machine lock only long enough
    /// to clone O(1) state: the persistent map's root ([`KvState::view`]), the internal state
    /// block, `last_applied` and the membership.
    async fn capture_snapshot_source(&self) -> SnapshotSource {
        let sm = self.sm_state.lock().await;
        SnapshotSource {
            view: sm.kv.view(),
            internal: sm.kv.internal_state(),
            meta: SnapshotMeta {
                last_log_id: sm.last_applied,
                last_membership: sm.membership.clone(),
            },
        }
    }

    /// Store a snapshot this node built, unless one at least as recent is already in place, and
    /// return whichever is in place afterwards
    ///
    /// openraft runs a build in a task of its own, concurrently with everything else the state
    /// machine does - including installing a snapshot the leader sent. That install can land
    /// between this build capturing its source and getting here, and it is always the newer of
    /// the two: a leader only sends a snapshot beyond what this node has committed. Storing the
    /// built one over it would leave storage holding a snapshot *behind* the point openraft
    /// already purged the log to on the install's behalf, and the next restart would restore
    /// that older state and silently skip the purged entries in between.
    ///
    /// Persisted before returning: openraft purges the log covered by a built snapshot right
    /// after, and a durable purge point with no durable snapshot is an unrecoverable state.
    async fn store_built_snapshot(&self, meta: EzSnapshotMeta, data: Vec<u8>) -> Result<EzSnapshot, std::io::Error> {
        let mut cached = self.snapshot.lock().await;

        if let Some(current) = cached.as_ref()
            && current.meta.last_log_id >= meta.last_log_id
        {
            tracing::info!(
                node_id = ?self.node_id(),
                built = ?meta.last_log_id,
                in_place = ?current.meta.last_log_id,
                "discarded a built snapshot: one at least as recent is already in place",
            );
            return Ok(new_snapshot(&current.meta, current.snapshot.get_ref()));
        }

        let mut storage = self.storage.lock().await;
        storage.persist(Persist::Snapshot(new_snapshot(&meta, &data))).await?;
        *cached = Some(new_snapshot(&meta, &data));

        Ok(new_snapshot(&meta, &data))
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
    use crate::command::Command;
    use crate::command::CommandBody;
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

    /// openraft builds a snapshot in a task of its own, so an install can land between the build
    /// capturing its source and storing its result. Run exactly that interleaving, one step at a
    /// time: the older, built snapshot must not replace the newer, installed one - openraft has
    /// already purged the log up to the installed one, so a restart from the older snapshot would
    /// silently skip every entry in between.
    #[tokio::test]
    async fn a_snapshot_built_from_older_state_never_replaces_a_newer_installed_one() {
        let store = SnapshotStore::default();
        let persisted = store.snapshot.clone();
        let (_log, mut sm) = open(store).await.unwrap();
        sm.install_snapshot(
            &snapshot_meta(10),
            Cursor::new(valid_snapshot_bytes(1, &[("a", b"1", 1, 1)])),
        )
        .await
        .unwrap();

        // The build captures the state machine at index 10 ...
        let SnapshotSource { view, internal, meta } = sm.capture_snapshot_source().await;
        let built = encode_snapshot(&view, &internal).unwrap();

        // ... the leader's snapshot at index 20 is installed while it is still encoding ...
        let installed = valid_snapshot_bytes(2, &[("a", b"1", 1, 1), ("b", b"2", 2, 2)]);
        sm.install_snapshot(&snapshot_meta(20), Cursor::new(installed.clone())).await.unwrap();

        // ... and only then does the build get to store its result.
        let in_place = sm.store_built_snapshot(meta, built).await.unwrap();

        assert_eq!(
            snapshot_meta(20),
            in_place.meta,
            "the build reports the snapshot that is in place"
        );
        assert_eq!(
            Some((snapshot_meta(20), installed.clone())),
            persisted.lock().unwrap().clone(),
            "storage still holds the installed snapshot",
        );
        let served = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(
            snapshot_meta(20),
            served.meta,
            "a lagging follower is still served the installed snapshot"
        );
        assert_eq!(installed, served.snapshot.into_inner());
    }

    /// The guard above must not get in the way of the ordinary case: a snapshot built from
    /// state beyond the one in place replaces it.
    #[tokio::test]
    async fn a_snapshot_built_from_newer_state_replaces_the_one_in_place() {
        let store = SnapshotStore::default();
        let persisted = store.snapshot.clone();
        let (_log, mut sm) = open(store).await.unwrap();
        sm.install_snapshot(
            &snapshot_meta(10),
            Cursor::new(valid_snapshot_bytes(1, &[("a", b"1", 1, 1)])),
        )
        .await
        .unwrap();

        let newer = valid_snapshot_bytes(2, &[("a", b"1", 1, 1), ("b", b"2", 2, 2)]);
        let in_place = sm.store_built_snapshot(snapshot_meta(11), newer.clone()).await.unwrap();

        assert_eq!(snapshot_meta(11), in_place.meta);
        assert_eq!(Some((snapshot_meta(11), newer)), persisted.lock().unwrap().clone());
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
    /// A command fenced to a term takes effect only in an entry appended in that same term. The
    /// second entry below is what a leader-only write looks like when its node lost leadership
    /// and won it back before the append: proposed for term 3, appended in term 4. Every replica
    /// must refuse it, from the entry alone, and leave no trace of it.
    #[tokio::test]
    async fn a_command_fenced_to_another_term_changes_nothing() {
        let store = SnapshotStore::default();
        let (_log, mut sm) = open(store).await.unwrap();

        let fenced = |entry_term: u64, index: u64, required_term: u64, key: &str| crate::entry::EzEntry {
            log_id: (entry_term, index),
            payload: EntryPayload::Normal(Command {
                request_id: crate::kv::RequestId::new(),
                ledger_capacity: 100,
                required_term: Some(required_term),
                body: CommandBody::Txn(Txn::new().put(key, Bytes::from_static(b"v"))),
            }),
        };

        let entries: Vec<Result<EntryResponder<OpenRaftTypes>, std::io::Error>> = vec![
            Ok((fenced(3, 1, 3, "kept"), None)),
            Ok((fenced(4, 2, 3, "refused"), None)),
        ];
        sm.apply(futures::stream::iter(entries)).await.unwrap();

        let view = sm.subscribe_view().current();
        assert_eq!(
            1,
            view.revision(),
            "only the entry appended in its own term advances the revision"
        );
        assert!(view.get("kept").is_some());
        assert_eq!(None, view.get("refused"));

        // Both entries were applied as far as Raft is concerned: refusing a command is an outcome,
        // not a failure to apply the entry.
        let (last_applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(Some(LogId::new_term_index(4, 2)), last_applied);
    }

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
