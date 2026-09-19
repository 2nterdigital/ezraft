//! The state machine half of the storage adapter: the KV, and its snapshot
//!
//! Owns the single [`Publisher`] DESIGN.md §5 calls for: every applied transaction publishes its
//! new view before answering the writer ([`RaftStateMachine::apply`]), and installing a snapshot
//! or finishing startup recovery publishes again ([`Self::new`],
//! [`RaftStateMachine::install_snapshot`]).

use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::Arc;

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
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::command::Command;
use crate::kv::KeyValue;
use crate::kv::KvState;
use crate::kv::Publisher;
use crate::kv::ViewReceiver;
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

    /// The single publisher DESIGN.md §5 calls for: every applied transaction, and installing a
    /// snapshot, publish through this one instance.
    publisher: Arc<Publisher>,

    /// A `view()` reader clone, handed out again by [`Self::subscribe_view`]; lock-free and
    /// never touches `sm_state`.
    view_rx: ViewReceiver,
}

impl Clone for StateMachineStore {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sm_state: self.sm_state.clone(),
            snapshot: self.snapshot.clone(),
            publisher: self.publisher.clone(),
            view_rx: self.view_rx.clone(),
        }
    }
}

/// The adapter's own tiny snapshot envelope: every entry plus the revision counter
///
/// The smallest encoding that lets a restart or a lagging joiner reconstruct identical KV state,
/// `revision`, `create_rev` and `mod_rev` included. ech0-media#28 replaces this with the
/// postcard v1 format (a magic, a version header, a lock-free build); this stays JSON like every
/// other Raft-adjacent type in this crate until then.
#[derive(Serialize, Deserialize)]
struct KvExport {
    revision: u64,
    entries: Vec<KeyValue>,
}

impl StateMachineStore {
    /// Restore the state machine from the snapshot the storage loaded, if there is one
    ///
    /// The snapshot data is restored here, not just its position: reporting `last_applied` at
    /// the snapshot makes openraft re-apply only the log tail after it, and skip installing this
    /// snapshot itself. The publisher this constructs already holds that restored view - the
    /// "publish after startup recovery" DESIGN.md §5 asks for falls out of the log tail's own
    /// applies, or, if there is no tail, out of this initial value.
    pub(super) fn new(storage: SharedStorage, snapshot: Option<EzSnapshot>) -> Result<Self, std::io::Error> {
        let (last_applied, membership, kv) = match &snapshot {
            Some(snap) => {
                let export: KvExport = serde_json::from_slice(snap.snapshot.get_ref())?;
                (
                    snap.meta.last_log_id,
                    snap.meta.last_membership.clone(),
                    KvState::restore(export.revision, export.entries),
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
        })
    }

    /// A clone of the reader every `view()` call reads from: lock-free, and never delayed by
    /// `apply` by more than a pointer swap.
    pub(crate) fn subscribe_view(&self) -> ViewReceiver {
        self.view_rx.clone()
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
                EntryPayload::Normal(Command::Txn(txn)) => {
                    // Publish before answering the writer (DESIGN.md §5): only a transaction
                    // that actually changed the view - i.e. did not conflict - has anything new
                    // to publish. The change group moves to the publisher; the writer hears only
                    // the revision it committed at.
                    let outcome = sm.kv.apply(&txn).map(|changes| {
                        let revision = changes.revision;
                        self.publisher.publish(sm.kv.view(), changes);
                        revision
                    });
                    Some(outcome)
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
        // Extract snapshot data
        let mut cursor = snapshot_data;
        cursor.seek(SeekFrom::Start(0))?;
        let mut data = Vec::new();
        cursor.read_to_end(&mut data)?;

        // Deserialized before anything keeps it. Bytes that do not parse - a corrupt transfer, a
        // peer of an incompatible version - are this transfer's problem, and persisting them would
        // make them every later start's problem too: [`Self::new`] reads the stored snapshot and
        // would fail on the same bytes, with the state they replaced already gone.
        let export: KvExport = serde_json::from_slice(&data)?;
        let kv = KvState::restore(export.revision, export.entries);

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
        // restored view (DESIGN.md §5).
        {
            let mut sm = self.sm_state.lock().await;
            sm.last_applied = snapshot_meta.last_log_id;
            sm.membership = snapshot_meta.last_membership.clone();
            let view = kv.view();
            sm.kv = kv;
            self.publisher.reset(view);
        }

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
        // Get current state machine state and build snapshot data
        let (last_applied, last_membership, snapshot_data) = {
            let sm = self.sm_state.lock().await;
            let (revision, entries) = sm.kv.export();
            let data = serde_json::to_vec(&KvExport { revision, entries })?;
            (sm.last_applied, sm.membership.clone(), data)
        };

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
    use crate::kv::Txn;
    use crate::storage::EzStorage;
    use crate::storage::Loaded;
    use crate::storage::adapter::open;

    /// A snapshot as it sits in storage: its metadata, and the bytes of the state
    type StoredSnapshot = Option<(EzSnapshotMeta, Vec<u8>)>;

    /// Storage that keeps the snapshot it was last asked to persist
    #[derive(Clone, Default)]
    struct SnapshotStore {
        snapshot: Arc<StdMutex<StoredSnapshot>>,
    }

    #[async_trait::async_trait]
    impl EzStorage for SnapshotStore {
        async fn load(&mut self) -> Result<Loaded, std::io::Error> {
            Ok(Loaded {
                meta: crate::meta::EzMeta::default(),
                snapshot: None,
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

    fn kv_export_bytes(revision: u64, entries: &[(&str, &[u8], u64, u64)]) -> Vec<u8> {
        let entries = entries
            .iter()
            .map(|(key, value, create_rev, mod_rev)| KeyValue {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
                create_rev: *create_rev,
                mod_rev: *mod_rev,
            })
            .collect();
        serde_json::to_vec(&KvExport { revision, entries }).unwrap()
    }

    /// A snapshot whose bytes do not deserialize must be refused whole. Keeping it would outlive
    /// the failed transfer: startup restores from the stored snapshot, so the next start would
    /// fail on the same bytes - with the state they replaced already gone.
    #[tokio::test]
    async fn an_invalid_snapshot_leaves_the_last_valid_one_in_place() {
        let store = SnapshotStore::default();
        let persisted = store.snapshot.clone();
        let (_log, mut sm) = open(store).await.unwrap();

        let valid = kv_export_bytes(1, &[("a", b"1", 1, 1)]);
        sm.install_snapshot(&snapshot_meta(10), Cursor::new(valid.clone())).await.unwrap();

        let err = sm.install_snapshot(&snapshot_meta(20), Cursor::new(b"not json".to_vec())).await.unwrap_err();
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
