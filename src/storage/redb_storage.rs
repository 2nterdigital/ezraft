//! The production [`EzStorage`]: one redb database, three tables
//!
//! One file per node holds everything Raft asks storage to keep: a `log` table (index ->
//! entry), a `meta` table (one row: vote, node id, log positions) and a `snapshot` table (the
//! current snapshot's metadata and data, two rows replaced together). [`EzStorage`] stays the
//! extension point - this is the implementation to reach for first, not a sketch of one.

use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use redb::Database;
use redb::DatabaseError;
use redb::Durability;
use redb::ReadableDatabase;
use redb::TableDefinition;
use redb::TableError;

use crate::entry::EzEntry;
use crate::meta::EzMeta;
use crate::snapshot::EzSnapshot;
use crate::storage::EzStorage;
use crate::storage::Loaded;
use crate::storage::Persist;

/// Log entries: index -> the entry's `serde_json` bytes
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("log");

/// Raft metadata: one row, under [`META_KEY`]
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const META_KEY: &str = "meta";

/// The current snapshot: [`SNAPSHOT_META_KEY`] and [`SNAPSHOT_DATA_KEY`], always written
/// together so a reader never sees one half of a replacement
const SNAPSHOT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshot");
const SNAPSHOT_META_KEY: &str = "meta";
const SNAPSHOT_DATA_KEY: &str = "data";

/// Keeps one node's Raft state in a redb database
///
/// # Layout
///
/// One file, opened with [`Self::open`]; missing parent directories are created. Two nodes
/// must never share it - redb enforces this with an OS file lock held for as long as the
/// [`redb::Database`] stays open, so a second [`Self::open`] on the same path fails loudly
/// (`DatabaseAlreadyOpen`) instead of silently corrupting the first node's log.
///
/// # Durability
///
/// Every [`persist`] call is exactly one redb write transaction, committed with
/// [`Durability::Immediate`]: the call returns only once the transaction's data would survive a
/// power loss, which on macOS is a `F_FULLFSYNC`, and elsewhere whatever `fsync` the platform
/// gives redb. This is the same contract [`EzStorage`] asks of any implementation, at a real
/// cost: one `fsync` per persisted operation, so a batch of N appended entries plus the metadata
/// update that follows it is N+1 `fsync`s. Frequent small batches pay for this in latency; the
/// crate accepts it rather than batching commits across calls, which would let [`persist`]
/// return before its caller's data is actually durable.
///
/// [`Persist::DeleteLogs`] deletes its whole index range in one redb range operation - not a
/// read-then-delete over every key in the table - so truncating a long tail or compacting a long
/// prefix costs work proportional to what is removed, not to the log's size.
/// [`Persist::Snapshot`] writes the new snapshot's metadata and data in the same transaction that
/// [`persist`] uses for everything else, so a reader (including a restart's own [`Self::open`])
/// never observes a snapshot with one half replaced and the other still the old one.
///
/// # Blocking
///
/// redb is a synchronous library, and an `Immediate` commit blocks on disk I/O. Every operation
/// here runs on a [`tokio::task::spawn_blocking`] thread rather than an async worker thread,
/// which async code must never block.
///
/// # Encoding
///
/// [`EzMeta`], [`EzEntry`] and the snapshot's metadata are `serde_json`, the same encoding
/// [`crate::EzStorage`]'s other implementations use and robust to any `T::Request` an
/// application defines. Snapshot data is stored as the raw bytes the state machine already
/// serialized; this storage does not interpret them.
///
/// [`persist`]: EzStorage::persist
pub struct RedbStorage {
    db: Arc<Database>,
}

impl RedbStorage {
    /// How long [`Self::open`] tolerates a redb "database already open" conflict as transient
    /// before reporting it, and how often it re-checks within that budget
    ///
    /// A restart on the same path only ever races the *previous* `RedbStorage` for that path,
    /// never a genuinely different node: see [`Self::open`]'s docs for why that race exists and
    /// why it is always brief.
    const RESTART_LOCK_BUDGET: Duration = Duration::from_secs(2);
    const RESTART_LOCK_POLL: Duration = Duration::from_millis(5);

    /// Open (or create) the redb database at `path`, creating missing parent directories
    ///
    /// One file per node: see the type's own docs for what sharing one does.
    ///
    /// # Reopening after a restart in the same process
    ///
    /// openraft's own state machine worker owns the last handle to a node's storage and drops
    /// it - releasing redb's file lock - only *after* [`crate::EzKv::shutdown`] returns,
    /// asynchronously, once that worker task is next scheduled. A caller that shuts a node down,
    /// drops it and immediately reopens the same path in the same process (exactly what an
    /// in-process restart test does; a real process restart never hits this, since exiting the
    /// old process releases the OS file lock unconditionally) can therefore see a brief
    /// `DatabaseAlreadyOpen` even though the old storage is on its way out. `open` tolerates
    /// that specific conflict for a couple of seconds, polling every few milliseconds, and
    /// reports it once that budget runs out - a bounded wait on a known transient condition, not
    /// a fixed sleep hiding a real one.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self, io::Error> {
        let path = path.into();
        let deadline = tokio::time::Instant::now() + Self::RESTART_LOCK_BUDGET;

        loop {
            let attempt_path = path.clone();
            let attempt = tokio::task::spawn_blocking(move || {
                if let Some(parent) = attempt_path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
                    std::fs::create_dir_all(parent).map_err(DatabaseError::from)?;
                }
                Database::create(&attempt_path)
            })
            .await;

            let result = match attempt {
                Ok(result) => result,
                Err(join_err) => return Err(wrap("redb worker thread", join_err)),
            };

            match result {
                Ok(db) => return Ok(Self { db: Arc::new(db) }),
                Err(DatabaseError::DatabaseAlreadyOpen) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Self::RESTART_LOCK_POLL).await;
                }
                Err(e) => return Err(wrap("open the database", e)),
            }
        }
    }
}

#[async_trait]
impl EzStorage for RedbStorage {
    async fn load(&mut self) -> Result<Loaded, io::Error> {
        let db = self.db.clone();
        blocking(move || load_blocking(&db)).await
    }

    async fn persist(&mut self, op: Persist) -> Result<(), io::Error> {
        let db = self.db.clone();
        blocking(move || persist_blocking(&db, op)).await
    }

    async fn read_logs(&mut self, start: u64, end: u64) -> Result<Vec<EzEntry>, io::Error> {
        let db = self.db.clone();
        blocking(move || read_logs_blocking(&db, start, end)).await
    }
}

/// Run a blocking redb operation on a blocking-pool thread
///
/// Never run this inline on an async worker thread: an `Immediate` commit fsyncs, and
/// `Database::create`/`open` do their own file I/O too.
async fn blocking<F, R>(f: F) -> Result<R, io::Error>
where
    F: FnOnce() -> Result<R, io::Error> + Send + 'static,
    R: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(join_err) => Err(wrap("redb worker thread", join_err)),
    }
}

/// Wrap a redb (or worker-thread) error with the operation it happened during
fn wrap(context: &str, err: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("redb: {context}: {err}"))
}

/// The error [`EzStorage::read_logs`] reports for an index its caller expected to find
fn missing_log_entry(index: u64) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("missing log entry {index}"))
}

/// [`EzStorage::load`], off the async runtime
fn load_blocking(db: &Database) -> Result<Loaded, io::Error> {
    let txn = db.begin_read().map_err(|e| wrap("begin a read transaction", e))?;

    let meta = match txn.open_table(META_TABLE) {
        Ok(table) => match table.get(META_KEY).map_err(|e| wrap("read the meta table", e))? {
            Some(bytes) => serde_json::from_slice(bytes.value())?,
            None => EzMeta::default(),
        },
        Err(TableError::TableDoesNotExist(_)) => EzMeta::default(),
        Err(e) => return Err(wrap("open the meta table", e)),
    };

    let snapshot = match txn.open_table(SNAPSHOT_TABLE) {
        Ok(table) => {
            let meta_bytes = table.get(SNAPSHOT_META_KEY).map_err(|e| wrap("read the snapshot table", e))?;
            let data_bytes = table.get(SNAPSHOT_DATA_KEY).map_err(|e| wrap("read the snapshot table", e))?;

            match (meta_bytes, data_bytes) {
                (Some(meta_bytes), Some(data_bytes)) => {
                    let meta = serde_json::from_slice(meta_bytes.value())?;
                    Some(EzSnapshot {
                        meta,
                        snapshot: Cursor::new(data_bytes.value().to_vec()),
                    })
                }
                // Half a snapshot never happens - both rows are written in the same transaction
                // - but an absent table (nothing ever persisted) reads as no snapshot rather
                // than an error.
                _ => None,
            }
        }
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(e) => return Err(wrap("open the snapshot table", e)),
    };

    Ok(Loaded { meta, snapshot })
}

/// [`EzStorage::persist`], off the async runtime
fn persist_blocking(db: &Database, op: Persist) -> Result<(), io::Error> {
    let mut txn = db.begin_write().map_err(|e| wrap("begin a write transaction", e))?;

    // Already redb's default for a fresh transaction; set explicitly so this durability
    // contract does not depend on that default staying unchanged underneath it.
    txn.set_durability(Durability::Immediate).map_err(|e| wrap("set durability", e))?;

    match op {
        Persist::Meta(meta) => {
            let bytes = serde_json::to_vec(&meta)?;
            let mut table = txn.open_table(META_TABLE).map_err(|e| wrap("open the meta table", e))?;
            table.insert(META_KEY, bytes.as_slice()).map_err(|e| wrap("write meta", e))?;
        }
        Persist::LogEntry(entry) => {
            let (_, index) = entry.log_id;
            let bytes = serde_json::to_vec(&entry)?;
            let mut table = txn.open_table(LOG_TABLE).map_err(|e| wrap("open the log table", e))?;
            table.insert(index, bytes.as_slice()).map_err(|e| wrap("write a log entry", e))?;
        }
        Persist::Snapshot(snapshot) => {
            let meta_bytes = serde_json::to_vec(&snapshot.meta)?;
            let data = snapshot.snapshot.into_inner();

            // Both rows land in this one write transaction, so `load` (and a lagging follower
            // served from storage) never sees new metadata paired with old data or the reverse.
            let mut table = txn.open_table(SNAPSHOT_TABLE).map_err(|e| wrap("open the snapshot table", e))?;
            table.insert(SNAPSHOT_META_KEY, meta_bytes.as_slice()).map_err(|e| wrap("write snapshot meta", e))?;
            table.insert(SNAPSHOT_DATA_KEY, data.as_slice()).map_err(|e| wrap("write snapshot data", e))?;
        }
        Persist::DeleteLogs { from, to } => {
            let mut table = txn.open_table(LOG_TABLE).map_err(|e| wrap("open the log table", e))?;

            // A range operation, not a read-then-delete over every key in the table: redb walks
            // only the `[from, to)` region of the tree. Both shapes DeleteLogs arrives in go
            // through here - a tail (`from..u64::MAX`) and a head (`0..n`) - since redb's range
            // delete handles either the same way.
            table.retain_in(from..to, |_, _| false).map_err(|e| wrap("delete a log range", e))?;
        }
    }

    txn.commit().map_err(|e| wrap("commit the write transaction", e))
}

/// [`EzStorage::read_logs`], off the async runtime
fn read_logs_blocking(db: &Database, start: u64, end: u64) -> Result<Vec<EzEntry>, io::Error> {
    if start >= end {
        return Ok(Vec::new());
    }

    let txn = db.begin_read().map_err(|e| wrap("begin a read transaction", e))?;
    let table = match txn.open_table(LOG_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Err(missing_log_entry(start)),
        Err(e) => return Err(wrap("open the log table", e)),
    };

    // One range scan rather than `end - start` individual lookups, and every index in the
    // range must be there in order: a gap must be reported, not returned as a shorter log that
    // Raft would read as the end of the entries.
    // Capped: the range is the caller's to choose, and a reservation sized by an open-ended one
    // (`0..u64::MAX`) would abort the process before a single row was read.
    let mut logs = Vec::with_capacity(usize::try_from(end - start).unwrap_or(usize::MAX).min(4096));
    let mut next = start;
    for row in table.range(start..end).map_err(|e| wrap("scan the log table", e))? {
        let (index, bytes) = row.map_err(|e| wrap("read a log entry", e))?;
        if index.value() != next {
            return Err(missing_log_entry(next));
        }
        logs.push(serde_json::from_slice(bytes.value())?);
        next += 1;
    }

    if next != end {
        return Err(missing_log_entry(next));
    }

    Ok(logs)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::Membership;
    use openraft::StoredMembership;
    use tempfile::tempdir;

    use super::*;
    use crate::command::Command;
    use crate::kv::Txn;
    use crate::snapshot::EzSnapshotMeta;
    use crate::type_config::EzVote;

    /// A log entry carrying a small, distinguishable `Txn`: a single `Put` whose value encodes
    /// `n`, so entries built with different `n` compare unequal and are easy to tell apart.
    fn log_entry(term: u64, index: u64, n: u64) -> EzEntry {
        EzEntry {
            log_id: (term, index),
            payload: EntryPayload::Normal(Command::Txn(Txn::new().put("k", Bytes::from(n.to_le_bytes().to_vec())))),
        }
    }

    /// The pair a caller compares an entry against: its log id and the numeric marker `log_entry`
    /// encoded into it.
    fn parts(entry: &EzEntry) -> ((u64, u64), u64) {
        let EntryPayload::Normal(Command::Txn(txn)) = &entry.payload else {
            panic!("expected a Txn payload, got {:?}", entry.payload);
        };
        let [crate::kv::Op::Put { value, .. }] = txn.ops.as_slice() else {
            panic!("expected exactly one Put op, got {:?}", txn.ops);
        };
        let n = u64::from_le_bytes(value.as_ref().try_into().expect("8-byte marker"));
        (entry.log_id, n)
    }

    fn snapshot(index: u64, data: &[u8]) -> EzSnapshot {
        let last_log_id = LogId::new_term_index(1, index);

        EzSnapshot {
            meta: EzSnapshotMeta {
                last_log_id: Some(last_log_id),
                last_membership: StoredMembership::new(Some(last_log_id), Membership::default()),
            },
            snapshot: Cursor::new(data.to_vec()),
        }
    }

    #[tokio::test]
    async fn a_fresh_database_loads_default_meta_and_no_snapshot() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::open(dir.path().join("node.redb")).await.unwrap();

        let loaded = EzStorage::load(&mut storage).await.unwrap();
        assert_eq!(EzMeta::default(), loaded.meta);
        assert!(loaded.snapshot.is_none());
    }

    #[tokio::test]
    async fn meta_round_trips_through_persist_and_load() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::open(dir.path().join("node.redb")).await.unwrap();

        let meta = EzMeta {
            node_id: Some(7),
            vote: Some(EzVote::new(3, 7)),
            last_log_id: Some((3, 42)),
            last_purged: None,
        };
        EzStorage::persist(&mut storage, Persist::Meta(meta.clone())).await.unwrap();

        let loaded = EzStorage::load(&mut storage).await.unwrap();
        assert_eq!(meta, loaded.meta);
    }

    #[tokio::test]
    async fn log_entries_round_trip_in_index_order() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::open(dir.path().join("node.redb")).await.unwrap();

        for index in 1..=4 {
            storage.persist(Persist::LogEntry(log_entry(7, index, index * 10))).await.unwrap();
        }

        let all = EzStorage::read_logs(&mut storage, 1, 5).await.unwrap();
        let read_back: Vec<((u64, u64), u64)> = all.iter().map(parts).collect();
        assert_eq!(vec![((7, 1), 10), ((7, 2), 20), ((7, 3), 30), ((7, 4), 40)], read_back);
    }

    #[tokio::test]
    async fn read_logs_errors_on_a_gap() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::open(dir.path().join("node.redb")).await.unwrap();

        storage.persist(Persist::LogEntry(log_entry(1, 1, 10))).await.unwrap();
        storage.persist(Persist::LogEntry(log_entry(1, 3, 30))).await.unwrap();

        let err = EzStorage::read_logs(&mut storage, 1, 4).await.expect_err("index 2 is missing");
        assert_eq!(io::ErrorKind::NotFound, err.kind());
    }

    /// Both shapes `DeleteLogs` arrives in: a tail truncation and a head compaction, each taking
    /// only its own range.
    #[tokio::test]
    async fn delete_logs_removes_only_its_range_as_a_tail_and_as_a_head() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::open(dir.path().join("node.redb")).await.unwrap();

        for index in 1..=5 {
            storage.persist(Persist::LogEntry(log_entry(1, index, index))).await.unwrap();
        }

        // Tail: drop everything from index 4 on, as a conflicting suffix would be truncated.
        let tail: Persist = Persist::DeleteLogs { from: 4, to: u64::MAX };
        storage.persist(tail).await.unwrap();
        let kept = EzStorage::read_logs(&mut storage, 1, 4).await.unwrap();
        assert_eq!(3, kept.len());
        assert!(EzStorage::read_logs(&mut storage, 4, 5).await.is_err());

        // Head: purge everything up to and including index 2, as a snapshot would compact it.
        let head: Persist = Persist::DeleteLogs { from: 0, to: 3 };
        storage.persist(head).await.unwrap();
        assert!(EzStorage::read_logs(&mut storage, 1, 2).await.is_err());
        let survivor = EzStorage::read_logs(&mut storage, 3, 4).await.unwrap();
        assert_eq!(1, survivor.len());
    }

    #[tokio::test]
    async fn a_new_snapshot_replaces_the_old_ones_meta_and_data_together() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::open(dir.path().join("node.redb")).await.unwrap();

        let first: Persist = Persist::Snapshot(snapshot(10, b"first"));
        let second: Persist = Persist::Snapshot(snapshot(20, b"second"));
        storage.persist(first).await.unwrap();
        storage.persist(second).await.unwrap();

        let loaded = EzStorage::load(&mut storage).await.unwrap();
        let loaded_snapshot = loaded.snapshot.expect("a snapshot was persisted");
        assert_eq!(snapshot(20, b"second").meta, loaded_snapshot.meta);
        assert_eq!(b"second".to_vec(), loaded_snapshot.snapshot.into_inner());
    }

    #[tokio::test]
    async fn a_missing_directory_chain_is_created() {
        let root = tempdir().unwrap();
        let path = root.path().join("cluster").join("node-1").join("node.redb");

        let mut storage = RedbStorage::open(&path).await.unwrap();
        storage.persist(Persist::LogEntry(log_entry(1, 1, 5))).await.unwrap();

        assert!(path.exists());
        let entries = EzStorage::read_logs(&mut storage, 1, 2).await.unwrap();
        assert_eq!(vec![((1, 1), 5)], entries.iter().map(parts).collect::<Vec<_>>());
    }

    /// The property the storage decision rests on: a node that closes its database and comes
    /// back finds everything it persisted, through a real close and reopen of the file - never
    /// a handle shared in memory.
    #[tokio::test]
    async fn state_survives_closing_and_reopening_the_same_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("node.redb");

        {
            let mut storage = RedbStorage::open(&path).await.unwrap();
            let meta: Persist = Persist::Meta(EzMeta {
                node_id: Some(3),
                vote: Some(EzVote::new(2, 3)),
                last_log_id: Some((2, 5)),
                last_purged: None,
            });
            storage.persist(meta).await.unwrap();
            for index in 1..=5 {
                storage.persist(Persist::LogEntry(log_entry(2, index, index))).await.unwrap();
            }
            let snap: Persist = Persist::Snapshot(snapshot(3, b"snap"));
            storage.persist(snap).await.unwrap();
            // `storage`, and the redb `Database` it owns, is dropped here - releasing the file.
        }

        let mut reopened = RedbStorage::open(&path).await.unwrap();
        let loaded = EzStorage::load(&mut reopened).await.unwrap();
        assert_eq!(Some(3), loaded.meta.node_id);
        assert_eq!(Some((2, 5)), loaded.meta.last_log_id);
        let snap = loaded.snapshot.expect("the snapshot survived the reopen");
        assert_eq!(b"snap".to_vec(), snap.snapshot.into_inner());

        let entries = EzStorage::read_logs(&mut reopened, 1, 6).await.unwrap();
        assert_eq!(5, entries.len());
    }
}
