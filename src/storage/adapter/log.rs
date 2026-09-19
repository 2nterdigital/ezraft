//! The log half of the storage adapter: entries, and the metadata that bounds them

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::alias::LogIdOf;
use openraft::log_id::LogIndexOptionExt;
use openraft::log_id::RaftLogId;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use tokio::sync::Mutex;

use crate::meta::EzMeta;
use crate::storage::EzStorage;
use crate::storage::Persist;
use crate::storage::adapter::SharedStorage;
use crate::type_config::OpenRaftTypes;

/// The log: entries, and the metadata that bounds them
///
/// Only metadata is cached in memory - entries are read from user storage on demand.
pub struct LogStore {
    storage: SharedStorage,

    /// Raft metadata: the vote, and the positions bounding the log
    meta: Arc<Mutex<EzMeta>>,
}

// Both stores are handles: cloning one shares its state.
impl Clone for LogStore {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            meta: self.meta.clone(),
        }
    }
}

impl LogStore {
    pub(super) fn new(storage: SharedStorage, meta: EzMeta) -> Self {
        Self {
            storage,
            meta: Arc::new(Mutex::new(meta)),
        }
    }

    /// Update metadata and persist to storage
    pub async fn save_meta(&self, f: impl FnOnce(&mut EzMeta)) -> Result<(), std::io::Error> {
        let mut meta = self.meta.lock().await;
        f(&mut meta);

        let mut storage = self.storage.lock().await;
        persist_meta(&mut meta, &mut **storage).await
    }

    /// Get the current node_id from cached metadata
    pub async fn node_id(&self) -> Option<u64> {
        self.meta.lock().await.node_id
    }
}

/// Write metadata out, with the log positions made consistent first
///
/// openraft reads both positions at startup and rejects `last_log_id < last_purged` as a corrupt
/// store. Installing a snapshot is what inverts them - the purge that follows moves the purge
/// point past a log that stopped short of the snapshot - so the repair belongs on the one path
/// every metadata write takes, not at the call sites.
///
/// Takes the two guards rather than the adapter, because most callers already hold both and the
/// mutex is not reentrant.
async fn persist_meta(meta: &mut EzMeta, storage: &mut dyn EzStorage) -> Result<(), std::io::Error> {
    meta.last_log_id = meta.last_log_id.max(meta.last_purged);

    storage.persist(Persist::Meta(meta.clone())).await
}

impl RaftLogStorage<OpenRaftTypes> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<OpenRaftTypes>, std::io::Error> {
        let meta = self.meta.lock().await;
        let last = meta.last_log_id.map(|(t, i)| LogId::new_term_index(t, i));
        let last_purged = meta.last_purged.map(|(t, i)| LogId::new_term_index(t, i));

        Ok(LogState {
            last_log_id: last,
            last_purged_log_id: last_purged,
        })
    }

    async fn save_vote(&mut self, vote: &<OpenRaftTypes as RaftTypeConfig>::Vote) -> Result<(), std::io::Error> {
        self.save_meta(|m| m.vote = Some(*vote)).await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<OpenRaftTypes>) -> Result<(), std::io::Error>
    where
        I: IntoIterator<Item = <OpenRaftTypes as RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // Both locks for the whole batch and the metadata that describes it, so no reader can
        // observe a log that reaches past the last_log_id recorded for it.
        let res = async {
            let mut meta = self.meta.lock().await;
            let mut storage = self.storage.lock().await;

            let mut last_log_id = None;

            // Save all log entries
            for entry in entries {
                last_log_id = Some(entry.log_id);
                storage.persist(Persist::LogEntry(entry)).await?;
            }

            // Update metadata once with the last entry's log_id
            if let Some(log_id) = last_log_id {
                meta.last_log_id = Some(log_id);
                persist_meta(&mut meta, &mut **storage).await?;
            }

            Ok::<_, std::io::Error>(())
        }
        .await;

        // openraft is waiting on this callback; returning the error alone would leave the append
        // in flight forever.
        match res {
            Ok(()) => {
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.io_completed(Err(std::io::Error::other(e.to_string())));
                Err(e)
            }
        }
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<OpenRaftTypes>>) -> Result<(), std::io::Error> {
        let from = last_log_id.map(|id| id.index).next_index();

        // Both held across the position and the delete it bounds, so no reader sees the log
        // shrink out from under the last_log_id recorded for it.
        let mut meta = self.meta.lock().await;
        let mut storage = self.storage.lock().await;

        // A caller may ask to truncate after a point the log has not reached yet - there is
        // nothing there to remove. Recording that higher position anyway would advance
        // `last_log_id` past what is actually stored, which the next read would then fail to
        // find: truncation only ever lowers `last_log_id`, matching what `DeleteLogs` removes,
        // so it is skipped once the log already ends at or before the requested position.
        let nothing_to_truncate = meta.last_log_id.is_some_and(|(_, index)| from > index);

        if !nothing_to_truncate {
            // The position first. A crash between the two then leaves entries past the recorded
            // `last_log_id`, which no read reaches and the next append overwrites; deleting
            // first would leave metadata promising entries that are already gone.
            meta.last_log_id = last_log_id.map(|id| id.to_type());
            persist_meta(&mut meta, &mut **storage).await?;
        }

        storage.persist(Persist::DeleteLogs { from, to: u64::MAX }).await
    }

    async fn purge(&mut self, log_id: LogIdOf<OpenRaftTypes>) -> Result<(), std::io::Error> {
        let mut meta = self.meta.lock().await;
        let mut storage = self.storage.lock().await;

        // The position first, as in `truncate_after`: entries below the recorded `last_purged`
        // are already unreachable, whereas a purge point lagging the delete points reads at
        // entries that are gone.
        meta.last_purged = Some(log_id.to_type());
        persist_meta(&mut meta, &mut **storage).await?;

        storage
            .persist(Persist::DeleteLogs {
                from: 0,
                to: log_id.index + 1,
            })
            .await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

impl RaftLogReader<OpenRaftTypes> for LogStore {
    async fn read_vote(&mut self) -> Result<Option<<OpenRaftTypes as RaftTypeConfig>::Vote>, std::io::Error> {
        Ok(self.meta.lock().await.vote)
    }

    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<OpenRaftTypes as RaftTypeConfig>::Entry>, std::io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        // Both held until the entries have been read: a purge landing in between would leave the
        // clamped range pointing at entries user storage has already deleted.
        let meta = self.meta.lock().await;
        let mut storage = self.storage.lock().await;

        // Available log range: [lo, hi)
        let lo = meta.last_purged.map(|(_, i)| i).next_index();
        let hi = meta.last_log_id.map(|(_, i)| i).next_index();

        let start = match range.start_bound() {
            std::ops::Bound::Included(&x) => x,
            std::ops::Bound::Excluded(&x) => x + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&x) => x + 1,
            std::ops::Bound::Excluded(&x) => x,
            std::ops::Bound::Unbounded => hi,
        };

        // Clamp to available range
        let start = std::cmp::max(start, lo);
        let end = std::cmp::min(end, hi);

        if start >= end {
            return Ok(Vec::new());
        }

        // The common case: the clamped range is exactly what the log has, in one read.
        match storage.read_logs(start, end).await {
            Ok(entries) => Ok(entries),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                match find_present_start(&mut **storage, start, end).await? {
                    Some(entries) => Ok(entries),
                    // No leading gap to trim: the failure is a hole after real entries, which
                    // must stay an error rather than be silently narrowed away.
                    None => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }
}

/// Find where log entries actually begin inside `[start, end)`, after a read of the whole range
/// reported one missing.
///
/// `last_purged` only tracks purges this adapter itself performed; a direct
/// [`RaftLogStorage::append`] call - openraft's own storage-conformance suite does this on
/// purpose, to probe exactly this boundary - can start a fresh log above `last_purged` without
/// ever purging what came before. [`EzStorage::read_logs`] then reports the untouched entries
/// below it as missing, which [`RaftLogReader::try_get_log_entries`] must tolerate at the front
/// of the range rather than fail on.
///
/// Probes index by index from `start`, and returns `None` - the original error is a real
/// "hole after real data" - as soon as the very first probe already finds `start` itself
/// present: `read_logs(start, end)` would not have failed on a leading gap that is zero wide.
/// Otherwise returns `Ok(Some(entries))` for the narrower `[first_present, end)` once the first
/// present index is found, so the caller propagates one coherent result rather than assembling
/// one itself.
async fn find_present_start(
    storage: &mut dyn EzStorage,
    start: u64,
    end: u64,
) -> Result<Option<Vec<<OpenRaftTypes as RaftTypeConfig>::Entry>>, std::io::Error> {
    let mut probe = start;

    loop {
        if probe >= end {
            return Ok(None);
        }

        match storage.read_logs(probe, probe + 1).await {
            Ok(found) if !found.is_empty() => break,
            Ok(_) => probe += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => probe += 1,
            Err(e) => return Err(e),
        }
    }

    if probe == start {
        // The full-range read failed on something other than a leading gap - a hole after real
        // entries - which must stay an error, not be silently trimmed away.
        return Ok(None);
    }

    storage.read_logs(probe, end).await.map(Some)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use openraft::LogId;

    use super::*;
    use crate::entry::EzEntry;
    use crate::storage::Loaded;
    use crate::storage::adapter::open;

    /// Storage that records what it was asked to write, in the order it was asked
    #[derive(Clone, Default)]
    struct Recorder {
        ops: Arc<StdMutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl EzStorage for Recorder {
        async fn load(&mut self) -> Result<Loaded, std::io::Error> {
            Ok(Loaded {
                meta: EzMeta::default(),
                snapshot: None,
            })
        }

        async fn persist(&mut self, op: Persist) -> Result<(), std::io::Error> {
            self.ops.lock().unwrap().push(op.to_string());
            Ok(())
        }

        async fn read_logs(&mut self, _start: u64, _end: u64) -> Result<Vec<EzEntry>, std::io::Error> {
            unreachable!("these tests only write")
        }
    }

    async fn recording_log_store() -> (LogStore, Arc<StdMutex<Vec<String>>>) {
        let recorder = Recorder::default();
        let ops = recorder.ops.clone();
        let (log, _sm) = open(recorder).await.unwrap();
        (log, ops)
    }

    /// A crash after the delete but before the metadata would leave a purge point that reads at
    /// entries the delete already removed.
    #[tokio::test]
    async fn purge_records_the_purge_point_before_deleting() {
        let (mut log, ops) = recording_log_store().await;

        log.purge(LogId::new_term_index(1, 9)).await.unwrap();

        assert_eq!(["Meta", "DeleteLogs(0..10)"], ops.lock().unwrap()[..]);
    }

    /// Same the other way round: metadata promising a log that the delete already truncated.
    #[tokio::test]
    async fn truncate_records_the_new_end_before_deleting() {
        let (mut log, ops) = recording_log_store().await;

        log.truncate_after(Some(LogId::new_term_index(1, 4))).await.unwrap();

        assert_eq!(
            ["Meta".to_string(), format!("DeleteLogs(5..{})", u64::MAX)],
            ops.lock().unwrap()[..]
        );
    }
}
