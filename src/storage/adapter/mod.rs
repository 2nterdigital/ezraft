//! Internal storage adapter
//!
//! This module bridges the user's [`EzStorage`] trait to OpenRaft's
//! [`RaftLogStorage`](openraft::storage::RaftLogStorage) and
//! [`RaftStateMachine`](openraft::storage::RaftStateMachine) traits, and owns the Raft state
//! machine itself: the KV (`crate::kv`).
//!
//! Users don't interact with this module directly - it's used internally by
//! [`crate::raft::EzKv`].

#[cfg(test)]
mod conformance;
mod log;
mod state_machine;

use std::sync::Arc;

pub use log::LogStore;
pub use state_machine::StateMachineStore;
use tokio::sync::Mutex;

use crate::storage::EzStorage;
use crate::storage::Loaded;

/// The user's storage, reached by both stores
///
/// Shared only because [`EzStorage::persist`] takes `&mut self`, so every write serializes
/// through it whichever store makes it. **Take a store's own cache before this, never the
/// reverse, and never two caches at once** - that ordering is the whole reason these cannot
/// deadlock.
type SharedStorage = Arc<Mutex<Box<dyn EzStorage>>>;

/// Open the user's storage as the two stores openraft asks for
///
/// One [`EzStorage::load`], because there is one storage behind both: the metadata it returns
/// belongs to the log, the snapshot to the state machine.
pub async fn open(mut user_storage: impl EzStorage) -> Result<(LogStore, StateMachineStore), std::io::Error> {
    // Load initial metadata and snapshot
    let Loaded { meta, snapshot } = user_storage.load().await?;

    let storage: Box<dyn EzStorage> = Box::new(user_storage);
    let storage: SharedStorage = Arc::new(Mutex::new(storage));

    let log = LogStore::new(storage.clone(), meta);
    let sm = StateMachineStore::new(storage, snapshot)?;

    Ok((log, sm))
}
