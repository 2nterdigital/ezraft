//! Storage conformance: openraft's own log-storage suite, run against [`RedbStorage`] through
//! the adapter
//!
//! This exercises the whole stack openraft sees - [`RedbStorage`] wrapped by [`LogStore`] and
//! [`StateMachineStore`] - not `RedbStorage` in isolation, so a defect the adapter introduces
//! would show up here even when `RedbStorage`'s own unit tests do not.

use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::StorageError;
use openraft::testing::log::StoreBuilder;
use openraft::testing::log::Suite;
use tempfile::TempDir;

use crate::storage::RedbStorage;
use crate::storage::adapter::LogStore;
use crate::storage::adapter::StateMachineStore;
use crate::storage::adapter::open;
use crate::type_config::OpenRaftTypes;

type C = OpenRaftTypes;

fn to_storage_error(e: std::io::Error) -> StorageError<C> {
    StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write, e)
}

/// Builds a fresh [`RedbStorage`] under its own temp directory, opened through the adapter, for
/// one suite test case; the returned [`TempDir`] is the suite's cleanup guard.
struct Builder;

impl StoreBuilder<C, LogStore, StateMachineStore, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, LogStore, StateMachineStore), StorageError<C>> {
        let dir = TempDir::new().map_err(to_storage_error)?;
        let storage = RedbStorage::open(dir.path().join("node.redb")).await.map_err(to_storage_error)?;
        let (log, sm) = open(storage).await.map_err(to_storage_error)?;
        Ok((dir, log, sm))
    }
}

/// `RedbStorage`, reached the way openraft itself reaches it - through [`LogStore`] and
/// [`StateMachineStore`] - passes openraft alpha.33's own log-storage consistency suite.
// `StorageError<C>` is openraft's own error type for this exact seam; boxing it here would only
// add indirection to a test that never runs on a hot path.
#[allow(clippy::result_large_err)]
#[tokio::test]
async fn redb_storage_passes_the_openraft_log_storage_suite() -> Result<(), StorageError<C>> {
    Suite::test_all(Builder).await
}
