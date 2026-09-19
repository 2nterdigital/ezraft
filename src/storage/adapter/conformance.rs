//! Storage conformance: openraft's own log-storage suite, run against [`RedbStorage`] through
//! the adapter
//!
//! This exercises the whole stack openraft sees - [`RedbStorage`] wrapped by [`LogStore`] and
//! [`StateMachineStore`] - not `RedbStorage` in isolation, so a defect the adapter introduces
//! would show up here even when `RedbStorage`'s own unit tests do not.

use async_trait::async_trait;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::StorageError;
use openraft::testing::log::StoreBuilder;
use openraft::testing::log::Suite;
use serde::Deserialize;
use serde::Serialize;
use tempfile::TempDir;

use crate::app::EzApp;
use crate::storage::RedbStorage;
use crate::storage::adapter::LogStore;
use crate::storage::adapter::StateMachineStore;
use crate::storage::adapter::open;
use crate::type_config::OpenRaftTypes;

/// A minimal application, just enough to satisfy the suite's bounds on `C = OpenRaftTypes<Self>`:
/// `C::D: Debug` (openraft's own `AppData` already requires `Request: Debug + Display`) and
/// `C::R: Debug`, i.e. `Option<Response>: Debug`, added here since `EzApp::Response` alone does
/// not require it.
#[derive(Debug, Default, Serialize, Deserialize)]
struct SuiteApp;

#[async_trait]
impl EzApp for SuiteApp {
    type Request = u64;
    type Response = u64;

    async fn apply(&mut self, req: u64) -> u64 {
        req
    }

    type ReadRequest = ();
    type ReadResponse = ();

    fn read(&self, _req: ()) {}
}

type C = OpenRaftTypes<SuiteApp>;

fn to_storage_error(e: std::io::Error) -> StorageError<C> {
    StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write, e)
}

/// Builds a fresh [`RedbStorage`] under its own temp directory, opened through the adapter, for
/// one suite test case; the returned [`TempDir`] is the suite's cleanup guard.
struct Builder;

impl StoreBuilder<C, LogStore<SuiteApp>, StateMachineStore<SuiteApp>, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, LogStore<SuiteApp>, StateMachineStore<SuiteApp>), StorageError<C>> {
        let dir = TempDir::new().map_err(to_storage_error)?;
        let storage = RedbStorage::open(dir.path().join("node.redb")).await.map_err(to_storage_error)?;
        let (log, sm) = open(storage, SuiteApp).await.map_err(to_storage_error)?;
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
