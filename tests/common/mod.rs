//! The harness these cluster tests share: a KV application, storage that survives a
//! node instance, and the waits that make cluster state assertable.
//!
//! Each file under `tests/` is its own binary and uses a subset of this, so unused
//! items here are expected rather than dead.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use ezraft::EzApp;
use ezraft::EzConfig;
use ezraft::EzRaft;
use ezraft::RedbStorage;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, derive_more::Display)]
pub enum Request {
    #[display("Set({key})")]
    Set { key: String, value: String },
    #[display("Get({key})")]
    Get { key: String },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub value: Option<String>,
}

pub fn set(key: &str, value: &str) -> Request {
    Request::Set {
        key: key.into(),
        value: value.into(),
    }
}

pub fn get(key: &str) -> Request {
    Request::Get { key: key.into() }
}

/// KV app; the framework owns the value, so tests read the applied state
/// back through `EzRaft::read` to compare the whole state instead of
/// sampling it key by key.
#[derive(Default, Serialize, Deserialize)]
pub struct KvSm {
    pub data: BTreeMap<String, String>,

    /// Which bucket of `APPLIED` this state records its applies in, empty unless a test asked
    /// for them. Part of the serialized state, so it survives a snapshot restore and a restart
    /// and replayed applies land in the same bucket as the originals.
    #[serde(default)]
    tag: String,
}

/// Every `apply` this process ran, in order, per tag
static APPLIED: Mutex<BTreeMap<String, Vec<String>>> = Mutex::new(BTreeMap::new());

pub fn tagged(tag: &str) -> KvSm {
    KvSm {
        tag: tag.into(),
        ..KvSm::default()
    }
}

pub fn applied(tag: &str) -> Vec<String> {
    APPLIED.lock().unwrap().get(tag).cloned().unwrap_or_default()
}

#[async_trait]
impl EzApp for KvSm {
    type Request = Request;
    type Response = Response;

    async fn apply(&mut self, req: Request) -> Response {
        if !self.tag.is_empty() {
            APPLIED.lock().unwrap().entry(self.tag.clone()).or_default().push(req.to_string());
        }

        match req {
            Request::Set { key, value } => {
                self.data.insert(key, value);
                Response { value: None }
            }
            Request::Get { key } => Response {
                value: self.data.get(&key).cloned(),
            },
        }
    }

    type ReadRequest = String;
    type ReadResponse = Option<String>;

    fn read(&self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
    }
}

/// A fresh temp directory for one node's redb file, kept on disk rather than deleted on drop:
/// a restart test shuts a node down, drops it, and opens [`RedbStorage`] again on this same
/// path, which redb allows only once the previous `Database` is fully closed. `TempDir::keep`
/// hands back the bare path so callers that never restart do not have to carry a guard around;
/// leftover directories are cleaned up the same way `examples/harness.rs` leaves failed runs
/// behind, by the OS's own temp directory housekeeping.
pub fn node_data_path() -> PathBuf {
    tempfile::tempdir().expect("temp dir for a node's redb file").keep().join("node.redb")
}

/// Storage for a fresh node: a redb file under its own temp directory.
///
/// Restart tests that need to reopen the same file after shutdown call
/// `RedbStorage::open(&path)` directly with a path from [`node_data_path`] they kept.
pub async fn node_storage() -> io::Result<RedbStorage> {
    RedbStorage::open(node_data_path()).await
}

/// Grab a free port; the listener is dropped so EzRaft can bind it.
pub fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

/// Short heartbeat so elections and failovers finish in test time.
pub fn config() -> EzConfig {
    EzConfig {
        heartbeat_interval: Duration::from_millis(100),
        ..EzConfig::default()
    }
}

/// Upper bound for every cluster-state wait in these tests
pub const WAIT: Option<Duration> = Some(Duration::from_secs(30));

pub fn expected_map(range: std::ops::Range<u32>) -> BTreeMap<String, String> {
    range.map(|i| (format!("k{}", i), format!("v{}", i))).collect()
}

/// Write through a node's HTTP API, whichever node it is.
///
/// `EzRaft::write` only writes where it is called and fails elsewhere with
/// `ForwardToLeader`; finding the leader belongs to the server layer, and this
/// is the path that exercises it. So any test that does not know which node
/// leads - after a failover, a transfer, or a demotion - writes through here.
///
/// A write refused for want of a leader lease is retried. openraft alpha.33 accepts a write only
/// while the leader holds a lease acknowledged by a quorum, and a leader has none for a moment
/// after it is elected or after the voter set changes under it: it answers with a
/// `ForwardToLeader` naming nobody, which the sample `/api/write` handler passes on as a 500.
/// That moment ends with the next heartbeat round, so it is waited out here rather than failing
/// whichever test happens to write during it.
pub async fn http_write(addr: &str, req: Request) -> io::Result<Response> {
    const NO_LEASE_YET: &str = "has to forward request to: None";

    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster waits are bounded");

    loop {
        let resp = client
            .post(format!("http://{}/api/write", addr))
            .json(&req)
            .send()
            .await
            .map_err(io::Error::other)?;

        let status = resp.status();
        let text = resp.text().await.map_err(io::Error::other)?;

        if !status.is_success() && text.contains(NO_LEASE_YET) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }

        assert!(status.is_success(), "POST /api/write responded {}: {}", status, text);

        return Ok(serde_json::from_str(&text)?);
    }
}

/// Serve a node in the background, which is also where a join's promotion is
/// collected - a joined node reaches the voter set only once this is running.
pub fn spawn_serve(node: &EzRaft<KvSm>) {
    let node = node.clone();
    tokio::spawn(async move { node.serve().await });
}

/// A founding node, serving and leading.
pub async fn founding_node() -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::create(&addr, KvSm::default(), node_storage().await?, config()).await?;
    spawn_serve(&node);

    // Leading is not enough. `create` writes the founding membership and winning the election
    // does not commit it, so a caller that changes the membership next is refused for having one
    // already in flight - which is a race it loses only under load.
    node.wait_metrics(
        WAIT,
        |m| m.current_leader == Some(0) && m.committed_membership_config.log_id().is_some(),
        "founding node leads, with its own membership committed",
    )
    .await?;
    Ok((addr, node))
}

/// A node that joined asking to be a voter, serving. Returns before the
/// promotion lands, so callers wait on the voter set.
pub async fn joined_voter(seed: &str) -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::join(&addr, seed, KvSm::default(), node_storage().await?, config()).await?;
    spawn_serve(&node);
    Ok((addr, node))
}

/// A node that joined as a learner, serving.
pub async fn joined_learner(seed: &str) -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::join_as_learner(&addr, seed, KvSm::default(), node_storage().await?, config()).await?;
    spawn_serve(&node);
    Ok((addr, node))
}

/// Wait until `node` has *committed* exactly `voters`, as a uniform config.
///
/// Committed, not merely appended: an appended membership is still a change in
/// flight, and the next one a test asks for is refused while it is. Uniform,
/// not joint: a joint config still counts the old voter set, so a test that
/// killed a node on the strength of it would be racing.
pub async fn wait_for_voters(node: &EzRaft<KvSm>, voters: BTreeSet<u64>, reason: &str) -> io::Result<()> {
    node.wait_metrics(
        WAIT,
        |m| *m.committed_membership_config.membership().get_joint_config() == [voters.clone()],
        reason,
    )
    .await?;
    Ok(())
}

/// Wait until `node` has applied everything `leader` had logged when asked.
///
/// Anchored to the leader's index, because a node that has replicated nothing
/// has its own `last_applied` and `last_log_index` both unset, and comparing
/// those two would pass instantly.
pub async fn wait_for_applied(node: &EzRaft<KvSm>, leader: &EzRaft<KvSm>) -> io::Result<()> {
    let target = leader.metrics().await.last_log_index.expect("the leader has written entries");
    node.wait_metrics(
        WAIT,
        |m| m.last_applied.map(|log_id| log_id.index) >= Some(target),
        "applied the leader's log",
    )
    .await?;
    Ok(())
}

/// Ask a node for a membership change and return its answer, redirect and all.
pub async fn admin_answer(addr: &str, body: serde_json::Value) -> io::Result<Redirect> {
    let resp = reqwest::Client::new()
        .post(format!("http://{}/api/membership", addr))
        .json(&body)
        .send()
        .await
        .map_err(io::Error::other)?;

    let status = resp.status();
    let text = resp.text().await.map_err(io::Error::other)?;
    assert!(
        status.is_success(),
        "POST /api/membership responded {}: {}",
        status,
        text
    );

    Ok(serde_json::from_str(&text)?)
}

/// What an admin endpoint answers: done, or the leader to ask instead.
pub type Redirect = Result<(), Option<String>>;

/// Drive a membership change over HTTP, the way an operator would.
///
/// A redirect is a test failure: these tests always ask the leader, so being
/// told to ask someone else means the test set itself up wrong.
pub async fn admin_post(addr: &str, body: serde_json::Value) -> io::Result<()> {
    admin_answer(addr, body)
        .await?
        .map_err(|leader| io::Error::other(format!("/api/membership redirected to {:?}", leader)))
}

/// Cluster helpers for the `fault-injection` hook: see `tests/common/faults.rs`.
mod faults;
// Not every test binary under `tests/` uses these, same as the rest of this file (see the
// `allow(dead_code)` above) - but a glob re-export warns per binary rather than per item.
#[allow(unused_imports)]
pub use faults::*;
