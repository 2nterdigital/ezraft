//! The harness these cluster tests share: a node fixture that owns its temp directory, and the
//! waits that make cluster and KV state assertable.
//!
//! Each file under `tests/` is its own binary and uses a subset of this, so unused items here
//! are expected rather than dead.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::KeyValue;
use ezraft::RedbStorage;
use ezraft::Txn;
use ezraft::View;

/// A `Put` on `key` = `value`, the shape almost every test's transaction is
pub fn set(key: &str, value: &str) -> Txn {
    Txn::new().put(key.to_string(), value.to_string())
}

/// A `KeyValue`'s value decoded as UTF-8 text (lossily) - every value these tests write is
/// plain text, so this is the common case when reading one back.
pub fn text(kv: &KeyValue) -> String {
    String::from_utf8_lossy(&kv.value).into_owned()
}

/// Every key and value currently in `view`, decoded as UTF-8 text - the shape `expected_map`
/// compares against
pub fn view_map(view: &View) -> BTreeMap<String, String> {
    view.prefix("")
        .map(|kv| {
            let value = text(&kv);
            (kv.key, value)
        })
        .collect()
}

pub fn expected_map(range: std::ops::Range<u32>) -> BTreeMap<String, String> {
    range.map(|i| (format!("k{}", i), format!("v{}", i))).collect()
}

/// A node's own temp directory for its redb file, owned for the fixture's lifetime
///
/// Kept alive by whoever holds the fixture - a `let` binding for the length of the test is
/// enough - and deleted when it drops, so a passing run leaves nothing behind (the debt
/// `tests/common::node_data_path()` used to leave: it called `TempDir::keep()` and never deleted
/// anything). A restart test keeps one `NodeData` alive across the shutdown and calls
/// [`Self::open`] again to reopen the same file.
pub struct NodeData {
    dir: tempfile::TempDir,
}

impl Default for NodeData {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeData {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("temp dir for a node's redb file"),
        }
    }

    /// This fixture's redb file path, stable for its whole lifetime
    pub fn path(&self) -> PathBuf {
        self.dir.path().join("node.redb")
    }

    /// Open (or reopen) this fixture's redb file
    pub async fn open(&self) -> io::Result<RedbStorage> {
        RedbStorage::open(self.path()).await
    }
}

/// Grab a free port; the listener is dropped so `EzKv` can bind it.
pub fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

/// The shared cluster token every node this harness builds authenticates with
///
/// One fixed value for the whole test suite: every node in one test cluster is built from
/// [`config`] (or a config that starts from it), so they all agree on it and every request this
/// harness itself sends (`admin_post`, `admin_answer`) carries it.
pub const CLUSTER_TOKEN: &str = "test-cluster-token";

/// Short heartbeat so elections and failovers finish in test time.
pub fn config() -> EzConfig {
    EzConfig {
        heartbeat_interval: Duration::from_millis(100),
        cluster_token: CLUSTER_TOKEN.to_string(),
        ..EzConfig::default()
    }
}

/// Upper bound for every cluster-state wait in these tests
pub const WAIT: Option<Duration> = Some(Duration::from_secs(30));

/// How often a polling wait re-checks its condition
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Write `txn` on the first of `nodes` that accepts it
///
/// Superseded by ech0-media#30: `EzKv::write` now forwards to the leader and retries with the
/// same request id on its own, so calling it on any one live node already suffices - callers
/// across this suite still pass more than one handle (from when only the leader itself could
/// accept a write), so this keeps trying the rest only as a defensive fallback if the first one
/// given happens to error.
pub async fn write_through_leader(nodes: &[&EzKv], txn: Txn) -> io::Result<u64> {
    let mut last_err = None;
    for node in nodes {
        match node.write(txn.clone()).await {
            Ok(revision) => return Ok(revision),
            Err(e) => last_err = Some(io::Error::other(e.to_string())),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("write_through_leader called with no nodes")))
}

/// Wait until `node`'s local view has reached at least `revision`, and return that view
pub async fn wait_for_revision(node: &EzKv, revision: u64) -> io::Result<View> {
    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster waits are bounded");

    loop {
        let view = node.view();
        if view.revision() >= revision {
            return Ok(view);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "node {} never reached revision {} (stuck at {})",
                node.node_id(),
                revision,
                view.revision()
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Serve a node in the background, which is also where a join's promotion is
/// collected - a joined node reaches the voter set only once this is running.
pub fn spawn_serve(node: &EzKv) {
    let node = node.clone();
    tokio::spawn(async move { node.serve().await });
}

/// Build a node from `config`: the founding member of a new cluster
///
/// The one way every node-building helper in this harness bottoms out in a config; the fault
/// handle a `fault-injection` test needs is just a field on it
/// (`tests/common/faults.rs::config_with_faults`).
pub async fn founding_node_with(config: EzConfig) -> io::Result<(String, EzKv, NodeData)> {
    let addr = free_addr();
    let data = NodeData::new();
    let node = EzKv::create(&addr, data.open().await?, config).await?;
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
    Ok((addr, node, data))
}

/// A founding node, serving and leading, with the harness's usual short-heartbeat config
pub async fn founding_node() -> io::Result<(String, EzKv, NodeData)> {
    founding_node_with(config()).await
}

/// A node that joined asking to be a voter, serving, built with an explicit config
///
/// Returns before the promotion lands, so callers wait on the voter set.
pub async fn joined_voter_with(seed: &str, config: EzConfig) -> io::Result<(String, EzKv, NodeData)> {
    let addr = free_addr();
    let data = NodeData::new();
    let node = EzKv::join(&addr, seed, data.open().await?, config).await?;
    spawn_serve(&node);
    Ok((addr, node, data))
}

/// A node that joined asking to be a voter, serving, with the harness's usual config
pub async fn joined_voter(seed: &str) -> io::Result<(String, EzKv, NodeData)> {
    joined_voter_with(seed, config()).await
}

/// A node that joined as a learner, serving, built with an explicit config
pub async fn joined_learner_with(seed: &str, config: EzConfig) -> io::Result<(String, EzKv, NodeData)> {
    let addr = free_addr();
    let data = NodeData::new();
    let node = EzKv::join_as_learner(&addr, seed, data.open().await?, config).await?;
    spawn_serve(&node);
    Ok((addr, node, data))
}

/// A node that joined as a learner, serving, with the harness's usual config
pub async fn joined_learner(seed: &str) -> io::Result<(String, EzKv, NodeData)> {
    joined_learner_with(seed, config()).await
}

/// Wait until `node` has *committed* exactly `voters`, as a uniform config.
///
/// Committed, not merely appended: an appended membership is still a change in
/// flight, and the next one a test asks for is refused while it is. Uniform,
/// not joint: a joint config still counts the old voter set, so a test that
/// killed a node on the strength of it would be racing.
pub async fn wait_for_voters(node: &EzKv, voters: BTreeSet<u64>, reason: &str) -> io::Result<()> {
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
pub async fn wait_for_applied(node: &EzKv, leader: &EzKv) -> io::Result<()> {
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
///
/// Carries [`CLUSTER_TOKEN`], like every request a real node would send: this is the "legitimate
/// operator" path. A request that deliberately omits or mismatches the token belongs in
/// `tests/auth.rs`, driven directly over `reqwest` instead.
pub async fn admin_answer(addr: &str, body: serde_json::Value) -> io::Result<Redirect> {
    let resp = reqwest::Client::new()
        .post(format!("http://{}/api/membership", addr))
        .bearer_auth(CLUSTER_TOKEN)
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

/// `POST http://{addr}/{path}` with `body` and, if given, `Authorization: Bearer {token}` -
/// deliberately raw, for `tests/auth.rs` to drive requests with a missing, wrong, or mismatched
/// token straight at a route (`raft/*`, `api/*`, `rpc/*`) and inspect the HTTP status itself.
pub async fn raw_post(
    addr: &str,
    path: &str,
    token: Option<&str>,
    body: &serde_json::Value,
) -> io::Result<reqwest::Response> {
    let mut req = reqwest::Client::new().post(format!("http://{addr}/{path}")).json(body);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    req.send().await.map_err(io::Error::other)
}

/// Wait until `addr`'s HTTP server answers `GET /api/metrics` with the right token
///
/// A lone founding node can commit its own membership - what `founding_node`'s own wait checks -
/// without its HTTP server having finished binding yet: a single voter needs no network round
/// trip to reach a quorum of itself. A test that fires a *raw*, unauthenticated request right
/// after `founding_node` (rather than joining, which already retries through `AdminClient`) needs
/// this instead, so a `401` it observes is the auth check and not a connection that never landed.
pub async fn wait_until_serving(addr: &str) -> io::Result<()> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster waits are bounded");

    loop {
        if let Ok(resp) = client.get(format!("http://{addr}/api/metrics")).bearer_auth(CLUSTER_TOKEN).send().await
            && resp.status().is_success()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "{addr} never started serving HTTP within the wait budget"
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Cluster helpers for the `fault-injection` hook: see `tests/common/faults.rs`.
mod faults;
// Not every test binary under `tests/` uses these, same as the rest of this file (see the
// `allow(dead_code)` above) - but a glob re-export warns per binary rather than per item.
#[allow(unused_imports)]
pub use faults::*;

/// The crash-injecting `EzStorage` wrapper for storage power-loss tests: see
/// `tests/common/crash.rs`.
mod crash;
#[allow(unused_imports)]
pub use crash::*;

/// Helpers for driving and asserting on `watch`/`view_and_watch` streams: see
/// `tests/common/watch.rs`.
mod watch;
#[allow(unused_imports)]
pub use watch::*;
