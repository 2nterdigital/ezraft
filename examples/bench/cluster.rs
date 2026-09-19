//! Shared setup for every benchmark scenario: found a fresh real-process cluster from the
//! release-built kvstore example, wait for it to serve, and read back OS-level facts about a node
//! process that its HTTP API does not expose (resident memory, established TCP connections) - the
//! ticket's own escape hatch ("extend the example... rather than the library") applied through the
//! OS instead of through a new route.

use std::io;
use std::process::Command;
use std::time::Duration;
use std::time::Instant;

use crate::api_client;
use crate::harness::Example;
use crate::harness::Node;
use crate::harness::free_addr_pair;

/// Generous upper bound for a node to come up and a cluster to stabilize: real process starts and
/// real disk I/O on a shared dev machine, not the tighter bound the in-process tests use.
pub const READY: Duration = Duration::from_secs(20);

/// A key/value pair every [`Cluster::start`] writes through the founder before any joiner starts,
/// so a joiner's own readiness wait has something to wait for.
const READY_KEY: &str = "__bench_ready__";
const READY_VALUE: &str = "1";

/// A freshly founded real-process cluster: nodes in spawn order (index 0 is the founder), each
/// with its own (internal, external) address pair. Every child process is killed when `nodes`
/// drops (`harness::Node`'s own `Drop`), including on an early return via `?` or a panic - a
/// scenario never has to remember to clean up.
pub struct Cluster {
    pub nodes: Vec<Node>,
    pub addrs: Vec<String>,
    pub apis: Vec<String>,
}

impl Cluster {
    /// Found an `n`-voter cluster (`n >= 1`), every node started with `extra_args`, and wait until
    /// the founder serves and every joiner has replicated the founder's first write - the same
    /// readiness signal `kvstore_test.rs` uses.
    pub async fn start(example: &Example, n: usize, extra_args: &[&str]) -> io::Result<Self> {
        assert!(n >= 1, "a cluster needs at least one node");
        let example = example.clone().node_args(extra_args);

        let (addr0, api0) = free_addr_pair();
        let node0 = example.spawn(&addr0, &api0, None)?;
        let deadline = Instant::now() + READY;
        api_client::write(&[&api0], api_client::put(READY_KEY, READY_VALUE), deadline)
            .await
            .map_err(|e| io::Error::other(format!("founding node never became ready: {e}")))?;

        let mut cluster = Self {
            nodes: vec![node0],
            addrs: vec![addr0],
            apis: vec![api0],
        };
        for _ in 1..n {
            cluster.join(&example, &[], READY_KEY, READY_VALUE, Instant::now() + READY).await?;
        }
        Ok(cluster)
    }

    /// Join one more voter to this cluster through its founder, wait until it has replicated
    /// `catch_up_key`/`catch_up_value`, and return its (internal, external) address pair.
    ///
    /// Kept separate from [`Self::start`] so a scenario can build a write backlog *before* the
    /// joiner exists, to measure how long catching up on it takes (the request body limit
    /// scenario, ech0-media#36).
    pub async fn join(
        &mut self,
        example: &Example,
        extra_args: &[&str],
        catch_up_key: &str,
        catch_up_value: &str,
        deadline: Instant,
    ) -> io::Result<(String, String)> {
        let example = example.clone().node_args(extra_args);
        let (addr, api) = free_addr_pair();
        let node = example.spawn(&addr, &api, Some(&self.addrs[0]))?;
        api_client::wait_for_value(&api, catch_up_key, Some(catch_up_value), deadline)
            .await
            .map_err(|e| io::Error::other(format!("joiner {addr} never caught up: {e}")))?;
        self.nodes.push(node);
        self.addrs.push(addr.clone());
        self.apis.push(api.clone());
        Ok((addr, api))
    }

    /// Poll every node's `GET /status` until one reports `is_leader: true`; return its index in
    /// [`Self::apis`]/[`Self::addrs`], its external address and its OS pid.
    pub async fn leader(&self, deadline: Instant) -> io::Result<(usize, String, u32)> {
        loop {
            for (i, api) in self.apis.iter().enumerate() {
                if let Ok(status) = api_client::status(api).await
                    && status["is_leader"].as_bool() == Some(true)
                {
                    return Ok((i, api.clone(), self.nodes[i].pid()));
                }
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other("no node in the cluster ever reported is_leader"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// This node's OS pid, by its index in [`Self::apis`]/[`Self::addrs`].
    pub fn pid(&self, index: usize) -> u32 {
        self.nodes[index].pid()
    }

    /// Poll `api` (any node) until its registry (`GET /status`'s `nodes`) lists every node this
    /// `Cluster` has spawned so far.
    ///
    /// A node's own entry lands in the registry only once its startup self-registration task
    /// (ech0-media#33) succeeds - a background retry loop, not something [`Self::start`]/
    /// [`Self::join`] wait for, since most scenarios never look at the registry. A scenario that
    /// SIGKILLs a node and expects to see it in the registry afterwards (as "Down", not simply
    /// absent) must call this first: a node killed before it ever registered leaves no entry to
    /// mark "Down" at all.
    pub async fn wait_for_full_registry(&self, api: &str, deadline: Instant) -> io::Result<()> {
        loop {
            if let Ok(status) = api_client::status(api).await
                && let Some(nodes) = status["nodes"].as_array()
                && nodes.len() >= self.apis.len()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other("the registry never listed every spawned node"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// The port `addr` ("host:port") names.
pub fn port_of(addr: &str) -> u16 {
    addr.rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .unwrap_or_else(|| panic!("not a host:port address: {addr}"))
}

/// This process's resident set size, in kibibytes. `ps -o rss=` reports kibibytes on both Linux
/// and macOS. `None` once the process has exited, or if `ps` itself is unavailable.
pub fn rss_kib(pid: u32) -> Option<u64> {
    let output = Command::new("ps").args(["-o", "rss=", "-p", &pid.to_string()]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

/// How many of `pid`'s TCP sockets are `ESTABLISHED` with `peer_port` as their remote port - i.e.
/// how many connections this node's outbound `reqwest` pool currently holds open to one peer.
///
/// Localhost-only (every node here binds `127.0.0.1`) and needs `lsof` (macOS, Linux). `None`
/// means `lsof` itself could not be spawned - it exits non-zero when a process simply has no
/// matching descriptors, which is a legitimate zero, not a failure, so only a spawn error (a
/// missing binary) is treated as "unknown" here.
pub fn established_connections_to(pid: u32, peer_port: u16) -> Option<usize> {
    let output = Command::new("lsof").args(["-nP", "-iTCP", "-a", "-p", &pid.to_string()]).output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    let needle = format!("127.0.0.1:{peer_port} (ESTABLISHED)");
    Some(text.lines().filter(|line| line.contains("->") && line.contains(&needle)).count())
}
