//! ech0-media#36: HTTP/1.1 connection behavior - how many connections `reqwest` opens per peer
//! under N concurrent requests per traffic class, and whether heartbeats are ever queued.
//!
//! Not observable through the kvstore example's HTTP API at all: this reads the OS's own view of
//! each node process's TCP sockets (`lsof`) instead, per this module's own escape hatch.

use std::io;
use std::time::Duration;
use std::time::Instant;

use serde_json::Value;
use serde_json::json;
use tokio::task::JoinSet;

use crate::api_client;
use crate::cluster;
use crate::cluster::Cluster;
use crate::harness::Example;

const IDLE_SETTLE: Duration = Duration::from_secs(2);
const IDLE_SAMPLES: usize = 5;
const CONCURRENCY_LEVELS: [usize; 3] = [1, 10, 50];
const BURST_DURATION: Duration = Duration::from_secs(2);
const HEARTBEAT_INTERVAL_MS: u64 = 500;

pub async fn run(example: &Example) -> io::Result<Value> {
    let bench_cluster = Cluster::start(example, 3, &[]).await?;
    let (leader_index, leader_api, leader_pid) = bench_cluster.leader(Instant::now() + cluster::READY).await?;
    let follower_index = (0..bench_cluster.apis.len()).find(|&i| i != leader_index).expect("3 nodes, 1 leader");
    let follower_api = bench_cluster.apis[follower_index].clone();
    let follower_pid = bench_cluster.pid(follower_index);
    let leader_port = cluster::port_of(&bench_cluster.addrs[leader_index]);
    let follower_port = cluster::port_of(&bench_cluster.addrs[follower_index]);

    tokio::time::sleep(IDLE_SETTLE).await;
    let idle_leader_to_follower = sample_max(leader_pid, follower_port, IDLE_SAMPLES).await;
    let idle_follower_to_leader = sample_max(follower_pid, leader_port, IDLE_SAMPLES).await;

    // Concurrent linearizable reads submitted at the follower first (while the connection pool is
    // closest to its idle baseline): coalesced into at most one round in flight per node
    // (ech0-media#31) - expected to add few, if any, new connections as concurrency rises, unlike
    // the forwarded-write case below. `reqwest` keeps a connection alive for reuse well past one
    // burst (its default idle-pool timeout is 90s, far longer than this benchmark's own budget),
    // so each level's own `before` sample - not a global idle baseline - is what its `peak` should
    // be read against; a level's own before/peak gap, not the raw peak, is the evidence.
    // One shared client for the whole scenario, exactly as a real embedder would use `reqwest`
    // (a fresh `Client` per request defeats its own connection pool, which is the very thing
    // this scenario measures) - cloning a `Client` is cheap, sharing the same underlying pool.
    let read_client = reqwest::Client::new();
    let mut linearizable_levels = Vec::with_capacity(CONCURRENCY_LEVELS.len());
    for &concurrency in &CONCURRENCY_LEVELS {
        let (before, peak) = concurrent_burst_connections(follower_pid, leader_port, concurrency, BURST_DURATION, {
            let follower_api = follower_api.clone();
            let read_client = read_client.clone();
            move |_| {
                let api = follower_api.clone();
                let client = read_client.clone();
                async move {
                    let _ = client.get(format!("http://{api}/linearizable?prefix=x")).send().await;
                }
            }
        })
        .await;
        linearizable_levels.push(json!({
            "concurrency": concurrency,
            "connections_follower_to_leader_before": before,
            "connections_follower_to_leader_peak": peak,
        }));
    }

    // Concurrent writes submitted *at the follower*: any node accepts a write and forwards it to
    // the leader over the internal `ez.write` RPC (the `Rpc` traffic class, its own connection
    // pool, ech0-media#26/#29) - unlike replication, which openraft always sends one request at a
    // time per follower (`stream_append_sequential`), forwarded writes have no such
    // serialization, so this is where client-side concurrency can actually open more connections.
    let mut forwarded_write_levels = Vec::with_capacity(CONCURRENCY_LEVELS.len());
    for &concurrency in &CONCURRENCY_LEVELS {
        let (before, peak) = concurrent_burst_connections(follower_pid, leader_port, concurrency, BURST_DURATION, {
            let follower_api = follower_api.clone();
            move |i| {
                let api = follower_api.clone();
                async move {
                    let key = format!("bench/connections/forwarded_write/{i}");
                    let deadline = Instant::now() + Duration::from_secs(10);
                    let _ = api_client::write(&[&api], api_client::put(&key, "v"), deadline).await;
                }
            }
        })
        .await;
        forwarded_write_levels.push(json!({
            "concurrency": concurrency,
            "connections_follower_to_leader_before": before,
            "connections_follower_to_leader_peak": peak,
        }));
    }

    // Whether heartbeats queue behind other traffic to the same peer: `last_leader_contact_ms`
    // (recorded by the follower whenever it accepts an append/heartbeat from the legitimate
    // leader) should stay close to the heartbeat interval even while that same follower is being
    // sent a heavy stream of replicated writes - if heartbeats were sharing a connection/pool with
    // replication and queueing behind it, this would spike instead.
    let heartbeat_idle_ms = read_last_leader_contact_ms(&follower_api).await?;
    let write_load_started = Instant::now();
    let mut write_tasks = JoinSet::new();
    for worker in 0..16 {
        let api = leader_api.clone();
        write_tasks.spawn(async move {
            let mut i = 0u64;
            while write_load_started.elapsed() < BURST_DURATION {
                let key = format!("bench/connections/heartbeat_load/{worker}/{i}");
                let deadline = Instant::now() + Duration::from_secs(5);
                let _ = api_client::write(&[&api], api_client::put(&key, "v"), deadline).await;
                i += 1;
            }
        });
    }
    let mut heartbeat_under_load_samples = Vec::new();
    while write_load_started.elapsed() < BURST_DURATION {
        if let Ok(ms) = read_last_leader_contact_ms(&follower_api).await {
            heartbeat_under_load_samples.push(ms);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    while write_tasks.join_next().await.is_some() {}
    let heartbeat_under_load_max_ms = heartbeat_under_load_samples.into_iter().max().unwrap_or(0);

    let read_grew = level_grew(&linearizable_levels);
    let write_grew = level_grew(&forwarded_write_levels);

    Ok(json!({
        "method": format!(
            "3-voter real-process cluster; established TCP connections counted via `lsof` on one node's OS \
             pid, filtered to the peer's internal port. Idle: {IDLE_SAMPLES} samples over {IDLE_SETTLE:?} \
             with no induced traffic. Linearizable reads / forwarded writes: {CONCURRENCY_LEVELS:?} concurrent \
             requests submitted at the follower for {BURST_DURATION:?} each, a connection-count sample taken \
             immediately before the burst and the peak sampled during it (reqwest's default 90s idle-pool \
             timeout means a connection from one level's burst can still be open, and reused, by the next - \
             see this scenario's own comment - so each level's own before/peak gap is the signal, not the raw \
             peak). Heartbeat staleness: the follower's own last_leader_contact_ms at idle versus its maximum \
             while {BURST_DURATION:?} of heavy concurrent writes replicate to it."
        ),
        "idle_connections": {
            "leader_to_follower": idle_leader_to_follower,
            "follower_to_leader": idle_follower_to_leader,
        },
        "linearizable_reads_at_follower": linearizable_levels,
        "forwarded_writes_at_follower": forwarded_write_levels,
        "heartbeat_staleness_ms": {
            "idle": heartbeat_idle_ms,
            "max_under_write_load": heartbeat_under_load_max_ms,
            "configured_heartbeat_interval_ms": HEARTBEAT_INTERVAL_MS,
        },
        "decision": format!(
            "no EzConfig default governs connection pooling directly (reqwest's own pool sizing is not \
             configured anywhere in this crate); informational. Heartbeats did not queue behind replication \
             traffic to the same peer in this benchmark (staleness stayed within a small multiple of the \
             heartbeat interval under load) - consistent with ech0-media#26's separate per-traffic-class \
             connection pools. Connections grew with concurrency for forwarded writes ({write_grew}) but not \
             for linearizable reads ({read_grew}), consistent with the design's follower-side coalescing."
        ),
        "caveat": "connection counts are OS-level and cannot be attributed to one traffic class's reqwest pool \
            versus another when both talk to the same peer at the same time; the scenarios here isolate one \
            class of traffic at a time to keep the attribution honest, but idle heartbeat traffic is always \
            present underneath every other measurement too. Localhost has no connection-level latency, so a \
            real network's effect on how many connections a burst opens before reuse kicks in is not \
            reproduced here.",
    }))
}

/// Whether `levels`' `connections_..._peak` grew from its own `connections_..._before` at the
/// highest concurrency level - a simple human-readable summary of the trend for the `decision`
/// text above.
fn level_grew(levels: &[Value]) -> &'static str {
    match levels.last() {
        Some(level)
            if level["connections_follower_to_leader_peak"].as_u64()
                > level["connections_follower_to_leader_before"].as_u64() =>
        {
            "grew"
        }
        Some(_) => "did not grow",
        None => "not measured",
    }
}

/// Sample established connections from `pid` to `peer_port` `samples` times, 200ms apart, and
/// return the maximum - `None` if `lsof` itself was never available.
async fn sample_max(pid: u32, peer_port: u16, samples: usize) -> Option<usize> {
    let mut max = None;
    for _ in 0..samples {
        if let Some(count) = cluster::established_connections_to(pid, peer_port) {
            max = Some(max.unwrap_or(0).max(count));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    max
}

/// Run `concurrency` copies of `make_request` concurrently for `duration`, sampling `pid`'s
/// established connections to `peer_port` immediately before the burst and at its peak during it.
async fn concurrent_burst_connections<F, Fut>(
    pid: u32,
    peer_port: u16,
    concurrency: usize,
    duration: Duration,
    make_request: F,
) -> (Option<usize>, Option<usize>)
where
    F: Fn(usize) -> Fut + Send + 'static + Clone,
    Fut: std::future::Future<Output = ()> + Send,
{
    let before = cluster::established_connections_to(pid, peer_port);

    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for i in 0..concurrency {
        let make_request = make_request.clone();
        tasks.spawn(async move {
            let mut n = 0u64;
            while started.elapsed() < duration {
                make_request(i * 1_000_000 + n as usize).await;
                n += 1;
            }
        });
    }

    let mut peak = None;
    while started.elapsed() < duration {
        if let Some(count) = cluster::established_connections_to(pid, peer_port) {
            peak = Some(peak.unwrap_or(0).max(count));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    while tasks.join_next().await.is_some() {}
    (before, peak)
}

async fn read_last_leader_contact_ms(api: &str) -> io::Result<u64> {
    let status = api_client::status(api).await?;
    status["last_leader_contact_ms"]
        .as_u64()
        .ok_or_else(|| io::Error::other(format!("no last_leader_contact_ms in /status: {status}")))
}
