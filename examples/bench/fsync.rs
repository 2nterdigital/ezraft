//! ech0-media#36: fsync cost per write batch, `Durability::Immediate`, on this machine.
//!
//! What this measures is the *end-to-end* write latency: one `POST /tx` round trip through the
//! leader, one Raft log append, one redb `Durability::Immediate` write transaction on every
//! voter, one commit, one local publish. `EzConfig` has no separate knob for the fsync policy
//! itself - the storage contract is one `persist` call per log entry, always at
//! `Durability::Immediate` (ech0-media#11's resolution) - so there is no default to change here.
//! This answers the other half of the ticket's wording ("每批写入的 fsync 成本"): what that fixed
//! cost actually is, so [`crate::body_limit`]'s throughput numbers and `write_deadline`'s margin
//! can be read against it.

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
use crate::stats::Summary;

const SINGLE_WRITER_SAMPLES: usize = 200;
const CONCURRENT_WRITERS: usize = 32;
const CONCURRENT_DURATION: Duration = Duration::from_secs(3);
const WRITE_DEADLINE: Duration = Duration::from_secs(10);

pub async fn run(example: &Example) -> io::Result<Value> {
    let bench_cluster = Cluster::start(example, 3, &[]).await?;
    let (_, leader_api, _) = bench_cluster.leader(Instant::now() + cluster::READY).await?;

    // Single writer, sequential: every request waits for the previous one's full round trip, so
    // consecutive samples never overlap in flight - the closest an external-API-only benchmark
    // can get to isolating one write's own persist cost from queuing behind another.
    let mut singles = Vec::with_capacity(SINGLE_WRITER_SAMPLES);
    for i in 0..SINGLE_WRITER_SAMPLES {
        let key = format!("bench/fsync/single/{i}");
        let started = Instant::now();
        write_once(&leader_api, &key).await?;
        singles.push(started.elapsed());
    }
    let single_writer_latency = Summary::of(singles);

    // Concurrent writers, fixed wall-clock duration: how many of these round trips the cluster
    // sustains per second once several are always in flight, which is what a real write path's
    // capacity depends on - not just the best-case single-writer latency above.
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..CONCURRENT_WRITERS {
        let leader_api = leader_api.clone();
        tasks.spawn(async move {
            let mut completed = 0u64;
            let mut i = 0u64;
            while started.elapsed() < CONCURRENT_DURATION {
                let key = format!("bench/fsync/concurrent/{worker}/{i}");
                if write_once(&leader_api, &key).await.is_ok() {
                    completed += 1;
                }
                i += 1;
            }
            completed
        });
    }
    let mut total_writes = 0u64;
    while let Some(result) = tasks.join_next().await {
        total_writes += result.unwrap_or(0);
    }
    let concurrent_writes_per_sec = total_writes as f64 / started.elapsed().as_secs_f64();

    let method = format!(
        "3-voter real-process cluster, RedbStorage on this machine's disk, Durability::Immediate \
         (unconditional, ech0-media#11 - no EzConfig knob to vary). Single-writer: {SINGLE_WRITER_SAMPLES} \
         sequential one-key POST /tx round trips against the leader, timed end to end. Concurrent: \
         {CONCURRENT_WRITERS} tasks hammering distinct keys against the leader for \
         {CONCURRENT_DURATION:?}, counting completions."
    );

    Ok(json!({
        "method": method,
        "single_writer_latency_ms": single_writer_latency,
        "concurrent_writers": CONCURRENT_WRITERS,
        "concurrent_writes_per_sec": concurrent_writes_per_sec,
        "caveat": "localhost, one shared physical disk for all three voters, no real inter-node \
            latency: a deployment with real network RTT will see higher single-writer latency, \
            and one with independent disks per voter may see higher concurrent throughput \
            (persists no longer contend for one disk's write queue).",
    }))
}

async fn write_once(api: &str, key: &str) -> io::Result<()> {
    let deadline = Instant::now() + WRITE_DEADLINE;
    match api_client::write(&[api], api_client::put(key, "v"), deadline).await? {
        api_client::TxOutcome::Success { .. } => Ok(()),
        api_client::TxOutcome::Conflict(value) => Err(io::Error::other(format!("unexpected conflict: {value}"))),
    }
}
