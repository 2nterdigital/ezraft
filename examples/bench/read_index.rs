//! ech0-media#36: ReadIndex load on the leader - `linearizable_view` throughput/latency from a
//! follower with follower-side coalescing, at increasing concurrency and alongside a concurrent
//! write load.
//!
//! openraft alpha.33 has no follower-side metric for how many `ez.read_index` RPC rounds the
//! leader actually served, and this crate's own request counters
//! (`request_count`/`request_count_by_class`) are test-only, gated behind the `fault-injection`
//! feature this release build does not enable - this scenario reads the coalescing effect through
//! wall-clock latency and throughput trends instead. What it actually found (see
//! `design_change_proposal` in its result) was not the flat latency a naive reading of
//! coalescing ("at most one round in flight per node") might predict: latency rose with read
//! concurrency and rose much further under concurrent write load, evidence that the leader's
//! write and read paths do compete for the same resources.

use std::io;
use std::time::Duration;
use std::time::Instant;

use reqwest::Client;
use serde_json::Value;
use serde_json::json;
use tokio::task::JoinSet;

use crate::api_client;
use crate::cluster;
use crate::cluster::Cluster;
use crate::harness::Example;
use crate::stats::Summary;

const CONCURRENCY_LEVELS: [usize; 4] = [1, 10, 50, 100];
const READ_DURATION: Duration = Duration::from_secs(2);
const WRITE_LOAD_WRITERS: usize = 16;

pub async fn run(example: &Example) -> io::Result<Value> {
    let bench_cluster = Cluster::start(example, 3, &[]).await?;
    let (leader_index, _, _) = bench_cluster.leader(Instant::now() + cluster::READY).await?;
    let follower_api = bench_cluster
        .apis
        .iter()
        .enumerate()
        .find(|&(i, _)| i != leader_index)
        .map(|(_, api)| api.clone())
        .expect("3 nodes, 1 leader");

    let mut idle_levels = Vec::with_capacity(CONCURRENCY_LEVELS.len());
    for &concurrency in &CONCURRENCY_LEVELS {
        let (summary, reads_per_sec) = measure_linearizable(&follower_api, concurrency, READ_DURATION).await?;
        idle_levels.push(json!({"concurrency": concurrency, "latency_ms": summary, "reads_per_sec": reads_per_sec}));
    }

    // Repeat the highest concurrency level while a concurrent write load keeps the leader busy -
    // the ticket's own wording ("ReadIndex 对 leader 的负载") cares about read latency from a
    // follower while the leader also has writes to commit, not only an idle leader.
    let &busiest = CONCURRENCY_LEVELS.last().expect("non-empty");
    let leader_api = bench_cluster.apis[leader_index].clone();
    let write_load_started = Instant::now();
    let mut write_tasks = JoinSet::new();
    for worker in 0..WRITE_LOAD_WRITERS {
        let api = leader_api.clone();
        write_tasks.spawn(async move {
            let mut i = 0u64;
            while write_load_started.elapsed() < READ_DURATION {
                let key = format!("bench/read_index/write_load/{worker}/{i}");
                let deadline = Instant::now() + Duration::from_secs(5);
                let _ = api_client::write(&[&api], api_client::put(&key, "v"), deadline).await;
                i += 1;
            }
        });
    }
    let (under_load_summary, under_load_reads_per_sec) =
        measure_linearizable(&follower_api, busiest, READ_DURATION).await?;
    while write_tasks.join_next().await.is_some() {}

    let idle_p50_at_1 = idle_levels[0]["latency_ms"]["p50_ms"].as_f64().unwrap_or(0.0);
    let idle_reads_per_sec_at_1 = idle_levels[0]["reads_per_sec"].as_f64().unwrap_or(0.0);
    let idle_p50_at_busiest = idle_levels.last().and_then(|v| v["latency_ms"]["p50_ms"].as_f64()).unwrap_or(0.0);
    let idle_reads_per_sec_at_busiest = idle_levels.last().and_then(|v| v["reads_per_sec"].as_f64()).unwrap_or(0.0);
    let load_p50 = under_load_summary.p50_ms;
    let load_slowdown = load_p50 / idle_p50_at_busiest.max(0.001);

    Ok(json!({
        "method": format!(
            "3-voter real-process cluster; GET /linearizable?prefix=x (an empty prefix scan, so it exercises \
             the full ReadIndex path with no data-setup cost) issued from a follower at concurrency levels \
             {CONCURRENCY_LEVELS:?}, {READ_DURATION:?} per level, latency and achieved reads/sec recorded; \
             then the highest level ({busiest}) repeated with {WRITE_LOAD_WRITERS} concurrent Put writers \
             against the leader running at the same time."
        ),
        "idle_leader": idle_levels,
        "leader_under_write_load": {
            "concurrency": busiest,
            "latency_ms": under_load_summary,
            "reads_per_sec": under_load_reads_per_sec,
        },
        "decision": "no EzConfig default governs ReadIndex load directly - there is no leader-side coalescing \
            or LeaseRead knob to tune; both are explicit design choices (ech0-media#16's resolution), so this \
            scenario is informational, not a default to set.",
        "design_change_proposal": format!(
            "the leader is real, measurable load on ReadIndex reads, contrary to this benchmark's own working \
             assumption going in: p50 latency rose from {idle_p50_at_1:.2} ms at concurrency 1 to \
             {idle_p50_at_busiest:.2} ms at concurrency {busiest} on an otherwise idle leader, and to \
             {load_p50:.1} ms ({load_slowdown:.0}x) once {WRITE_LOAD_WRITERS} concurrent writers kept that \
             same leader busy. Throughput also scaled well short of linearly with read concurrency \
             ({idle_reads_per_sec_at_1:.0}/s at concurrency 1 vs {idle_reads_per_sec_at_busiest:.0}/s at \
             concurrency {busiest}, not {busiest}x). Both point at the leader's write path and its ReadIndex \
             confirmation competing for the same resources - very plausibly the single append/fsync pipeline \
             this benchmark's own fsync scenario found to be the write bottleneck (see its own caveat: this \
             cannot be told apart from ReadIndex's own protocol cost through the external API alone). If a \
             production read rate needs ReadIndex latency to stay low while the leader is also under write \
             load, leader-side coalescing across multiple followers' concurrent requests, or LeaseRead (skip \
             the quorum confirmation within the leader lease), would remove the read path's dependency on \
             that pipeline - both were explicitly deferred pending measured load (ech0-media#16's resolution, \
             restated in the milestone's own out-of-scope list). This benchmark's numbers are that \
             measurement; implementing either remains a proposal, not a change made here."
        ),
        "caveat": "localhost has no real network latency, so the quorum round trip measured here is far \
            faster than a real deployment's would be, and the slowdown under write load could be dominated by \
            this specific machine's fsync cost (see the fsync scenario) rather than ReadIndex's own protocol \
            overhead; this benchmark has no way to count the leader's own RPC rounds directly (see this \
            module's own doc comment), so the 'load' conclusion rests on latency/throughput trends, not a \
            direct measurement of leader-side work.",
    }))
}

/// Run `concurrency` concurrent `GET /linearizable` loops against `api` for `duration`, and
/// return the latency distribution and achieved reads/sec.
async fn measure_linearizable(api: &str, concurrency: usize, duration: Duration) -> io::Result<(Summary, f64)> {
    let client = Client::new();
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for _ in 0..concurrency {
        let client = client.clone();
        let api = api.to_string();
        tasks.spawn(async move {
            let mut latencies = Vec::new();
            while started.elapsed() < duration {
                let read_started = Instant::now();
                let url = format!("http://{api}/linearizable?prefix=x");
                if client.get(url).send().await.is_ok() {
                    latencies.push(read_started.elapsed());
                }
            }
            latencies
        });
    }
    let mut all_latencies = Vec::new();
    while let Some(result) = tasks.join_next().await {
        all_latencies.extend(result.unwrap_or_default());
    }
    if all_latencies.is_empty() {
        return Err(io::Error::other(format!(
            "no successful GET /linearizable response at concurrency {concurrency} within {duration:?}"
        )));
    }
    let reads_per_sec = all_latencies.len() as f64 / started.elapsed().as_secs_f64();
    Ok((Summary::of(all_latencies), reads_per_sec))
}
