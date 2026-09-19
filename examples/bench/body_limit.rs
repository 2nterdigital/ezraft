//! ech0-media#36: the request body limit and the `max_payload_entries` it derives.
//!
//! `EzConfig::to_raft_config` derives openraft's `max_payload_entries` (how many log entries one
//! `/raft/append` request may batch) as `json_body_limit / (2 * kv_limits.max_txn_bytes)`, which
//! is **4** at the defaults (8 MiB / (2 * 1 MiB)), against openraft's own default of 300. This
//! scenario measures what that costs: write throughput under concurrency, and how long a follower
//! a few hundred entries behind takes to catch up by ordinary log replication (kept under
//! `replication_lag_threshold` so it replays log entries, not a snapshot install) - comparing the
//! derived default of 4 against a `json_body_limit` sized to reproduce openraft's own 300.

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

const DEFAULT_MAX_TXN_BYTES: u64 = 1024 * 1024;
const DEFAULT_JSON_BODY_LIMIT: u64 = 8 * 1024 * 1024;
/// `300 * 2 * DEFAULT_MAX_TXN_BYTES`: the `json_body_limit` that makes `EzConfig::to_raft_config`
/// derive openraft's own `max_payload_entries` default (300), for a like-for-like comparison
/// against the derived default of 4 - not a value this benchmark proposes shipping (see this
/// module's own doc comment: it only measures, it does not implement the alternative).
const LARGE_JSON_BODY_LIMIT: u64 = 300 * 2 * DEFAULT_MAX_TXN_BYTES;

const THROUGHPUT_WRITERS: usize = 32;
const THROUGHPUT_DURATION: Duration = Duration::from_secs(3);
const BACKLOG_ENTRIES: usize = 300;
const BACKLOG_FILL_CONCURRENCY: usize = 16;
const WRITE_DEADLINE: Duration = Duration::from_secs(15);
const CATCH_UP_DEADLINE: Duration = Duration::from_secs(60);

pub async fn run(example: &Example) -> io::Result<Value> {
    let default_config = measure_config(
        example,
        "derived default (max_payload_entries=4)",
        DEFAULT_JSON_BODY_LIMIT,
    )
    .await?;
    let large_config = measure_config(
        example,
        "openraft's own default (max_payload_entries=300)",
        LARGE_JSON_BODY_LIMIT,
    )
    .await?;

    let default_throughput = default_config["throughput_writes_per_sec"].as_f64().unwrap_or(0.0);
    let large_throughput = large_config["throughput_writes_per_sec"].as_f64().unwrap_or(0.0);
    let default_catch_up_ms = default_config["catch_up_ms"].as_u64().unwrap_or(0);
    let large_catch_up_ms = large_config["catch_up_ms"].as_u64().unwrap_or(0);

    let hurts = large_throughput > default_throughput * 1.15 || default_catch_up_ms > large_catch_up_ms.max(1) * 2;

    let decision = if hurts {
        format!(
            "keep json_body_limit at the current default (8 MiB, max_payload_entries=4) as the *ceiling* the \
             design already fixes (ech0-media#26: it exists to bound one request's worst-case size, not to \
             set the common-case batch size), but flag the measured cost: {default_throughput:.0} vs \
             {large_throughput:.0} writes/sec under {THROUGHPUT_WRITERS}-way concurrency, and \
             {default_catch_up_ms} ms vs {large_catch_up_ms} ms for a follower {BACKLOG_ENTRIES} entries \
             behind to catch up by log replication. Do not raise json_body_limit itself to fix this - that \
             only moves the same worst-case ceiling higher (see this scenario's own doc comment); propose the \
             byte-aware append-splitting design change below instead."
        )
    } else {
        format!(
            "keep json_body_limit at the current default (8 MiB, max_payload_entries=4): throughput \
             ({default_throughput:.0} vs {large_throughput:.0} writes/sec) and follower catch-up \
             ({default_catch_up_ms} ms vs {large_catch_up_ms} ms for {BACKLOG_ENTRIES} entries) were not \
             meaningfully worse than reproducing openraft's own 300-entry default on this benchmark's \
             small-entry workload."
        )
    };

    Ok(json!({
        "method": format!(
            "two 3-voter real-process clusters, one per json_body_limit value. Throughput: {THROUGHPUT_WRITERS} \
             concurrent one-key Put writers (tiny values) against the leader for {THROUGHPUT_DURATION:?}. \
             Catch-up: SIGKILL a follower, write {BACKLOG_ENTRIES} more small Put transactions ({} - under \
             the default replication_lag_threshold of 500, so the leader replicates by ordinary log entries, \
             never a snapshot install), restart the follower on the same data directory (same pattern as \
             examples/kvstore_kill_follower_test.rs), and time from restart until its own local view reflects \
             the last entry.",
            BACKLOG_ENTRIES
        ),
        "default_json_body_limit": default_config,
        "openraft_default_equivalent_json_body_limit": large_config,
        "decision": decision,
        "design_change_proposal": "if a production write rate needs both a small json_body_limit (bounding \
            worst-case request size for this deployment) and many small entries per append round trip, split \
            an oversized append by *bytes* inside Network::append_entries and answer openraft's \
            AppendEntriesResponse::PartialSuccess, so small entries still batch by the hundreds while the \
            body limit continues to bound the worst case. This is evidence and a proposal only - ech0-media#36 \
            does not implement it, per its own acceptance criteria.",
        "caveat": "localhost, one shared disk, tiny entries: the derived cap of 4 costs the most exactly when \
            entries are small and numerous (many round trips for little payload each) - a workload of \
            already-large entries would saturate the byte cap first regardless of the entry-count cap, and \
            would show a smaller gap here.",
    }))
}

async fn measure_config(example: &Example, label: &str, json_body_limit: u64) -> io::Result<Value> {
    let limit_arg = format!("--json-body-limit={json_body_limit}");
    let extra_args = [limit_arg.as_str()];

    let throughput = measure_throughput(example, &extra_args).await?;
    let catch_up_ms = measure_catch_up(example, &extra_args).await?;

    Ok(json!({
        "label": label,
        "json_body_limit": json_body_limit,
        "throughput_writes_per_sec": throughput,
        "catch_up_ms": catch_up_ms,
    }))
}

async fn measure_throughput(example: &Example, extra_args: &[&str]) -> io::Result<f64> {
    let bench_cluster = Cluster::start(example, 3, extra_args).await?;
    let (_, leader_api, _) = bench_cluster.leader(Instant::now() + cluster::READY).await?;

    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..THROUGHPUT_WRITERS {
        let api = leader_api.clone();
        tasks.spawn(async move {
            let mut completed = 0u64;
            let mut i = 0u64;
            while started.elapsed() < THROUGHPUT_DURATION {
                let key = format!("bench/body_limit/throughput/{worker}/{i}");
                if write_once(&api, &key, "v").await.is_ok() {
                    completed += 1;
                }
                i += 1;
            }
            completed
        });
    }
    let mut total = 0u64;
    while let Some(result) = tasks.join_next().await {
        total += result.unwrap_or(0);
    }
    Ok(total as f64 / started.elapsed().as_secs_f64())
}

async fn measure_catch_up(example: &Example, extra_args: &[&str]) -> io::Result<u64> {
    let mut bench_cluster = Cluster::start(example, 3, extra_args).await?;
    let deadline = Instant::now() + cluster::READY;
    let (leader_index, leader_api, _) = bench_cluster.leader(deadline).await?;
    let follower_index = (0..bench_cluster.apis.len()).find(|&i| i != leader_index).expect("3 nodes, 1 leader");

    bench_cluster.nodes[follower_index].kill()?;

    let last_key = "bench/body_limit/catch_up/last";
    fill_backlog(&leader_api, BACKLOG_ENTRIES, last_key).await?;

    let addr = bench_cluster.addrs[follower_index].clone();
    let api = bench_cluster.apis[follower_index].clone();
    // The same configuration the rest of this cluster runs, so the restarted follower's own
    // `json_body_limit` (and therefore `max_payload_entries`) matches what is being measured.
    let restart_example = example.clone().node_args(extra_args);
    let restart_started = Instant::now();
    // Restart on the same address and data directory - `EzKv::join`'s own docs: a node with a
    // persisted id takes over regardless of `seed`, the same pattern
    // examples/kvstore_kill_follower_test.rs uses for a SIGKILL-and-restart test.
    bench_cluster.nodes[follower_index] = restart_example.spawn(&addr, &api, None)?;

    api_client::wait_for_value(&api, last_key, Some("v"), Instant::now() + CATCH_UP_DEADLINE)
        .await
        .map_err(|e| io::Error::other(format!("restarted follower never caught up: {e}")))?;

    Ok(restart_started.elapsed().as_millis() as u64)
}

async fn fill_backlog(api: &str, count: usize, last_key: &str) -> io::Result<()> {
    let share = count / BACKLOG_FILL_CONCURRENCY;
    let remainder = count % BACKLOG_FILL_CONCURRENCY;
    let mut tasks = JoinSet::new();
    for worker in 0..BACKLOG_FILL_CONCURRENCY {
        let api = api.to_string();
        let n = share + usize::from(worker < remainder);
        tasks.spawn(async move {
            for i in 0..n {
                let key = format!("bench/body_limit/catch_up/{worker}/{i}");
                write_once(&api, &key, "v").await?;
            }
            Ok::<_, io::Error>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap_or_else(|e| Err(io::Error::other(e)))?;
    }
    write_once(api, last_key, "v").await
}

async fn write_once(api: &str, key: &str, value: &str) -> io::Result<()> {
    let deadline = Instant::now() + WRITE_DEADLINE;
    match api_client::write(&[api], api_client::put(key, value), deadline).await? {
        api_client::TxOutcome::Success { .. } => Ok(()),
        api_client::TxOutcome::Conflict(v) => Err(io::Error::other(format!("unexpected conflict: {v}"))),
    }
}
