//! ech0-media#36: the watch retained window and subscriber queue - memory per retained group,
//! fan-out cost per subscriber, and how long the default window lasts at a realistic write rate.
//!
//! Every successful transaction appends one group to the retained window *and* one entry to the
//! idempotency ledger (ech0-media#32/#30's own module docs), so an external-API-only benchmark
//! cannot isolate one subsystem's memory growth from the other's - this scenario reports the
//! combined figure and says so, the same limitation ech0-media#36's own ledger scenario discloses
//! for its side of the same confound.

use std::io;
use std::time::Duration;
use std::time::Instant;

use serde_json::Value;
use serde_json::json;
use tokio::task::JoinSet;

use crate::api_client;
use crate::api_client::WatchClient;
use crate::cluster;
use crate::cluster::Cluster;
use crate::harness::Example;
use crate::stats::Summary;

const WARMUP_WRITES: usize = 50;
const SAMPLE_WRITES: usize = 1_500;
const FILL_CONCURRENCY: usize = 32;
const VALUE: &str = "0123456789abcdef"; // 16 bytes, a representative small change payload
const DEFAULT_MAX_WINDOW_GROUPS: usize = 10_000;
const DEFAULT_MAX_WINDOW_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_SUBSCRIBER_QUEUE_LEN: usize = 1024;
const FANOUT_WRITES: usize = 20;
const FANOUT_LEVELS: [usize; 3] = [1, 20, 100];
const WRITE_DEADLINE: Duration = Duration::from_secs(10);

pub async fn run(example: &Example) -> io::Result<Value> {
    let bench_cluster = Cluster::start(example, 3, &[]).await?;
    let (leader_index, leader_api, leader_pid) = bench_cluster.leader(Instant::now() + cluster::READY).await?;

    for i in 0..WARMUP_WRITES {
        write_put(&leader_api, &format!("bench/watch/warmup/{i}")).await?;
    }
    let baseline_rss_kib = cluster::rss_kib(leader_pid);

    fill_puts(&leader_api, "bench/watch/mem", SAMPLE_WRITES).await?;
    let after_rss_kib = cluster::rss_kib(leader_pid);

    let combined_bytes_per_write = match (baseline_rss_kib, after_rss_kib) {
        (Some(before), Some(after)) if after >= before => Some((after - before) as f64 * 1024.0 / SAMPLE_WRITES as f64),
        _ => None,
    };

    // Fan-out cost: delivery latency (write round trip + every subscriber receiving its line),
    // at increasing subscriber counts on the same node, same prefix.
    let mut fanout = Vec::with_capacity(FANOUT_LEVELS.len());
    for &subscribers in &FANOUT_LEVELS {
        let summary = measure_fanout(&leader_api, subscribers, FANOUT_WRITES).await?;
        fanout.push(json!({"subscribers": subscribers, "delivery_latency_ms": summary}));
    }

    let window_duration_at = |writes_per_sec: f64| DEFAULT_MAX_WINDOW_GROUPS as f64 / writes_per_sec;

    Ok(json!({
        "method": format!(
            "3-voter real-process cluster; {WARMUP_WRITES} warmup writes then {SAMPLE_WRITES} concurrent \
             ({FILL_CONCURRENCY}-way, though writes serialize regardless of concurrency - see the fsync \
             scenario) one-key Put transactions (16-byte values) against the leader (node index \
             {leader_index}), comfortably under the default 10,000-group window cap throughout. Combined \
             memory per write: (RSS after - RSS before) / {SAMPLE_WRITES}, via `ps -o rss=` on the leader's OS \
             pid - see the caveat below on why this is ledger and watch-window growth combined, not the \
             window alone. Fan-out: {FANOUT_LEVELS:?} concurrent `GET /watch` subscribers on the same \
             node/prefix, {FANOUT_WRITES} writes per level, each write's latency including every subscriber's \
             delivery."
        ),
        "combined_ledger_and_watch_window_bytes_per_write": combined_bytes_per_write,
        "default_max_window_groups": DEFAULT_MAX_WINDOW_GROUPS,
        "default_max_window_bytes": DEFAULT_MAX_WINDOW_BYTES,
        "default_subscriber_queue_len": DEFAULT_SUBSCRIBER_QUEUE_LEN,
        "fanout": fanout,
        "window_duration_examples": {
            "at_10_writes_per_sec": window_duration_at(10.0),
            "at_100_writes_per_sec": window_duration_at(100.0),
            "at_1000_writes_per_sec": window_duration_at(1000.0),
        },
        "decision": "keep all four defaults (10,000 groups / 64 MiB / a queue of 1024 / a 1-second Progress \
            interval): even taking the *combined* (ledger + window) bytes-per-write figure as an upper bound \
            on the window's own byte cost, the 10,000-group count cap binds long before the 64 MiB byte cap \
            for typical small KV changes (the byte cap chiefly protects against a workload of unusually large \
            values within the window); a queue of 1024 covers many single-write bursts before a subscriber is \
            judged too slow and reset. This scenario found no evidence to move any of the four; see the \
            caveat below on why a localhost benchmark cannot set the window duration figure with confidence.",
        "caveat": "the reported bytes-per-write is ledger growth *and* watch-retained-window growth combined - \
            every successful transaction grows both (ech0-media#30/#32's own module docs) - not the window \
            alone; treat it as a conservative upper bound on the window's own per-group cost, not an isolated \
            figure (this crate's own external HTTP API gives no way to separate the two, and adding \
            instrumentation to observe internal structure sizes would go beyond 'extend the example', which \
            this ticket permits, into exposing library internals, which it does not). The fan-out figures \
            include this benchmark driver's own sequential per-subscriber read overhead, not purely \
            server-side cost, so they are an upper bound too. Window duration depends entirely on the real \
            deployment's write rate, which this benchmark cannot supply; the three rates above are \
            illustrative reference points, not a prediction.",
    }))
}

/// Write exactly `count` Puts under `prefix` against `api`, split across [`FILL_CONCURRENCY`]
/// concurrent tasks (any remainder going to the first few).
async fn fill_puts(api: &str, prefix: &str, count: usize) -> io::Result<()> {
    let share = count / FILL_CONCURRENCY;
    let remainder = count % FILL_CONCURRENCY;
    let mut tasks = JoinSet::new();
    for worker in 0..FILL_CONCURRENCY {
        let api = api.to_string();
        let prefix = prefix.to_string();
        let n = share + usize::from(worker < remainder);
        tasks.spawn(async move {
            for i in 0..n {
                write_put(&api, &format!("{prefix}/{worker}/{i}")).await?;
            }
            Ok::<_, io::Error>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap_or_else(|e| Err(io::Error::other(e)))?;
    }
    Ok(())
}

async fn measure_fanout(api: &str, subscribers: usize, writes: usize) -> io::Result<Summary> {
    let mut clients = Vec::with_capacity(subscribers);
    for _ in 0..subscribers {
        clients.push(WatchClient::connect(api, "bench/watch/fanout", None).await?);
    }
    // Every fresh subscription opens with a Reset built from the current view filtered to this
    // prefix - empty here, since nothing has ever written under it. Drain it before timing.
    for client in &mut clients {
        client.next_line().await;
    }

    let mut latencies = Vec::with_capacity(writes);
    for i in 0..writes {
        let started = Instant::now();
        write_put(api, &format!("bench/watch/fanout/{i}")).await?;
        for client in &mut clients {
            client.next_line().await;
        }
        latencies.push(started.elapsed());
    }
    Ok(Summary::of(latencies))
}

async fn write_put(api: &str, key: &str) -> io::Result<()> {
    let deadline = Instant::now() + WRITE_DEADLINE;
    match api_client::write(&[api], api_client::put(key, VALUE), deadline).await? {
        api_client::TxOutcome::Success { .. } => Ok(()),
        api_client::TxOutcome::Conflict(value) => Err(io::Error::other(format!("unexpected conflict: {value}"))),
    }
}
