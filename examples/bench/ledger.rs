//! ech0-media#36: idempotency ledger capacity - memory per entry, apply cost, and the capacity
//! that covers `write_deadline` at a realistic write rate with margin.
//!
//! Every write gets a ledger entry, not only a retried one (`KvState::apply_idempotent` consults
//! it before *every* command), so an empty transaction (`{"conditions": [], "ops": []}`) grows
//! the ledger by exactly one entry per write while leaving the user KV state untouched
//! (ech0-media#30's own module docs; `Txn::validate` places no minimum on op count, so this is a
//! legitimate write, not a synthetic shortcut). One confound the external API cannot remove: every
//! successful transaction - even an empty one - also appends one group to this node's *watch*
//! retained window (ech0-media#32's own module docs: "每个成功事务都有一组（可为空）"), so the
//! memory delta measured here is ledger *and* watch-window growth combined, not the ledger alone -
//! see the caveat in the result for why isolating them exactly was not attempted (this
//! benchmark's own fsync scenario found writes serialize regardless of concurrency, at roughly one
//! write every 15 ms on this machine, so reaching the watch window's own 10,000-group cap to let
//! it plateau first, as an earlier version of this scenario attempted, would have taken minutes
//! per run - not a repeatable benchmark on a dev machine).

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

const WARMUP_WRITES: usize = 50;
const SAMPLE_WRITES: usize = 2_000;
const FILL_CONCURRENCY: usize = 32;
const DEFAULT_LEDGER_CAPACITY: usize = 100_000;
const DEFAULT_WRITE_DEADLINE: Duration = Duration::from_secs(10);

fn empty_txn() -> Value {
    json!({"conditions": [], "ops": []})
}

pub async fn run(example: &Example, concurrent_writes_per_sec: f64) -> io::Result<Value> {
    let bench_cluster = Cluster::start(example, 3, &[]).await?;
    let (leader_index, leader_api, leader_pid) = bench_cluster.leader(Instant::now() + cluster::READY).await?;

    // Warm up sequentially: let the process settle (allocator arenas, lazily-initialized
    // buffers) before the baseline reading.
    for _ in 0..WARMUP_WRITES {
        write_empty(&leader_api).await?;
    }
    let baseline_rss_kib = cluster::rss_kib(leader_pid);

    let started = Instant::now();
    fill_concurrently(&leader_api, SAMPLE_WRITES).await?;
    let fill_elapsed = started.elapsed();
    let after_rss_kib = cluster::rss_kib(leader_pid);

    let combined_bytes_per_entry = match (baseline_rss_kib, after_rss_kib) {
        (Some(before), Some(after)) if after >= before => Some((after - before) as f64 * 1024.0 / SAMPLE_WRITES as f64),
        _ => None,
    };
    let empty_txn_writes_per_sec = SAMPLE_WRITES as f64 / fill_elapsed.as_secs_f64();

    // The ledger's purpose (ech0-media#30) is answering a retry, under the same request id,
    // within `write_deadline` of the original write - so the capacity that matters is how many
    // *other* writes the whole cluster can commit in that window, at the highest sustained write
    // rate this benchmark measured (`fsync`'s concurrent Put figure), with margin for a slow
    // client plus ordinary operational headroom. Using the *combined* (ledger + watch window)
    // bytes-per-entry figure as the per-entry cost is conservative - it overestimates the
    // ledger's own share, which only pushes the capacity math toward keeping more margin, never
    // less.
    const MARGIN: f64 = 3.0;
    let needed_capacity = (concurrent_writes_per_sec * DEFAULT_WRITE_DEADLINE.as_secs_f64() * MARGIN).ceil() as u64;

    let projected_full_ledger_mib =
        combined_bytes_per_entry.map(|b| b * DEFAULT_LEDGER_CAPACITY as f64 / (1024.0 * 1024.0));

    let decision = if needed_capacity <= DEFAULT_LEDGER_CAPACITY as u64 {
        format!(
            "keep the default at {DEFAULT_LEDGER_CAPACITY} entries: {needed_capacity} entries would cover \
             write_deadline ({DEFAULT_WRITE_DEADLINE:?}) at the measured concurrent write rate \
             ({concurrent_writes_per_sec:.0}/s, from the fsync scenario) with a {MARGIN}x margin, comfortably \
             under the default."
        )
    } else {
        format!(
            "raise the default from {DEFAULT_LEDGER_CAPACITY} to at least {needed_capacity} entries: that many \
             would be needed to cover write_deadline ({DEFAULT_WRITE_DEADLINE:?}) at the measured concurrent \
             write rate ({concurrent_writes_per_sec:.0}/s) with a {MARGIN}x margin."
        )
    };

    Ok(json!({
        "method": format!(
            "3-voter real-process cluster; {WARMUP_WRITES} warmup writes then a baseline RSS reading, then \
             {SAMPLE_WRITES} more ({FILL_CONCURRENCY}-way concurrent, though writes serialize regardless - \
             see this module's own doc comment) empty transactions ({{\"conditions\": [], \"ops\": []}}) \
             against the leader (node index {leader_index}), each with its own request id, and a final RSS \
             reading. Memory per entry: (final RSS - baseline RSS) / {SAMPLE_WRITES}, via `ps -o rss=` on the \
             leader's OS pid."
        ),
        "sample_writes": SAMPLE_WRITES,
        "combined_ledger_and_watch_window_bytes_per_entry": combined_bytes_per_entry,
        "projected_full_ledger_mib_at_default_capacity": projected_full_ledger_mib,
        "empty_txn_writes_per_sec": empty_txn_writes_per_sec,
        "fsync_scenario_concurrent_writes_per_sec": concurrent_writes_per_sec,
        "assumed_write_deadline_secs": DEFAULT_WRITE_DEADLINE.as_secs(),
        "needed_capacity_for_margin": needed_capacity,
        "current_default_capacity": DEFAULT_LEDGER_CAPACITY,
        "decision": decision,
        "caveat": "the reported bytes-per-entry is ledger growth *and* watch-retained-window growth combined \
            (both grow on every successful transaction, including an empty one - see this module's own doc \
            comment) - not the ledger alone, and is therefore a conservative (upper-bound) input to the \
            capacity math above, not an exact isolated figure. Apply cost (the ledger's own lookup-then-insert \
            inside `apply`) is likewise not separable from network round trip and fsync cost through the \
            external HTTP API alone; comparing empty_txn_writes_per_sec here to the fsync scenario's Put-based \
            concurrent figure - both roughly equal - is this benchmark's evidence that the ledger is not the \
            throughput bottleneck (fsync is, see the fsync scenario), rather than a direct, isolated timing of \
            apply_idempotent itself.",
    }))
}

/// Write exactly `count` empty transactions against `api`, split across [`FILL_CONCURRENCY`]
/// concurrent tasks (any remainder going to the first few) - concurrency does not raise
/// throughput here (writes serialize on the leader's own append pipeline, see this module's own
/// doc comment), but it does keep this benchmark's own wall-clock time closer to the achievable
/// rate than a single sequential loop with its own added round-trip overhead would.
async fn fill_concurrently(api: &str, count: usize) -> io::Result<()> {
    let share = count / FILL_CONCURRENCY;
    let remainder = count % FILL_CONCURRENCY;
    let mut tasks = JoinSet::new();
    for worker in 0..FILL_CONCURRENCY {
        let api = api.to_string();
        let n = share + usize::from(worker < remainder);
        tasks.spawn(async move {
            for _ in 0..n {
                write_empty(&api).await?;
            }
            Ok::<_, io::Error>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap_or_else(|e| Err(io::Error::other(e)))?;
    }
    Ok(())
}

async fn write_empty(api: &str) -> io::Result<()> {
    let deadline = Instant::now() + DEFAULT_WRITE_DEADLINE;
    match api_client::write(&[api], empty_txn(), deadline).await? {
        api_client::TxOutcome::Success { .. } => Ok(()),
        api_client::TxOutcome::Conflict(value) => Err(io::Error::other(format!("unexpected conflict: {value}"))),
    }
}
