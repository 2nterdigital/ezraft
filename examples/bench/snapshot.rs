//! ech0-media#36: the snapshot timeout - build, transfer and install time against snapshot size,
//! and the timeout that covers it with margin.
//!
//! Snapshot build/transfer/install timing is not exposed through the kvstore example's HTTP API;
//! ech0-media#26 already logs it as structured `tracing` events on the storage/network paths, so
//! this scenario reads those back from each node's own log file (redirected there by
//! `harness::Example::spawn`) instead of adding a route for it - the ticket's own escape hatch,
//! applied to logs already emitted rather than a new HTTP surface.

use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use serde_json::Value;
use serde_json::json;
use tokio::task::JoinSet;

use crate::api_client;
use crate::cluster;
use crate::cluster::Cluster;
use crate::harness::Example;

const FILL_CONCURRENCY: usize = 16;
const LOG_FILTER: &str = "warn,ezraft::storage::adapter::state_machine=info,ezraft::network=info";
const WRITE_DEADLINE: Duration = Duration::from_secs(30);
const JOIN_DEADLINE: Duration = Duration::from_secs(60);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

struct Tier {
    /// A short, path/key-safe id.
    id: &'static str,
    snapshot_interval: u64,
    value_bytes: usize,
}

const TIERS: [Tier; 3] = [
    Tier {
        id: "small",
        snapshot_interval: 100,
        value_bytes: 4 * 1024,
    },
    Tier {
        id: "medium",
        snapshot_interval: 500,
        value_bytes: 16 * 1024,
    },
    Tier {
        id: "large",
        snapshot_interval: 800,
        value_bytes: 64 * 1024,
    },
];

pub async fn run(example: &Example) -> io::Result<Value> {
    let mut tiers = Vec::with_capacity(TIERS.len());
    for tier in &TIERS {
        tiers.push(run_tier(example, tier).await?);
    }

    let max_elapsed_ms = tiers.iter().filter_map(|t| t["sent_snapshot_elapsed_ms"].as_u64()).max().unwrap_or(1);
    let margin = DEFAULT_TIMEOUT.as_millis() as f64 / max_elapsed_ms.max(1) as f64;
    let decision = format!(
        "keep the default at {DEFAULT_TIMEOUT:?}: the slowest observed transfer+install (the largest tier) \
         took {max_elapsed_ms} ms, a {margin:.1}x margin under the {DEFAULT_TIMEOUT:?} budget on this machine."
    );

    Ok(json!({
        "method": format!(
            "one fresh 3-voter cluster per size tier, --snapshot-interval set so exactly one automatic \
             snapshot covers approximately the whole tier; write that many Put transactions of the tier's \
             value size ({FILL_CONCURRENCY}-way concurrent), then join a brand-new 4th node, which always \
             needs the full snapshot (everything before it was already purged). Build time: the leader's own \
             \"built a snapshot\" log line (elapsed_ms, size_bytes). Transfer+install, as observed by the \
             sender (who waits for the receiver's install to finish before its own request returns): the \
             leader's \"sent a snapshot\" log line. Install only: the joiner's own \"installed a snapshot\" \
             log line. Transfer alone is approximately sent minus installed."
        ),
        "tiers": tiers,
        "configured_snapshot_max_bytes_mib": 64,
        "current_default_timeout_secs": DEFAULT_TIMEOUT.as_secs(),
        "decision": decision,
        "caveat": "localhost, one shared physical disk for all three voters, and the receiving node's decode \
            time is included in the sender's own \"sent\" elapsed - a real deployment's network transfer time \
            adds on top of, not instead of, the install time measured here. The largest tier is sized well \
            under snapshot_max_bytes (64 MiB) with margin, not at the ceiling, since writing a full 64 MiB of \
            maximally-sized transactions would not fit this benchmark's own wall-clock budget.",
    }))
}

async fn run_tier(example: &Example, tier: &Tier) -> io::Result<Value> {
    let interval_arg = format!("--snapshot-interval={}", tier.snapshot_interval);
    let log_filter_arg = format!("--log-filter={LOG_FILTER}");
    let extra_args = [interval_arg.as_str(), log_filter_arg.as_str()];

    let mut bench_cluster = Cluster::start(example, 3, &extra_args).await?;
    let (leader_index, leader_api, _) = bench_cluster.leader(Instant::now() + cluster::READY).await?;

    let value = "x".repeat(tier.value_bytes);
    let last_key = format!("bench/snapshot/{}/last", tier.id);
    fill(&leader_api, tier.snapshot_interval as usize, &value, &last_key).await?;

    let leader_log = example.log_path(&bench_cluster.addrs[leader_index]);
    let built = wait_for_log_event(&leader_log, "built a snapshot", Duration::from_secs(15)).await?;

    let (joiner_addr, _joiner_api) =
        bench_cluster.join(example, &extra_args, &last_key, &value, Instant::now() + JOIN_DEADLINE).await?;

    let joiner_log = example.log_path(&joiner_addr);
    let installed = wait_for_log_event(&joiner_log, "installed a snapshot", Duration::from_secs(10)).await?;
    let sent = wait_for_log_event(&leader_log, "sent a snapshot", Duration::from_secs(10)).await?;

    Ok(json!({
        "tier": tier.id,
        "snapshot_interval": tier.snapshot_interval,
        "value_bytes": tier.value_bytes,
        "build_elapsed_ms": built.elapsed_ms,
        "build_size_bytes": built.size_bytes,
        "sent_snapshot_elapsed_ms": sent.elapsed_ms,
        "installed_elapsed_ms": installed.elapsed_ms,
        "installed_size_bytes": installed.size_bytes,
        "transfer_only_estimate_ms": sent.elapsed_ms.saturating_sub(installed.elapsed_ms),
    }))
}

async fn fill(api: &str, count: usize, value: &str, last_key: &str) -> io::Result<()> {
    let share = count / FILL_CONCURRENCY;
    let remainder = count % FILL_CONCURRENCY;
    let mut tasks = JoinSet::new();
    for worker in 0..FILL_CONCURRENCY {
        let api = api.to_string();
        let value = value.to_string();
        let n = share + usize::from(worker < remainder);
        tasks.spawn(async move {
            for i in 0..n {
                let key = format!("bench/snapshot/fill/{worker}/{i}");
                write_once(&api, &key, &value).await?;
            }
            Ok::<_, io::Error>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap_or_else(|e| Err(io::Error::other(e)))?;
    }
    // One more write, distinctly keyed, so a joiner's readiness wait has an exact value to match.
    write_once(api, last_key, value).await
}

async fn write_once(api: &str, key: &str, value: &str) -> io::Result<()> {
    let deadline = Instant::now() + WRITE_DEADLINE;
    match api_client::write(&[api], api_client::put(key, value), deadline).await? {
        api_client::TxOutcome::Success { .. } => Ok(()),
        api_client::TxOutcome::Conflict(v) => Err(io::Error::other(format!("unexpected conflict: {v}"))),
    }
}

struct LogEvent {
    elapsed_ms: u64,
    size_bytes: u64,
}

/// Poll `path` (a node's log file) until a line containing `needle` appears, and parse its
/// `elapsed_ms`/`size_bytes` fields - see [`parse_field`].
async fn wait_for_log_event(path: &Path, needle: &str, timeout: Duration) -> io::Result<LogEvent> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(text) = fs::read_to_string(path)
            && let Some(line) = text.lines().rev().find(|line| line.contains(needle))
        {
            let elapsed_ms = parse_field(line, "elapsed_ms")
                .ok_or_else(|| io::Error::other(format!("no elapsed_ms in log line: {line}")))?;
            let size_bytes = parse_field(line, "size_bytes").unwrap_or(0);
            return Ok(LogEvent { elapsed_ms, size_bytes });
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "no {path:?} log line containing {needle:?} appeared within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Extract the value after `field=` in one `tracing_subscriber::fmt` log line - digits only, the
/// shape every field this scenario reads (`elapsed_ms`, `size_bytes`) always has.
fn parse_field(line: &str, field: &str) -> Option<u64> {
    let marker = format!("{field}=");
    let start = line.find(&marker)? + marker.len();
    let digits: String = line[start..].chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}
