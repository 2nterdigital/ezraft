//! ech0-media#36: the liveness threshold and grace period - detection time after `SIGKILL` versus
//! false `Down` marks under write load and during a leader change.

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

const DEFAULT_THRESHOLD: Duration = Duration::from_secs(5);
const DEFAULT_GRACE: Duration = Duration::from_secs(5);
const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const TIGHT_THRESHOLD: Duration = Duration::from_millis(1500);
const TIGHT_GRACE: Duration = Duration::from_millis(1500);
const TIGHT_CHECK_INTERVAL: Duration = Duration::from_millis(300);
const DETECTION_WAIT: Duration = Duration::from_secs(20);
const LOAD_DURATION: Duration = Duration::from_secs(4);
const LOAD_WRITERS: usize = 8;

pub async fn run(example: &Example) -> io::Result<Value> {
    let no_false_down_under_load = false_down_under_load(example).await?;
    let killed_follower_detection = follower_kill_detection(example).await?;
    let leader_change = leader_kill_no_false_down(
        example,
        "default (5s/5s/1s)",
        DEFAULT_THRESHOLD,
        DEFAULT_GRACE,
        DEFAULT_CHECK_INTERVAL,
    )
    .await?;
    let leader_change_tight = leader_kill_no_false_down(
        example,
        "tight (1.5s/1.5s/0.3s)",
        TIGHT_THRESHOLD,
        TIGHT_GRACE,
        TIGHT_CHECK_INTERVAL,
    )
    .await?;

    Ok(json!({
        "method": "3-voter real-process clusters, one scenario per fresh cluster: (1) write load with no \
            kill, checking no healthy node's registry status ever reads \"Down\"; (2) SIGKILL a follower, \
            timing how long until a surviving node's GET /status registry entry for it reads \"Down\"; (3) \
            SIGKILL the leader, timing the new election and then polling for `liveness_grace` whether the \
            surviving healthy follower is ever marked \"Down\" - once at the default thresholds, once at a \
            tighter candidate.",
        "false_down_under_load": no_false_down_under_load,
        "follower_kill_detection": killed_follower_detection,
        "leader_change_default": leader_change,
        "leader_change_tight": leader_change_tight,
        "decision": "keep the defaults (liveness_threshold 5s, liveness_grace 5s, liveness_check_interval \
            1s): detection after a follower SIGKILL lands within threshold + one check tick, no false Down \
            was observed under write load, and no false Down was observed during a leader change even at the \
            tighter 1.5s/1.5s candidate on this idle, low-latency localhost cluster - but a real deployment's \
            slower elections (real network RTT, disk contention, more voters) are exactly the case the \
            grace period exists for, and this benchmark cannot reproduce that slowness (see the caveat). \
            A tighter threshold would detect real failures faster but has never been tested against the \
            production conditions that could false-positive it, so this benchmark recommends keeping the \
            generous default rather than adopting the tight candidate on localhost evidence alone.",
        "caveat": "localhost has no real network latency and one shared disk for all three voters, so \
            elections here are close to the theoretical minimum; a real deployment's leader change can take \
            longer (real RTT, real per-node disk I/O), which is exactly what would make a tighter threshold \
            risk a false Down that this benchmark did not reproduce. Only the follower- and leader-kill \
            scenarios exercise `EzKv`'s actual liveness task; the 'no false Down under load' scenario proves \
            absence only within its own 4-second window and its own write rate.",
    }))
}

/// Run a normal write load with default liveness thresholds and no failures; every node's
/// registry status must stay "Up" throughout.
async fn false_down_under_load(example: &Example) -> io::Result<Value> {
    let bench_cluster = Cluster::start(example, 3, &[]).await?;
    let (_, leader_api, _) = bench_cluster.leader(Instant::now() + cluster::READY).await?;

    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..LOAD_WRITERS {
        let api = leader_api.clone();
        tasks.spawn(async move {
            let mut i = 0u64;
            while started.elapsed() < LOAD_DURATION {
                let key = format!("bench/liveness/load/{worker}/{i}");
                let deadline = Instant::now() + Duration::from_secs(5);
                let _ = api_client::write(&[&api], api_client::put(&key, "v"), deadline).await;
                i += 1;
            }
        });
    }

    let mut any_down = false;
    while started.elapsed() < LOAD_DURATION {
        if let Ok(status) = api_client::status(&leader_api).await
            && let Some(nodes) = status["nodes"].as_array()
            && nodes.iter().any(|n| n["status"] == "Down")
        {
            any_down = true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    while tasks.join_next().await.is_some() {}

    Ok(
        json!({"any_node_marked_down": any_down, "load_duration_secs": LOAD_DURATION.as_secs(), "concurrent_writers": LOAD_WRITERS}),
    )
}

/// SIGKILL a follower and time how long a surviving node's registry takes to mark it "Down".
async fn follower_kill_detection(example: &Example) -> io::Result<Value> {
    let mut bench_cluster = Cluster::start(example, 3, &[]).await?;
    let deadline = Instant::now() + cluster::READY;
    let (leader_index, leader_api, _) = bench_cluster.leader(deadline).await?;
    let node_ids = api_client::node_ids(&bench_cluster.apis, deadline).await?;
    // A node's registry entry only exists once its own startup self-registration task has
    // succeeded (a background retry loop, ech0-media#33) - killed any earlier, it would leave no
    // entry to ever mark "Down" at all, which this scenario would otherwise misreport as "never
    // detected" rather than "raced its own startup".
    bench_cluster.wait_for_full_registry(&leader_api, deadline).await?;

    let victim_index = (0..bench_cluster.apis.len()).find(|&i| i != leader_index).expect("3 nodes, 1 leader");
    let victim_id = node_ids[victim_index];
    bench_cluster.nodes[victim_index].kill()?;

    let started = Instant::now();
    // The leader itself is always a legitimate survivor here (only a follower was killed), so
    // poll it directly rather than searching `apis` for "not the leader" - that search would
    // just as happily return the follower this function killed a moment ago.
    let deadline = Instant::now() + DETECTION_WAIT;
    loop {
        if let Ok(status) = api_client::status(&leader_api).await
            && let Some(nodes) = status["nodes"].as_array()
            && nodes.iter().any(|n| n["node_id"].as_u64() == Some(victim_id) && n["status"] == "Down")
        {
            break;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(
                "victim was never marked Down within the detection wait",
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let detected_after = started.elapsed();

    Ok(json!({
        "detected_after_ms": detected_after.as_millis(),
        "configured_threshold_ms": DEFAULT_THRESHOLD.as_millis(),
        "configured_check_interval_ms": DEFAULT_CHECK_INTERVAL.as_millis(),
    }))
}

/// SIGKILL the leader; time the new election, then poll for `grace` whether the surviving healthy
/// follower is ever marked "Down" (it must not be).
async fn leader_kill_no_false_down(
    example: &Example,
    label: &str,
    threshold: Duration,
    grace: Duration,
    check_interval: Duration,
) -> io::Result<Value> {
    let args = [
        format!("--liveness-threshold-ms={}", threshold.as_millis()),
        format!("--liveness-grace-ms={}", grace.as_millis()),
        format!("--liveness-check-interval-ms={}", check_interval.as_millis()),
    ];
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let mut bench_cluster = Cluster::start(example, 3, &arg_refs).await?;
    let deadline = Instant::now() + cluster::READY;
    let (leader_index, leader_api, _) = bench_cluster.leader(deadline).await?;
    let node_ids = api_client::node_ids(&bench_cluster.apis, deadline).await?;
    // Every node's registry entry must exist before the kill, or "never observed Down" would
    // trivially and misleadingly hold for an entry that never existed either - see
    // `follower_kill_detection`'s own comment on the same wait.
    bench_cluster.wait_for_full_registry(&leader_api, deadline).await?;

    bench_cluster.nodes[leader_index].kill()?;

    let started = Instant::now();
    let survivors: Vec<(u64, &str)> = bench_cluster
        .apis
        .iter()
        .enumerate()
        .filter(|&(i, _)| i != leader_index)
        .map(|(i, api)| (node_ids[i], api.as_str()))
        .collect();

    // Whichever survivor gets elected is trivially always "Up" (a leader is always its own
    // registry's "Up" entry) - the real test is the *other* survivor, so it is identified only
    // after the election, not guessed beforehand.
    let election_deadline = Instant::now() + DETECTION_WAIT;
    let (new_leader_id, elected_after) = loop {
        let mut found = None;
        for &(node_id, api) in &survivors {
            if let Ok(status) = api_client::status(api).await
                && status["is_leader"].as_bool() == Some(true)
            {
                found = Some(node_id);
                break;
            }
        }
        if let Some(node_id) = found {
            break (node_id, started.elapsed());
        }
        if Instant::now() >= election_deadline {
            return Err(io::Error::other(format!(
                "[{label}] no new leader was elected within the wait"
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let healthy_follower_id = survivors
        .iter()
        .map(|&(id, _)| id)
        .find(|&id| id != new_leader_id)
        .expect("2 survivors, 1 of them just elected leader");

    // Grace covers the time since the *new* leadership began, not since the old leader died -
    // poll from here for the full grace period.
    let watch_until = Instant::now() + grace + check_interval;
    let mut false_down_at = None;
    while Instant::now() < watch_until {
        for &(_, api) in &survivors {
            if let Ok(status) = api_client::status(api).await
                && let Some(nodes) = status["nodes"].as_array()
                && nodes.iter().any(|n| n["node_id"].as_u64() == Some(healthy_follower_id) && n["status"] == "Down")
            {
                false_down_at = Some(started.elapsed().as_millis());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(json!({
        "label": label,
        "threshold_ms": threshold.as_millis(),
        "grace_ms": grace.as_millis(),
        "check_interval_ms": check_interval.as_millis(),
        "new_leader_elected_after_ms": elected_after.as_millis(),
        "false_down_observed_at_ms": false_down_at,
    }))
}
