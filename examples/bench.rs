//! ech0-media#36: repeatable benchmark driver for the foundation's measured defaults.
//!
//! Builds the `kvstore` example in release mode and drives it as a set of real, three-node (or
//! more) process clusters on this machine, exercising only its external HTTP API - plus a
//! handful of CLI flags this ticket added to that example (see `examples/kvstore.rs`'s `Args`),
//! and OS-level facts about its processes (`ps`, `lsof`) that the HTTP API cannot expose - never
//! the library directly. Prints one JSON object to stdout: the environment this ran on, then one
//! section per ticket item, each with its own method, numbers, and a decided default.
//!
//! Run with `make bench`, or directly:
//!
//! ```bash
//! cargo run --release --example bench                 # the whole suite
//! cargo run --release --example bench -- --only fsync  # one scenario, while iterating
//! ```
//!
//! Every scenario founds its own fresh cluster and tears it down before returning
//! (`harness::Node` kills its child process on drop, including on an early `?` return or a
//! panic), so no scenario is confused by another's leftover state, and no child process outlives
//! this run.

mod api_client;
mod harness;

#[path = "bench/body_limit.rs"]
mod body_limit;
#[path = "bench/cluster.rs"]
mod cluster;
#[path = "bench/connections.rs"]
mod connections;
#[path = "bench/env_info.rs"]
mod env_info;
#[path = "bench/fsync.rs"]
mod fsync;
#[path = "bench/ledger.rs"]
mod ledger;
#[path = "bench/liveness.rs"]
mod liveness;
#[path = "bench/read_index.rs"]
mod read_index;
#[path = "bench/snapshot.rs"]
mod snapshot;
#[path = "bench/stats.rs"]
mod stats;
#[path = "bench/watch.rs"]
mod watch;

use std::future::Future;
use std::io;
use std::time::Instant;

use clap::Parser;
use harness::Example;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

/// Command-line arguments for the benchmark driver
#[derive(Parser)]
struct Args {
    /// Run only the named scenario instead of the whole suite - for fast iteration. `ledger`
    /// depends on `fsync`'s own throughput figure and falls back to a placeholder (0) when run
    /// alone; only a full run produces the report this ticket asks for.
    #[arg(long)]
    only: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    let started = Instant::now();

    eprintln!("building the kvstore example in release mode...");
    let example = Example::build_release("kvstore")?;

    let mut report = Map::new();
    report.insert(
        "environment".to_string(),
        serde_json::to_value(env_info::collect()).map_err(io::Error::other)?,
    );

    run_scenario(&mut report, &args.only, "fsync", fsync::run(&example)).await?;
    let fsync_writes_per_sec = report.get("fsync").and_then(|v| v["concurrent_writes_per_sec"].as_f64()).unwrap_or(0.0);

    run_scenario(
        &mut report,
        &args.only,
        "ledger",
        ledger::run(&example, fsync_writes_per_sec),
    )
    .await?;

    run_scenario(&mut report, &args.only, "watch", watch::run(&example)).await?;
    run_scenario(&mut report, &args.only, "snapshot", snapshot::run(&example)).await?;
    run_scenario(&mut report, &args.only, "body_limit", body_limit::run(&example)).await?;
    run_scenario(&mut report, &args.only, "liveness", liveness::run(&example)).await?;
    run_scenario(&mut report, &args.only, "read_index", read_index::run(&example)).await?;
    run_scenario(&mut report, &args.only, "connections", connections::run(&example)).await?;

    // Every scenario's own `Cluster` already killed its child processes on drop; this only
    // removes the shared scratch directory (node data, logs) once nothing needs it any more.
    let _ = example.cleanup();

    let output = json!({
        "total_elapsed_secs": started.elapsed().as_secs_f64(),
        "scenarios": report,
    });
    println!("{}", serde_json::to_string_pretty(&output).map_err(io::Error::other)?);
    Ok(())
}

/// Run one scenario's future and record its result under `name`, unless `--only` names a
/// different scenario - in which case `fut` is dropped unpolled, so it never spawns a process.
async fn run_scenario(
    report: &mut Map<String, Value>,
    only: &Option<String>,
    name: &str,
    fut: impl Future<Output = io::Result<Value>>,
) -> io::Result<()> {
    if only.as_deref().is_some_and(|wanted| wanted != name) {
        return Ok(());
    }
    eprintln!("running {name}...");
    let scenario_started = Instant::now();
    let value = fut.await?;
    eprintln!("  {name} done in {:.1}s", scenario_started.elapsed().as_secs_f64());
    report.insert(name.to_string(), value);
    Ok(())
}
