//! EzKv KV Store Example
//!
//! A distributed key-value store built entirely on `EzKv`'s public API - this is the reference
//! for how an embedder uses the foundation, and the binary ech0-media#36's benchmarks drive.
//!
//! A node runs on **two** ports:
//! - the **internal port** (`--addr`), the foundation's own: Raft traffic and inter-node RPC,
//!   guarded by the cluster token every node shares (`--token`, or the `EZRAFT_CLUSTER_TOKEN`
//!   environment variable);
//! - a separate **external port** (`--api-addr`), this example's own small HTTP API - the one thing
//!   a client ever talks to. See `examples/kvstore/api.rs` for its routes.
//!
//! Run three instances to form a cluster:
//!
//! ```bash
//! # Terminal 1 (first node - creates the cluster)
//! cargo run --example kvstore -- --addr 127.0.0.1:8080 --api-addr 127.0.0.1:9080
//!
//! # Terminal 2 (joins via seed node)
//! cargo run --example kvstore -- --addr 127.0.0.1:8081 --api-addr 127.0.0.1:9081 --seed 127.0.0.1:8080
//!
//! # Terminal 3 (joins via seed node)
//! cargo run --example kvstore -- --addr 127.0.0.1:8082 --api-addr 127.0.0.1:9082 --seed 127.0.0.1:8080
//!
//! # A read replica: replicates the log, never votes, and writes never wait for it
//! cargo run --example kvstore -- --addr 127.0.0.1:8083 --api-addr 127.0.0.1:9083 \
//!     --seed 127.0.0.1:8080 --learner
//! ```
//!
//! Then drive it through the external API - see `print_next_steps` below for ready-to-run
//! commands once a node is up, or `examples/kvstore/api.rs`'s own doc comment for the full route
//! table. Unlike the internal port, nothing on the external port checks the cluster token: a real
//! deployment authenticates its own client-facing API however it sees fit (LiveKit-style JWTs for
//! ech0-media, something else elsewhere) - this example only demonstrates the foundation, not that
//! choice, so it leaves the external port open.

// This example's crate root is `examples/kvstore.rs`; Cargo's own module-resolution convention
// for a crate root only looks beside it (`examples/api.rs`), which would collide with any other
// example's own module of the same name - `#[path]` puts this example's modules in their own
// `examples/kvstore/` directory instead, the adjacent-module-root layout applied to an example
// binary rather than to `src/`.
#[path = "kvstore/api.rs"]
mod api;

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::RedbStorage;
use tracing_subscriber::EnvFilter;

/// Command-line arguments for the KV store
#[derive(clap::Parser)]
struct Args {
    /// Internal port bind address: Raft traffic and inter-node RPC (e.g. "127.0.0.1:8080")
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    /// External port bind address: this example's own client API (e.g. "127.0.0.1:9080")
    ///
    /// The one address a client ever needs - see `examples/kvstore/api.rs` for its routes. Always
    /// a different address from `--addr`: no route of the internal port is reachable from here,
    /// and no route of this API is reachable from there.
    #[arg(long, default_value = "127.0.0.1:9080")]
    api_addr: String,

    /// Seed node's internal-port address, to join the cluster it belongs to
    #[arg(long)]
    seed: Option<String>,

    /// Join as a learner: replicate the log and answer reads, but never vote
    ///
    /// Adds read capacity without adding a node that writes have to wait for. Promote it later by
    /// changing its role through the internal port's admin API.
    #[arg(long)]
    learner: bool,

    /// The cluster token every node on the internal port must share
    ///
    /// A node refuses to start without one; every request to `/raft/*`, `/api/*`, `/rpc/*` on the
    /// internal port must present it as `Authorization: Bearer <token>`. Settable through the
    /// environment instead, so it never has to appear in a process listing.
    #[arg(long, env = "EZRAFT_CLUSTER_TOKEN", default_value = "dev-cluster-token")]
    token: String,

    /// Where this node's `RedbStorage` file lives
    ///
    /// Defaults to a directory derived from `--addr`, so two nodes never collide by accident when
    /// run from the same working directory. Set it explicitly to control exactly where a node's
    /// data lives - for instance to restart a node on the data directory a previous run of it left
    /// behind.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Override `EzConfig::snapshot_interval` (log entries between automatic snapshots)
    ///
    /// Exists so ech0-media#36's benchmark driver can force a snapshot at a chosen cadence
    /// instead of waiting out the default 500-entry interval; ordinary runs of this example never
    /// need it.
    #[arg(long, default_value_t = EzConfig::default().snapshot_interval)]
    snapshot_interval: u64,

    /// Override `EzConfig::json_body_limit`, in bytes
    ///
    /// `EzConfig::to_raft_config` derives openraft's `max_payload_entries` from this value and
    /// `kv_limits.max_txn_bytes`; ech0-media#36's benchmark driver raises it to reproduce
    /// openraft's own 300-entry default for comparison against the derived default of 4.
    /// Ordinary runs of this example never need to change it.
    #[arg(long, default_value_t = EzConfig::default().json_body_limit)]
    json_body_limit: usize,

    /// Override `EzConfig::liveness_threshold`, in milliseconds
    ///
    /// Exists so ech0-media#36's benchmark driver can compare candidate thresholds for false
    /// `Down` marks under load and during leader changes; ordinary runs of this example never
    /// need it.
    #[arg(long, default_value_t = EzConfig::default().liveness_threshold.as_millis() as u64)]
    liveness_threshold_ms: u64,

    /// Override `EzConfig::liveness_grace`, in milliseconds - see `--liveness-threshold-ms`
    #[arg(long, default_value_t = EzConfig::default().liveness_grace.as_millis() as u64)]
    liveness_grace_ms: u64,

    /// Override `EzConfig::liveness_check_interval`, in milliseconds - see
    /// `--liveness-threshold-ms`
    #[arg(long, default_value_t = EzConfig::default().liveness_check_interval.as_millis() as u64)]
    liveness_check_interval_ms: u64,

    /// Trace filter for `tracing_subscriber`'s `EnvFilter` (e.g. "warn", "info",
    /// "warn,ezraft::network=info")
    ///
    /// Settable through `RUST_LOG`, this crate's usual convention (see the module doc comment
    /// above); ech0-media#36's benchmark driver passes `--log-filter` directly instead of
    /// threading an environment variable through its child-process harness.
    #[arg(long, env = "RUST_LOG", default_value = "warn")]
    log_filter: String,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    // Without this, everything Raft reports about a cluster that is not working goes nowhere.
    // Warnings and errors by default; set RUST_LOG=info (or debug) to follow what Raft is doing,
    // or pass --log-filter directly (see that flag's own doc comment).
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_new(&args.log_filter).unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();

    // The founding node is the cluster's first voter and cannot be anything else: a cluster whose
    // only member does not vote can never commit, not even the change that would fix it.
    if args.learner && args.seed.is_none() {
        return Err(io::Error::other(
            "--learner needs --seed: the founding node has to be a voter",
        ));
    }

    let base_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("./data/{}", args.addr.replace(':', "-"))));

    // The production storage: one redb file for this node's vote, log, meta and snapshot.
    let storage = RedbStorage::open(base_dir.join("node.redb")).await?;

    let config = EzConfig {
        cluster_token: args.token.clone(),
        // What this node registers in the cluster's node registry: `--api-addr` is its whole
        // answer to "where do clients reach me", so any node can hand a client the address of
        // any other (`GET /status` lists them).
        external_addr: args.api_addr.clone(),
        // The five fields below default to `EzConfig::default()`'s own values (see each flag's
        // own doc comment) and only move when ech0-media#36's benchmark driver passes them
        // explicitly; every other run of this example gets the library's ordinary defaults.
        snapshot_interval: args.snapshot_interval,
        json_body_limit: args.json_body_limit,
        liveness_threshold: Duration::from_millis(args.liveness_threshold_ms),
        liveness_grace: Duration::from_millis(args.liveness_grace_ms),
        liveness_check_interval: Duration::from_millis(args.liveness_check_interval_ms),
        ..EzConfig::default()
    };

    // Create the node handle: the first node starts the cluster, the rest join it. On restart
    // (this same `--addr`, same data directory) the persisted node id takes over regardless of
    // `--seed`/`--learner` - see `EzKv::join`'s own docs.
    let node = match &args.seed {
        Some(seed) if args.learner => EzKv::join_as_learner(&args.addr, seed, storage, config).await?,
        Some(seed) => EzKv::join(&args.addr, seed, storage, config).await?,
        None => EzKv::create(&args.addr, storage, config).await?,
    };

    println!(
        "Node {} listening on {} (internal), serving its client API on {} (external)",
        node.node_id(),
        args.addr,
        args.api_addr
    );
    print_next_steps(&args.addr, &args.api_addr, args.seed.is_some());

    // Two servers, two ports: the foundation's own (internal, cluster-token-guarded) and this
    // example's client-facing one (external). Both run for as long as this node does, so
    // whichever stops first is what `main` returns - the other is aborted rather than left
    // running with nothing left to answer for.
    let mut internal = tokio::spawn(node.clone().serve());
    let mut external = tokio::spawn(api::bind(node, args.api_addr)?);

    tokio::select! {
        result = &mut internal => {
            external.abort();
            result.map_err(io::Error::other)?
        }
        result = &mut external => {
            internal.abort();
            result.map_err(io::Error::other)?
        }
    }
}

/// Print the commands that drive the node just started
///
/// A node that announces only that it is listening leaves the reader with nothing to type: the
/// external API's request and response shapes cannot be guessed from outside the source.
fn print_next_steps(addr: &str, api_addr: &str, seed_given: bool) {
    // Without a seed this node forms a cluster of its own, and every other node must be pointed
    // at it. A reader who starts a second node without `--seed` gets two separate clusters that
    // never merge, so the invitation to join is printed with the address already filled in.
    if !seed_given
        && let Some(next) = next_addr(addr)
        && let Some(next_api) = next_addr(api_addr)
    {
        println!(
            "\nNo --seed given, so this node is the cluster's founding member. \
             Every other node joins through it - from another terminal:\n    \
             cargo run --example kvstore -- --addr {next} --api-addr {next_api} --seed {addr}"
        );
    }

    println!(
        r#"
Submit a transaction - conditions and ops as JSON, values as base64 (any node accepts it and
forwards to the leader if needed); `conditions` is required even when empty:
    curl -X POST {api_addr}/tx -H 'Content-Type: application/json' \
        -d '{{"conditions": [], "ops": [{{"Put": {{"key": "hello", "value": "d29ybGQ="}}}}]}}'

Read it back from this node's own local view - no consensus round:
    curl '{api_addr}/view?key=hello'

Read every key sharing a prefix, linearizably - reflects every write acknowledged anywhere before
the call started, even on a node that has not applied it yet:
    curl '{api_addr}/linearizable?prefix=he'

Stream changes to a prefix, from now on:
    curl -N '{api_addr}/watch?prefix=he'

This node's status - id, role, revision, membership:
    curl {api_addr}/status
"#
    );
}

/// The address one port up, to suggest where the next node could listen
fn next_addr(addr: &str) -> Option<String> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?.checked_add(1)?;
    Some(format!("{}:{}", host, port))
}
