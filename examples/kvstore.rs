//! EzKv KV Store Example
//!
//! A simple distributed key-value store built on the foundation's `EzKv`. Run multiple instances
//! to form a cluster:
//!
//! ```bash
//! # Terminal 1 (first node - creates cluster)
//! cargo run --example kvstore -- --addr 127.0.0.1:8080
//!
//! # Terminal 2 (joins via seed node)
//! cargo run --example kvstore -- --addr 127.0.0.1:8081 --seed 127.0.0.1:8080
//!
//! # Terminal 3 (joins via seed node)
//! cargo run --example kvstore -- --addr 127.0.0.1:8082 --seed 127.0.0.1:8080
//!
//! # A read replica: replicates the log, never votes, and writes never wait for it
//! cargo run --example kvstore -- --addr 127.0.0.1:8083 --seed 127.0.0.1:8080 --learner
//! ```
//!
//! Then drive it through the write API (`/api/write`, `/api/read`) below. These are this
//! example's own routes, added on top of `EzKv::serve_with` - `EzKv::write` only ever succeeds
//! on the current leader (any-node forwarding is ech0-media#30), so a write to a follower fails
//! and has to be retried against another node. Every route on the internal port, this example's
//! own included, requires the cluster token as `Authorization: Bearer <token>` - the default
//! below matches `--token`'s own default, so it needs no flag for a single-command demo:
//!
//! ```bash
//! curl -X POST 127.0.0.1:8080/api/write -H 'Content-Type: application/json' \
//!     -H 'Authorization: Bearer dev-cluster-token' \
//!     -d '{"op": "Set", "key": "hello", "value": "world"}'
//!
//! curl -X POST 127.0.0.1:8080/api/read -H 'Content-Type: application/json' \
//!     -H 'Authorization: Bearer dev-cluster-token' \
//!     -d '{"op": "Get", "key": "hello"}'
//! ```

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;

use actix_web::HttpResponse;
use actix_web::Responder;
use actix_web::web;
use clap::Parser;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::RedbStorage;
use ezraft::Txn;
use serde::Deserialize;
use serde::Serialize;
use tracing_subscriber::EnvFilter;

/// Body of `POST /api/write`: a `Set` or a `Delete` on one key
///
/// Translated into a `Txn` with a single `Put`/`Delete` op and handed to `EzKv::write` - this
/// example's own thin write API, not part of the crate.
#[derive(Deserialize)]
#[serde(tag = "op")]
enum WriteRequest {
    Set { key: String, value: String },
    Delete { key: String },
}

/// Body of `POST /api/read`: a point lookup or a prefix scan
///
/// A read never enters the log, so it is not bound to what a write can say: `Prefix` asks a
/// question the key-at-a-time write API has no way to phrase.
#[derive(Deserialize)]
#[serde(tag = "op")]
enum ReadRequest {
    Get { key: String },
    Prefix { prefix: String },
}

/// What `POST /api/write` answers: the transaction's new revision
#[derive(Serialize)]
struct WriteResponse {
    revision: u64,
}

async fn handle_write(node: web::Data<EzKv>, req: web::Json<WriteRequest>) -> actix_web::Result<impl Responder> {
    let txn = match req.into_inner() {
        WriteRequest::Set { key, value } => Txn::new().put(key, value),
        WriteRequest::Delete { key } => Txn::new().delete(key),
    };

    let revision = node.write(txn).await.map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;

    Ok(HttpResponse::Ok().json(WriteResponse { revision }))
}

/// Answers `POST /api/read` straight from this node's local view - no consensus round, and it
/// already reflects every write this node has itself applied.
async fn handle_read(node: web::Data<EzKv>, req: web::Json<ReadRequest>) -> actix_web::Result<impl Responder> {
    let view = node.view();

    let entries: BTreeMap<String, String> = match req.into_inner() {
        ReadRequest::Get { key } => view
            .get(&key)
            .map(|kv| (kv.key, String::from_utf8_lossy(&kv.value).into_owned()))
            .into_iter()
            .collect(),
        ReadRequest::Prefix { prefix } => {
            view.prefix(&prefix).map(|kv| (kv.key, String::from_utf8_lossy(&kv.value).into_owned())).collect()
        }
    };

    Ok(HttpResponse::Ok().json(entries))
}

/// Command-line arguments for the KV store
#[derive(clap::Parser)]
struct Args {
    /// HTTP bind address (e.g., "127.0.0.1:8080")
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    /// Seed node address to join existing cluster
    #[arg(long)]
    seed: Option<String>,

    /// Join as a learner: replicate the log and answer reads, but never vote
    ///
    /// Adds read capacity without adding a node that writes have to wait for. Promote it with
    /// `POST /api/membership` whenever you decide to.
    #[arg(long)]
    learner: bool,

    /// The cluster token every node on this internal port must share
    ///
    /// A node refuses to start without one; every request to `/raft/*`, `/api/*`, `/rpc/*` -
    /// including this example's own `/api/write`/`/api/read` - must present it as
    /// `Authorization: Bearer <token>`.
    #[arg(long, default_value = "dev-cluster-token")]
    token: String,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Without this, everything Raft reports about a cluster that is not working goes nowhere.
    // Warnings and errors by default; set RUST_LOG=info (or debug) to follow what Raft is doing.
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();

    let args = Args::parse();
    let addr = args.addr;
    let seed = args.seed;

    // The founding node is the cluster's first voter and cannot be anything else: a cluster whose
    // only member does not vote can never commit, not even the change that would fix it.
    if args.learner && seed.is_none() {
        return Err(io::Error::other(
            "--learner needs --seed: the founding node has to be a voter",
        ));
    }

    // Create storage (use addr for directory name)
    let base_dir = PathBuf::from(format!("./data/{}", addr.replace(':', "-")));

    // The production storage: one redb file for this node's vote, log, meta and snapshot.
    let storage = RedbStorage::open(base_dir.join("node.redb")).await?;

    // Create the node handle: the first node starts the cluster, the rest join it
    let token = args.token;
    let config = EzConfig {
        cluster_token: token.clone(),
        ..EzConfig::default()
    };
    let node = match &seed {
        Some(seed) if args.learner => EzKv::join_as_learner(&addr, seed, storage, config).await?,
        Some(seed) => EzKv::join(&addr, seed, storage, config).await?,
        None => EzKv::create(&addr, storage, config).await?,
    };

    println!("Node {} listening on {}", node.node_id(), addr);
    print_next_steps(&addr, seed.is_some(), &token);

    // Start the HTTP server: the crate's `/raft/*` and admin routes, plus this example's own
    // `/api/write` and `/api/read`.
    let node_data = web::Data::new(node.clone());
    node.serve_with(move |cfg| {
        cfg.app_data(node_data.clone())
            .route("/api/write", web::post().to(handle_write))
            .route("/api/read", web::post().to(handle_read));
    })
    .await
}

/// Print the commands that drive the node just started
///
/// A node that announces only that it is listening leaves the reader with nothing to type: the
/// body `/api/write` takes is this file's `WriteRequest` enum as JSON, which cannot be guessed
/// from outside the source.
fn print_next_steps(addr: &str, seed_given: bool, token: &str) {
    // Without a seed this node forms a cluster of its own, and every other node must be pointed
    // at it. A reader who starts a second node without `--seed` gets two separate clusters that
    // never merge, so the invitation to join is printed with the address already filled in.
    if !seed_given && let Some(next) = next_addr(addr) {
        println!(
            "\nNo --seed given, so this node is the cluster's founding member. \
             Every other node joins through it - from another terminal:\n    \
             cargo run --example kvstore -- --addr {next} --seed {addr} --token {token}"
        );
    }

    println!(
        r#"
Every request below needs the cluster token this node was started with ({token}).

Write a key - only the leader accepts it (any-node forwarding is ech0-media#30):
    curl -X POST {addr}/api/write -H 'Content-Type: application/json' \
        -H 'Authorization: Bearer {token}' \
        -d '{{"op": "Set", "key": "hello", "value": "world"}}'

Read it back - answered from this node's local view, no consensus round:
    curl -X POST {addr}/api/read -H 'Content-Type: application/json' \
        -H 'Authorization: Bearer {token}' \
        -d '{{"op": "Get", "key": "hello"}}'

Read every key sharing a prefix - a read the write API cannot phrase:
    curl -X POST {addr}/api/read -H 'Content-Type: application/json' \
        -H 'Authorization: Bearer {token}' \
        -d '{{"op": "Prefix", "prefix": "he"}}'

Cluster state - leader, term, log index, membership:
    curl {addr}/api/metrics -H 'Authorization: Bearer {token}'

Change a node's role - a voter counts towards a quorum, a learner does not:
    curl -X POST {addr}/api/membership -H 'Content-Type: application/json' \
        -H 'Authorization: Bearer {token}' \
        -d '{{"op": "SetRole", "node_id": 1, "role": "Voter"}}'
"#
    );
}

/// The address one port up, to suggest where the next node could listen
fn next_addr(addr: &str) -> Option<String> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?.checked_add(1)?;
    Some(format!("{}:{}", host, port))
}
