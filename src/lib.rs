//! EzRaft - the foundation (底座) ech0-media builds on
//!
//! EzRaft runs a replicated key-value store on several machines at once and keeps their state
//! identical, so the service survives losing some of them. You write one thing - everything that
//! makes the machines agree is handled internally:
//!
//! - Storage persistence via [`EzStorage`]
//!
//! See the guide (`docs/guide.md`) for the full API surface, every error's semantics, and the
//! usage conventions ech0-media's own upstream code follows on top of this crate. This page stays
//! a short orientation plus one working example.
//!
//! # What a cluster does
//!
//! A *cluster* is a set of *nodes*, each one a process running this crate, replicating the same
//! log and applying it, in order, to the same key-value state machine ([`EzKv`]) - every node
//! ends up with the same state, the same `revision`, and the same `create_rev`/`mod_rev` on every
//! key. Because every node holds the whole state, any node answers a read from memory
//! ([`EzKv::view`]) without talking to anyone, and any node accepts a write ([`EzKv::write`]):
//! the leader proposes directly, any other node forwards it and retries under one request id
//! until either a definite outcome or a configurable deadline (ech0-media#30). A *snapshot* is
//! that state serialized, letting a node that fell too far behind catch up in one transfer
//! instead of replaying the whole log; EzRaft builds and installs snapshots on its own. What
//! matters for fault tolerance is that a majority survives: three nodes tolerate one failure,
//! five tolerate two.
//!
//! # Quick start
//!
//! A complete key-value node. This is `examples/kvstore.rs` without its command line - run that
//! with `cargo run --example kvstore` to get a cluster going.
//!
//! ```no_run
//! use std::io;
//!
//! use ezraft::EzConfig;
//! use ezraft::EzKv;
//! use ezraft::RedbStorage;
//! use ezraft::Txn;
//!
//! #[tokio::main]
//! async fn main() -> io::Result<()> {
//!     // 1. Where this node's state goes. One redb file per node. `RedbStorage`
//!     //    is the production `EzStorage` this crate ships.
//!     let storage = RedbStorage::open("./data/node1.redb").await?;
//!
//!     // 2. Every node on the same internal port needs the same cluster token - a
//!     //    node refuses to start without one.
//!     let config = EzConfig {
//!         cluster_token: "change-me-to-a-real-secret".to_string(),
//!         ..EzConfig::default()
//!     };
//!
//!     // 3. The first node of a cluster creates it; every other node joins
//!     //    through any node already in it:
//!     //      EzKv::join("127.0.0.1:8081", "127.0.0.1:8080", storage, config)
//!     let node = EzKv::create("127.0.0.1:8080", storage, config).await?;
//!
//!     // 4. Write a key, then read it back from this node's own local view - no
//!     //    consensus round for the read, and it already reflects the write.
//!     node.write(Txn::new().put("hello", "world")).await.map_err(io::Error::other)?;
//!     println!("{:?}", node.view().get("hello"));
//!
//!     // 5. Serve the Raft RPCs peers need, plus the admin API. This blocks;
//!     //    `tokio::spawn` it when the caller has other work to do.
//!     node.serve().await
//! }
//! ```
//!
//! # Errors
//!
//! [`EzKv::write`] returns a typed [`WriteError`]: a limit violation, a failed condition
//! ([`Conflict`]), [`WriteError::Indeterminate`] (no definite outcome within the deadline - retry
//! with [`EzKv::write_with_id`] and the same request id), or [`WriteError::LocalLag`] (committed,
//! but this node has not applied it yet - retry with [`EzKv::wait_revision`]). Every other
//! fallible method returns [`std::io::Error`], including the ones that fail for reasons that have
//! nothing to do with I/O - a write issued to a stopped node means the same thing to a caller as
//! any other retryable failure. [`EzStorage`] is where a user's own storage errors originate, and
//! those are I/O errors already.

pub mod admin;
mod auth;
mod cluster;
mod command;
pub mod config;
pub mod entry;
mod kv;
mod leader_task;
mod liveness;
mod membership;
pub mod meta;
mod network;
pub mod node_role;
pub mod raft;
mod read;
mod registry;
mod rpc;
mod server;
pub mod snapshot;
pub mod storage;
mod transport;
mod type_config;
mod watch;
mod write;

// Re-export public API
pub use cluster::ClusterState;
pub use config::EzConfig;
pub use entry::EzEntry;
pub use entry::EzLogId;
pub use kv::Change;
pub use kv::Changes;
pub use kv::Condition;
pub use kv::Conflict;
pub use kv::KeyValue;
pub use kv::KvLimits;
pub use kv::LimitError;
pub use kv::NodeInfo;
pub use kv::NodeStatus;
pub use kv::Op;
pub use kv::Page;
pub use kv::Registry;
pub use kv::RequestId;
pub use kv::Txn;
pub use kv::View;
pub use leader_task::LeaderCtx;
pub use meta::EzMeta;
pub use node_role::NodeRole;
pub use openraft::RaftMetrics;
pub use openraft::RaftTypeConfig;
pub use openraft::ServerState;
pub use raft::EzKv;
pub use read::ReadError;
pub use registry::RegistryWriteError;
pub use rpc::Rpc;
pub use rpc::RpcError;
pub use rpc::RpcRegisterError;
pub use snapshot::EzSnapshot;
pub use snapshot::EzSnapshotMeta;
pub use storage::EzStorage;
pub use storage::Loaded;
pub use storage::Persist;
pub use storage::RedbStorage;
#[cfg(feature = "fault-injection")]
pub use transport::fault;
pub use type_config::EzVote;
pub use type_config::Node;
pub use type_config::OpenRaftTypes;
pub use watch::ResetCause;
pub use watch::WatchLimits;
pub use watch::WatchStart;
pub use watch::WatchStream;
pub use watch::WatchUpdate;
pub use write::LeaderWriteError;
pub use write::LocalLag;
pub use write::WriteError;

// Every Rust code block in the guide (`docs/guide.md`) becomes a `cargo test --doc` test through
// this module, so the guide cannot silently drift from the implemented API: `cfg(doctest)` holds
// only while rustdoc collects and compiles doctests (`cargo test --doc`), never for `cargo
// build`, `cargo check`, or even `cargo doc` itself, so this module and the guide's Markdown
// contribute nothing to a normal build or to the rendered API docs. Most of the guide's snippets
// are function definitions that are never called (compiling them already proves the types and
// signatures they use still match this crate); the few that run a whole node are `no_run`.
#[cfg(doctest)]
mod guide_doctest {
    #![doc = include_str!("../docs/guide.md")]
}
