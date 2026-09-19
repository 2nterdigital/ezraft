//! EzRaft - the foundation (底座) ech0-media builds on
//!
//! EzRaft runs a replicated key-value store on several machines at once and keeps their state
//! identical, so the service survives losing some of them. You write one thing - everything that
//! makes the machines agree is handled internally:
//!
//! - Storage persistence via [`EzStorage`]
//!
//! # What a cluster does
//!
//! A *cluster* is a set of *nodes*, each one a process running this crate. Every node keeps the
//! same list of transactions - the *log* - in the same order. A transaction is *committed* once a
//! majority of the nodes have stored it, which is the point at which it can no longer be lost.
//! Each node then *applies* committed transactions to its own copy of the KV, in log order, so
//! every node arrives at the same state, with the same `revision` and the same `create_rev`/
//! `mod_rev` on every key.
//!
//! Because every node holds the whole state, any node can answer a read from memory
//! ([`EzKv::view`]) without talking to anyone. Writes take the longer path: [`EzKv::write`]
//! proposes on the node it is called on, so it only succeeds on the current leader (a follower
//! gets a typed [`NotLeader`] error) - forwarding a write to whichever node leads is
//! ech0-media#30.
//!
//! A *snapshot* is the state serialized. It lets a node that has fallen too far behind be
//! caught up in one transfer instead of replaying the log, and it lets old log entries be
//! deleted. EzRaft builds and installs snapshots on its own.
//!
//! What matters for fault tolerance is that a majority survives: three nodes tolerate one
//! failure, five tolerate two.
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
//! ([`Conflict`]), or [`NotLeader`]. Every other fallible method returns [`std::io::Error`],
//! including the ones that fail for reasons that have nothing to do with I/O - a write that
//! finds no leader, and one issued to a stopped node, mean the same thing to a caller: try again,
//! or ask the node the error names. [`EzStorage`] is where a user's own storage errors
//! originate, and those are I/O errors already.

pub mod admin;
mod auth;
mod command;
pub mod config;
pub mod entry;
pub mod kv;
pub mod meta;
mod network;
pub mod node_role;
pub mod raft;
mod read;
mod rpc;
pub mod server;
pub mod snapshot;
pub mod storage;
mod transport;
mod type_config;
mod write;

// Re-export public API
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
pub use kv::Op;
pub use kv::Page;
pub use kv::Txn;
pub use kv::View;
pub use meta::EzMeta;
pub use node_role::NodeRole;
pub use openraft::RaftMetrics;
pub use openraft::RaftTypeConfig;
pub use openraft::ServerState;
pub use raft::EzKv;
pub use read::ReadError;
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
pub use write::NotLeader;
pub use write::WriteError;
