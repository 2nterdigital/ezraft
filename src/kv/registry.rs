//! The node registry: `node_id -> NodeInfo`, the foundation-internal sibling of the idempotency
//! ledger inside `InternalState` (ech0-media#33)
//!
//! Never part of the user key space `kv::state::KvState::kv` holds: a separate, small block next
//! to it, folded into the same snapshot. Every entry is registered or updated by the node it
//! describes (`crate::registry`'s startup task and `EzKv::update_node_meta`), and removed by the
//! leader once `EzKv::remove_node` commits the matching membership change. This module owns only
//! the data - [`NodeInfo`], [`Registry`], and the two command payloads that mutate it
//! ([`RegisterNode`], [`RemoveNode`]) - never how they reach the log or the retry that gets them
//! there (`crate::registry`, `crate::write`).

use bytes::Bytes;
use imbl::OrdMap;
use serde::Deserialize;
use serde::Serialize;

use crate::kv::bytes_codec;

/// Whether the foundation currently believes a node is reachable
///
/// Stored and exposed here, decided elsewhere: the leader's built-in liveness task
/// (`crate::liveness`, ech0-media#34) sets it from heartbeat acknowledgements, writing only when it
/// changes. Every entry starts, and every re-registration resets it to, [`Self::Up`]: a node
/// registering or updating its own entry is, by definition, up right now.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// The foundation has no reason to believe this node is unreachable
    #[default]
    Up,
    /// The foundation's liveness judgment currently considers this node unreachable
    Down,
}

/// One node's registry entry: how a client reaches it, what the application says about it, and
/// the foundation's own liveness judgment
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// The address clients and business code use to reach this node - distinct from the
    /// internal-port address membership carries (`crate::Node::internal_addr`), which is never
    /// meant for anything but Raft traffic and inter-node RPC
    pub external_addr: String,
    /// Opaque, application-defined bytes: the foundation stores and hands these back verbatim,
    /// never interpreting them
    #[serde(with = "bytes_codec")]
    pub meta: Bytes,
    /// The foundation's current liveness judgment for this node
    pub status: NodeStatus,
}

/// `node_id -> NodeInfo`: the whole node registry
///
/// One entry per cluster member, so this is small by construction - shared as a single immutable
/// value, the same way [`crate::View`] is, rather than a ring buffer of changes: cloning it is
/// O(1) (`imbl::OrdMap`'s structural sharing), which is what makes it cheap to hand to every
/// `EzKv::registry()` caller and to publish wholesale on every change.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registry(OrdMap<u64, NodeInfo>);

impl Registry {
    /// `node_id`'s current entry, if it has one
    pub fn get(&self, node_id: u64) -> Option<NodeInfo> {
        self.0.get(&node_id).cloned()
    }

    /// Every entry, in node id order
    pub fn iter(&self) -> impl Iterator<Item = (u64, NodeInfo)> + '_ {
        self.0.iter().map(|(id, info)| (*id, info.clone()))
    }

    /// How many nodes are currently registered
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no node is currently registered
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Insert or overwrite `node_id`'s entry
    pub(crate) fn set(&mut self, node_id: u64, info: NodeInfo) {
        self.0.insert(node_id, info);
    }

    /// Remove `node_id`'s entry, if it has one
    pub(crate) fn remove(&mut self, node_id: u64) {
        self.0.remove(&node_id);
    }
}

/// Register or update this node's own registry entry
///
/// The command body [`crate::registry`] proposes at startup (retried until it succeeds) and on
/// every [`crate::EzKv::update_node_meta`] call. Applying it always sets [`NodeStatus::Up`] - see
/// [`NodeStatus`]'s own docs for why re-registering never needs to preserve a prior `Down`.
///
/// `pub`, like [`crate::kv::Txn`], only so [`crate::command::CommandBody`] - which must stay `pub`
/// for [`crate::entry::EzEntry`] to type-check without a `private_interfaces` warning - can embed
/// it; `kv.rs` re-exports it as `pub(crate)`, so nothing outside this crate can actually name it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterNode {
    pub(crate) node_id: u64,
    pub(crate) external_addr: String,
    #[serde(with = "bytes_codec")]
    pub(crate) meta: Bytes,
}

/// Remove a node's registry entry
///
/// The command body [`crate::EzKv::remove_node`] proposes, on the leader, once the matching
/// membership change has committed. `pub` for the same reason [`RegisterNode`] is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveNode {
    pub(crate) node_id: u64,
}

/// Set one node's liveness status (ech0-media#34)
///
/// The command body the built-in liveness task proposes, through the leader-only propose path
/// `crate::leader_task::LeaderCtx::write` shares (`crate::write::propose_leader_only`) - never
/// forwarded, never proposed by anything else. The threshold/grace decision that produced this
/// command already happened on the proposing leader, from its own configuration and its own view
/// of openraft's per-peer heartbeat metrics; applying it is a pure assignment, with nothing left
/// to decide (the determinism rule: thresholds are the leader task's concern, never `apply`'s).
/// `pub` for the same reason [`RegisterNode`] is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetStatus {
    pub(crate) node_id: u64,
    pub(crate) status: NodeStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(addr: &str) -> NodeInfo {
        NodeInfo {
            external_addr: addr.to_string(),
            meta: Bytes::from_static(b"meta"),
            status: NodeStatus::Up,
        }
    }

    #[test]
    fn a_registry_starts_empty() {
        let registry = Registry::default();
        assert!(registry.is_empty());
        assert_eq!(0, registry.len());
        assert_eq!(None, registry.get(1));
    }

    #[test]
    fn set_then_get_returns_the_stored_entry() {
        let mut registry = Registry::default();
        registry.set(1, info("127.0.0.1:9001"));
        assert_eq!(Some(info("127.0.0.1:9001")), registry.get(1));
        assert_eq!(1, registry.len());
    }

    #[test]
    fn set_overwrites_an_existing_entry() {
        let mut registry = Registry::default();
        registry.set(1, info("127.0.0.1:9001"));
        registry.set(1, info("127.0.0.1:9002"));
        assert_eq!(Some(info("127.0.0.1:9002")), registry.get(1));
        assert_eq!(1, registry.len(), "overwriting keeps a single entry");
    }

    #[test]
    fn remove_drops_the_entry_and_is_harmless_when_absent() {
        let mut registry = Registry::default();
        registry.set(1, info("127.0.0.1:9001"));

        registry.remove(1);
        assert_eq!(None, registry.get(1));
        assert!(registry.is_empty());

        // Removing again, or removing a node id that was never present, is not an error.
        registry.remove(1);
        registry.remove(42);
        assert!(registry.is_empty());
    }

    #[test]
    fn iter_lists_every_entry_in_node_id_order() {
        let mut registry = Registry::default();
        registry.set(3, info("c"));
        registry.set(1, info("a"));
        registry.set(2, info("b"));

        let ids: Vec<u64> = registry.iter().map(|(id, _)| id).collect();
        assert_eq!(vec![1, 2, 3], ids);
    }

    #[test]
    fn registry_serde_round_trips_in_json_and_postcard() {
        let mut registry = Registry::default();
        registry.set(1, info("127.0.0.1:9001"));
        registry.set(2, NodeInfo {
            external_addr: "127.0.0.1:9002".to_string(),
            meta: Bytes::from_static(b"\x00\xff"),
            status: NodeStatus::Down,
        });

        let json = serde_json::to_string(&registry).unwrap();
        assert_eq!(registry, serde_json::from_str(&json).unwrap());

        let packed = postcard::to_allocvec(&registry).unwrap();
        assert_eq!(registry, postcard::from_bytes(&packed).unwrap());
    }
}
