//! `EzKv`: the replicated key-value store (ech0-media#27)
//!
//! This module is the pure state machine the foundation replicates: conditional transactions,
//! revisions, consistent views and the change groups `watch` (ech0-media#32) will deliver. It has
//! no knowledge of Raft, networking or storage itself - `KvState::apply` (crate-internal) is a
//! plain function from a transaction to a result, deterministic on every replica.
//! `crate::storage::adapter::state_machine` wires this state machine into Raft, and
//! [`crate::EzKv`] is the public handle onto it (`EzKv::write`, `EzKv::view`).
//!
//! # The model
//!
//! - **Keys** are UTF-8 strings ordered by bytes (`str`'s own `Ord`), `/`-separated by convention;
//!   this module understands only prefixes, never path structure. **Values** are opaque bytes
//!   ([`bytes::Bytes`]): callers choose their own encoding. One flat key space.
//! - **`revision`** is the KV's own counter, unrelated to the Raft log index: every transaction
//!   whose conditions all hold gets `revision + 1`, whether or not it changes any key, and every
//!   change it makes shares that revision. A transaction that fails a condition changes nothing and
//!   leaves the revision untouched, so revisions are contiguous. Each key carries the `create_rev`
//!   and `mod_rev` of the transaction that created or last changed it.
//! - **[`txn::Txn`]** is `{ conditions, ops }`, all-or-nothing: every [`txn::Condition`] is
//!   evaluated against the state *before* the transaction, in order; the first one that fails is
//!   reported as a [`txn::Conflict`] and nothing is applied. Ops ([`txn::Op`]) then apply in order,
//!   a later one seeing the effect of an earlier one.
//! - **[`view::View`]** is a cheap, lock-free, point-in-time read handle: cloning it clones only
//!   the persistent map's root, never blocking `KvState::apply` and never blocking other readers.
//! - **[`change::Changes`]** is the group of [`change::Change`]s one successful transaction
//!   produced, in op order - what a future `watch` (ech0-media#32) delivers per revision.
//!
//! # Foundation-internal state
//!
//! `KvState` (crate-internal, wired into the Raft state machine by
//! `crate::storage::adapter::state_machine`) holds the user's KV, the revision counter, and a
//! third sibling field for foundation-internal replicated state - never part of the user key
//! space above. Its first tenant is the idempotency ledger (ech0-media#30): [`RequestId`] -> the
//! first real outcome recorded for it, consulted by `KvState::apply_idempotent` before a command
//! ever reaches `KvState::apply`, evicted first-in-first-out by entry count as a pure function of
//! the log (see `ledger`'s own module docs). The node registry (ech0-media#33,
//! [`NodeInfo`]/[`Registry`]) is the second: `node_id` -> how to reach that node and what the
//! application says about it, mutated only by internal commands (`crate::registry`,
//! `crate::command::CommandBody::RegisterNode`/`RemoveNode`) that reuse the very same forwarding,
//! retry and ledger machinery ech0-media#30 built for transactions - see `registry`'s own module
//! docs. Both sibling fields are folded into the same snapshot without a format-version bump.
//!
//! # Snapshots
//!
//! `snapshot_codec` (crate-internal) is the postcard encoding of the whole state machine: a
//! small header carrying a magic and a format version, checked before anything else is
//! interpreted, ahead of the v1 body (every key, the current revision, and the internal state
//! block above). `crate::storage::adapter::state_machine` calls it to build and install
//! snapshots; an unknown version is a typed, explicit error that stops the node rather than
//! guessing at a layout it does not know.
//!
//! # Publishing views
//!
//! `publish::Publisher` (crate-internal) is the one place a new [`view::View`] becomes visible
//! to readers, and it publishes before the writer is answered. The adapter owns the single
//! instance; [`EzKv::view`](crate::EzKv::view) reads through a cloned `publish::ViewReceiver`,
//! never touching the state machine's lock.

mod bytes_codec;
mod change;
mod ledger;
mod publish;
mod registry;
mod snapshot_codec;
mod state;
mod txn;
mod view;

pub use change::Change;
pub use change::Changes;
pub use ledger::RequestId;
pub(crate) use publish::Publisher;
pub(crate) use publish::ViewReceiver;
pub use registry::NodeInfo;
pub use registry::NodeStatus;
pub(crate) use registry::RegisterNode;
pub use registry::Registry;
pub(crate) use registry::RemoveNode;
pub(crate) use registry::SetStatus;
pub(crate) use snapshot_codec::decode_snapshot;
pub(crate) use snapshot_codec::encode_snapshot;
pub(crate) use state::KvState;
pub use txn::Condition;
pub use txn::Conflict;
pub use txn::KvLimits;
pub use txn::LimitError;
pub use txn::Op;
pub use txn::Txn;
pub use view::KeyValue;
pub use view::Page;
pub use view::View;
