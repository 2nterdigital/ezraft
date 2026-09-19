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
//! `crate::storage::adapter::state_machine`) holds exactly the user's KV and the revision
//! counter today. The idempotency ledger (ech0-media#30) and the node registry (ech0-media#33)
//! are foundation-internal replicated state - never part of the user key space above - and belong
//! in a sibling field next to the KV inside `KvState`, applied by the same `apply` call and folded
//! into the same snapshot (ech0-media#28). See the note on `KvState` (`src/kv/state.rs`) for
//! exactly where.
//!
//! # Publishing views
//!
//! `publish::Publisher` (crate-internal) is the one place a new [`view::View`] becomes visible
//! to readers, and it publishes before the writer is answered. The adapter owns the single
//! instance; [`EzKv::view`](crate::EzKv::view) reads through a cloned `publish::ViewReceiver`,
//! never touching the state machine's lock.

mod bytes_codec;
mod change;
mod publish;
mod state;
mod txn;
mod view;

pub use change::Change;
pub use change::Changes;
pub(crate) use publish::Publisher;
pub(crate) use publish::ViewReceiver;
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
