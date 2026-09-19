//! `EzKv`: the replicated key-value store (ech0-media#27)
//!
//! This module is the pure, self-contained state machine the foundation replicates: conditional
//! transactions, revisions, consistent views and the change groups `watch` (ech0-media#32) will
//! deliver. It has no knowledge of Raft, networking or storage - `KvState::apply` (crate-internal
//! until ech0-media#27b wires it in) is a plain function from a transaction to a result,
//! deterministic on every replica. Wiring it into the Raft state machine, and exposing the public
//! `EzKv` handle, is ech0-media#27b.
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
//! `KvState` (crate-internal until ech0-media#27b) holds exactly the user's KV and the revision
//! counter today. The idempotency ledger (ech0-media#30) and the node registry (ech0-media#33)
//! are foundation-internal replicated state - never part of the user key space above - and belong
//! in a sibling field next to the KV inside `KvState`, applied by the same `apply` call and folded
//! into the same snapshot (ech0-media#28). See the note on `KvState` (`src/kv/state.rs`) for
//! exactly where.

mod bytes_codec;
mod change;
mod state;
mod txn;
mod view;

pub use change::Change;
pub use change::Changes;
// Unused until ech0-media#27b calls into the state machine from the Raft adapter; remove this
// `allow` in the same change that adds that first caller.
#[allow(unused_imports)]
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
