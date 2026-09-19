//! Why a write did not produce a new revision: the public error types of the write path

use crate::kv::Conflict;
use crate::kv::LimitError;
use crate::kv::RequestId;

/// Why [`crate::EzKv::write`] or [`crate::EzKv::write_with_id`] did not produce a new revision
///
/// [`Self::LimitExceeded`] and [`Self::Conflict`] mean nothing was proposed anywhere, or - if a
/// forward happened - that whichever node actually proposed found nothing to apply: every
/// replica is untouched. There is no `NotLeader` here: any node answers `write`, forwarding to the
/// leader on its own.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// `txn` exceeded a configured limit - either this node's own (checked first, nothing sent),
    /// or the proposing node's (checked again before entering the log)
    #[error(transparent)]
    LimitExceeded(#[from] LimitError),

    /// `txn`'s conditions were not all satisfied
    #[error(transparent)]
    Conflict(#[from] Conflict),

    /// The deadline passed with no definite outcome: the write may or may not have committed.
    /// [`crate::EzKv::write_with_id`] with this same `request_id` resolves it later, within the
    /// idempotency ledger's window.
    #[error("write outcome unknown after the deadline (request {request_id})")]
    Indeterminate {
        /// The id a later [`crate::EzKv::write_with_id`] call must reuse to find out what really
        /// happened
        request_id: RequestId,
    },

    /// The write committed, but this node's own applied state did not catch up to it within the
    /// deadline
    #[error(transparent)]
    LocalLag(#[from] LocalLag),
}

/// The write committed at `revision`, but this node's own applied state has not caught up to it
/// (yet)
///
/// Not a failure: the data is durably committed and already visible on whichever node's log it
/// reached first. [`crate::EzKv::wait_revision`] (or another
/// [`crate::EzKv::write`]/[`crate::EzKv::view`] later on this same node) resolves it once ordinary
/// replication catches this node up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("write committed at revision {revision}, but this node has not caught up to it yet")]
pub struct LocalLag {
    /// The revision this node has not yet applied
    pub revision: u64,
}

/// Why [`crate::leader_task::LeaderCtx::write`] (or the built-in liveness task's own internal
/// commands) did not produce a result (ech0-media#34)
///
/// Nothing here is about forwarding: the leader-only propose path never sends `body` anywhere but
/// to `node` itself. [`Self::NotLeader`] is final - the context is dead - while
/// [`Self::Indeterminate`] is not, and the difference matters to a task deciding whether to go on.
#[derive(Debug, thiserror::Error)]
pub enum LeaderWriteError {
    /// `txn` exceeded a configured limit (a transaction-only concern)
    #[error(transparent)]
    LimitExceeded(#[from] LimitError),

    /// `txn`'s conditions were not all satisfied
    #[error(transparent)]
    Conflict(#[from] Conflict),

    /// `node` no longer leads in `term` - it lost leadership, or won it again in a later term -
    /// and the write did not and will not take effect. Never forwarded: whoever holds this context
    /// must go through the ordinary [`crate::EzKv::write`] path on the *current* leader instead, if
    /// the write still needs to happen.
    #[error("this node is not confirmed to be leading in term {term} anymore")]
    NotLeader {
        /// The term this write was scoped to - [`crate::leader_task::LeaderCtx::term`], or the
        /// term the built-in liveness task's own command was proposed under
        term: u64,
    },

    /// The bounded wait ran out while this node still appeared to lead in `term`, without a
    /// definite answer: the write may have been appended and may yet commit, or may never have
    /// been accepted at all (a leader cut off from its quorum gets neither a lease nor a commit)
    ///
    /// Deliberately not [`Self::NotLeader`]. That one says the context is dead; this one says
    /// nothing about it - the node may go on leading in this very term once the trouble passes. A
    /// task that ended on it would not run again until the next term, so the thing to do is to
    /// read the state again and, if the write is still needed, issue it again under its CAS
    /// conditions, which are what make a late commit of the first attempt harmless.
    #[error("no definite outcome within the deadline while still leading in term {term}")]
    Indeterminate {
        /// The term this write was scoped to
        term: u64,
    },
}
