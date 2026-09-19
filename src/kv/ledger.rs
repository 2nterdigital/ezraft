//! `RequestId`: a caller-chosen idempotency key, and the ledger that remembers each one's first
//! real outcome
//!
//! [`RequestId`] is generated fresh by [`crate::EzKv::write`] for every call, or supplied by the
//! caller through [`crate::EzKv::write_with_id`] to retry a specific earlier attempt. It travels
//! next to the transaction in the Raft log's [`crate::command::Command`], so
//! [`KvState::apply_idempotent`] can recognize a retried command before ever touching `kv`.
//!
//! [`Ledger`] is the foundation-internal state this recognition is built on: request id -> the
//! first real outcome recorded for it (a revision or a [`Conflict`]), evicted first-in-first-out
//! once more than a configured number of entries have been recorded. It lives inside
//! [`crate::kv::state::InternalState`], goes into the same snapshot as the user's KV, and is never
//! part of the user key space.
//!
//! # Determinism
//!
//! [`Ledger::record`]'s `capacity` is not read from this node's own configuration: the caller -
//! [`KvState::apply_idempotent`] - is handed it by the very [`crate::command::Command`] being
//! applied, stamped in by whichever node proposed it. Eviction is therefore a pure function of the
//! log: given the same sequence of commands, every replica evicts the same entries at the same
//! point, whatever its own locally configured `EzConfig::ledger_capacity` happens to be. A node
//! that evicted by its *own* capacity instead would diverge from a replica configured
//! differently the moment one of them deduplicated a retry the other re-executed.
//!
//! Eviction also never orders by [`RequestId`] itself (a ULID's timestamp bits reflect the
//! *generating* node's clock, which is not comparable across nodes with any clock skew): `Ledger`
//! keeps its own strictly-increasing sequence number, assigned only when an entry is actually
//! recorded, which is exactly and only what every replica agrees happened, in the same order,
//! because every replica applies the same log.

use std::fmt;

use imbl::OrdMap;
use serde::Deserialize;
use serde::Serialize;
use ulid::Ulid;

use crate::kv::txn::Conflict;

/// A caller-chosen idempotency key for one write
///
/// `Copy`, ordered, and plain-serde: it travels through the Raft log, a postcard snapshot (as
/// part of the ledger), and JSON (the forwarded-write RPC and `write_with_id`'s public
/// signature) unmodified. Two [`RequestId`]s are equal only if they are the exact same value -
/// there is no notion of "close enough".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RequestId(Ulid);

impl RequestId {
    /// A fresh id, effectively unique among every other id ever generated on any node
    pub fn new() -> Self {
        Self(Ulid::generate())
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// [`Display`](fmt::Display) covers every debugging need this id has (a request id never carries
/// caller data worth a separate `Debug` shape), so `Debug` just delegates to it.
impl fmt::Debug for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RequestId({self})")
    }
}

/// What one recorded command produced: the revision it committed at, or the conflict it hit -
/// exactly what a retry of the same request id gets back from [`Ledger::get`] without anything
/// running again.
pub(crate) type TxnOutcome = Result<u64, Conflict>;

/// One ledger entry: the recorded outcome, plus the insertion order it was recorded in
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    seq: u64,
    outcome: TxnOutcome,
}

/// Request id -> first real outcome, evicted first-in-first-out by entry count
///
/// Two maps, kept in step, both `imbl::OrdMap` so cloning either - and so cloning a whole
/// [`Ledger`] - is O(1), the same discipline the user's own KV follows for `KvState::view`
/// (`crate::kv::state`'s own module docs):
/// - `by_id` answers "have I recorded this request id" in O(log n), keyed the way callers ask.
/// - `by_seq` answers "which request ids are oldest" in O(log n), which `by_id` alone cannot: see
///   this module's own doc comment for why eviction cannot use [`RequestId`]'s own order instead.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Ledger {
    by_id: OrdMap<RequestId, Entry>,
    by_seq: OrdMap<u64, RequestId>,
    next_seq: u64,
}

impl Ledger {
    /// The outcome recorded for `id`, if [`Self::record`] has ever recorded one still within the
    /// eviction window
    pub(crate) fn get(&self, id: RequestId) -> Option<TxnOutcome> {
        self.by_id.get(&id).map(|entry| entry.outcome.clone())
    }

    /// Record `id`'s first real outcome, then evict the oldest entries until at most `capacity`
    /// remain
    ///
    /// A no-op if `id` is already recorded: [`KvState::apply_idempotent`] only calls this once
    /// [`Self::get`] has already reported nothing for `id`, and a second insert for the same id
    /// would itself be nondeterministic - which of two outcomes "wins" would depend on which
    /// replica happened to race which. See this module's own doc comment for why `capacity` comes
    /// from the caller (the command being applied) rather than this node's own configuration.
    pub(crate) fn record(&mut self, id: RequestId, outcome: TxnOutcome, capacity: usize) {
        if self.by_id.contains_key(&id) {
            return;
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        self.by_id.insert(id, Entry { seq, outcome });
        self.by_seq.insert(seq, id);

        while self.by_id.len() > capacity {
            let Some(&(oldest_seq, oldest_id)) = self.by_seq.get_min() else {
                break;
            };
            self.by_seq.remove(&oldest_seq);
            self.by_id.remove(&oldest_id);
        }
    }

    /// How many entries this ledger currently holds - test-only introspection of what is
    /// otherwise a purely internal structure.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_id.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conflict(current_mod_rev: u64) -> Conflict {
        Conflict {
            index: 0,
            condition: crate::kv::txn::Condition::Exists { key: "a".to_string() },
            key: "a".to_string(),
            current_mod_rev,
        }
    }

    #[test]
    fn an_unrecorded_id_reports_nothing() {
        let ledger = Ledger::default();
        assert_eq!(None, ledger.get(RequestId::new()));
    }

    #[test]
    fn a_recorded_success_is_returned_by_get() {
        let mut ledger = Ledger::default();
        let id = RequestId::new();
        ledger.record(id, Ok(7), 100);
        assert_eq!(Some(Ok(7)), ledger.get(id));
    }

    #[test]
    fn a_recorded_conflict_is_returned_by_get() {
        let mut ledger = Ledger::default();
        let id = RequestId::new();
        ledger.record(id, Err(conflict(3)), 100);
        assert_eq!(Some(Err(conflict(3))), ledger.get(id));
    }

    #[test]
    fn recording_the_same_id_twice_keeps_the_first_outcome() {
        let mut ledger = Ledger::default();
        let id = RequestId::new();
        ledger.record(id, Ok(1), 100);
        ledger.record(id, Ok(999), 100);
        assert_eq!(Some(Ok(1)), ledger.get(id));
        assert_eq!(1, ledger.len());
    }

    #[test]
    fn recording_within_capacity_keeps_every_entry() {
        let mut ledger = Ledger::default();
        let ids: Vec<RequestId> = (0..5).map(|_| RequestId::new()).collect();
        for (i, id) in ids.iter().enumerate() {
            ledger.record(*id, Ok(i as u64), 5);
        }
        assert_eq!(5, ledger.len());
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(Some(Ok(i as u64)), ledger.get(*id));
        }
    }

    #[test]
    fn recording_past_capacity_evicts_the_oldest_first() {
        let mut ledger = Ledger::default();
        let ids: Vec<RequestId> = (0..5).map(|_| RequestId::new()).collect();
        for (i, id) in ids.iter().enumerate() {
            ledger.record(*id, Ok(i as u64), 3);
        }

        assert_eq!(3, ledger.len());
        assert_eq!(None, ledger.get(ids[0]), "oldest evicted first");
        assert_eq!(None, ledger.get(ids[1]), "second-oldest evicted next");
        assert_eq!(Some(Ok(2)), ledger.get(ids[2]));
        assert_eq!(Some(Ok(3)), ledger.get(ids[3]));
        assert_eq!(Some(Ok(4)), ledger.get(ids[4]));
    }

    #[test]
    fn eviction_order_is_insertion_order_not_request_id_order() {
        // A ledger has no way to know which of two request ids was generated "first" across
        // nodes with clock skew - and must not pretend to: eviction must follow strictly the
        // order entries were recorded in, regardless of how the ids themselves compare.
        let mut ledger = Ledger::default();
        let low = RequestId(Ulid::from_parts(0, 0));
        let high = RequestId(Ulid::from_parts(u64::MAX, u128::MAX));

        // Recorded in this order: `high` first, `low` second.
        ledger.record(high, Ok(1), 1);
        ledger.record(low, Ok(2), 1);

        assert_eq!(None, ledger.get(high), "recorded first, evicted first");
        assert_eq!(Some(Ok(2)), ledger.get(low), "recorded second, survives");
    }

    #[test]
    fn a_capacity_of_zero_never_deduplicates_anything() {
        let mut ledger = Ledger::default();
        let id = RequestId::new();
        ledger.record(id, Ok(1), 0);
        assert_eq!(0, ledger.len());
        assert_eq!(None, ledger.get(id));
    }

    #[test]
    fn request_id_display_is_stable_and_never_empty() {
        let id = RequestId::new();
        assert!(!id.to_string().is_empty());
    }

    #[test]
    fn request_id_serde_round_trips_in_json_and_postcard() {
        let id = RequestId::new();

        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(id, serde_json::from_str(&json).unwrap());

        let packed = postcard::to_allocvec(&id).unwrap();
        assert_eq!(id, postcard::from_bytes(&packed).unwrap());
    }

    #[test]
    fn ledger_serde_round_trips_in_postcard_with_entries() {
        let mut ledger = Ledger::default();
        ledger.record(RequestId::new(), Ok(1), 100);
        ledger.record(RequestId::new(), Err(conflict(5)), 100);

        let packed = postcard::to_allocvec(&ledger).unwrap();
        let decoded: Ledger = postcard::from_bytes(&packed).unwrap();
        assert_eq!(ledger, decoded);
    }
}
