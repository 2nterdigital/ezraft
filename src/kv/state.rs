//! `KvState`: the deterministic state machine every replica applies identically
//!
//! [`KvState::apply`] is a plain function from a [`Txn`] to a result: no clocks, no randomness,
//! no I/O, no iteration order that depends on anything but key order. Given the same sequence of
//! transactions, every replica ends with the same `kv`, the same `revision`, and the same
//! `create_rev`/`mod_rev` on every key - which is the property the Raft wiring
//! (`crate::storage::adapter::state_machine`) depends on to keep replicas identical.

use std::sync::Arc;

use imbl::OrdMap;
use serde::Deserialize;
use serde::Serialize;

use crate::kv::change::Change;
use crate::kv::change::Changes;
use crate::kv::ledger::Ledger;
use crate::kv::ledger::RequestId;
use crate::kv::ledger::TxnOutcome;
use crate::kv::txn::Condition;
use crate::kv::txn::Conflict;
use crate::kv::txn::Op;
use crate::kv::txn::Txn;
use crate::kv::view::KeyValue;
use crate::kv::view::StoredValue;
use crate::kv::view::View;
use crate::kv::view::to_key_value;

/// The foundation-internal replicated state block, next to the user's KV
///
/// Holds the idempotency ledger (ech0-media#30) today; the node registry (ech0-media#33) adds a
/// sibling field the same way. Applied by the same [`KvState::apply_idempotent`] call as the
/// user's KV, and folded into the same snapshot right next to it (`crate::kv::snapshot_codec`),
/// without ever bumping the snapshot's format version. Plain serde derives only (no
/// `tag`/`untagged`/`flatten`/`skip_serializing_if`/`default`): this type is embedded directly in
/// the postcard snapshot body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InternalState {
    ledger: Ledger,
}

/// What [`KvState::apply_idempotent`] produced
pub(crate) struct Applied {
    /// The command's outcome - freshly computed, or the ledger's already-recorded one for a
    /// retried request id.
    pub(crate) outcome: TxnOutcome,
    /// The change group to publish, if `apply_idempotent` actually executed `txn` this call.
    /// `None` when the request id was already recorded: nothing changed, so there is nothing new
    /// for `crate::storage::adapter::state_machine` to publish.
    pub(crate) changes: Option<Changes>,
}

/// The KV state machine: the user's key space, the revision counter, and the foundation-internal
/// state block
///
/// # Where foundation-internal state goes
///
/// `internal` is the foundation-internal replicated state block: never part of the user key
/// space in `kv` above, applied by this same [`apply_idempotent`](Self::apply_idempotent) call,
/// and folded into the same snapshot next to `kv` (ech0-media#28). It holds the idempotency
/// ledger (ech0-media#30) today; the node registry (ech0-media#33) adds a field to
/// [`InternalState`] itself, not to `KvState`, so the snapshot's v1 layout never needs a version
/// bump for it either.
pub(crate) struct KvState {
    /// The user's key space: `Arc<str>` keys so revisions that touch only a few keys share the
    /// rest of their key strings by pointer instead of cloning them.
    kv: OrdMap<Arc<str>, StoredValue>,
    /// The KV's own revision counter: `0` before any transaction has ever committed, incremented
    /// by exactly one on every transaction whose conditions all hold.
    revision: u64,
    /// The foundation-internal state block - see [`InternalState`]'s own docs. Carried through
    /// untouched by every [`apply`](Self::apply) call today, since nothing writes to it yet.
    internal: InternalState,
}

impl KvState {
    /// An empty state machine at revision `0`
    pub(crate) fn new() -> Self {
        Self {
            kv: OrdMap::new(),
            revision: 0,
            internal: InternalState::default(),
        }
    }

    /// A consistent, point-in-time view of the current state
    ///
    /// O(1): clones the persistent map's root and the revision counter. The clone shares
    /// structure with `self.kv`, so holding the returned [`View`] indefinitely - across any
    /// number of later [`apply`](Self::apply) calls - never blocks them and never changes what
    /// the view reads.
    pub(crate) fn view(&self) -> View {
        View::new(self.revision, self.kv.clone())
    }

    /// A cheap clone of the foundation-internal state block, for building a snapshot
    ///
    /// O(1): the ledger's maps are `imbl::OrdMap`s (an `Arc`-backed persistent structure, the same
    /// discipline `kv` itself follows) - ledger/registry tickets that add further fields to
    /// [`InternalState`] must keep that discipline too, so building a snapshot never has to block
    /// `apply` to read this either.
    pub(crate) fn internal_state(&self) -> InternalState {
        self.internal.clone()
    }

    /// Apply one command's transaction, consulting the idempotency ledger first
    ///
    /// If `request_id` is already recorded, `txn` never runs again: this returns the recorded
    /// outcome unchanged and [`Applied::changes`] is `None` - nothing for the caller to publish,
    /// because nothing changed. Otherwise this calls [`Self::apply`], records the resulting
    /// outcome (success or conflict alike - see [`crate::kv::ledger`]'s own docs for why both are
    /// recorded), and evicts down to `ledger_capacity`.
    ///
    /// `ledger_capacity` is never this node's own configured value read here: it is handed in by
    /// the caller (`crate::storage::adapter::state_machine`), which reads it from the very
    /// [`crate::command::Command`] being applied - see [`crate::kv::ledger`]'s module docs for why
    /// that is the determinism this method depends on.
    pub(crate) fn apply_idempotent(&mut self, request_id: RequestId, ledger_capacity: usize, txn: &Txn) -> Applied {
        if let Some(outcome) = self.internal.ledger.get(request_id) {
            return Applied { outcome, changes: None };
        }

        match self.apply(txn) {
            Ok(changes) => {
                let outcome: TxnOutcome = Ok(changes.revision);
                self.internal.ledger.record(request_id, outcome.clone(), ledger_capacity);
                Applied {
                    outcome,
                    changes: Some(changes),
                }
            }
            Err(conflict) => {
                let outcome: TxnOutcome = Err(conflict);
                self.internal.ledger.record(request_id, outcome.clone(), ledger_capacity);
                Applied { outcome, changes: None }
            }
        }
    }

    /// Evaluate `txn`'s conditions and, if they all hold, apply its ops
    ///
    /// Conditions are evaluated against the state as it was *before* this call, in order; the
    /// first one that does not hold aborts the whole transaction - `self` is left exactly as it
    /// was, revision included - and is reported as a [`Conflict`]. If every condition holds, the
    /// revision advances by exactly one (even if no op ends up changing anything) and every op
    /// then applies in order, each one seeing the effects of the ones before it within this same
    /// call.
    ///
    /// Deliberately does not check [`Txn::validate`]'s limits: a node with a different configured
    /// limit than the one that originally accepted this transaction must still apply it the same
    /// way every other replica does, or replicas diverge. Every caller that can propose a
    /// transaction validates it itself, before it ever reaches here.
    pub(crate) fn apply(&mut self, txn: &Txn) -> Result<Changes, Conflict> {
        for (index, condition) in txn.conditions.iter().enumerate() {
            let key = condition.key();
            let current_mod_rev = self.kv.get(key).map_or(0, |stored| stored.mod_rev);
            let holds = match condition {
                Condition::ModRevEq { mod_rev, .. } => current_mod_rev == *mod_rev,
                Condition::NotExists { .. } => current_mod_rev == 0,
                Condition::Exists { .. } => current_mod_rev != 0,
            };
            if !holds {
                return Err(Conflict {
                    index,
                    condition: condition.clone(),
                    key: key.to_string(),
                    current_mod_rev,
                });
            }
        }

        let revision = self.revision + 1;
        let mut items = Vec::new();

        for op in &txn.ops {
            match op {
                Op::Put { key, value } => {
                    let create_rev = self.kv.get(key.as_str()).map_or(revision, |stored| stored.create_rev);
                    self.kv.insert(Arc::<str>::from(key.as_str()), StoredValue {
                        value: value.clone(),
                        create_rev,
                        mod_rev: revision,
                    });
                    items.push(Change::Put {
                        key: key.clone(),
                        value: value.clone(),
                        create_rev,
                        mod_rev: revision,
                    });
                }
                Op::Delete { key } => {
                    if let Some(removed) = self.kv.remove(key.as_str()) {
                        items.push(Change::Delete {
                            key: key.clone(),
                            prev: to_key_value(key, &removed),
                        });
                    }
                }
                Op::DeletePrefix { prefix } => {
                    // Collected before removing anything: `self.kv` cannot be mutated while a
                    // borrow of it from `range` is still alive. Key order is preserved because
                    // `range` walks the map in order.
                    let matches: Vec<Arc<str>> = keys_with_prefix(&self.kv, prefix).collect();
                    for key in matches {
                        if let Some(removed) = self.kv.remove(&key) {
                            items.push(Change::Delete {
                                key: key.to_string(),
                                prev: to_key_value(&key, &removed),
                            });
                        }
                    }
                }
            }
        }

        self.revision = revision;
        Ok(Changes { revision, items })
    }

    /// Rebuild a state machine from a decoded snapshot's parts
    ///
    /// The counterpart to [`Self::view`] and [`Self::internal_state`]: `crate::kv::snapshot_codec`
    /// is what actually produces `entries` (from a view's `prefix("")`, off the state machine's
    /// lock) and `internal`, so this is the smallest seam that lets the Raft adapter rebuild an
    /// identical state machine from what a snapshot decode handed back.
    pub(crate) fn restore(revision: u64, entries: Vec<KeyValue>, internal: InternalState) -> Self {
        let mut kv = OrdMap::new();
        for entry in entries {
            kv.insert(Arc::<str>::from(entry.key.as_str()), StoredValue {
                value: entry.value,
                create_rev: entry.create_rev,
                mod_rev: entry.mod_rev,
            });
        }
        Self { kv, revision, internal }
    }
}

/// Every key in `kv` that starts with `prefix`, in key order
///
/// Shared by [`KvState::apply`]'s `DeletePrefix` handling and (via [`View::prefix`]) reads, so
/// the two agree on exactly what a prefix matches - see `view.rs` for the raw-string-prefix
/// semantics this implies (a prefix like `"room/1"` matches `"room/10"` too).
fn keys_with_prefix<'a>(kv: &'a OrdMap<Arc<str>, StoredValue>, prefix: &'a str) -> impl Iterator<Item = Arc<str>> + 'a {
    kv.range::<_, str>((std::ops::Bound::Included(prefix), std::ops::Bound::Unbounded))
        .take_while(move |(key, _)| key.starts_with(prefix))
        .map(|(key, _)| key.clone())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn txn() -> Txn {
        Txn::new()
    }

    #[test]
    fn new_state_starts_at_revision_zero_and_empty() {
        let state = KvState::new();
        assert_eq!(0, state.view().revision());
        assert_eq!(None, state.view().get("a"));
    }

    #[test]
    fn a_successful_transaction_advances_revision_by_exactly_one() {
        let mut state = KvState::new();
        let changes = state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap();
        assert_eq!(1, changes.revision);
        assert_eq!(1, state.view().revision());
    }

    #[test]
    fn a_transaction_with_no_ops_still_advances_revision_by_one() {
        let mut state = KvState::new();
        let changes = state.apply(&txn()).unwrap();
        assert_eq!(1, changes.revision);
        assert_eq!(1, state.view().revision());
        assert_eq!(Vec::<Change>::new(), changes.items);
    }

    #[test]
    fn put_on_a_new_key_sets_create_rev_and_mod_rev_to_the_new_revision() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap();
        let kv = state.view().get("a").unwrap();
        assert_eq!(1, kv.create_rev);
        assert_eq!(1, kv.mod_rev);
        assert_eq!(Bytes::from_static(b"1"), kv.value);
    }

    #[test]
    fn put_on_an_existing_key_keeps_create_rev_and_advances_mod_rev() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap();
        state.apply(&txn().put("a", Bytes::from_static(b"2"))).unwrap();
        let kv = state.view().get("a").unwrap();
        assert_eq!(1, kv.create_rev);
        assert_eq!(2, kv.mod_rev);
        assert_eq!(Bytes::from_static(b"2"), kv.value);
    }

    #[test]
    fn create_rev_resets_after_delete_and_re_put() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap(); // revision 1
        state.apply(&txn().delete("a")).unwrap(); // revision 2
        state.apply(&txn().put("a", Bytes::from_static(b"3"))).unwrap(); // revision 3
        let kv = state.view().get("a").unwrap();
        assert_eq!(3, kv.create_rev);
        assert_eq!(3, kv.mod_rev);
    }

    #[test]
    fn delete_of_an_absent_key_is_not_an_error_and_makes_no_change() {
        let mut state = KvState::new();
        let changes = state.apply(&txn().delete("missing")).unwrap();
        assert_eq!(1, changes.revision);
        assert_eq!(Vec::<Change>::new(), changes.items);
    }

    #[test]
    fn delete_reports_the_previous_value() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap();
        let changes = state.apply(&txn().delete("a")).unwrap();
        assert_eq!(
            vec![Change::Delete {
                key: "a".to_string(),
                prev: crate::kv::KeyValue {
                    key: "a".to_string(),
                    value: Bytes::from_static(b"1"),
                    create_rev: 1,
                    mod_rev: 1,
                },
            }],
            changes.items
        );
        assert_eq!(None, state.view().get("a"));
    }

    #[test]
    fn delete_prefix_removes_exactly_the_prefix_not_byte_order_neighbors() {
        let mut state = KvState::new();
        state
            .apply(
                &txn()
                    .put("room/1", Bytes::from_static(b"a"))
                    .put("room/10", Bytes::from_static(b"b"))
                    .put("room0", Bytes::from_static(b"c")),
            )
            .unwrap();

        let changes = state.apply(&txn().delete_prefix("room/1/")).unwrap();

        // "room/1/" (trailing separator) matches neither "room/1" nor "room/10": both are
        // untouched. Nothing under "room/1/" exists yet, so this is a no-op change-wise.
        assert_eq!(Vec::<Change>::new(), changes.items);
        assert!(state.view().get("room/1").is_some());
        assert!(state.view().get("room/10").is_some());
        assert!(state.view().get("room0").is_some());
    }

    #[test]
    fn delete_prefix_removes_room_10_when_asked_for_room_1() {
        // The other side of the same trap: prefix "room/1" (no trailing separator) legitimately
        // matches "room/10" too, since it is a plain string prefix.
        let mut state = KvState::new();
        state
            .apply(
                &txn()
                    .put("room/1", Bytes::from_static(b"a"))
                    .put("room/10", Bytes::from_static(b"b"))
                    .put("room0", Bytes::from_static(b"c")),
            )
            .unwrap();

        state.apply(&txn().delete_prefix("room/1")).unwrap();

        assert_eq!(None, state.view().get("room/1"));
        assert_eq!(None, state.view().get("room/10"));
        assert!(state.view().get("room0").is_some());
    }

    #[test]
    fn delete_prefix_reports_prev_for_every_key_removed_in_key_order() {
        let mut state = KvState::new();
        state
            .apply(
                &txn()
                    .put("room/1/b", Bytes::from_static(b"2"))
                    .put("room/1/a", Bytes::from_static(b"1"))
                    .put("room/1/c", Bytes::from_static(b"3")),
            )
            .unwrap();

        let changes = state.apply(&txn().delete_prefix("room/1/")).unwrap();

        let deleted_keys: Vec<&str> = changes
            .items
            .iter()
            .map(|change| match change {
                Change::Delete { key, .. } => key.as_str(),
                Change::Put { .. } => panic!("unexpected Put in a DeletePrefix's changes"),
            })
            .collect();
        assert_eq!(vec!["room/1/a", "room/1/b", "room/1/c"], deleted_keys);
    }

    #[test]
    fn delete_prefix_then_put_in_one_transaction_resets_create_rev() {
        let mut state = KvState::new();
        state.apply(&txn().put("room/1/a", Bytes::from_static(b"old"))).unwrap(); // revision 1

        let changes = state
            .apply(&txn().delete_prefix("room/1/").put("room/1/a", Bytes::from_static(b"new"))) // revision 2
            .unwrap();

        assert_eq!(
            vec![
                Change::Delete {
                    key: "room/1/a".to_string(),
                    prev: crate::kv::KeyValue {
                        key: "room/1/a".to_string(),
                        value: Bytes::from_static(b"old"),
                        create_rev: 1,
                        mod_rev: 1,
                    },
                },
                Change::Put {
                    key: "room/1/a".to_string(),
                    value: Bytes::from_static(b"new"),
                    create_rev: 2,
                    mod_rev: 2,
                },
            ],
            changes.items
        );
        let kv = state.view().get("room/1/a").unwrap();
        assert_eq!(
            2, kv.create_rev,
            "the re-put after the delete starts a fresh create_rev"
        );
        assert_eq!(Bytes::from_static(b"new"), kv.value);
    }

    #[test]
    fn items_are_listed_in_op_order_not_grouped_by_op_kind() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1")).put("b", Bytes::from_static(b"2"))).unwrap();

        let changes = state.apply(&txn().delete("a").put("c", Bytes::from_static(b"3")).delete("b")).unwrap();

        let keys: Vec<&str> = changes
            .items
            .iter()
            .map(|change| match change {
                Change::Put { key, .. } | Change::Delete { key, .. } => key.as_str(),
            })
            .collect();
        assert_eq!(vec!["a", "c", "b"], keys);
    }

    #[test]
    fn mod_rev_eq_condition_passes_when_it_matches_and_fails_otherwise() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap(); // mod_rev 1

        assert!(
            state
                .apply(
                    &txn()
                        .when(Condition::ModRevEq {
                            key: "a".to_string(),
                            mod_rev: 1
                        })
                        .put("a", Bytes::from_static(b"2"))
                )
                .is_ok()
        );

        let conflict = state
            .apply(
                &txn()
                    .when(Condition::ModRevEq {
                        key: "a".to_string(),
                        mod_rev: 1,
                    })
                    .put("a", Bytes::from_static(b"3")),
            )
            .unwrap_err();
        assert_eq!(
            Conflict {
                index: 0,
                condition: Condition::ModRevEq {
                    key: "a".to_string(),
                    mod_rev: 1
                },
                key: "a".to_string(),
                current_mod_rev: 2,
            },
            conflict
        );
    }

    #[test]
    fn not_exists_condition_reports_zero_for_an_absent_key_and_the_real_mod_rev_when_present() {
        let mut state = KvState::new();

        assert!(
            state
                .apply(&txn().when(Condition::NotExists { key: "a".to_string() }).put("a", Bytes::from_static(b"1")))
                .is_ok()
        );

        let conflict = state
            .apply(&txn().when(Condition::NotExists { key: "a".to_string() }).put("a", Bytes::from_static(b"2")))
            .unwrap_err();
        assert_eq!(
            Conflict {
                index: 0,
                condition: Condition::NotExists { key: "a".to_string() },
                key: "a".to_string(),
                current_mod_rev: 1,
            },
            conflict
        );
    }

    #[test]
    fn exists_condition_fails_with_mod_rev_zero_for_an_absent_key() {
        let mut state = KvState::new();
        let conflict = state
            .apply(
                &txn()
                    .when(Condition::Exists {
                        key: "missing".to_string(),
                    })
                    .put("a", Bytes::from_static(b"1")),
            )
            .unwrap_err();
        assert_eq!(
            Conflict {
                index: 0,
                condition: Condition::Exists {
                    key: "missing".to_string()
                },
                key: "missing".to_string(),
                current_mod_rev: 0,
            },
            conflict
        );
    }

    #[test]
    fn conflict_reports_the_first_failing_condition_not_a_later_one() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap();

        // Condition 0 holds (a exists); condition 1 fails (b does not exist) - the report must
        // name index 1, not be confused by the passing condition before it.
        let conflict = state
            .apply(
                &txn()
                    .when(Condition::Exists { key: "a".to_string() })
                    .when(Condition::Exists { key: "b".to_string() })
                    .put("c", Bytes::from_static(b"3")),
            )
            .unwrap_err();
        assert_eq!(1, conflict.index);
        assert_eq!("b", conflict.key);
        assert_eq!(0, conflict.current_mod_rev);
    }

    #[test]
    fn a_conflicting_transaction_leaves_data_and_revision_untouched() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap(); // revision 1

        let result = state.apply(
            &txn()
                .when(Condition::NotExists { key: "a".to_string() })
                .put("a", Bytes::from_static(b"2"))
                .put("b", Bytes::from_static(b"new")),
        );
        assert!(result.is_err());

        assert_eq!(1, state.view().revision());
        assert_eq!(Bytes::from_static(b"1"), state.view().get("a").unwrap().value);
        assert_eq!(None, state.view().get("b"));
    }

    #[test]
    fn a_view_taken_before_later_applies_never_changes() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap();
        let held = state.view();

        state.apply(&txn().put("a", Bytes::from_static(b"2"))).unwrap();
        state.apply(&txn().put("b", Bytes::from_static(b"3"))).unwrap();
        state.apply(&txn().delete("a")).unwrap();

        assert_eq!(1, held.revision());
        assert_eq!(Bytes::from_static(b"1"), held.get("a").unwrap().value);
        assert_eq!(None, held.get("b"));

        // The live state, meanwhile, reflects every apply since.
        assert_eq!(4, state.view().revision());
        assert_eq!(None, state.view().get("a"));
        assert_eq!(Bytes::from_static(b"3"), state.view().get("b").unwrap().value);
    }

    #[test]
    fn two_replicas_applying_the_same_sequence_end_byte_identical() {
        let sequence = [
            txn().put("room/1", Bytes::from_static(b"lobby")),
            txn().put("room/1/a", Bytes::from_static(b"alice")),
            txn()
                .when(Condition::ModRevEq {
                    key: "room/1".to_string(),
                    mod_rev: 1,
                })
                .put("room/1", Bytes::from_static(b"lobby2")),
            txn().delete_prefix("room/1/"),
            txn(),
        ];

        let mut replica_a = KvState::new();
        let mut replica_b = KvState::new();
        for txn in &sequence {
            let result_a = replica_a.apply(txn);
            let result_b = replica_b.apply(txn);
            assert_eq!(result_a, result_b, "replicas diverged applying {txn:?}");
        }

        assert_eq!(replica_a.view().revision(), replica_b.view().revision());
        let all_a: Vec<crate::kv::KeyValue> = replica_a.view().prefix("").collect();
        let all_b: Vec<crate::kv::KeyValue> = replica_b.view().prefix("").collect();
        assert_eq!(all_a, all_b);
    }

    #[test]
    fn restoring_from_a_view_reproduces_the_same_state() {
        let mut state = KvState::new();
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap();
        state.apply(&txn().put("b", Bytes::from_static(b"2"))).unwrap();
        state.apply(&txn().put("a", Bytes::from_static(b"3"))).unwrap();

        let view = state.view();
        let entries: Vec<crate::kv::KeyValue> = view.prefix("").collect();
        let restored = KvState::restore(view.revision(), entries, state.internal_state());

        assert_eq!(state.view().revision(), restored.view().revision());
        let original: Vec<crate::kv::KeyValue> = state.view().prefix("").collect();
        let round_tripped: Vec<crate::kv::KeyValue> = restored.view().prefix("").collect();
        assert_eq!(
            original, round_tripped,
            "revision, create_rev and mod_rev must all survive"
        );
    }

    #[test]
    fn restoring_empty_entries_gives_an_empty_state_at_that_revision() {
        let restored = KvState::restore(0, Vec::new(), InternalState::default());
        assert_eq!(0, restored.view().revision());
        assert_eq!(0, restored.view().prefix("").count());
    }

    #[test]
    fn apply_idempotent_executes_an_unseen_request_id_and_reports_changes_to_publish() {
        let mut state = KvState::new();
        let applied = state.apply_idempotent(RequestId::new(), 100, &txn().put("a", Bytes::from_static(b"1")));

        assert_eq!(Ok(1), applied.outcome);
        assert!(applied.changes.is_some(), "a first execution must be published");
        assert_eq!(1, state.view().revision());
    }

    #[test]
    fn apply_idempotent_deduplicates_a_repeated_request_id_without_executing_again() {
        let mut state = KvState::new();
        let id = RequestId::new();

        let first = state.apply_idempotent(id, 100, &txn().put("a", Bytes::from_static(b"1")));
        assert_eq!(Ok(1), first.outcome);

        // A second command with the same request id, even one that would itself succeed and
        // change more state, must not run: the ledger's recorded outcome comes back unchanged,
        // and nothing new is published.
        let second = state.apply_idempotent(
            id,
            100,
            &txn().put("a", Bytes::from_static(b"2")).put("b", Bytes::from_static(b"3")),
        );

        assert_eq!(Ok(1), second.outcome, "the first outcome is returned, not a fresh one");
        assert!(
            second.changes.is_none(),
            "a deduplicated command has nothing new to publish"
        );
        assert_eq!(1, state.view().revision(), "revision does not advance for a duplicate");
        assert_eq!(
            Bytes::from_static(b"1"),
            state.view().get("a").unwrap().value,
            "the duplicate's ops never ran"
        );
        assert_eq!(None, state.view().get("b"));
    }

    #[test]
    fn apply_idempotent_records_a_conflict_and_a_retry_gets_the_same_conflict_even_after_state_changes() {
        let mut state = KvState::new();
        let id = RequestId::new();

        let condition = Condition::NotExists { key: "a".to_string() };
        state.apply(&txn().put("a", Bytes::from_static(b"1"))).unwrap(); // revision 1: "a" now exists

        let first = state.apply_idempotent(
            id,
            100,
            &txn().when(condition.clone()).put("a", Bytes::from_static(b"2")),
        );
        assert!(first.outcome.is_err(), "the condition fails: \"a\" already exists");
        assert!(first.changes.is_none(), "a conflict never has anything to publish");

        // Deleting "a" would make the very same condition hold now - but the retry must still see
        // the *recorded* conflict, not re-evaluate it.
        state.apply(&txn().delete("a")).unwrap(); // revision 2

        let retry = state.apply_idempotent(id, 100, &txn().when(condition).put("a", Bytes::from_static(b"2")));
        assert_eq!(
            first.outcome, retry.outcome,
            "a retry gets the recorded conflict, not a fresh evaluation"
        );
        assert!(retry.changes.is_none());
        assert_eq!(None, state.view().get("a"), "the retry's op never ran either");
    }

    #[test]
    fn apply_idempotent_evicts_by_the_capacity_the_command_carries_not_a_fixed_value() {
        let mut state = KvState::new();
        let first_id = RequestId::new();
        state.apply_idempotent(first_id, 1, &txn().put("a", Bytes::from_static(b"1")));

        // A capacity of 1, carried by *this* command, evicts the previous entry even though a
        // later command could carry a different capacity - eviction follows whatever the command
        // being applied says, never a value read from this replica's own configuration.
        state.apply_idempotent(RequestId::new(), 1, &txn().put("b", Bytes::from_static(b"2")));

        let retried = state.apply_idempotent(first_id, 1, &txn().put("a", Bytes::from_static(b"3")));
        assert_eq!(Ok(3), retried.outcome, "outside the window, the same id executes again");
        assert!(retried.changes.is_some());
    }

    #[test]
    fn the_ledger_survives_a_restore_from_a_snapshot() {
        let mut state = KvState::new();
        let id = RequestId::new();
        state.apply_idempotent(id, 100, &txn().put("a", Bytes::from_static(b"1")));

        let restored = KvState::restore(
            state.view().revision(),
            state.view().prefix("").collect(),
            state.internal_state(),
        );

        let retried = restored.internal.ledger.get(id);
        assert_eq!(
            Some(Ok(1)),
            retried,
            "the ledger entry recorded before the snapshot is still there"
        );
    }
}
