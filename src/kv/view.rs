//! `View`: a lock-free, point-in-time read handle over the KV
//!
//! `KvState::view` (crate-internal until ech0-media#27b) hands out a `View` by cloning the
//! persistent map's root - O(1), never blocking `apply` and never blocking other readers. Every
//! read a `View` answers
//! comes from the exact same revision, so a caller reading several keys never observes half of a
//! multi-key transaction.

use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use imbl::OrdMap;
use serde::Deserialize;
use serde::Serialize;

use crate::kv::bytes_codec;

/// A key's value together with its revision bookkeeping
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyValue {
    /// The key
    pub key: String,
    /// The opaque bytes stored under `key`
    #[serde(with = "bytes_codec")]
    pub value: Bytes,
    /// The revision at which `key` was created (or last re-created, after a delete)
    pub create_rev: u64,
    /// The revision of the transaction that last wrote `key`
    pub mod_rev: u64,
}

/// A key's value and revision bookkeeping, as stored inside the KV
///
/// The map key (the string) is not repeated here: [`OrdMap`] already holds it as the entry's key,
/// so [`to_key_value`] only clones it when it actually hands a [`KeyValue`] back to a caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StoredValue {
    pub(super) value: Bytes,
    pub(super) create_rev: u64,
    pub(super) mod_rev: u64,
}

/// Build the public [`KeyValue`] a caller sees from a map entry
pub(super) fn to_key_value(key: &str, stored: &StoredValue) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: stored.value.clone(),
        create_rev: stored.create_rev,
        mod_rev: stored.mod_rev,
    }
}

/// A page of [`KeyValue`]s from [`View::page`], plus the cursor for the next page
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Page {
    /// The items in this page, in key order
    pub items: Vec<KeyValue>,
    /// Pass this as `after` to fetch the next page; `None` once the prefix is exhausted
    pub next: Option<String>,
}

/// A consistent, point-in-time view of the KV at a single revision
///
/// `Clone` is O(1): it clones the persistent map's root, the same structural sharing that lets
/// `KvState::apply` (crate-internal until ech0-media#27b) keep running while a `View` is held
/// indefinitely - see `state::tests::a_view_taken_before_later_applies_never_changes` for the
/// property this exists to guarantee.
#[derive(Clone)]
pub struct View {
    revision: u64,
    kv: OrdMap<Arc<str>, StoredValue>,
}

impl View {
    /// Build a view from the state machine's current revision and map
    pub(super) fn new(revision: u64, kv: OrdMap<Arc<str>, StoredValue>) -> Self {
        Self { revision, kv }
    }

    /// The revision every read from this view answers from
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Look up a single key
    pub fn get(&self, key: &str) -> Option<KeyValue> {
        self.kv.get(key).map(|stored| to_key_value(key, stored))
    }

    /// Look up several keys at once, from the same revision
    ///
    /// Returns one `Option<KeyValue>` per input key, in the same order, so a caller assembling a
    /// response from several keys never has to guess which result matched which request.
    pub fn get_many<'a, I>(&self, keys: I) -> Vec<Option<KeyValue>>
    where I: IntoIterator<Item = &'a str> {
        keys.into_iter().map(|key| self.get(key)).collect()
    }

    /// Iterate every key that starts with `prefix`, in key order
    ///
    /// The underlying range query starts at `prefix` itself (never at the beginning of the map),
    /// so a prefix deep in a large key space costs O(log n + matches), not O(n).
    pub fn prefix<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = KeyValue> + 'a {
        self.kv
            .range::<_, str>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(key, stored)| to_key_value(key, stored))
    }

    /// A page of at most `limit` keys starting with `prefix`, resuming after `after` if given
    ///
    /// `after` is the `next` cursor a previous call returned; passing `None` starts from the
    /// beginning of the prefix. `after` may point anywhere, not just inside the prefix: one
    /// before it is treated the same as starting fresh (`None`), and one at or past its end
    /// yields an empty, exhausted page - so a caller does not have to validate a saved cursor
    /// before reusing it. `limit == 0` returns no items and echoes `after` back unchanged, a
    /// no-op rather than a silent skip.
    ///
    /// `next` is `None` once there is nothing left after this page, and `Some(key)` to pass as
    /// `after` for the next call otherwise.
    pub fn page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Page {
        if limit == 0 {
            return Page {
                items: Vec::new(),
                next: after.map(str::to_string),
            };
        }

        let start = match after {
            Some(cursor) if cursor >= prefix => Bound::Excluded(cursor),
            _ => Bound::Included(prefix),
        };

        let iter = self
            .kv
            .range::<_, str>((start, Bound::Unbounded))
            .take_while(move |(key, _)| key.starts_with(prefix));

        // One item beyond `limit` is fetched but never pushed - only used to tell "more remain"
        // (`exhausted = false`) from "this was everything" (`exhausted = true`, the loop runs out
        // on its own). `next` is the last item actually returned, matching `after`'s own meaning
        // ("resume strictly after this key"), not the peeked-but-withheld one.
        let mut items = Vec::with_capacity(limit.min(1024));
        let mut exhausted = true;
        for (key, stored) in iter {
            if items.len() == limit {
                exhausted = false;
                break;
            }
            items.push(to_key_value(key, stored));
        }

        let next = if exhausted {
            None
        } else {
            items.last().map(|kv| kv.key.clone())
        };
        Page { items, next }
    }
}

impl fmt::Debug for View {
    /// Summary only: `revision` and entry count, never the KV's contents
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("View").field("revision", &self.revision).field("len", &self.kv.len()).finish()
    }
}

/// Compile-time proof that `View` keeps the bounds ech0-media#27 promises its callers: `Clone`
/// (checked structurally by `#[derive(Clone)]` above too) and safe to hold across threads and
/// beyond any particular borrow (`Send + Sync + 'static`).
const _: fn() = || {
    fn assert_bounds<T: Clone + Send + Sync + 'static>() {}
    assert_bounds::<View>();
};

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(value: &'static str, create_rev: u64, mod_rev: u64) -> StoredValue {
        StoredValue {
            value: Bytes::from_static(value.as_bytes()),
            create_rev,
            mod_rev,
        }
    }

    fn view_with(entries: &[(&str, StoredValue)]) -> View {
        let mut kv = OrdMap::new();
        for (key, value) in entries {
            kv.insert(Arc::<str>::from(*key), value.clone());
        }
        View::new(entries.len() as u64, kv)
    }

    #[test]
    fn revision_reads_back_what_view_was_built_with() {
        let view = View::new(7, OrdMap::new());
        assert_eq!(7, view.revision());
    }

    #[test]
    fn get_finds_an_existing_key() {
        let view = view_with(&[("room/1", stored("lobby", 1, 1))]);
        assert_eq!(
            Some(KeyValue {
                key: "room/1".to_string(),
                value: Bytes::from_static(b"lobby"),
                create_rev: 1,
                mod_rev: 1,
            }),
            view.get("room/1")
        );
    }

    #[test]
    fn get_returns_none_for_an_absent_key() {
        let view = view_with(&[]);
        assert_eq!(None, view.get("missing"));
    }

    #[test]
    fn get_many_preserves_input_order_and_mixes_hits_and_misses() {
        let view = view_with(&[("a", stored("1", 1, 1)), ("b", stored("2", 2, 2))]);
        let results = view.get_many(["b", "missing", "a"]);
        assert_eq!(
            vec![
                Some(KeyValue {
                    key: "b".to_string(),
                    value: Bytes::from_static(b"2"),
                    create_rev: 2,
                    mod_rev: 2
                }),
                None,
                Some(KeyValue {
                    key: "a".to_string(),
                    value: Bytes::from_static(b"1"),
                    create_rev: 1,
                    mod_rev: 1
                }),
            ],
            results
        );
    }

    #[test]
    fn prefix_is_a_pure_string_prefix_room_10_included_room0_excluded() {
        // The classic trap: prefix matching is a raw string prefix, not a path-segment
        // boundary. "room/10" genuinely starts with "room/1" (as a string) and must be
        // included; "room0" does not start with "room/1" (it diverges at the 5th byte, '0' vs
        // '/') and must be excluded. A caller that wants only true path children asks for
        // "room/1/" (trailing separator) instead - see the next test.
        let view = view_with(&[
            ("room/1", stored("a", 1, 1)),
            ("room/1/participant/x", stored("b", 2, 2)),
            ("room/10", stored("c", 3, 3)),
            ("room0", stored("d", 4, 4)),
        ]);

        let keys: Vec<String> = view.prefix("room/1").map(|kv| kv.key).collect();
        assert_eq!(
            vec![
                "room/1".to_string(),
                "room/1/participant/x".to_string(),
                "room/10".to_string()
            ],
            keys
        );
    }

    #[test]
    fn prefix_with_trailing_separator_excludes_numeric_extensions() {
        let view = view_with(&[
            ("room/1", stored("a", 1, 1)),
            ("room/1/participant/x", stored("b", 2, 2)),
            ("room/10", stored("c", 3, 3)),
        ]);

        let keys: Vec<String> = view.prefix("room/1/").map(|kv| kv.key).collect();
        assert_eq!(vec!["room/1/participant/x".to_string()], keys);
    }

    #[test]
    fn prefix_returns_nothing_when_no_key_matches() {
        let view = view_with(&[("a", stored("1", 1, 1))]);
        assert_eq!(0, view.prefix("z").count());
    }

    #[test]
    fn page_walks_a_prefix_exactly_once_across_page_boundaries() {
        let entries: Vec<(String, StoredValue)> =
            (0..23).map(|i| (format!("room/{i:02}"), stored("v", 1, 1))).collect();
        let mut kv = OrdMap::new();
        for (key, value) in &entries {
            kv.insert(Arc::<str>::from(key.as_str()), value.clone());
        }
        let view = View::new(1, kv);

        let mut seen = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let page = view.page("room/", after.as_deref(), 5);
            seen.extend(page.items.into_iter().map(|kv| kv.key));
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }

        let mut expected: Vec<String> = entries.into_iter().map(|(key, _)| key).collect();
        expected.sort();
        assert_eq!(expected, seen);
    }

    #[test]
    fn page_with_limit_zero_returns_no_items_and_echoes_the_cursor() {
        let view = view_with(&[("a", stored("1", 1, 1))]);
        let page = view.page("a", Some("z"), 0);
        assert_eq!(
            Page {
                items: Vec::new(),
                next: Some("z".to_string())
            },
            page
        );
    }

    #[test]
    fn page_with_after_before_the_prefix_starts_from_the_beginning() {
        let view = view_with(&[("room/1", stored("a", 1, 1)), ("room/2", stored("b", 2, 2))]);
        let page = view.page("room/", Some("aaa"), 10);
        let keys: Vec<String> = page.items.into_iter().map(|kv| kv.key).collect();
        assert_eq!(vec!["room/1".to_string(), "room/2".to_string()], keys);
        assert_eq!(None, page.next);
    }

    #[test]
    fn page_with_after_past_the_prefix_is_empty_and_exhausted() {
        let view = view_with(&[("room/1", stored("a", 1, 1))]);
        let page = view.page("room/", Some("zzz"), 10);
        assert_eq!(
            Page {
                items: Vec::new(),
                next: None
            },
            page
        );
    }

    #[test]
    fn cloning_a_view_is_independent_of_later_mutation_through_the_original_map() {
        let mut kv = OrdMap::new();
        kv.insert(Arc::<str>::from("a"), stored("1", 1, 1));
        let view = View::new(1, kv.clone());

        kv.insert(Arc::<str>::from("a"), stored("2", 2, 2));
        kv.insert(Arc::<str>::from("b"), stored("3", 2, 2));

        // The view's own clone of the map root never observes the later mutations - the same
        // guarantee `KvState::view` relies on to let readers keep a view across further applies.
        assert_eq!(
            Some(KeyValue {
                key: "a".to_string(),
                value: Bytes::from_static(b"1"),
                create_rev: 1,
                mod_rev: 1
            }),
            view.get("a")
        );
        assert_eq!(None, view.get("b"));
    }
}
