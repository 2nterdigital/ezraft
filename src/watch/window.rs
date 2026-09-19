//! The retained window: an in-memory ring of the most recent change groups
//!
//! Every successful transaction produces a [`Changes`] group - also an empty one - so the
//! window's contents cover a contiguous revision range with a well-defined lower bound.
//! [`Window`] is never snapshotted and never replicated: it exists purely to let a subscriber
//! resume from a recent revision without this node re-deriving anything, and to let
//! [`Window::snapshot`] hand a new subscriber the whole retained range in O(1) inside the watch
//! hub's critical section (an [`imbl::Vector`] clones its root the same way `KvState`'s map
//! does), with filtering to a subscriber's prefix done afterward, outside that section.

use std::sync::Arc;

use crate::kv::Change;
use crate::kv::Changes;

/// The in-memory ring of retained change groups, bounded by count and by an approximate byte size
pub(super) struct Window {
    /// Retained groups, oldest first. Every group in here has `revision > self.lower_bound`, and
    /// the whole set is contiguous: no revision between `self.lower_bound` and the newest group's
    /// revision is ever missing, because every successful transaction pushes exactly one group.
    groups: imbl::Vector<Arc<Changes>>,
    /// Sum of [`group_bytes`] over every group currently in `groups`
    bytes: usize,
    max_groups: usize,
    max_bytes: usize,
    /// The newest revision no longer covered by this window: a resume position at or after this
    /// value can be served from `groups`; anything older is gone and needs a `Reset`. Starts at
    /// the revision the window was created at (a fresh node, or the revision a snapshot install
    /// restarted the window from) and advances to the evicted group's own revision every time
    /// [`Self::push`] evicts one.
    lower_bound: u64,
}

impl Window {
    /// A fresh, empty window starting at `lower_bound`
    pub(super) fn new(lower_bound: u64, max_groups: usize, max_bytes: usize) -> Self {
        Self {
            groups: imbl::Vector::new(),
            bytes: 0,
            max_groups,
            max_bytes,
            lower_bound,
        }
    }

    /// Discard everything retained and start a fresh, empty window at `lower_bound`
    ///
    /// Used when this node installs a snapshot: the state it retained groups against no longer
    /// exists, so nothing in the old window can be replayed against the new one.
    pub(super) fn reset(&mut self, lower_bound: u64) {
        self.groups = imbl::Vector::new();
        self.bytes = 0;
        self.lower_bound = lower_bound;
    }

    /// Append the group one more applied transaction produced, evicting the oldest retained
    /// groups until both limits are satisfied again
    ///
    /// A single group larger than `max_bytes` on its own is not an error: eviction still
    /// terminates (the window can end up empty), it just retains nothing until a smaller group is
    /// pushed. `lower_bound` tracks exactly the revision of the last group evicted, which is what
    /// keeps [`Self::lower_bound`] a precise boundary between "still resumable" and "needs a
    /// `Reset`" - see this module's own doc comment.
    pub(super) fn push(&mut self, group: Arc<Changes>) {
        self.bytes += group_bytes(&group);
        self.groups.push_back(group);

        while self.groups.len() > self.max_groups || self.bytes > self.max_bytes {
            let Some(evicted) = self.groups.pop_front() else {
                break;
            };
            self.bytes -= group_bytes(&evicted);
            self.lower_bound = evicted.revision;
        }
    }

    /// The newest revision this window can no longer serve a resume request for - see the
    /// `lower_bound` field's own doc comment
    pub(super) fn lower_bound(&self) -> u64 {
        self.lower_bound
    }

    /// An O(1) clone of everything currently retained, for a new subscriber to filter to its own
    /// prefix outside the hub's lock
    pub(super) fn snapshot(&self) -> imbl::Vector<Arc<Changes>> {
        self.groups.clone()
    }
}

/// A conservative estimate of one group's retained size: the key and value bytes of every item,
/// mirroring what a caller actually cares about bounding (payload size), without paying for a
/// real serialization on every `apply` just to measure it
fn group_bytes(group: &Changes) -> usize {
    group
        .items
        .iter()
        .map(|change| match change {
            Change::Put { key, value, .. } => key.len() + value.len(),
            Change::Delete { key, prev } => key.len() + prev.key.len() + prev.value.len(),
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::kv::Change;

    fn put_group(revision: u64, key: &str, value: &str) -> Arc<Changes> {
        Arc::new(Changes {
            revision,
            items: vec![Change::Put {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value.as_bytes()),
                create_rev: revision,
                mod_rev: revision,
            }],
        })
    }

    #[test]
    fn a_fresh_window_s_lower_bound_is_where_it_was_created() {
        let window = Window::new(5, 10, 1024);
        assert_eq!(5, window.lower_bound());
        assert_eq!(0, window.snapshot().len());
    }

    #[test]
    fn pushing_within_both_limits_retains_every_group_and_never_moves_the_lower_bound() {
        let mut window = Window::new(0, 10, 1024);
        window.push(put_group(1, "a", "1"));
        window.push(put_group(2, "b", "2"));

        assert_eq!(0, window.lower_bound());
        let snapshot = window.snapshot();
        assert_eq!(vec![1, 2], snapshot.iter().map(|g| g.revision).collect::<Vec<_>>());
    }

    #[test]
    fn exceeding_the_group_count_evicts_the_oldest_and_advances_the_lower_bound_to_it() {
        let mut window = Window::new(0, 2, 1024);
        window.push(put_group(1, "a", "1"));
        window.push(put_group(2, "b", "2"));
        window.push(put_group(3, "c", "3"));

        assert_eq!(
            1,
            window.lower_bound(),
            "revision 1 was evicted, so it is the new floor"
        );
        let snapshot = window.snapshot();
        assert_eq!(vec![2, 3], snapshot.iter().map(|g| g.revision).collect::<Vec<_>>());
    }

    #[test]
    fn exceeding_the_byte_limit_evicts_the_oldest_even_with_room_left_on_the_count_limit() {
        let mut window = Window::new(0, 100, 4);
        window.push(put_group(1, "a", "1")); // 2 bytes
        window.push(put_group(2, "b", "2")); // 2 bytes, total 4: still within the 4-byte limit
        window.push(put_group(3, "c", "3")); // pushes total to 6: revision 1 must go

        assert_eq!(1, window.lower_bound());
        let snapshot = window.snapshot();
        assert_eq!(vec![2, 3], snapshot.iter().map(|g| g.revision).collect::<Vec<_>>());
    }

    #[test]
    fn a_single_group_larger_than_the_byte_limit_empties_the_window_but_never_panics() {
        let mut window = Window::new(0, 100, 1);
        window.push(put_group(1, "a", "1")); // 2 bytes alone exceeds the 1-byte limit

        assert_eq!(
            1,
            window.lower_bound(),
            "the oversized group was evicted, so it is the new floor"
        );
        assert_eq!(0, window.snapshot().len());

        // The window keeps working afterwards: a group that does fit its limits is retained.
        let mut window = Window::new(0, 100, 3);
        window.push(put_group(1, "a", "0123456789"));
        window.push(put_group(2, "b", "2"));
        assert_eq!(1, window.lower_bound());
        assert_eq!(
            vec![2],
            window.snapshot().iter().map(|g| g.revision).collect::<Vec<_>>()
        );
    }

    #[test]
    fn reset_discards_everything_and_starts_a_fresh_lower_bound() {
        let mut window = Window::new(0, 10, 1024);
        window.push(put_group(1, "a", "1"));
        window.push(put_group(2, "b", "2"));

        window.reset(10);

        assert_eq!(10, window.lower_bound());
        assert_eq!(0, window.snapshot().len());
    }
}
