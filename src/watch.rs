//! `watch`: resumable, grouped subscriptions to the KV's changes (ech0-media#32)
//!
//! [`EzKv::watch`] and [`EzKv::view_and_watch`] let a caller follow a key prefix's changes as they
//! are applied, in strictly increasing revision order, without polling [`View`] in a loop. Both
//! share the single publish source `crate::kv::publish::Publisher` already uses for `view()`
//! (`hub::Hub`, crate-internal): the same critical section that swaps the current view also
//! appends the transaction's group to the retained window and hands it to every subscriber, which
//! is what makes "the view, then everything after it" atomic for [`EzKv::view_and_watch`] and for
//! a resuming [`WatchStart::After`].
//!
//! # What the stream delivers
//!
//! - [`WatchUpdate::Changes`] - one transaction's changes (the crate's own vocabulary for a
//!   replicated-state change; an *event* is what an upstream caller derives from one), filtered to
//!   the subscribed prefix, as one group, items in op order. A transaction that touched nothing
//!   inside the prefix produces no item at all.
//! - [`WatchUpdate::Reset`] - everything before its `view`'s revision may have been missed; discard
//!   whatever was derived from earlier items and start over from `view`. Sent, and only sent, in
//!   the three situations [`ResetCause`] names. The crate's guarantee: **no `Reset`, no gap** - see
//!   `hub::Hub`'s own doc comment for the argument.
//! - [`WatchUpdate::Progress`] - the revision advanced with nothing in it for this subscriber; lets
//!   a resume cursor keep advancing even during a quiet prefix. Rate-limited per subscriber and
//!   never observable ahead of a `Changes` this same subscriber still has coming for an equal or
//!   lower revision (`hub::Hub::record_and_fanout` and the progress task both enqueue through the
//!   very same lock and the very same per-subscriber queue, so program order there is delivery
//!   order here).
//!
//! # The retained window
//!
//! An in-memory ring of the most recent change groups (`window::Window`, crate-internal), bounded
//! by [`WatchLimits::max_window_groups`] and [`WatchLimits::max_window_bytes`]. Never snapshotted,
//! never replicated: after a restart it starts empty, at the revision the node started from, and
//! fills back up as the log tail replays - a consumer that resumed across the restart may see some
//! of those replayed groups again, deduplicated by each key's `mod_rev`.

mod hub;
mod stream;
mod window;

use std::time::Duration;

pub(crate) use hub::Hub;
pub use stream::WatchStream;

use crate::kv::Changes;
use crate::kv::View;
use crate::raft::EzKv;

/// Where a [`EzKv::watch`] subscription begins
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchStart {
    /// Only changes committed after this call; no historical replay
    Now,
    /// Resume after a previously observed revision - typically one saved from an earlier
    /// [`WatchUpdate::Changes`] or [`WatchUpdate::Progress`]. A revision this node has not reached
    /// yet is accepted: the subscription simply waits for it. A revision older than the retained
    /// window produces a [`WatchUpdate::Reset`] with [`ResetCause::ResumeTooOld`] as the stream's
    /// first item, then continues live from there.
    After(u64),
}

/// One item a [`WatchStream`] yields, in strictly increasing revision order
#[derive(Clone, Debug)]
pub enum WatchUpdate {
    /// One transaction's changes, filtered to the subscribed prefix, in op order
    Changes(Changes),
    /// Discard whatever was derived from earlier items: this subscription may have missed
    /// something before `view`'s revision, and `view` is where delivery resumes from
    Reset {
        /// A view at exactly the revision delivery resumes from: every later `Changes` this
        /// stream yields has a revision strictly greater than `view.revision()`
        view: View,
        /// Why this reset happened
        cause: ResetCause,
    },
    /// The replicated revision advanced with nothing to deliver to this subscription for it
    Progress {
        /// The new revision; safe to save as a [`WatchStart::After`] resume cursor
        revision: u64,
    },
}

/// Why a [`WatchUpdate::Reset`] was sent
#[derive(Clone, Debug)]
pub enum ResetCause {
    /// The requested resume revision is older than the retained window's lower bound: whatever
    /// changed between the two is no longer in this node's memory
    ResumeTooOld {
        /// The revision the caller asked to resume after
        requested: u64,
        /// The oldest revision this node's window still covers everything after
        window_floor: u64,
    },
    /// This subscription's queue filled up before it was drained, so its whole backlog was
    /// dropped rather than slowing down `apply`
    SubscriberTooSlow,
    /// This node replaced its whole state by installing a snapshot
    SnapshotInstalled,
}

/// Size, count and timing limits for `watch` subscriptions on one node
///
/// Configurable through [`crate::EzConfig::watch_limits`]. ech0-media#36 measured these defaults
/// (about 4 KB of memory per retained small write at most, fan-out latency flat from 1 to 100
/// subscribers) and kept them; with small changes the group count binds long before the byte
/// limit, which is there for unusually large values (`FORK.md`, 参数定值).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchLimits {
    /// Maximum number of change groups the retained window keeps. Default: 10,000.
    pub max_window_groups: usize,
    /// Maximum total approximate byte size (summed key and value bytes across every retained
    /// group's items) the retained window keeps. Default: 64 MiB.
    pub max_window_bytes: usize,
    /// Maximum number of undelivered items queued per subscriber before it is judged too slow and
    /// reset. Default: 1024.
    pub subscriber_queue_len: usize,
    /// How often, at most, a quiet subscriber (one whose prefix has no matching changes) receives
    /// a `Progress` update. Default: 1 second.
    pub progress_interval: Duration,
}

impl Default for WatchLimits {
    fn default() -> Self {
        Self {
            max_window_groups: 10_000,
            max_window_bytes: 64 * 1024 * 1024,
            subscriber_queue_len: 1024,
            progress_interval: Duration::from_secs(1),
        }
    }
}

impl EzKv {
    /// Subscribe to every change to `prefix`, in strictly increasing revision order
    ///
    /// An empty `prefix` subscribes to everything. `start` controls where the stream begins - see
    /// [`WatchStart`]. Dropping the returned stream unsubscribes; a subscriber that never polls
    /// never slows down this node's `apply` (its backlog is dropped and it is reset instead - see
    /// [`ResetCause::SubscriberTooSlow`]).
    pub fn watch(&self, prefix: impl Into<String>, start: WatchStart) -> WatchStream {
        self.watch_publisher().watch(prefix.into(), start).1
    }

    /// Atomically take a [`View`] of `prefix` together with the stream of everything after it
    ///
    /// Equivalent to `watch(prefix, WatchStart::Now)`, except the returned view and the stream are
    /// guaranteed adjacent: nothing that happened at or before the view's revision is delivered
    /// again, and nothing after it is missed. This is the seam a caller uses to get a full initial
    /// listing and a no-gap incremental feed from a single call, rather than racing a separate
    /// `view()` against a separately started `watch`.
    pub fn view_and_watch(&self, prefix: impl Into<String>) -> (View, WatchStream) {
        self.watch_publisher().watch(prefix.into(), WatchStart::Now)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::kv::Change;

    /// `Changes` filtered by `hub::filter_group`'s rule is exercised at the hub's own seam
    /// (`tests/watch.rs`, driven through `EzKv`); this only pins the public enum's shape so a
    /// caller can destructure it without reaching into crate internals.
    #[test]
    fn watch_update_variants_carry_the_documented_data() {
        let changes = WatchUpdate::Changes(Changes {
            revision: 1,
            items: vec![Change::Put {
                key: "a".to_string(),
                value: Bytes::from_static(b"1"),
                create_rev: 1,
                mod_rev: 1,
            }],
        });
        assert!(matches!(changes, WatchUpdate::Changes(_)));

        let progress = WatchUpdate::Progress { revision: 7 };
        assert!(matches!(progress, WatchUpdate::Progress { revision: 7 }));
    }

    #[test]
    fn watch_limits_default_matches_the_documented_values() {
        let limits = WatchLimits::default();
        assert_eq!(10_000, limits.max_window_groups);
        assert_eq!(64 * 1024 * 1024, limits.max_window_bytes);
        assert_eq!(1024, limits.subscriber_queue_len);
        assert_eq!(Duration::from_secs(1), limits.progress_interval);
    }
}
