//! The single publish source behind `EzKv::view` (DESIGN.md §5's "publish-before-reply")
//!
//! The state-machine adapter (`crate::storage::adapter::state_machine`) owns exactly one
//! [`Publisher`]: for every applied transaction it publishes the new view *before* answering the
//! writer, and again after installing a snapshot and after startup recovery. A cloned
//! [`ViewReceiver`] is what [`crate::raft::EzKv::view`] reads from directly - no lock, no await,
//! never more than a pointer swap behind the state machine's own apply loop.
//!
//! `changes` travels through [`Publisher::publish`] purely so the seam is already shaped for
//! ech0-media#32, which plugs the watch ring buffer into this same publisher; until then it has
//! no consumer and is dropped rather than buffered.

use tokio::sync::watch;

use crate::kv::change::Changes;
use crate::kv::view::View;

/// A cheap, `Clone`-able read handle onto the latest published [`View`]
///
/// [`Self::current`] never awaits and never blocks the [`Publisher`] side: it is a plain
/// `watch::Receiver::borrow().clone()`.
#[derive(Clone)]
pub(crate) struct ViewReceiver(watch::Receiver<View>);

impl ViewReceiver {
    /// The latest view [`Publisher::publish`] or [`Publisher::reset`] set
    pub(crate) fn current(&self) -> View {
        self.0.borrow().clone()
    }
}

/// The one place a new [`View`] becomes visible to every [`ViewReceiver`]
pub(crate) struct Publisher {
    tx: watch::Sender<View>,
}

impl Publisher {
    /// A publisher already holding `view` - the "publish after startup recovery" case falls out
    /// of this for free: whatever view construction restored (fresh, from a snapshot, or after
    /// replaying the log tail past one) is what the first [`ViewReceiver::current`] call sees.
    pub(crate) fn new(view: View) -> (Self, ViewReceiver) {
        let (tx, rx) = watch::channel(view);
        (Self { tx }, ViewReceiver(rx))
    }

    /// Publish the view one applied transaction produced, together with the changes it made
    pub(crate) fn publish(&self, view: View, changes: Changes) {
        drop(changes);
        self.set(view);
    }

    /// Publish the view after installing a snapshot or replaying the log at startup
    ///
    /// Neither has a discrete change group the way an applied transaction does - only the
    /// resulting view - which is exactly the shape ech0-media#32's `Reset` needs.
    pub(crate) fn reset(&self, view: View) {
        self.set(view);
    }

    fn set(&self, view: View) {
        // Never fails: every `ViewReceiver` this crate hands out is a clone of the one made in
        // `Self::new`, and the `Publisher` itself outlives the state machine that owns it, so the
        // receiver side is never dropped first.
        self.tx.send_replace(view);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::kv::state::KvState;

    #[test]
    fn a_view_receiver_starts_with_the_publisher_s_initial_view() {
        let mut state = KvState::new();
        state.apply(&crate::kv::Txn::new().put("a", Bytes::from_static(b"1"))).unwrap();

        let (_publisher, view_rx) = Publisher::new(state.view());

        assert_eq!(1, view_rx.current().revision());
        assert_eq!(Bytes::from_static(b"1"), view_rx.current().get("a").unwrap().value);
    }

    #[test]
    fn publish_updates_every_clone_of_the_receiver() {
        let (publisher, view_rx) = Publisher::new(KvState::new().view());
        let clone = view_rx.clone();

        let mut state = KvState::new();
        let changes = state.apply(&crate::kv::Txn::new().put("a", Bytes::from_static(b"1"))).unwrap();
        publisher.publish(state.view(), changes);

        assert_eq!(1, view_rx.current().revision());
        assert_eq!(
            1,
            clone.current().revision(),
            "a receiver cloned before publish still sees it"
        );
    }

    #[test]
    fn reset_replaces_the_view_with_no_changes_argument() {
        let mut state = KvState::new();
        state.apply(&crate::kv::Txn::new().put("a", Bytes::from_static(b"1"))).unwrap();
        let (publisher, view_rx) = Publisher::new(state.view());

        let mut restored = KvState::new();
        restored.apply(&crate::kv::Txn::new().put("b", Bytes::from_static(b"2"))).unwrap();
        publisher.reset(restored.view());

        assert_eq!(None, view_rx.current().get("a"), "reset replaces the view wholesale");
        assert_eq!(Bytes::from_static(b"2"), view_rx.current().get("b").unwrap().value);
    }
}
