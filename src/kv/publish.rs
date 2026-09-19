//! The single publish source behind `EzKv::view` and `watch`: publish first, reply to the writer
//! second
//!
//! The state-machine adapter (`crate::storage::adapter::state_machine`) owns exactly one
//! [`Publisher`]: for every applied transaction it publishes the new view *before* answering the
//! writer, and again after installing a snapshot and after startup recovery. A cloned
//! [`ViewReceiver`] is what [`crate::raft::EzKv::view`] reads from directly - no lock, no await,
//! never more than a pointer swap behind the state machine's own apply loop.
//!
//! `changes` travels through [`Publisher::publish`] into the watch hub (`crate::watch`,
//! ech0-media#32): the hub's own lock is what makes "swap the view, append to the retained window,
//! hand the group to every subscriber" one atomic step, so a `watch`/`view_and_watch` registration
//! can never land between a transaction's view becoming visible and its group reaching the window.
//! [`Publisher`] itself stays the single source both sides read from - the hub never gets its own,
//! separately-updated copy of "the current view" to disagree with what [`ViewReceiver`] answers.

use tokio::sync::watch;

use crate::kv::change::Changes;
use crate::kv::view::View;
use crate::watch::Hub;
use crate::watch::WatchLimits;
use crate::watch::WatchStart;
use crate::watch::WatchStream;

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

/// The one place a new [`View`] becomes visible to every [`ViewReceiver`], and the watch side
/// (`crate::watch::Hub`) that shares it
pub(crate) struct Publisher {
    tx: watch::Sender<View>,
    hub: std::sync::Arc<Hub>,
}

impl Publisher {
    /// A publisher already holding `view` - the "publish after startup recovery" case falls out
    /// of this for free: whatever view construction restored (fresh, from a snapshot, or after
    /// replaying the log tail past one) is what the first [`ViewReceiver::current`] call sees.
    ///
    /// The watch hub starts with [`WatchLimits::default`]; `crate::raft::EzKv::new` narrows or
    /// widens it to the node's real `EzConfig::watch_limits` via [`Self::configure_watch`] before
    /// starting the Raft engine, since this constructor runs before that configuration reaches it.
    pub(crate) fn new(view: View) -> (Self, ViewReceiver) {
        let hub = Hub::new(view.clone(), WatchLimits::default());
        let (tx, rx) = watch::channel(view);
        (Self { tx, hub }, ViewReceiver(rx))
    }

    /// Publish the view one applied transaction produced, together with the changes it made
    ///
    /// One critical section inside [`Hub::record_and_fanout`]: the retained window gets the new
    /// group, every matching subscriber gets it too, and only then does this view become the one
    /// [`ViewReceiver::current`] answers - see this module's own doc comment for why the ordering
    /// matters.
    pub(crate) fn publish(&self, view: View, changes: Changes) {
        self.hub.record_and_fanout(view, changes, &self.tx);
    }

    /// Publish the view after installing a snapshot, resetting every subscriber to it
    ///
    /// A snapshot install has no discrete change group the way an applied transaction does - only
    /// the resulting view - so every subscriber is told to discard whatever it derived from
    /// earlier items and start over from here instead of trying to reconcile a gap.
    pub(crate) fn reset(&self, view: View) {
        self.hub.reset_all(view, &self.tx);
    }

    /// Replace the watch hub's retention and delivery limits with the node's configured ones
    ///
    /// See [`Self::new`] for why this is a separate step rather than a constructor argument.
    pub(crate) fn configure_watch(&self, limits: WatchLimits) {
        self.hub.configure(limits);
    }

    /// End every watch stream: the node has shut down and nothing further will be published
    pub(crate) fn close_watch(&self) {
        self.hub.close();
    }

    /// Register a `watch`/`view_and_watch` subscription, atomically with reading the current view
    pub(crate) fn watch(&self, prefix: String, start: WatchStart) -> (View, WatchStream) {
        self.hub.register(prefix, start, &self.tx)
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
