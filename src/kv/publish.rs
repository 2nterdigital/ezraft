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
use crate::kv::registry::Registry;
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

    /// Wait until the published view has reached `revision`, or `deadline` passes first
    ///
    /// Woken by the publish itself rather than polling for it: this is the wait behind a write's
    /// read-your-writes guarantee, so every millisecond of polling interval would be added to the
    /// latency of every write issued on a follower. `false` if the deadline passed first, or the
    /// node shut down and nothing will ever be published again.
    pub(crate) async fn wait_for_revision(&self, revision: u64, deadline: tokio::time::Instant) -> bool {
        // A clone, because waiting needs `&mut`: the receiver this node reads `view()` through is
        // shared by every handle and must never be left marked as having seen a value.
        let mut rx = self.0.clone();
        tokio::time::timeout_at(deadline, rx.wait_for(|view| view.revision() >= revision))
            .await
            .is_ok_and(|reached| reached.is_ok())
    }
}

/// The one place a new [`View`] becomes visible to every [`ViewReceiver`], and the watch side
/// (`crate::watch::Hub`) that shares it
///
/// Also the one place a new [`Registry`] becomes visible (ech0-media#33): a plain
/// `tokio::sync::watch<Registry>`, not the windowed hub above - the registry is small (one entry
/// per node), so every subscriber is simply handed the whole current value, the same way
/// [`ViewReceiver`] itself works, rather than a ring buffer of changes to replay.
pub(crate) struct Publisher {
    tx: watch::Sender<View>,
    hub: std::sync::Arc<Hub>,
    registry_tx: watch::Sender<Registry>,
}

impl Publisher {
    /// A publisher already holding `view` and `registry` - the "publish after startup recovery"
    /// case falls out of this for free: whatever construction restored (fresh, from a snapshot, or
    /// after replaying the log tail past one) is what the first [`ViewReceiver::current`] call, and
    /// the first read of the registry watch channel, see.
    ///
    /// The watch hub starts with [`WatchLimits::default`]; `crate::raft::EzKv::new` narrows or
    /// widens it to the node's real `EzConfig::watch_limits` via [`Self::configure_watch`] before
    /// starting the Raft engine, since this constructor runs before that configuration reaches it.
    pub(crate) fn new(view: View, registry: Registry) -> (Self, ViewReceiver, watch::Receiver<Registry>) {
        let hub = Hub::new(view.clone(), WatchLimits::default());
        let (tx, rx) = watch::channel(view);
        let (registry_tx, registry_rx) = watch::channel(registry);
        (Self { tx, hub, registry_tx }, ViewReceiver(rx), registry_rx)
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

    /// Publish the registry after a registry command applied, or after installing a snapshot
    ///
    /// A no-op notification-wise when `registry` is unchanged from what is already published
    /// (`send_if_modified`): the registry is small enough that a caller can simply hand this the
    /// whole current value every time, without tracking for itself whether anything actually
    /// changed.
    pub(crate) fn publish_registry(&self, registry: Registry) {
        self.registry_tx.send_if_modified(|current| {
            let changed = *current != registry;
            if changed {
                *current = registry;
            }
            changed
        });
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

        let (_publisher, view_rx, _registry_rx) = Publisher::new(state.view(), state.registry());

        assert_eq!(1, view_rx.current().revision());
        assert_eq!(Bytes::from_static(b"1"), view_rx.current().get("a").unwrap().value);
    }

    #[test]
    fn publish_updates_every_clone_of_the_receiver() {
        let (publisher, view_rx, _registry_rx) = Publisher::new(KvState::new().view(), Registry::default());
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
        let (publisher, view_rx, _registry_rx) = Publisher::new(state.view(), state.registry());

        let mut restored = KvState::new();
        restored.apply(&crate::kv::Txn::new().put("b", Bytes::from_static(b"2"))).unwrap();
        publisher.reset(restored.view());

        assert_eq!(None, view_rx.current().get("a"), "reset replaces the view wholesale");
        assert_eq!(Bytes::from_static(b"2"), view_rx.current().get("b").unwrap().value);
    }

    #[test]
    fn a_registry_receiver_starts_with_the_publisher_s_initial_registry() {
        let mut state = KvState::new();
        state.apply_registry_command(crate::kv::RequestId::new(), 100, |registry| {
            registry.set(1, crate::kv::NodeInfo {
                external_addr: "127.0.0.1:9001".to_string(),
                meta: Bytes::from_static(b"m"),
                status: crate::kv::NodeStatus::Up,
            });
        });

        let (_publisher, _view_rx, registry_rx) = Publisher::new(state.view(), state.registry());

        assert_eq!("127.0.0.1:9001", registry_rx.borrow().get(1).unwrap().external_addr);
    }

    #[test]
    fn publish_registry_updates_every_clone_and_is_a_no_op_when_unchanged() {
        let (publisher, _view_rx, registry_rx) = Publisher::new(KvState::new().view(), Registry::default());
        let mut clone = registry_rx.clone();

        let mut registry = Registry::default();
        registry.set(1, crate::kv::NodeInfo {
            external_addr: "127.0.0.1:9001".to_string(),
            meta: Bytes::from_static(b"m"),
            status: crate::kv::NodeStatus::Up,
        });
        publisher.publish_registry(registry.clone());

        assert!(clone.has_changed().unwrap(), "a real change notifies subscribers");
        assert_eq!(
            "127.0.0.1:9001",
            clone.borrow_and_update().get(1).unwrap().external_addr
        );

        // Publishing the exact same registry again must not raise a further change - a caller
        // that always publishes "the whole current value" should never spuriously wake anyone.
        publisher.publish_registry(registry);
        assert!(
            !clone.has_changed().unwrap(),
            "an unchanged registry must not notify again"
        );
    }
}
