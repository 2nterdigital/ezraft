//! [`WatchStream`]: the `futures::Stream` `watch`/`view_and_watch` hand back
//!
//! Delivers a subscription's own local replay buffer (the retained window's contents at
//! registration time, already filtered to its prefix - see `super::hub::Hub::register`) before
//! switching to polling its live queue in the hub. Dropping it unsubscribes: `Drop` removes this
//! subscriber's entry from the hub, so nothing keeps fanning out to a stream nobody is polling
//! anymore.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::Stream;

use super::WatchUpdate;
use super::hub::Hub;

/// A live subscription to a key prefix's changes, in strictly increasing revision order
///
/// Implements [`futures::Stream`]; call [`futures::StreamExt::next`] to await the next item. The
/// stream ends (`None`) only when this node shuts down, after delivering whatever it already held;
/// until then it simply waits for the next change. Dropping it unsubscribes.
pub struct WatchStream {
    hub: Arc<Hub>,
    id: u64,
    /// Items already decided at registration time (a window replay, or a single `Reset`),
    /// delivered before anything the hub's own live queue holds. Kept local to this stream rather
    /// than pushed into the hub's bounded per-subscriber queue, so a catch-up replay - bounded by
    /// the retained window's own limits, not by the (typically much smaller) live queue capacity
    /// - can never itself be mistaken for a slow consumer.
    replay: VecDeque<WatchUpdate>,
}

impl WatchStream {
    pub(super) fn new(hub: Arc<Hub>, id: u64, replay: VecDeque<WatchUpdate>) -> Self {
        Self { hub, id, replay }
    }
}

impl Stream for WatchStream {
    type Item = WatchUpdate;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(item) = this.replay.pop_front() {
            return Poll::Ready(Some(item));
        }
        this.hub.poll_subscriber(this.id, cx)
    }
}

impl Drop for WatchStream {
    fn drop(&mut self) {
        self.hub.unregister(self.id);
    }
}
