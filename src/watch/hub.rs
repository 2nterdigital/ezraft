//! The watch hub: the retained window and every subscriber, behind one short critical section
//!
//! [`Hub`] is what makes `view()` and `watch` share a single publish source
//! (`crate::kv::publish::Publisher` holds one, alongside the `tokio::sync::watch` channel
//! `view()` reads from). Every state change here happens inside one `std::sync::Mutex` lock:
//! [`Hub::record_and_fanout`] (called from every applied transaction), [`Hub::reset_all`] (called
//! when this node installs a snapshot) and [`Hub::register`] (called by `watch`/`view_and_watch`)
//! all take the same lock, which is what makes "the view, then everything after it, with no gap
//! and no duplicate" atomic: a registration can never land between a transaction's window append
//! and its `View` becoming visible, because both happen inside the same critical section.
//!
//! Never held across an `.await` and never used to run a subscriber's own code: enqueuing an item
//! is a bounded `VecDeque` push (or, once a subscriber's queue is full, a clear-and-replace with a
//! single `Reset`), never a blocking send to the subscriber's own consumer.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::watch;

use super::ResetCause;
use super::WatchLimits;
use super::WatchStart;
use super::WatchUpdate;
use super::stream::WatchStream;
use super::window::Window;
use crate::kv::Change;
use crate::kv::Changes;
use crate::kv::View;

/// One subscriber's own delivery state
struct Subscriber {
    /// The key prefix this subscriber filters every group to; `""` matches everything
    prefix: String,
    /// Items ready for this subscriber's [`WatchStream`] to pop, in delivery order
    queue: VecDeque<WatchUpdate>,
    /// [`WatchLimits::subscriber_queue_len`]
    max_queue_len: usize,
    /// The revision this subscriber has been given something for (a `Changes`, `Reset` or
    /// `Progress`), or its registration revision if nothing has been sent yet. `Progress` is only
    /// due once the current revision passes this.
    last_delivered_revision: u64,
    /// When something was last enqueued for this subscriber - what rate-limits `Progress`
    last_sent_at: Instant,
    /// The `WatchStream::poll_next` waker to notify once `queue` has something in it again
    waker: Option<Waker>,
}

impl Subscriber {
    fn new(prefix: String, max_queue_len: usize) -> Self {
        Self {
            prefix,
            queue: VecDeque::new(),
            max_queue_len,
            last_delivered_revision: 0,
            last_sent_at: Instant::now(),
            waker: None,
        }
    }
}

/// Everything [`Hub`]'s lock guards
struct Inner {
    window: Window,
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    limits: WatchLimits,
    /// A copy of the latest published [`View`], kept alongside `window` so a `Reset` built from
    /// the progress-rate-limit task (which has no other way to reach the current view) is always
    /// exactly what `crate::raft::EzKv::view` would answer at that instant. Written in the same
    /// critical section as the `tokio::sync::watch` sender `record_and_fanout`/`reset_all` also
    /// update, so the two can never disagree - this is a cache of that single source, not a
    /// second one.
    current_view: View,
    /// Set once by [`Hub::close`] when the node shuts down: every stream ends once it has
    /// delivered what it already holds, instead of waiting forever for changes that cannot come.
    closed: bool,
}

/// The shared state behind every `watch`/`view_and_watch` subscription on one node
pub(crate) struct Hub {
    inner: Mutex<Inner>,
}

impl Hub {
    /// A fresh hub with no subscribers, its window starting at `view`'s revision
    ///
    /// Spawns the background task that delivers rate-limited `Progress`; it stops on its own once
    /// every clone of the returned `Arc` is dropped (it holds only a `Weak` reference), the same
    /// lifetime every other per-node background task in this crate already has. Every real
    /// caller (`crate::kv::publish::Publisher::new`, always reached through the async
    /// `crate::raft::EzKv::new`) runs inside a Tokio runtime, so the task is always spawned there;
    /// a plain, non-async unit test that builds a `Publisher` directly has no runtime to spawn
    /// onto and simply gets a hub whose `Progress` task never runs - harmless, since such a test
    /// exercises `view()`/`publish()`/`reset()` only, never `watch`.
    pub(crate) fn new(view: View, limits: WatchLimits) -> Arc<Self> {
        let hub = Arc::new(Self {
            inner: Mutex::new(Inner {
                window: Window::new(view.revision(), limits.max_window_groups, limits.max_window_bytes),
                subscribers: HashMap::new(),
                next_id: 0,
                limits,
                current_view: view,
                closed: false,
            }),
        });
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(progress_loop(Arc::downgrade(&hub)));
        }
        hub
    }

    /// Record one applied transaction's changes and hand each subscriber whatever of it matches
    /// its prefix - the critical section `crate::kv::publish::Publisher::publish` runs before
    /// answering the writer
    pub(crate) fn record_and_fanout(&self, view: View, changes: Changes, tx: &watch::Sender<View>) {
        let group = Arc::new(changes);
        let mut inner = self.inner.lock().unwrap();

        inner.window.push(group.clone());

        for (&id, sub) in inner.subscribers.iter_mut() {
            // A subscriber that registered with `WatchStart::After(rev)` for a `rev` this node
            // had not yet reached at registration time must not see anything at or before that
            // `rev` here: the live path has no other way to know it should still be waiting (see
            // `Self::register`'s own doc comment - only the *replay* it built at registration is
            // filtered by `after_rev`; this check is what makes the live path honor the same
            // starting point once it catches up to it).
            if group.revision <= sub.last_delivered_revision {
                continue;
            }
            if let Some(filtered) = filter_group(&group, &sub.prefix) {
                push_item(id, sub, WatchUpdate::Changes(filtered), &view);
            }
        }

        inner.current_view = view.clone();
        tx.send_replace(view);
    }

    /// Reset every subscriber to `view` because this node just installed a snapshot - the
    /// critical section `crate::kv::publish::Publisher::reset` runs
    pub(crate) fn reset_all(&self, view: View, tx: &watch::Sender<View>) {
        let mut inner = self.inner.lock().unwrap();

        inner.window.reset(view.revision());
        for (&id, sub) in inner.subscribers.iter_mut() {
            emit_reset(id, sub, &view, ResetCause::SnapshotInstalled);
        }

        inner.current_view = view.clone();
        tx.send_replace(view);
    }

    /// Register a new subscription and return the view it starts from together with its stream -
    /// `crate::raft::EzKv::watch`/`view_and_watch`'s seam into the hub
    ///
    /// The lock is held only long enough to read the current view, decide whether a `Reset` is
    /// needed, insert the subscriber, and take an O(1) clone of the retained window; filtering
    /// that clone to `prefix` happens afterward, outside the lock, so a subscriber joining a large
    /// window never delays a concurrent `apply`.
    pub(crate) fn register(
        self: &Arc<Self>,
        prefix: String,
        start: WatchStart,
        tx: &watch::Sender<View>,
    ) -> (View, WatchStream) {
        let (id, current_view, mut replay, window_groups, after_rev) = {
            let mut inner = self.inner.lock().unwrap();
            let current_view = tx.borrow().clone();
            let lower_bound = inner.window.lower_bound();
            let id = inner.next_id;
            inner.next_id += 1;

            let mut sub = Subscriber::new(prefix.clone(), inner.limits.subscriber_queue_len);

            let (replay, needs_window_replay, after_rev) = match start {
                // Nothing retained is newer than the view this starts from, so there is nothing
                // to replay.
                WatchStart::Now => (VecDeque::new(), false, current_view.revision()),
                WatchStart::After(rev) if rev < lower_bound => {
                    emit_reset(id, &mut sub, &current_view, ResetCause::ResumeTooOld {
                        requested: rev,
                        window_floor: lower_bound,
                    });
                    (std::mem::take(&mut sub.queue), false, current_view.revision())
                }
                WatchStart::After(rev) => (VecDeque::new(), true, rev),
            };

            // Every branch above lands on the revision delivery resumes strictly after -
            // including `rev` itself when it names a revision this node has not reached yet, so
            // the live check in `Self::record_and_fanout` correctly withholds anything at or
            // before it until the log genuinely catches up past it. `emit_reset` already set this
            // to the same value for the reset branch; setting it again here is a no-op for that
            // case and the only place it happens for the other two.
            sub.last_delivered_revision = after_rev;

            let window_groups = if needs_window_replay {
                inner.window.snapshot()
            } else {
                imbl::Vector::new()
            };

            inner.subscribers.insert(id, sub);

            (id, current_view, replay, window_groups, after_rev)
        };

        for group in window_groups.iter() {
            if group.revision <= after_rev {
                continue;
            }
            if let Some(filtered) = filter_group(group, &prefix) {
                replay.push_back(WatchUpdate::Changes(filtered));
            }
        }

        (current_view, WatchStream::new(self.clone(), id, replay))
    }

    /// Poll a registered subscriber's live queue - called only by its own [`WatchStream`], after
    /// that stream's own local replay buffer has been drained
    pub(crate) fn poll_subscriber(&self, id: u64, cx: &mut Context<'_>) -> Poll<Option<WatchUpdate>> {
        let mut inner = self.inner.lock().unwrap();
        let closed = inner.closed;
        // A `WatchStream` polls only while its own subscriber entry is registered - `Drop` removes
        // it last - so there is always one. Were there not, the stream has nothing left to
        // deliver: that is an ended stream, not a reason to panic in the consumer's task.
        let Some(sub) = inner.subscribers.get_mut(&id) else {
            return Poll::Ready(None);
        };

        match sub.queue.pop_front() {
            Some(item) => Poll::Ready(Some(item)),
            // Whatever was already queued is still delivered first; only an empty queue ends.
            None if closed => Poll::Ready(None),
            None => {
                sub.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// End every subscription: this node has shut down, so no further change can arrive
    ///
    /// Each stream still delivers what is already queued for it and then ends (`None`), rather
    /// than leaving its consumer waiting forever. A subscription registered after this ends the
    /// same way.
    pub(crate) fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        for sub in inner.subscribers.values_mut() {
            wake(sub);
        }
    }

    /// Drop a subscriber - called from `WatchStream`'s own `Drop` impl
    pub(crate) fn unregister(&self, id: u64) {
        self.inner.lock().unwrap().subscribers.remove(&id);
    }

    /// One tick of the background `Progress` task: for every subscriber whose rate-limit interval
    /// has elapsed since it last received anything, and whose revision has advanced since then,
    /// send it the current revision
    fn tick_progress(&self) {
        let mut inner = self.inner.lock().unwrap();
        let now = Instant::now();
        let interval = inner.limits.progress_interval;
        let revision = inner.current_view.revision();
        let view = inner.current_view.clone();

        for (&id, sub) in inner.subscribers.iter_mut() {
            if revision > sub.last_delivered_revision && now.duration_since(sub.last_sent_at) >= interval {
                push_item(id, sub, WatchUpdate::Progress { revision }, &view);
            }
        }
    }

    /// The interval the background `Progress` task should currently sleep for before its next
    /// check - re-read every iteration, so [`Self::configure`] takes effect on the task's very
    /// next wake rather than requiring it to be restarted
    fn progress_interval(&self) -> Duration {
        self.inner.lock().unwrap().limits.progress_interval
    }
}

/// The background task behind rate-limited `Progress`: wakes at a resolution finer than the
/// configured interval (so the interval is enforced to within that resolution, not up to a whole
/// extra interval late) and stops as soon as the hub itself is dropped
async fn progress_loop(hub: Weak<Hub>) {
    loop {
        let Some(strong) = hub.upgrade() else { return };
        let resolution = tick_resolution(strong.progress_interval());
        drop(strong);

        tokio::time::sleep(resolution).await;

        let Some(strong) = hub.upgrade() else { return };
        strong.tick_progress();
    }
}

/// A quarter of `interval`, floored at 5ms so a very small configured interval (as a test might
/// use) still does not spin the task needlessly fast
fn tick_resolution(interval: Duration) -> Duration {
    (interval / 4).max(Duration::from_millis(5))
}

/// `changes` filtered to `prefix`, or `None` if nothing in it matched - the shared rule both the
/// live fan-out and a new subscriber's window replay filter by
fn filter_group(changes: &Changes, prefix: &str) -> Option<Changes> {
    let items: Vec<Change> = changes.items.iter().filter(|change| matches_prefix(change, prefix)).cloned().collect();
    if items.is_empty() {
        None
    } else {
        Some(Changes {
            revision: changes.revision,
            items,
        })
    }
}

/// Whether one change's key starts with `prefix` - an empty prefix matches every key
fn matches_prefix(change: &Change, prefix: &str) -> bool {
    let key = match change {
        Change::Put { key, .. } | Change::Delete { key, .. } => key,
    };
    key.starts_with(prefix)
}

/// Enqueue `item` for `sub`, or - if its queue is already at `max_queue_len` - treat it as too
/// slow and replace its whole backlog with a `Reset` instead (see [`emit_reset`])
///
/// `id` is only used for the tracing event a resulting reset logs.
fn push_item(id: u64, sub: &mut Subscriber, item: WatchUpdate, view: &View) {
    if sub.queue.len() >= sub.max_queue_len {
        emit_reset(id, sub, view, ResetCause::SubscriberTooSlow);
        return;
    }

    sub.queue.push_back(item);
    sub.last_delivered_revision = view.revision();
    sub.last_sent_at = Instant::now();
    wake(sub);
}

/// Clear `sub`'s backlog and replace it with exactly one `Reset`, logging why
///
/// Always succeeds regardless of how full the queue already was: a reset supersedes whatever was
/// queued, since `view` already reflects every change up to and including its own revision.
fn emit_reset(id: u64, sub: &mut Subscriber, view: &View, cause: ResetCause) {
    tracing::info!(
        subscriber_id = id,
        prefix = %sub.prefix,
        cause = ?cause,
        last_delivered_revision = sub.last_delivered_revision,
        new_revision = view.revision(),
        "watch: sending a Reset to a subscriber",
    );

    sub.queue.clear();
    sub.queue.push_back(WatchUpdate::Reset {
        view: view.clone(),
        cause,
    });
    sub.last_delivered_revision = view.revision();
    sub.last_sent_at = Instant::now();
    wake(sub);
}

fn wake(sub: &mut Subscriber) {
    if let Some(waker) = sub.waker.take() {
        waker.wake();
    }
}
