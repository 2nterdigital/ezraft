//! Leader-only background tasks: `EzKv::spawn_leader_task` and `LeaderCtx` (ech0-media#34)
//!
//! [`EzKv::spawn_leader_task`] registers a task *factory* for the life of the node. One
//! supervisor per node ([`LeaderTasks`], started once from [`crate::raft::EzKv::new`]) watches
//! [`crate::EzKv::cluster`]: whenever this node becomes leader in term T, it calls every
//! registered factory with a fresh [`LeaderCtx`] carrying that term, and it aborts every run it
//! started the moment this node stops leading or the term changes - a node that loses and regains
//! leadership gets an entirely fresh run, never a resumed one. A factory registered while the node
//! already leads is started immediately, in the epoch already running, rather than waiting for a
//! `cluster()` change that may never come again this term.
//!
//! # Task supervision
//!
//! [`JoinHandle::abort`] is cooperative: an aborted task only actually stops the next time its
//! future would be polled (or is dropped outright if it is not currently running), so the
//! supervisor always `.await`s every handle of an ending epoch before it starts a new one - never
//! leaving two runs of the same task, one still finishing its teardown while the other has already
//! started, overlapping even briefly. Every run's own completion is observed the same way: whether
//! it returns normally or panics, [`spawn_one`] logs it and never restarts it - only a *new* epoch
//! (a fresh leadership, a new term) starts a fresh run of the same factory. A run this supervisor
//! aborts is not logged as "ended" or "panicked" here: cancellation is the expected way a run
//! stops when leadership moves on, not a failure of the run itself.
//!
//! # Fencing
//!
//! [`LeaderCtx::write`] never forwards: see [`crate::write::propose_leader_only`]'s own docs for
//! the argument that a write issued through a stale context can never take effect, on this node or
//! any other. Duplicate actions across a leadership hand-over - two nodes' tasks both trying to do
//! the same thing around the moment leadership moves - are the task author's to fence with CAS
//! conditions (`crate::Condition`) in the transaction itself; this framework only guarantees that
//! *this* context cannot act once it is stale, not that some other context does not race it.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use futures::FutureExt;
use openraft::ServerState;
use tokio::task::JoinHandle;

use crate::cluster::ClusterState;
use crate::command::CommandBody;
use crate::command::CommandOutcome;
use crate::kv::Txn;
use crate::raft::EzKv;
use crate::write::LeaderWriteError;

/// One run of a registered factory, boxed so [`LeaderTasks`] can hold factories of different
/// concrete closure/future types uniformly
type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A registered leader-task factory: cheap to clone (`Arc`), called once per leadership epoch for
/// every factory currently registered
type Factory = Arc<dyn Fn(LeaderCtx) -> BoxFuture + Send + Sync>;

/// What one leader-only task run is given: which term it runs under, when this leadership began,
/// and the node handle for reads and leader-only writes
///
/// Destructuring `LeaderCtx { term, .. }` is deliberate - [`Self::term`] is the one field a task
/// is expected to read directly; the node handle is reached through [`Self::node`] instead.
pub struct LeaderCtx {
    /// The term this node was leading in when this run started. [`Self::write`] is fenced to
    /// exactly this term: it never succeeds once this node's current term has moved past it,
    /// whether or not this node is (again) leading some later term.
    pub term: u64,
    node: EzKv,
    leader_since: Instant,
}

impl LeaderCtx {
    fn new(node: EzKv, term: u64, leader_since: Instant) -> Self {
        Self {
            term,
            node,
            leader_since,
        }
    }

    /// The node handle this run started on - for reads ([`EzKv::view`],
    /// [`EzKv::linearizable_view`], [`EzKv::registry`]) and inter-node RPC ([`EzKv::rpc`])
    /// exactly as any other caller would use them. [`Self::write`] is the one write path scoped
    /// to this run's own term.
    pub fn node(&self) -> &EzKv {
        &self.node
    }

    /// How long ago this leadership began - the reference point the built-in liveness task's new-
    /// leader grace period measures from (`crate::liveness`), not exposed as a public field since
    /// no other leader task needs it today.
    pub(crate) fn leader_since(&self) -> Instant {
        self.leader_since
    }

    /// Commit `txn`, only if this node is still leading in [`Self::term`]
    ///
    /// Never forwards: if this node no longer leads in `term`, this returns
    /// [`LeaderWriteError::NotLeader`] instead of asking any other node to commit it, and the
    /// guarantee is exact - the write is fenced to `term` in the log itself, so it cannot take
    /// effect under a later leadership either, not even one this same node wins. If this node
    /// *is* leading in `term` but the alpha.33 no-lease gap has not closed yet (freshly elected,
    /// before the first quorum acknowledgement), this waits it out, retrying within the same term,
    /// for up to [`crate::EzConfig::write_deadline`].
    ///
    /// [`LeaderWriteError::Indeterminate`] is the one answer not to end the task on: the deadline
    /// passed while this node still led, so the context may well still be good. A run that returns
    /// is not started again until the next term - read the state again and, if the write is still
    /// needed, issue it again under its CAS conditions.
    ///
    /// Same success/conflict/limit results as [`EzKv::write`]; read-your-writes holds trivially,
    /// since a committed write is this node's own leader-applied state before this call returns.
    /// Two calls under a hand-over (a stale run still executing when a new one starts) are not
    /// fenced against *each other* by this alone - see this module's own docs on CAS conditions.
    ///
    /// # Errors
    ///
    /// See [`LeaderWriteError`].
    pub async fn write(&self, txn: Txn) -> Result<u64, LeaderWriteError> {
        match crate::write::propose_leader_only(&self.node, self.term, CommandBody::Txn(txn)).await? {
            CommandOutcome::Txn(Ok(revision)) => Ok(revision),
            CommandOutcome::Txn(Err(conflict)) => Err(LeaderWriteError::Conflict(conflict)),
            // Not an answer a transaction gets, and the propose path has already turned a stale
            // term into `NotLeader`: nothing here can be vouched for as having happened.
            CommandOutcome::Registry | CommandOutcome::StaleTerm => {
                Err(LeaderWriteError::Indeterminate { term: self.term })
            }
        }
    }
}

/// One leadership epoch's still-running task handles, for the supervisor to abort as a whole
struct Epoch {
    term: u64,
    leader_since: Instant,
    handles: Vec<JoinHandle<()>>,
}

/// The supervisor's own bookkeeping, behind one short-held lock - never held across an `.await`:
/// every actual wait (aborting and joining a stale epoch's handles) happens with this lock
/// already released, so a concurrent [`LeaderTasks::register`] is blocked for at most a few
/// non-blocking operations, never for as long as a task takes to unwind.
struct Inner {
    /// Every factory registered so far, in registration order - replayed in full for every fresh
    /// epoch, so a factory registered before this node ever led still runs once it does.
    factories: Vec<(String, Factory)>,
    /// The currently running leadership epoch, if this node is leading right now
    epoch: Option<Epoch>,
    /// Set once this node's Raft engine has shut down for good - after this,
    /// [`LeaderTasks::register`] still records the factory (harmless) but never starts it:
    /// there is no leadership left to start it under.
    closed: bool,
}

/// One node's leader-task supervisor: the mechanism behind [`EzKv::spawn_leader_task`]
pub(crate) struct LeaderTasks {
    inner: Mutex<Inner>,
}

impl LeaderTasks {
    /// An empty supervisor, with no background task running yet
    ///
    /// Two-phase construction, like `crate::registry`'s self-registration: [`Self::new`] has to
    /// run *before* [`crate::raft::EzKv::new`] can build the very [`EzKv`] this supervisor's own
    /// background task needs to hand every task run, so starting that task is a separate step
    /// ([`Self::spawn_supervisor`]), called once `EzKv::new`'s own handle exists.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                factories: Vec::new(),
                epoch: None,
                closed: false,
            }),
        })
    }

    /// Start this node's one supervisor task: watch `node.cluster()` for the rest of this node's
    /// life, reconciling which factories are running against whether - and in which term - this
    /// node currently leads
    pub(crate) fn spawn_supervisor(self: &Arc<Self>, node: EzKv) {
        let shared = self.clone();
        tokio::spawn(async move { shared.run(node).await });
    }

    async fn run(self: Arc<Self>, node: EzKv) {
        let mut cluster_rx = node.cluster();
        loop {
            // Checked before waiting, not only after a change: a single-node cluster can already
            // be leading (a fresh `create` wins its own election synchronously) by the time this
            // task gets its first chance to run at all.
            let state = cluster_rx.borrow_and_update().clone();
            self.reconcile(&node, &state).await;

            if cluster_rx.changed().await.is_err() {
                break;
            }
        }

        // `cluster()`'s stream ends only when this node's Raft engine has shut down for good (see
        // `crate::cluster`'s own docs) - stop whatever is still running rather than leaking it.
        self.teardown().await;
    }

    /// Bring the running epoch in line with `state`: tear down a stale one (wrong term, or no
    /// longer leading), then start a fresh one if `state` says this node leads and none is running
    /// for that exact term yet
    async fn reconcile(&self, node: &EzKv, state: &ClusterState) {
        let stale = {
            let mut inner = self.inner.lock().unwrap();
            let current_epoch_matches =
                matches!(&inner.epoch, Some(epoch) if state.role == ServerState::Leader && epoch.term == state.term);
            if current_epoch_matches {
                None
            } else {
                inner.epoch.take()
            }
        };

        if let Some(epoch) = stale {
            tracing::debug!(
                term = epoch.term,
                "leadership ended or its term changed; cancelling its leader tasks"
            );
            abort_and_join(epoch.handles).await;
        }

        if state.role != ServerState::Leader {
            return;
        }

        let mut inner = self.inner.lock().unwrap();
        if inner.epoch.is_some() || inner.closed {
            // Either the epoch above already matched (nothing stale, nothing to start), or this
            // node has shut down since the loop last read `cluster()` - either way, no fresh epoch
            // belongs here.
            return;
        }

        let leader_since = Instant::now();
        let handles = inner
            .factories
            .iter()
            .map(|(name, factory)| {
                spawn_one(
                    name.clone(),
                    factory.clone(),
                    LeaderCtx::new(node.clone(), state.term, leader_since),
                )
            })
            .collect();
        tracing::debug!(term = state.term, "leading; starting every registered leader task");
        inner.epoch = Some(Epoch {
            term: state.term,
            leader_since,
            handles,
        });
    }

    /// Register `factory` under `name` for the life of the node
    ///
    /// If this node is currently leading, `factory` is started at once, in the epoch already
    /// running - it does not wait for the next `cluster()` change, which this leadership may never
    /// produce again.
    pub(crate) fn register(&self, node: &EzKv, name: String, factory: Factory) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.closed
            && let Some(epoch) = &mut inner.epoch
        {
            let ctx = LeaderCtx::new(node.clone(), epoch.term, epoch.leader_since);
            epoch.handles.push(spawn_one(name.clone(), factory.clone(), ctx));
        }
        inner.factories.push((name, factory));
    }

    /// Stop whatever is still running and refuse to start anything new - called once, when
    /// [`Self::run`]'s own loop ends at node shutdown
    async fn teardown(&self) {
        let epoch = {
            let mut inner = self.inner.lock().unwrap();
            inner.closed = true;
            inner.epoch.take()
        };
        if let Some(epoch) = epoch {
            abort_and_join(epoch.handles).await;
        }
    }
}

/// Abort every handle, then wait for each to actually finish, before returning
///
/// The two steps are deliberately separate: aborting is cooperative (a task only stops at its next
/// poll), so joining every handle first is what guarantees a stale epoch has genuinely stopped
/// before [`LeaderTasks::reconcile`] starts a fresh one - never two runs of the same factory
/// overlapping, even for the width of one poll.
async fn abort_and_join(handles: Vec<JoinHandle<()>>) {
    for handle in &handles {
        handle.abort();
    }
    for handle in handles {
        let _ = handle.await;
    }
}

/// Spawn one factory's run for one leadership epoch, logging how it ends
///
/// A panic inside `factory(ctx)` is caught here, not left to unwind the task: the run is over
/// either way, but tokio's own default panic hook would otherwise print it unconditionally,
/// bypassing this crate's structured `tracing` output. A run this supervisor aborts never reaches
/// either branch below - see this module's own docs.
fn spawn_one(name: String, factory: Factory, ctx: LeaderCtx) -> JoinHandle<()> {
    let term = ctx.term;
    tokio::spawn(async move {
        match AssertUnwindSafe(factory(ctx)).catch_unwind().await {
            Ok(()) => {
                tracing::info!(task = %name, term, "leader task ended; it will not be restarted within this term");
            }
            Err(panic) => {
                tracing::error!(
                    task = %name,
                    term,
                    panic = %panic_message(&panic),
                    "leader task panicked; it will not be restarted within this term",
                );
            }
        }
    })
}

/// A best-effort, human-readable description of a caught panic's payload
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

impl EzKv {
    /// Register a leader-only task factory for the life of this node
    ///
    /// Whenever this node becomes leader in term T, `f` is called once with a fresh
    /// [`LeaderCtx`] whose [`LeaderCtx::term`] is T, and its returned future is run; the run is
    /// cancelled the moment this node stops leading or its term changes, and it is not restarted
    /// within that same term if it returns or panics on its own first. A node that loses and later
    /// regains leadership gets an entirely fresh run, with the new term. `f` is called again, once
    /// per fresh leadership, for as long as this node's process runs - registering it once, at
    /// construction time, is the usual way to use this.
    ///
    /// `name` labels the `tracing` events this run's supervision produces; it is not required to
    /// be unique.
    ///
    /// If this node is already leading when this is called, `f` is started immediately, in the
    /// leadership already under way, rather than waiting for a `cluster()` change this leadership
    /// may never produce again.
    pub fn spawn_leader_task<F, Fut>(&self, name: impl Into<String>, f: F)
    where
        F: Fn(LeaderCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let factory: Factory = Arc::new(move |ctx: LeaderCtx| -> BoxFuture { Box::pin(f(ctx)) });
        self.leader_tasks().register(self, name.into(), factory);
    }
}
