//! Leader-only background tasks: `EzKv::spawn_leader_task`, `LeaderCtx::write`'s fencing, and
//! task supervision (ech0-media#34)
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes. A registered
//! factory's own effects are observed through a channel it sends on directly - never by inspecting
//! internal structure - the same "condition wait, not a sleep" discipline the rest of this harness
//! follows.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::sync::Mutex;
use std::time::Duration;

use common::*;
use ezraft::EzKv;
use ezraft::LeaderCtx;
use ezraft::LeaderWriteError;
use ezraft::ServerState;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// One observable moment in a test leader task's life
///
/// [`Event::Ended`] is reported by [`EndedSignal`]'s `Drop`, which fires whether a run returns on
/// its own or is cancelled mid-`.await` by the supervisor - Rust always runs `Drop` for values a
/// dropped future was holding, cancellation included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    Started { node_id: u64, term: u64 },
    Ended { node_id: u64, term: u64 },
}

struct EndedSignal {
    tx: mpsc::UnboundedSender<Event>,
    node_id: u64,
    term: u64,
}

impl Drop for EndedSignal {
    fn drop(&mut self) {
        let _ = self.tx.send(Event::Ended {
            node_id: self.node_id,
            term: self.term,
        });
    }
}

/// Register a task on `node` that reports [`Event::Started`] and then waits forever - until the
/// supervisor cancels it, which is when [`EndedSignal`] reports [`Event::Ended`]
fn spawn_reporting_task(node: &EzKv, tx: mpsc::UnboundedSender<Event>) {
    let node_id = node.node_id();
    node.spawn_leader_task("test.reporting", move |ctx: LeaderCtx| {
        let tx = tx.clone();
        async move {
            let _signal = EndedSignal {
                tx: tx.clone(),
                node_id,
                term: ctx.term,
            };
            let _ = tx.send(Event::Started {
                node_id,
                term: ctx.term,
            });
            std::future::pending::<()>().await;
        }
    });
}

/// Receive the next event, bounded by [`WAIT`]
async fn next_event(rx: &mut mpsc::UnboundedReceiver<Event>) -> io::Result<Event> {
    tokio::time::timeout(WAIT.expect("bounded"), rx.recv())
        .await
        .map_err(|_elapsed| io::Error::other("timed out waiting for the next leader-task event"))?
        .ok_or_else(|| io::Error::other("the leader-task event channel ended"))
}

/// A registered task runs on the leader only; after a leadership transfer, the old leader's run is
/// cancelled and the new leader's run starts, with a higher term.
#[tokio::test(flavor = "multi_thread")]
async fn a_registered_task_runs_only_on_the_leader_and_migrates_after_transfer() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    wait_for_applied(&c, &a).await?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    for node in [&a, &b, &c] {
        spawn_reporting_task(node, tx.clone());
    }
    drop(tx);

    let Event::Started {
        node_id,
        term: old_term,
    } = next_event(&mut rx).await?
    else {
        return Err(io::Error::other("expected a Started event first"));
    };
    assert_eq!(0, node_id, "only the founding leader's task runs at first");

    let target = c.node_id();
    a.transfer_leader(target).await?;

    assert_eq!(
        Event::Ended {
            node_id: 0,
            term: old_term
        },
        next_event(&mut rx).await?,
        "the old leader's run is cancelled",
    );

    let Event::Started {
        node_id: new_leader,
        term: new_term,
    } = next_event(&mut rx).await?
    else {
        return Err(io::Error::other("expected the new leader's Started event"));
    };
    assert_eq!(target, new_leader);
    assert!(new_term > old_term, "a leadership transfer must raise the term");

    Ok(())
}

/// A factory registered after the node is already leading starts at once, rather than waiting for
/// a `cluster()` change this leadership may never produce again.
#[tokio::test(flavor = "multi_thread")]
async fn a_factory_registered_while_the_node_already_leads_starts_at_once() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;
    wait_for_cluster(&a, |cs| cs.role == ServerState::Leader, "the founding node leads").await?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    spawn_reporting_task(&a, tx);

    let event = next_event(&mut rx).await?;
    assert!(
        matches!(event, Event::Started { node_id: 0, .. }),
        "unexpected first event: {event:?}",
    );

    Ok(())
}

/// A run that returns on its own is not restarted while the term stays the same; a genuine
/// leadership change - even back onto the same node - starts a fresh run.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_returns_is_not_restarted_within_the_same_term() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;

    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "both nodes are voters").await?;
    wait_for_applied(&b, &a).await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<()>();
    for node in [&a, &b] {
        let tx = tx.clone();
        node.spawn_leader_task("test.returns_once", move |_ctx: LeaderCtx| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(());
            }
        });
    }
    drop(tx);

    rx.recv().await.expect("the founding leader's run reports once");

    // Proving a negative, bounded well above the harness's heartbeat interval so a wrongful
    // restart would have every chance to arrive, but far under the overall `WAIT` budget.
    let extra = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
    assert!(
        extra.is_err(),
        "a run that returns must not be restarted within the same term, got: {extra:?}"
    );

    // A genuine leadership change - even back onto the very node that just ran once - gets a
    // fresh run: term T's run returning is not "still running" for a later term T' to reuse.
    a.transfer_leader(b.node_id()).await?;
    b.wait_metrics(WAIT, |m| m.current_leader == Some(b.node_id()), "b takes over").await?;

    rx.recv().await.expect("the new leader's fresh epoch runs the task once more");

    Ok(())
}

/// A context captured from the old leader's run is rejected with `NotLeader` once this node has
/// lost leadership, and nothing it attempts commits anywhere.
#[tokio::test(flavor = "multi_thread")]
async fn ctx_write_from_a_stale_context_is_rejected_after_losing_leadership() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    wait_for_applied(&c, &a).await?;

    let (tx, rx) = oneshot::channel();
    // `spawn_leader_task`'s factory is an `Fn`, callable more than once in principle, even though
    // this test only ever needs its very first (and, here, only) call to fire: a `Mutex<Option<_>>`
    // hands the one-shot sender out exactly once.
    let tx = Mutex::new(Some(tx));
    a.spawn_leader_task("test.capture_ctx", move |ctx: LeaderCtx| {
        let sender = tx.lock().unwrap().take();
        async move {
            if let Some(sender) = sender {
                let _ = sender.send(ctx);
            }
        }
    });

    let ctx = rx.await.map_err(io::Error::other)?;
    assert_eq!(0, ctx.node().node_id());
    let captured_term = ctx.term;

    let target = c.node_id();
    a.transfer_leader(target).await?;
    wait_for_cluster(&a, |cs| cs.role != ServerState::Leader, "the old leader steps down").await?;

    match ctx.write(set("stale", "write")).await {
        Err(LeaderWriteError::NotLeader { term }) => assert_eq!(captured_term, term),
        other => return Err(io::Error::other(format!("expected NotLeader, got {other:?}"))),
    }

    // Nothing the stale context attempted ever committed - on the new leader or anywhere else.
    wait_for_cluster(&c, |cs| cs.role == ServerState::Leader, "the new leader settles").await?;
    c.write(set("marker", "after")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(
        None,
        c.view().get("stale"),
        "the stale context's write must never have committed"
    );

    Ok(())
}

/// Right after an election, `ctx.write` succeeds even when called the instant the new leadership
/// starts - waiting out the alpha.33 no-lease gap rather than failing.
#[tokio::test(flavor = "multi_thread")]
async fn ctx_write_succeeds_through_the_no_lease_gap_right_after_an_election() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    wait_for_applied(&c, &a).await?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    for node in [&a, &b, &c] {
        let tx = tx.clone();
        node.spawn_leader_task("test.write_on_start", move |ctx: LeaderCtx| {
            let tx = tx.clone();
            async move {
                let node_id = ctx.node().node_id();
                let result = ctx.write(set("elected", "yes")).await;
                let _ = tx.send((node_id, result.map_err(|e| e.to_string())));
            }
        });
    }
    drop(tx);

    let (leader_id, first) = rx.recv().await.expect("the founding leader's own run reports first");
    assert_eq!(0, leader_id);
    assert!(first.is_ok(), "the founding leader's own write must succeed: {first:?}");

    let target = c.node_id();
    a.transfer_leader(target).await?;

    let (new_leader, result) = rx.recv().await.expect("the new leader's own run reports");
    assert_eq!(target, new_leader);
    assert!(
        result.is_ok(),
        "ctx.write must wait out the no-lease gap and succeed: {result:?}"
    );

    Ok(())
}

/// Every registered leader task is cancelled when the node shuts down.
#[tokio::test(flavor = "multi_thread")]
async fn everything_stops_at_shutdown() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    spawn_reporting_task(&a, tx);

    let started = next_event(&mut rx).await?;
    assert!(
        matches!(started, Event::Started { node_id: 0, .. }),
        "unexpected event: {started:?}"
    );

    a.shutdown().await?;

    let ended = next_event(&mut rx).await?;
    assert!(
        matches!(ended, Event::Ended { node_id: 0, .. }),
        "unexpected event: {ended:?}"
    );

    Ok(())
}
