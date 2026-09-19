//! `watch` `Reset` acceptance tests (ech0-media#32): the three situations that produce one - a
//! resume position older than the retained window, a subscriber too slow to drain its queue, and
//! this node installing a snapshot - each followed by further increments with no gap, and the
//! guarantee that an unpolled subscription never slows down `apply`.

mod common;

use std::io;

use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::ResetCause;
use ezraft::Txn;
use ezraft::WatchLimits;
use ezraft::WatchStart;
use ezraft::WatchUpdate;

/// A resume position older than the retained window's lower bound gets a `Reset` naming both the
/// requested revision and the window's actual floor, then delivery continues from that reset's
/// view with no gap.
#[tokio::test(flavor = "multi_thread")]
async fn a_resume_position_older_than_the_window_is_reset_then_continues() -> io::Result<()> {
    let tiny_window = EzConfig {
        watch_limits: WatchLimits {
            max_window_groups: 2,
            ..WatchLimits::default()
        },
        ..config()
    };
    let (_, a, _data) = founding_node_with(tiny_window).await?;

    // Five transactions with a two-group window: the window ends up holding only revisions 4 and
    // 5, so its floor (`window_floor`) is exactly 3.
    let mut last_revision = 0;
    for i in 0..5 {
        last_revision = a
            .write(Txn::new().put(format!("k{i}"), format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    assert_eq!(5, last_revision);

    let mut stream = a.watch("", WatchStart::After(1));

    match next_update(&mut stream).await {
        WatchUpdate::Reset {
            view,
            cause: ResetCause::ResumeTooOld {
                requested,
                window_floor,
            },
        } => {
            assert_eq!(1, requested);
            assert_eq!(3, window_floor);
            assert_eq!(a.view().revision(), view.revision());
        }
        other => panic!("expected a ResumeTooOld reset, got {other:?}"),
    }

    let next_revision = a.write(Txn::new().put("after", "x")).await.map_err(|e| io::Error::other(e.to_string()))?;
    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => assert_eq!(next_revision, changes.revision),
        other => panic!("expected Changes with no gap after the reset, got {other:?}"),
    }

    Ok(())
}

/// `After` a revision the window still covers - including exactly its floor - needs no reset at
/// all: this is the boundary the `ResumeTooOld` test's `window_floor` is checked against.
#[tokio::test(flavor = "multi_thread")]
async fn a_resume_position_exactly_at_the_window_floor_needs_no_reset() -> io::Result<()> {
    let tiny_window = EzConfig {
        watch_limits: WatchLimits {
            max_window_groups: 2,
            ..WatchLimits::default()
        },
        ..config()
    };
    let (_, a, _data) = founding_node_with(tiny_window).await?;

    for i in 0..5 {
        a.write(Txn::new().put(format!("k{i}"), format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    // Floor is 3 (see the test above); resuming from exactly there must replay revision 4 and 5
    // from the window, never a reset.
    let mut stream = a.watch("", WatchStart::After(3));

    for expected in [4u64, 5] {
        match next_update(&mut stream).await {
            WatchUpdate::Changes(changes) => assert_eq!(expected, changes.revision),
            other => panic!("expected Changes (revision {expected}), got {other:?}"),
        }
    }

    Ok(())
}

/// A subscriber whose queue fills up before it is drained has its whole backlog dropped and is
/// reset instead - to a view at least as new as the group that overflowed it - then delivery
/// continues from there with no gap.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_too_slow_to_drain_its_queue_is_reset_then_continues() -> io::Result<()> {
    let tiny_queue = EzConfig {
        watch_limits: WatchLimits {
            subscriber_queue_len: 2,
            ..WatchLimits::default()
        },
        ..config()
    };
    let (_, a, _data) = founding_node_with(tiny_queue).await?;

    let mut stream = a.watch("", WatchStart::Now);

    // Committed without ever polling `stream`: five groups through a two-slot queue must overflow
    // it at least once.
    let mut last_revision = 0;
    for i in 0..5 {
        last_revision = a
            .write(Txn::new().put(format!("k{i}"), format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    assert_eq!(5, last_revision);

    match next_update(&mut stream).await {
        WatchUpdate::Reset {
            view,
            cause: ResetCause::SubscriberTooSlow,
        } => {
            assert_eq!(
                last_revision,
                view.revision(),
                "the reset's view is at least as new as what overflowed it"
            );
        }
        other => panic!("expected a SubscriberTooSlow reset, got {other:?}"),
    }

    let next_revision = a.write(Txn::new().put("after", "x")).await.map_err(|e| io::Error::other(e.to_string()))?;
    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => assert_eq!(next_revision, changes.revision),
        other => panic!("expected Changes with no gap after the reset, got {other:?}"),
    }

    Ok(())
}

/// A subscription nobody ever polls does not slow `apply` down: a run of writes still completes
/// and is acknowledged within the harness's usual wait budget, and the subscriber is left reset
/// rather than dragging every write out behind a full queue.
#[tokio::test(flavor = "multi_thread")]
async fn a_never_polled_subscriber_never_slows_down_apply() -> io::Result<()> {
    let tiny_queue = EzConfig {
        watch_limits: WatchLimits {
            subscriber_queue_len: 2,
            ..WatchLimits::default()
        },
        ..config()
    };
    let (_, a, _data) = founding_node_with(tiny_queue).await?;

    let mut stream = a.watch("", WatchStart::Now);

    const WRITES: usize = 100;
    let per_write_budget = WAIT.expect("cluster waits are bounded");
    for i in 0..WRITES {
        tokio::time::timeout(per_write_budget, a.write(Txn::new().put(format!("k{i}"), format!("v{i}"))))
            .await
            .unwrap_or_else(|_| {
                panic!("write {i} did not complete within {per_write_budget:?} while an unpolled watch subscription was attached")
            })
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    match next_update(&mut stream).await {
        WatchUpdate::Reset {
            cause: ResetCause::SubscriberTooSlow,
            ..
        } => {}
        other => panic!("expected a SubscriberTooSlow reset, got {other:?}"),
    }

    Ok(())
}

/// This node installing a snapshot (the only way a joining node with a purged leader log can ever
/// catch up) resets every active subscriber to the newly installed view, then delivery continues
/// with no gap.
#[tokio::test(flavor = "multi_thread")]
async fn installing_a_snapshot_resets_a_lagging_subscriber_to_the_new_view() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;

    for i in 0..10 {
        a.write(Txn::new().put(format!("k{i}"), format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    a.trigger_snapshot().await?;
    let snapshot_index =
        a.wait_metrics(WAIT, |m| m.snapshot.is_some(), "snapshot built").await?.snapshot.unwrap().index;
    a.purge_log(snapshot_index).await?;
    a.wait_metrics(
        WAIT,
        |m| m.purged.map(|log_id| log_id.index) == Some(snapshot_index),
        "log purged up to the snapshot",
    )
    .await?;

    let addr_c = free_addr();
    let data_c = NodeData::new();
    let c = EzKv::join_as_learner(&addr_c, &addr_a, data_c.open().await?, config()).await?;

    // Subscribed before `c` ever serves, so this registers against its still-empty initial state
    // - only the snapshot transfer that follows can be what resets it.
    let mut stream = c.watch("", WatchStart::Now);

    spawn_serve(&c);
    wait_for_applied(&c, &a).await?;

    match next_update(&mut stream).await {
        WatchUpdate::Reset {
            view,
            cause: ResetCause::SnapshotInstalled,
        } => {
            assert_eq!(expected_map(0..10), view_map(&view));
        }
        other => panic!("expected a SnapshotInstalled reset, got {other:?}"),
    }

    write_through_leader(&[&a, &c], Txn::new().put("k10", "v10")).await?;
    wait_for_applied(&c, &a).await?;
    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => {
            assert_eq!(1, changes.items.len(), "no gap: exactly the one write after the reset")
        }
        other => panic!("expected Changes with no gap after the reset, got {other:?}"),
    }

    Ok(())
}
