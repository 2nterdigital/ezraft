//! `watch` acceptance tests for restart and cluster-wide delivery (ech0-media#32): a restarted
//! node's log-tail replay reaches a resumed subscription with no gap, a fresh `view_and_watch`
//! after replay is itself consistent, and followers/learners deliver the same groups as the
//! leader (watch is a per-node concern).

mod common;

use std::collections::BTreeSet;
use std::io;

use common::*;
use ezraft::EzKv;
use ezraft::WatchStart;
use ezraft::WatchUpdate;

/// A node restarted from a snapshot plus a log tail replays that tail into `watch` exactly like
/// live traffic would: a subscription resumed from the snapshot's own revision (the window's
/// lower bound right after restart) sees every replayed group with no gap, folding them
/// (upserting by key, the ordinary way a consumer applies `mod_rev`-ordered changes) reconstructs
/// the exact post-restart state, and a fresh `view_and_watch` taken once replay has finished is
/// itself consistent with further live writes.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_replays_the_log_tail_into_watch_with_no_gap() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();

    let a = EzKv::create(&addr, data.open().await?, config()).await?;
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "single node leads").await?;

    for i in 0..10 {
        a.write(set(&format!("k{i}"), &format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    a.trigger_snapshot().await?;
    a.wait_metrics(WAIT, |m| m.snapshot.is_some(), "snapshot built").await?;
    let snapshot_revision = a.view().revision();
    assert_eq!(
        10, snapshot_revision,
        "no further writes have landed since the snapshot yet"
    );

    let mut last_revision = snapshot_revision;
    for i in 10..15 {
        last_revision = a
            .write(set(&format!("k{i}"), &format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    a.shutdown().await?;
    drop(a);

    let restarted = EzKv::create(&addr, data.open().await?, config()).await?;
    restarted.wait_metrics(WAIT, |m| m.current_leader == Some(0), "restarted node leads").await?;

    // Resumed from exactly the snapshot's own revision - the window's lower bound right after
    // restart - so this delivers the whole replayed log tail with no gap, regardless of how much
    // of it has already replayed by the time this call registers.
    let mut resumed = restarted.watch("", WatchStart::After(snapshot_revision));

    let mut map = expected_map(0..10);
    let mut seen_revision = snapshot_revision;
    while seen_revision < last_revision {
        match next_update(&mut resumed).await {
            WatchUpdate::Changes(changes) => {
                assert!(changes.revision > seen_revision, "revisions must strictly increase");
                fold_changes(&mut map, &changes);
                seen_revision = changes.revision;
            }
            WatchUpdate::Progress { revision } => seen_revision = seen_revision.max(revision),
            other @ WatchUpdate::Reset { .. } => panic!("unexpected {other:?}"),
        }
    }

    restarted
        .wait_metrics(
            WAIT,
            |m| m.last_applied.map(|log_id| log_id.index) == m.last_log_index,
            "log tail replayed",
        )
        .await?;
    assert_eq!(
        view_map(&restarted.view()),
        map,
        "replayed groups, folded by key, must reconstruct the exact post-restart state"
    );

    // A fresh view_and_watch, taken once replay has finished, is itself consistent, and further
    // writes continue delivering with no gap.
    let (view, mut fresh_stream) = restarted.view_and_watch("");
    assert_eq!(last_revision, view.revision());
    assert_eq!(expected_map(0..15), view_map(&view));

    let next_revision = restarted.write(set("k15", "v15")).await.map_err(|e| io::Error::other(e.to_string()))?;
    match next_update(&mut fresh_stream).await {
        WatchUpdate::Changes(changes) => assert_eq!(next_revision, changes.revision),
        other => panic!("expected Changes (revision {next_revision}), got {other:?}"),
    }

    Ok(())
}

/// `watch` is a per-node concern: the leader, a voter follower and a learner each deliver their
/// own copy of the same group for the same write.
#[tokio::test(flavor = "multi_thread")]
async fn followers_and_learners_deliver_the_same_groups_as_the_leader() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    wait_for_voters(&b, BTreeSet::from([0, b.node_id()]), "b promoted to voter").await?;
    let (_, c, _data_c) = joined_learner(&addr_a).await?;

    let mut stream_a = a.watch("room/", WatchStart::Now);
    let mut stream_b = b.watch("room/", WatchStart::Now);
    let mut stream_c = c.watch("room/", WatchStart::Now);

    let revision = write_through_leader(&[&a, &b], set("room/1", "hello")).await?;

    for stream in [&mut stream_a, &mut stream_b, &mut stream_c] {
        match next_update(stream).await {
            WatchUpdate::Changes(changes) => {
                assert_eq!(revision, changes.revision);
                assert_eq!(vec!["room/1"], changes.items.iter().map(change_key).collect::<Vec<_>>());
            }
            other => panic!("expected Changes (revision {revision}), got {other:?}"),
        }
    }

    Ok(())
}
