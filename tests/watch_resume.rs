//! `watch(prefix, After(rev))` acceptance tests (ech0-media#32): resuming inside the retained
//! window delivers exactly what came after `rev`, and a revision saved from a consumed `Progress`
//! is a safe resume cursor - nothing after it is missed.

mod common;

use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::Txn;
use ezraft::WatchLimits;
use ezraft::WatchStart;
use ezraft::WatchUpdate;

/// `After(rev)` inside the retained window delivers exactly the groups whose revision is greater
/// than `rev`, in order - no replay of anything at or before it, nothing skipped after it.
#[tokio::test(flavor = "multi_thread")]
async fn after_rev_inside_the_window_delivers_exactly_the_groups_after_rev() -> io::Result<()> {
    let (_, a, _data) = founding_node().await?;

    let mut revisions = Vec::new();
    for i in 0..5 {
        revisions.push(
            a.write(Txn::new().put(format!("k{i}"), format!("v{i}")))
                .await
                .map_err(|e| io::Error::other(e.to_string()))?,
        );
    }

    let mut stream = a.watch("", WatchStart::After(revisions[1]));
    for expected in &revisions[2..] {
        match next_update(&mut stream).await {
            WatchUpdate::Changes(changes) => assert_eq!(*expected, changes.revision),
            other => panic!("expected Changes (revision {expected}), got {other:?}"),
        }
    }

    Ok(())
}

/// `After(rev)` for a revision this node has not reached yet simply waits for it, rather than
/// resetting or erroring.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_revision_not_reached_yet_waits_for_it() -> io::Result<()> {
    let (_, a, _data) = founding_node().await?;

    let mut stream = a.watch("", WatchStart::After(10));

    let mut last_revision = 0;
    for i in 0..10 {
        last_revision = a
            .write(Txn::new().put(format!("k{i}"), format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    assert_eq!(10, last_revision);
    let next_revision = a.write(Txn::new().put("k10", "v10")).await.map_err(|e| io::Error::other(e.to_string()))?;

    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => assert_eq!(next_revision, changes.revision),
        other => panic!("expected Changes (revision {next_revision}), got {other:?}"),
    }

    Ok(())
}

/// A revision saved from a consumed `Progress` is a safe resume cursor: a fresh subscription
/// resuming from it sees the next matching change and nothing that was already accounted for.
#[tokio::test(flavor = "multi_thread")]
async fn resuming_from_a_consumed_progress_revision_misses_nothing() -> io::Result<()> {
    let progress_interval = Duration::from_millis(20);
    let quick_progress = EzConfig {
        watch_limits: WatchLimits {
            progress_interval,
            ..WatchLimits::default()
        },
        ..config()
    };
    let (_, a, _data) = founding_node_with(quick_progress).await?;
    let mut stream = a.watch("match/", WatchStart::Now);

    let before = a.write(Txn::new().put("match/1", "a")).await.map_err(|e| io::Error::other(e.to_string()))?;
    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => assert_eq!(before, changes.revision),
        other => panic!("expected Changes (revision {before}), got {other:?}"),
    }

    // Several quiet (non-matching) writes, spaced so `Progress` gets a chance to fire and move
    // this subscription's cursor forward past `before` without another matching change.
    for i in 0..5 {
        a.write(Txn::new().put(format!("other/{i}"), "x"))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        tokio::time::sleep(progress_interval).await;
    }
    let progress_revision = loop {
        match next_update(&mut stream).await {
            WatchUpdate::Progress { revision } => break revision,
            other @ WatchUpdate::Reset { .. } => panic!("unexpected {other:?}"),
            WatchUpdate::Changes(_) => continue,
        }
    };
    assert!(
        progress_revision > before,
        "Progress must have moved the cursor past the earlier change"
    );

    let after = a.write(Txn::new().put("match/2", "b")).await.map_err(|e| io::Error::other(e.to_string()))?;

    // A brand-new subscription resuming from the consumed Progress revision must deliver `after`
    // and never replay `before`.
    let mut resumed = a.watch("match/", WatchStart::After(progress_revision));
    match next_update(&mut resumed).await {
        WatchUpdate::Changes(changes) => assert_eq!(after, changes.revision),
        other => panic!("expected Changes (revision {after}), got {other:?}"),
    }

    Ok(())
}
