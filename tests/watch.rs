//! `watch`/`view_and_watch` acceptance tests (ech0-media#32): grouping, op order, prefix
//! filtering, and the core no-gap/no-duplicate invariant under concurrent writers.
//!
//! Reset, slow-consumer and snapshot-install behavior live in `tests/watch_reset.rs`; `Progress`
//! in `tests/watch_progress.rs`; resuming with `After` in `tests/watch_resume.rs`; restart and
//! cluster-wide delivery in `tests/watch_restart.rs`. Every test drives `EzKv` through its public
//! API and real HTTP between nodes, same as the rest of this crate's cluster tests.

mod common;

use std::collections::BTreeMap;
use std::io;

use common::*;
use ezraft::Change;
use ezraft::EzKv;
use ezraft::Txn;
use ezraft::WatchStart;
use ezraft::WatchStream;
use ezraft::WatchUpdate;
use futures::StreamExt;

/// A multi-key transaction produces exactly one `Changes` group, and its items are listed in the
/// transaction's own op order - not grouped by op kind.
#[tokio::test(flavor = "multi_thread")]
async fn one_group_per_transaction_with_items_in_op_order() -> io::Result<()> {
    let (_, a, _data) = founding_node().await?;
    let mut stream = a.watch("room/1/", WatchStart::Now);

    let revision = a
        .write(Txn::new().put("room/1/a", "1").put("room/1/b", "2").delete("room/1/a"))
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => {
            assert_eq!(revision, changes.revision);
            let keys: Vec<&str> = changes.items.iter().map(change_key).collect();
            assert_eq!(
                vec!["room/1/a", "room/1/b", "room/1/a"],
                keys,
                "op order, not grouped by kind"
            );
        }
        other => panic!("expected Changes, got {other:?}"),
    }

    Ok(())
}

/// `DeletePrefix` expands into one `Delete` per removed key, in key order, each carrying the
/// value and revisions it had immediately before the delete.
#[tokio::test(flavor = "multi_thread")]
async fn delete_prefix_yields_one_delete_per_key_in_key_order_with_the_previous_value() -> io::Result<()> {
    let (_, a, _data) = founding_node().await?;

    a.write(
        Txn::new()
            .put("room/1/b", "2")
            .put("room/1/a", "1")
            .put("room/1/c", "3")
            .put("room/2/x", "elsewhere"),
    )
    .await
    .map_err(|e| io::Error::other(e.to_string()))?;

    let mut stream = a.watch("room/1/", WatchStart::Now);
    let revision = a.write(Txn::new().delete_prefix("room/1/")).await.map_err(|e| io::Error::other(e.to_string()))?;

    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => {
            assert_eq!(revision, changes.revision);
            assert_eq!(3, changes.items.len());
            let mut prior = Vec::new();
            for change in &changes.items {
                match change {
                    Change::Delete { key, prev } => {
                        prior.push((key.clone(), text(prev)));
                        assert_eq!(1, prev.create_rev);
                        assert_eq!(1, prev.mod_rev);
                    }
                    Change::Put { .. } => panic!("delete_prefix must never produce a Put"),
                }
            }
            assert_eq!(
                vec![
                    ("room/1/a".to_string(), "1".to_string()),
                    ("room/1/b".to_string(), "2".to_string()),
                    ("room/1/c".to_string(), "3".to_string()),
                ],
                prior,
                "deletes must be in key order, each with its pre-delete value"
            );
        }
        other => panic!("expected Changes, got {other:?}"),
    }

    Ok(())
}

/// A transaction touching keys both inside and outside the subscribed prefix yields only the
/// inside items - the outside ones are filtered out of the group entirely, not merely hidden.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_spanning_the_prefix_boundary_yields_only_the_inside_items() -> io::Result<()> {
    let (_, a, _data) = founding_node().await?;
    let mut stream = a.watch("room/1/", WatchStart::Now);

    let revision = a
        .write(Txn::new().put("room/1/a", "1").put("room/2/b", "2").put("elsewhere", "3"))
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => {
            assert_eq!(revision, changes.revision);
            assert_eq!(
                vec!["room/1/a"],
                changes.items.iter().map(change_key).collect::<Vec<_>>()
            );
        }
        other => panic!("expected Changes, got {other:?}"),
    }

    Ok(())
}

/// A transaction with no item inside the subscribed prefix produces no `Changes` at all for that
/// subscription - not an empty one.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_with_no_matching_item_yields_no_changes() -> io::Result<()> {
    let (_, a, _data) = founding_node().await?;
    let mut stream = a.watch("room/1/", WatchStart::Now);

    a.write(Txn::new().put("other/x", "1")).await.map_err(|e| io::Error::other(e.to_string()))?;
    let inside_revision =
        a.write(Txn::new().put("room/1/a", "2")).await.map_err(|e| io::Error::other(e.to_string()))?;

    // Whatever this subscription delivers next must be the inside transaction, never the
    // outside-only one that came before it (nor a duplicate/empty item for it).
    loop {
        match next_update(&mut stream).await {
            WatchUpdate::Changes(changes) => {
                assert_eq!(inside_revision, changes.revision);
                break;
            }
            WatchUpdate::Progress { revision } => assert!(
                revision < inside_revision,
                "a Progress must never anticipate a Changes this subscriber still has coming"
            ),
            other @ WatchUpdate::Reset { .. } => panic!("unexpected {other:?}"),
        }
    }

    Ok(())
}

/// An empty prefix subscribes to every key, not just those under some path.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_prefix_subscribes_to_everything() -> io::Result<()> {
    let (_, a, _data) = founding_node().await?;
    let mut stream = a.watch("", WatchStart::Now);

    let revision = a
        .write(Txn::new().put("room/1/a", "1").put("completely/unrelated", "2"))
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => {
            assert_eq!(revision, changes.revision);
            assert_eq!(
                vec!["room/1/a", "completely/unrelated"],
                changes.items.iter().map(change_key).collect::<Vec<_>>()
            );
        }
        other => panic!("expected Changes, got {other:?}"),
    }

    Ok(())
}

/// The central invariant: `view_and_watch(prefix)`'s initial view, folded together with every
/// subsequent `Changes`/`Progress` it delivers, always equals `view()` filtered to that prefix -
/// with no gap and no duplicate - even while several writers commit concurrently (puts, deletes,
/// prefix deletes, and transactions that also touch keys outside the prefix). Checked once at an
/// intermediate checkpoint (writers paused, so the comparison view is stable) and once at the end.
#[tokio::test(flavor = "multi_thread")]
async fn view_and_watch_matches_view_with_no_gap_or_duplicate_under_concurrent_writers() -> io::Result<()> {
    const PREFIX: &str = "room/";
    const WRITERS: usize = 4;
    const WRITES_PER_PHASE: usize = 25;

    let (_, a, _data) = founding_node().await?;

    let (initial_view, mut stream) = a.view_and_watch(PREFIX);
    let mut map = view_map_prefix(&initial_view, PREFIX);
    let mut last_revision = initial_view.revision();

    // Phase 1, then a checkpoint against a snapshot taken right after every writer of it joined.
    run_concurrent_writers(&a, WRITERS, WRITES_PER_PHASE, 1).await;
    let checkpoint = a.view();
    drain_to(&mut stream, &mut map, &mut last_revision, checkpoint.revision()).await;
    assert_eq!(view_map_prefix(&checkpoint, PREFIX), map, "checkpoint mismatch");

    // Phase 2, then the final comparison.
    run_concurrent_writers(&a, WRITERS, WRITES_PER_PHASE, 2).await;
    let final_view = a.view();
    drain_to(&mut stream, &mut map, &mut last_revision, final_view.revision()).await;
    assert_eq!(view_map_prefix(&final_view, PREFIX), map, "final mismatch");

    Ok(())
}

/// `WRITERS` concurrent tasks, each committing `writes_per_writer` transactions against its own
/// disjoint sub-namespace `room/{phase}/{writer}/*` (so writers never race each other's keys):
/// most iterations `Put`, one in five also touches a key outside `room/` in the same transaction,
/// one in five `Delete`s the key the previous iteration just put, and one in five `DeletePrefix`es
/// the writer's own whole subtree for this phase - a real prefix delete, not a no-op.
async fn run_concurrent_writers(a: &EzKv, writers: usize, writes_per_writer: usize, phase: usize) {
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let a = a.clone();
            tokio::spawn(async move {
                for i in 0..writes_per_writer {
                    let txn = match i % 5 {
                        0 | 1 => Txn::new().put(format!("room/{phase}/{w}/{i}"), format!("v{phase}-{w}-{i}")),
                        2 => Txn::new()
                            .put(format!("room/{phase}/{w}/{i}"), format!("v{phase}-{w}-{i}"))
                            .put(format!("other/{phase}/{w}/{i}"), "outside"),
                        3 => Txn::new().delete(format!("room/{phase}/{w}/{}", i - 1)),
                        _ => Txn::new().delete_prefix(format!("room/{phase}/{w}/")),
                    };
                    a.write(txn).await.unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("a writer task panicked");
    }
}

/// A consumer waiting on a node that has shut down would otherwise wait forever: nothing will
/// ever be applied there again. The stream first delivers what it already held, then ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_delivers_what_it_holds_and_then_ends_when_its_node_shuts_down() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    let mut stream = a.watch("room/", WatchStart::Now);
    let revision = a.write(set("room/1", "open")).await.map_err(|e| io::Error::other(e.to_string()))?;

    a.shutdown().await?;

    match next_update(&mut stream).await {
        WatchUpdate::Changes(changes) => assert_eq!(revision, changes.revision),
        other => panic!("the change queued before the shutdown must still be delivered, got {other:?}"),
    }

    let ended = tokio::time::timeout(WAIT.expect("cluster waits are bounded"), stream.next())
        .await
        .expect("a stream on a shut-down node must end rather than wait forever");
    assert!(ended.is_none(), "expected the stream to end, got {ended:?}");

    // A subscription made after the shutdown ends the same way.
    let mut late = a.watch("room/", WatchStart::Now);
    let ended = tokio::time::timeout(WAIT.expect("cluster waits are bounded"), late.next())
        .await
        .expect("a stream registered on a shut-down node must end rather than wait forever");
    assert!(ended.is_none(), "expected the late stream to end, got {ended:?}");

    Ok(())
}

/// Drain `stream` into `map`/`last_revision` until `last_revision` reaches `target`, asserting
/// that every `Changes` this loop sees carries a strictly greater revision than the one before it.
async fn drain_to(stream: &mut WatchStream, map: &mut BTreeMap<String, String>, last_revision: &mut u64, target: u64) {
    while *last_revision < target {
        match next_update(stream).await {
            WatchUpdate::Changes(changes) => {
                assert!(
                    changes.revision > *last_revision,
                    "revisions must strictly increase: {} did not exceed {}",
                    changes.revision,
                    *last_revision
                );
                fold_changes(map, &changes);
                *last_revision = changes.revision;
            }
            WatchUpdate::Progress { revision } => {
                assert!(revision >= *last_revision);
                *last_revision = revision;
            }
            other @ WatchUpdate::Reset { .. } => {
                panic!("no Reset expected: the default window is far larger than this test's writes - got {other:?}")
            }
        }
    }
}
