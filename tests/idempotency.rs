//! Same-request-id retry and the idempotency ledger (ech0-media#30): exactly-once writes across a
//! leader change, `Indeterminate` and its later resolution, recorded conflicts, eviction by
//! count, and surviving a snapshot.
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::Condition;
use ezraft::Conflict;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::LocalLag;
use ezraft::RequestId;
use ezraft::Txn;
use ezraft::WriteError;
use ezraft::fault::Fault;
use ezraft::fault::Faults;

/// Drive one `write_with_id` call to a *resolved* outcome: `Indeterminate` is retried (the same
/// id, the same transaction) until it stops being ambiguous, and `LocalLag` is treated as the
/// success it is - waiting out the local catch-up itself - so the caller always learns the
/// transaction's real fate. Bounded by the harness's own `WAIT` budget, like every other wait
/// here.
async fn write_cas_resolved(node: &EzKv, id: RequestId, txn: Txn) -> Result<u64, Conflict> {
    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster waits are bounded");
    loop {
        match node.write_with_id(id, txn.clone()).await {
            Ok(revision) => return Ok(revision),
            Err(WriteError::LocalLag(LocalLag { revision })) => {
                let _ = node.wait_revision(revision, WAIT.expect("cluster waits are bounded")).await;
                return Ok(revision);
            }
            Err(WriteError::Conflict(conflict)) => return Err(conflict),
            Err(WriteError::Indeterminate { .. }) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "write_with_id did not resolve within the wait budget"
                );
            }
            Err(WriteError::LimitExceeded(err)) => panic!("unexpected limit error in a pure CAS loop: {err}"),
        }
    }
}

/// A conditional put on `key`, built from the writer's own last-known view of it: `NotExists` the
/// first time, `ModRevEq` afterward - the shape of a single-writer compare-and-set loop.
fn cas_increment(view: &ezraft::View, key: &str) -> Txn {
    match view.get(key) {
        None => Txn::new().when(Condition::NotExists { key: key.to_string() }).put(key, "1"),
        Some(kv) => {
            let next = text(&kv).parse::<u64>().expect("counter is always a decimal integer") + 1;
            Txn::new()
                .when(Condition::ModRevEq {
                    key: key.to_string(),
                    mod_rev: kv.mod_rev,
                })
                .put(key, next.to_string())
        }
    }
}

/// A single writer on a follower runs a long sequence of compare-and-set increments while the
/// leader is killed mid-run: every write that resolves to a success increments exactly once, the
/// final counter equals the number of successes, and - because there is only one writer - a
/// retry that was re-executed instead of deduplicated would surface as a spurious conflict, which
/// must never happen.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_commits_exactly_once_across_a_leader_kill() -> io::Result<()> {
    const ITERATIONS: u64 = 30;

    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([0, b.node_id(), c.node_id()]),
        "every joined node is a voter",
    )
    .await?;

    let writer = {
        let b = b.clone();
        tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                let txn = cas_increment(&b.view(), "counter");
                write_cas_resolved(&b, RequestId::new(), txn).await.unwrap_or_else(|conflict| {
                    panic!("a single writer must never see a spurious conflict: {conflict:?}")
                });
            }
        })
    };

    // Give the run a moment to start, then kill the leader mid-run.
    tokio::time::sleep(Duration::from_millis(150)).await;
    a.shutdown().await?;

    writer.await.expect("writer task panicked");

    let view = wait_for_revision(&b, ITERATIONS).await?;
    assert_eq!(ITERATIONS, view.revision(), "every iteration committed exactly once");
    let counter = view.get("counter").expect("the counter key must exist");
    assert_eq!(ITERATIONS.to_string(), text(&counter));
    assert_eq!(ITERATIONS, counter.mod_rev);

    Ok(())
}

/// With `Fault::DropResponse` between a follower and the leader, the write still commits on the
/// leader, but the follower - never hearing back - reports `Indeterminate`. After healing,
/// `write_with_id` with that same id returns the original success and the revision it committed
/// at, and the key was written exactly once (its `mod_rev` equals that revision).
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_response_is_indeterminate_until_write_with_id_resolves_it() -> io::Result<()> {
    let faults = Faults::new();
    let node_config = EzConfig {
        write_deadline: Duration::from_millis(300),
        ..config_with_faults(&faults)
    };
    let (addr_a, a, _data_a) = founding_node_with(node_config.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, node_config).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;
    assert!(a.is_leader());

    // The request is delivered and applied; only the answer back to `b` is lost.
    faults.set(b.node_id(), a.node_id(), Fault::DropResponse);

    let id = RequestId::new();
    let err = b.write_with_id(id, set("k1", "v1")).await.expect_err("b never hears an answer back");
    let WriteError::Indeterminate { request_id } = err else {
        panic!("expected Indeterminate, got {err}");
    };
    assert_eq!(id, request_id);

    // The write really did commit, even though `b` could not tell.
    let leader_view = wait_for_revision(&a, 1).await?;
    assert_eq!(Some("v1".to_string()), leader_view.get("k1").map(|kv| text(&kv)));

    faults.heal();

    let revision = b.write_with_id(id, set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(1, revision, "write_with_id resolves to the original success");

    let view = wait_for_revision(&a, revision).await?;
    let kv = view.get("k1").expect("the key exists");
    assert_eq!(revision, kv.mod_rev, "the key was written exactly once");

    Ok(())
}

/// Once a request id's conflict is recorded, a retry of that same id gets the recorded conflict
/// back - never a fresh evaluation - even after the state changes so that the transaction's
/// condition would now hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_conflict_is_returned_unchanged_even_after_the_state_that_caused_it_changes() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    a.write(set("room/1", "lobby")).await.map_err(|e| io::Error::other(e.to_string()))?;

    let id = RequestId::new();
    let txn = Txn::new()
        .when(Condition::NotExists {
            key: "room/1".to_string(),
        })
        .put("room/1", "clobber");

    let first = a.write_with_id(id, txn.clone()).await.expect_err("the condition does not hold: room/1 exists");
    let WriteError::Conflict(first_conflict) = first else {
        panic!("expected a Conflict, got {first}");
    };

    // Delete the key: the very same condition (`NotExists`) would hold now.
    a.write(Txn::new().delete("room/1")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(None, a.view().get("room/1"));

    let retried = a.write_with_id(id, txn).await.expect_err("a retry of the same id must get the recorded conflict");
    let WriteError::Conflict(retried_conflict) = retried else {
        panic!("expected a Conflict, got {retried}");
    };
    assert_eq!(
        first_conflict, retried_conflict,
        "the recorded conflict, not a fresh evaluation"
    );

    // And the retry's `put` never ran either.
    assert_eq!(None, a.view().get("room/1"));

    Ok(())
}

/// With a tiny ledger capacity, a request id evicted past the window executes again - a fresh
/// revision, a changed `mod_rev` - while ids still inside the window keep deduplicating. Every
/// replica agrees on the result.
#[tokio::test(flavor = "multi_thread")]
async fn eviction_by_count_lets_old_ids_execute_again_while_recent_ones_still_dedupe() -> io::Result<()> {
    let node_config = EzConfig {
        ledger_capacity: 2,
        ..config()
    };
    let (addr_a, a, _data_a) = founding_node_with(node_config.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, node_config).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;

    let id0 = RequestId::new();
    let id1 = RequestId::new();
    let id2 = RequestId::new();
    let txn0 = set("k0", "v0");

    assert_eq!(
        1,
        a.write_with_id(id0, txn0.clone()).await.map_err(|e| io::Error::other(e.to_string()))?
    );
    assert_eq!(
        2,
        a.write_with_id(id1, set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?
    );
    // Capacity 2: recording id2 evicts id0, the oldest - id1 and id2 stay in the window.
    assert_eq!(
        3,
        a.write_with_id(id2, set("k2", "v2")).await.map_err(|e| io::Error::other(e.to_string()))?
    );

    // id1 and id2 are still inside the window: retrying them (with different ops, to prove they
    // truly never ran) changes nothing. Checked *before* touching id0 below: a deduplicated
    // retry never records a new ledger entry, so it cannot itself evict anything - only a fresh
    // execution can.
    assert_eq!(
        2,
        a.write_with_id(id1, set("k1", "ignored")).await.map_err(|e| io::Error::other(e.to_string()))?
    );
    assert_eq!(
        3,
        a.write_with_id(id2, set("k2", "ignored")).await.map_err(|e| io::Error::other(e.to_string()))?
    );

    // id0 is outside the window now: the very same request id executes again, as if new. This
    // fresh execution is itself a new ledger insertion, which in turn evicts id1 (now the
    // oldest) - a further, expected consequence of the same FIFO-by-count rule, not asserted on
    // here since the point already stands: id0 ran again.
    let re_executed = a.write_with_id(id0, txn0).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(4, re_executed, "an evicted id proposes a fresh transaction");

    let view_a = a.view();
    assert_eq!(4, view_a.revision());
    assert_eq!(
        4,
        view_a.get("k0").unwrap().mod_rev,
        "k0 was rewritten by the re-executed id0"
    );
    assert_eq!(
        2,
        view_a.get("k1").unwrap().mod_rev,
        "k1 untouched by the earlier deduplicated retry"
    );
    assert_eq!(
        3,
        view_a.get("k2").unwrap().mod_rev,
        "k2 untouched by the earlier deduplicated retry"
    );

    // Every replica applied the exact same sequence and agrees.
    let view_b = wait_for_revision(&b, 4).await?;
    assert_eq!(view_map(&view_a), view_map(&view_b));

    Ok(())
}

/// A node that catches up through a snapshot - never replaying the log entry that recorded a
/// request id - must still deduplicate that id once it becomes leader itself: the ledger travels
/// inside the snapshot, not just the log.
#[tokio::test(flavor = "multi_thread")]
async fn the_ledger_survives_a_snapshot_and_still_deduplicates_after_a_leadership_change() -> io::Result<()> {
    let node_config = EzConfig {
        snapshot_interval: 5,
        ..config()
    };
    let (addr_a, a, _data_a) = founding_node_with(node_config.clone()).await?;

    let id = RequestId::new();
    let txn = set("room/1", "lobby");
    assert_eq!(
        1,
        a.write_with_id(id, txn.clone()).await.map_err(|e| io::Error::other(e.to_string()))?
    );

    // Enough further writes to cross `snapshot_interval`, then snapshot and purge the log that
    // covers the recorded id: a joiner from here on can only catch up through the snapshot.
    for i in 0..10 {
        a.write(set(&format!("k{i}"), &format!("v{i}")))
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

    let (_, b, _data_b) = joined_voter_with(&addr_a, node_config).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([0, b.node_id()]),
        "the snapshot-fed joiner is promoted",
    )
    .await?;
    assert_eq!(
        Some("lobby".to_string()),
        b.view().get("room/1").map(|kv| text(&kv)),
        "the whole pre-purge state, including the recorded write, arrived through the snapshot"
    );

    // Hand leadership to the snapshot-fed joiner and retry the very same id there.
    a.transfer_leader(b.node_id()).await?;
    b.wait_metrics(
        WAIT,
        |m| m.current_leader == Some(b.node_id()),
        "the joiner takes leadership",
    )
    .await?;

    let revision_before = b.view().revision();
    let deduplicated = b.write_with_id(id, txn).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(1, deduplicated, "the original, pre-snapshot outcome is returned");
    assert_eq!(
        revision_before,
        b.view().revision(),
        "a deduplicated command must not advance the revision"
    );

    Ok(())
}
