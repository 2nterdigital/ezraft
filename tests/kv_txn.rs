//! `EzKv::write` acceptance tests (ech0-media#27): publish-before-reply, conflicts, limits,
//! `DeletePrefix` atomicity, and cross-replica revision agreement.
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes. Any-node
//! forwarding, same-request-id retry and the idempotency ledger (ech0-media#30 - what replaced
//! this file's old "a follower's write returns `NotLeader`" case) live in `tests/write_forward.rs`
//! and `tests/idempotency.rs` instead.

mod common;

use std::collections::BTreeSet;
use std::io;

use common::*;
use ezraft::Condition;
use ezraft::EzConfig;
use ezraft::KvLimits;
use ezraft::LimitError;
use ezraft::Txn;
use ezraft::WriteError;

/// Writing on the leader, then calling `view()`, must show the write with no wait and no retry
/// loop: the adapter publishes before answering the writer.
#[tokio::test(flavor = "multi_thread")]
async fn write_on_the_leader_is_immediately_visible_in_its_own_view() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    let revision = a.write(set("room/1", "lobby")).await.map_err(|e| io::Error::other(e.to_string()))?;

    let view = a.view();
    assert_eq!(revision, view.revision());
    assert_eq!(Some("lobby".to_string()), view.get("room/1").map(|kv| text(&kv)));

    Ok(())
}

/// A failed condition must change nothing anywhere and report the first failing condition with
/// the key's current `mod_rev`.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_condition_changes_nothing_and_reports_the_first_failure() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    a.write(set("room/1", "lobby")).await.map_err(|e| io::Error::other(e.to_string()))?;
    let revision_before = a.view().revision();

    let txn = Txn::new()
        .when(Condition::ModRevEq {
            key: "room/1".to_string(),
            mod_rev: 999,
        })
        .put("room/1", "changed")
        .put("room/2", "new");

    let err = a.write(txn).await.expect_err("the condition does not hold");
    let WriteError::Conflict(conflict) = err else {
        panic!("expected a Conflict, got {err}");
    };
    assert_eq!(0, conflict.index);
    assert_eq!(
        Condition::ModRevEq {
            key: "room/1".to_string(),
            mod_rev: 999
        },
        conflict.condition
    );
    assert_eq!("room/1", conflict.key);
    assert_eq!(revision_before, conflict.current_mod_rev);

    // Nothing changed anywhere: not the conditioned key, not the revision, not the key the
    // (never-run) op would have created.
    let view = a.view();
    assert_eq!(revision_before, view.revision());
    assert_eq!(Some("lobby".to_string()), view.get("room/1").map(|kv| text(&kv)));
    assert_eq!(None, view.get("room/2"));

    Ok(())
}

/// A condition about a key that does not exist reports `mod_rev == 0`, the same convention
/// `View` and `Conflict` share for "absent".
#[tokio::test(flavor = "multi_thread")]
async fn a_condition_on_an_absent_key_reports_mod_rev_zero() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    let txn = Txn::new()
        .when(Condition::Exists {
            key: "missing".to_string(),
        })
        .put("a", "1");
    let err = a.write(txn).await.expect_err("the key does not exist");
    let WriteError::Conflict(conflict) = err else {
        panic!("expected a Conflict, got {err}");
    };
    assert_eq!("missing", conflict.key);
    assert_eq!(0, conflict.current_mod_rev);
    assert_eq!(
        0,
        a.view().revision(),
        "a conflicting write never advances the revision"
    );
    assert_eq!(None, a.view().get("a"), "the op behind the failed condition never ran");

    Ok(())
}

/// `DeletePrefix` must remove its whole prefix as one atomic change: a concurrent reader polling
/// `view()` never observes a count between "every key still there" and "none of them are".
#[tokio::test(flavor = "multi_thread")]
async fn delete_prefix_removes_the_whole_prefix_atomically() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    let writer = {
        let a = a.clone();
        tokio::spawn(async move {
            for _ in 0..200 {
                a.write(Txn::new().put("room/1/a", "alice").put("room/1/b", "bob").put("room/1/c", "carol"))
                    .await
                    .unwrap();
                a.write(Txn::new().delete_prefix("room/1/")).await.unwrap();
            }
        })
    };

    let reader = {
        let a = a.clone();
        tokio::spawn(async move {
            let mut saw_partial = false;
            for _ in 0..20_000 {
                let remaining = a.view().prefix("room/1/").count();
                if remaining != 0 && remaining != 3 {
                    saw_partial = true;
                }
            }
            saw_partial
        })
    };

    writer.await.expect("writer task panicked");
    let saw_partial = reader.await.expect("reader task panicked");
    assert!(
        !saw_partial,
        "a reader must never observe a partial multi-key transaction"
    );

    // An unrelated prefix must never be touched by `DeletePrefix`.
    a.write(set("room/2/a", "dave")).await.map_err(|e| io::Error::other(e.to_string()))?;
    a.write(Txn::new().delete_prefix("room/1/")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(0, a.view().prefix("room/1/").count());
    assert_eq!(1, a.view().prefix("room/2/").count());

    Ok(())
}

/// After the same writes reach three replicas, every one of them must agree on the revision and
/// on each key's `create_rev`/`mod_rev`; revisions successive successful writes observe must be
/// contiguous, and a conflicting write must not consume one.
#[tokio::test(flavor = "multi_thread")]
async fn three_replicas_agree_on_revisions_after_the_same_writes() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([0, b.node_id(), c.node_id()]),
        "every joined node is a voter",
    )
    .await?;

    let mut revisions = Vec::new();
    for i in 0..5 {
        revisions.push(write_through_leader(&[&a, &b, &c], set(&format!("k{i}"), &format!("v{i}"))).await?);
    }

    // Contiguous: each successful write's revision is exactly one past the last.
    for pair in revisions.windows(2) {
        assert_eq!(pair[0] + 1, pair[1], "revisions must be contiguous: {:?}", revisions);
    }

    // A conflicting write must not consume a revision.
    let before_conflict = a.view().revision();
    let conflict = a.write(Txn::new().when(Condition::NotExists { key: "k0".to_string() }).put("k0", "clobber")).await;
    assert!(matches!(conflict, Err(WriteError::Conflict(_))), "{conflict:?}");
    assert_eq!(before_conflict, a.view().revision());

    wait_for_revision(&b, before_conflict).await?;
    wait_for_revision(&c, before_conflict).await?;

    for node in [&a, &b, &c] {
        let view = node.view();
        assert_eq!(
            before_conflict,
            view.revision(),
            "node {} must not have advanced past the conflicting write",
            node.node_id()
        );
        for (i, revision) in revisions.iter().enumerate() {
            let key = format!("k{i}");
            let kv = view.get(&key).unwrap_or_else(|| panic!("{key} missing on node {}", node.node_id()));
            assert_eq!(
                *revision,
                kv.create_rev,
                "create_rev for {key} on node {}",
                node.node_id()
            );
            assert_eq!(*revision, kv.mod_rev, "mod_rev for {key} on node {}", node.node_id());
        }
    }

    Ok(())
}

/// Every limit: exactly at it is accepted, one over is rejected naming that limit, and nothing
/// is written either way.
#[tokio::test(flavor = "multi_thread")]
async fn every_key_value_and_op_count_limit_is_enforced_before_anything_is_proposed() -> io::Result<()> {
    let limits = KvLimits {
        max_key_bytes: 4,
        max_value_bytes: 4,
        max_txn_ops: 2,
        max_txn_bytes: 1024,
    };
    let node_config = EzConfig {
        kv_limits: limits,
        ..config()
    };
    let (_, a, _data_a) = founding_node_with(node_config).await?;

    // Exactly at the key limit is accepted.
    let revision = a.write(Txn::new().put("abcd", "x")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(1, revision);

    // One byte over the key limit is rejected, and nothing changes.
    let err = a.write(Txn::new().put("abcde", "x")).await.expect_err("the key is one byte too long");
    assert!(
        matches!(err, WriteError::LimitExceeded(LimitError::KeyTooLarge { .. })),
        "{err}"
    );
    assert_eq!(1, a.view().revision());
    assert_eq!(None, a.view().get("abcde"));

    // Exactly at the value limit is accepted.
    let revision = a.write(Txn::new().put("v", "abcd")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(2, revision);

    // One byte over the value limit is rejected, and nothing changes.
    let err = a.write(Txn::new().put("v2", "abcde")).await.expect_err("the value is one byte too long");
    assert!(
        matches!(err, WriteError::LimitExceeded(LimitError::ValueTooLarge { .. })),
        "{err}"
    );
    assert_eq!(2, a.view().revision());
    assert_eq!(None, a.view().get("v2"));

    // Exactly at the op-count limit is accepted.
    let revision = a
        .write(Txn::new().put("o1", "1").put("o2", "1"))
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(3, revision);

    // One op over the limit is rejected, and nothing changes.
    let err = a.write(Txn::new().put("o3", "1").put("o4", "1").put("o5", "1")).await.expect_err("one op too many");
    assert!(
        matches!(err, WriteError::LimitExceeded(LimitError::TooManyOps { .. })),
        "{err}"
    );
    assert_eq!(3, a.view().revision());
    assert_eq!(None, a.view().get("o3"));

    Ok(())
}

/// The total-transaction-size limit, checked on its own: exactly at it is accepted, one byte
/// over is rejected naming it, and nothing is written either way.
#[tokio::test(flavor = "multi_thread")]
async fn the_total_transaction_size_limit_is_enforced_before_anything_is_proposed() -> io::Result<()> {
    let limits = KvLimits {
        max_key_bytes: 1024,
        max_value_bytes: 1024,
        max_txn_ops: 1024,
        // key "a" (1 byte) + value "bc" (2 bytes) = 3 bytes total.
        max_txn_bytes: 3,
    };
    let node_config = EzConfig {
        kv_limits: limits,
        ..config()
    };
    let (_, a, _data_a) = founding_node_with(node_config).await?;

    let revision = a.write(Txn::new().put("a", "bc")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(1, revision);

    let err = a.write(Txn::new().put("d", "bcd")).await.expect_err("one byte over the total-size limit");
    assert!(
        matches!(err, WriteError::LimitExceeded(LimitError::TooLarge { .. })),
        "{err}"
    );
    assert_eq!(1, a.view().revision());
    assert_eq!(None, a.view().get("d"));

    Ok(())
}
