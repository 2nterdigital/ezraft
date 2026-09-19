//! `EzKv::write` on any node: forwarding to the leader and read-your-writes (ech0-media#30)
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes. Same-request-id
//! retry across a leader change, `Indeterminate`, and the idempotency ledger's own behavior
//! (including eviction and snapshot survival) live in `tests/idempotency.rs` instead.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::KvLimits;
use ezraft::LimitError;
use ezraft::LocalLag;
use ezraft::WriteError;
use ezraft::fault::Fault;
use ezraft::fault::Faults;

/// A follower's `write` must forward, succeed, and already be visible on `view()` when it
/// returns - no retry loop needed here, because `write` itself already waits for local catch-up.
#[tokio::test(flavor = "multi_thread")]
async fn a_followers_write_forwards_and_is_immediately_visible_locally() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;
    assert!(!b.is_leader());

    let revision = b.write(set("room/1", "lobby")).await.map_err(|e| io::Error::other(e.to_string()))?;

    let view = b.view();
    assert_eq!(revision, view.revision());
    assert_eq!(Some("lobby".to_string()), view.get("room/1").map(|kv| text(&kv)));

    // The leader agrees too.
    assert_eq!(Some("lobby".to_string()), a.view().get("room/1").map(|kv| text(&kv)));

    Ok(())
}

/// A learner - never a voter, never eligible to lead - must be able to write the same way: it
/// only ever forwards.
#[tokio::test(flavor = "multi_thread")]
async fn a_learners_write_forwards_and_is_immediately_visible_locally() -> io::Result<()> {
    let (addr_a, _a, _data_a) = founding_node().await?;
    let (_, learner, _data_l) = joined_learner(&addr_a).await?;
    learner.wait_metrics(WAIT, |m| m.current_leader.is_some(), "the learner learns who leads").await?;

    let revision = learner.write(set("room/1", "lobby")).await.map_err(|e| io::Error::other(e.to_string()))?;

    let view = learner.view();
    assert_eq!(revision, view.revision());
    assert_eq!(Some("lobby".to_string()), view.get("room/1").map(|kv| text(&kv)));

    Ok(())
}

/// A conflict answered by a *remote* leader must still be visible on the calling node's own
/// view by the time `write` returns it: `write` waits, best-effort, for the local revision to
/// reach the conflict's `current_mod_rev` before handing the conflict back.
#[tokio::test(flavor = "multi_thread")]
async fn a_followers_conflict_is_visible_locally_before_write_returns_it() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;

    // Written through the leader directly, so the follower's own local view starts out unaware
    // of it - exactly the gap that would make a naive local re-read miss the conflict's cause.
    a.write(set("room/1", "lobby")).await.map_err(|e| io::Error::other(e.to_string()))?;

    let txn = ezraft::Txn::new()
        .when(ezraft::Condition::NotExists {
            key: "room/1".to_string(),
        })
        .put("room/1", "clobber");
    let err = b.write(txn).await.expect_err("the condition does not hold");
    let WriteError::Conflict(conflict) = err else {
        panic!("expected a Conflict, got {err}");
    };
    assert_eq!(1, conflict.current_mod_rev);

    // No extra wait here: by the time `write` returned, `b`'s own view must already show what
    // the conflict was about.
    assert_eq!(
        Some("lobby".to_string()),
        b.view().get("room/1").map(|kv| text(&kv)),
        "the follower's view must already reflect the state the conflict was reported against"
    );

    Ok(())
}

/// Cutting replication from the leader to one follower - while that follower's own outbound RPC
/// to the leader keeps working - must turn a successful, committed write into `LocalLag` on that
/// follower specifically: the other nodes already show it.
#[tokio::test(flavor = "multi_thread")]
async fn a_followers_write_reports_local_lag_when_replication_to_it_is_cut() -> io::Result<()> {
    let faults = Faults::new();
    // Long enough that the write itself - a forward to the leader and a quorum commit with real
    // fsyncs - always finishes inside it even on a loaded machine: what runs out must be the wait
    // for this follower's own replica, or the answer would be `Indeterminate` instead.
    let node_config = EzConfig {
        write_deadline: Duration::from_secs(2),
        ..config_with_faults(&faults)
    };
    let (addr_a, a, _data_a) = founding_node_with(node_config.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, node_config.clone()).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, node_config).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([0, b.node_id(), c.node_id()]),
        "every joined node is a voter",
    )
    .await?;
    assert!(a.is_leader());

    // Every node's own startup self-registration (ech0-media#33) must be done before the tight
    // 500ms deadline below is measured: it forwards over this same any-node command path too, and
    // a still-retrying attempt sharing the RPC connection pool could otherwise eat into this
    // test's own tiny margin.
    wait_for_registry(
        &a,
        |r| r.get(0).is_some() && r.get(b.node_id()).is_some() && r.get(c.node_id()).is_some(),
        "every node finishes registering",
    )
    .await?;

    // Cut only the leader's outbound replication to `b`. `b`'s own outbound RPC to the leader
    // (its forwarded write) is untouched, and `c` keeps replicating normally.
    faults.set(a.node_id(), b.node_id(), Fault::Drop);

    let err = b.write(set("k1", "v1")).await.expect_err("b's own applied state cannot catch up while cut off");
    let WriteError::LocalLag(LocalLag { revision }) = err else {
        panic!("expected LocalLag, got {err}");
    };
    assert_eq!(1, revision);
    assert_eq!(
        0,
        b.view().revision(),
        "b's local view never advanced while replication was cut"
    );

    // Committed, and already visible on the leader and on the unaffected follower.
    let view_a = wait_for_revision(&a, revision).await?;
    assert_eq!(Some("v1".to_string()), view_a.get("k1").map(|kv| text(&kv)));
    let view_c = wait_for_revision(&c, revision).await?;
    assert_eq!(Some("v1".to_string()), view_c.get("k1").map(|kv| text(&kv)));

    // Healing lets `b` catch up, and `wait_revision` observes exactly that.
    faults.clear(a.node_id(), b.node_id());
    b.wait_revision(revision, WAIT.expect("cluster waits are bounded"))
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(Some("v1".to_string()), b.view().get("k1").map(|kv| text(&kv)));

    Ok(())
}

/// A leader cut off from its quorum while its lease still holds appends the entry and then waits
/// for a commit that cannot come - openraft's own `client_write` never times out. `write` must
/// still honor its deadline, and the honest answer is `Indeterminate`: the entry is in the old
/// leader's log and only the rest of the cluster decides whether it survives.
#[tokio::test(flavor = "multi_thread")]
async fn an_isolated_leaders_write_honors_the_deadline_and_reports_indeterminate() -> io::Result<()> {
    let faults = Faults::new();
    let deadline = Duration::from_millis(800);
    let node_config = EzConfig {
        write_deadline: deadline,
        ..config_with_faults(&faults)
    };
    let (addr_a, a, _data_a) = founding_node_with(node_config.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, node_config.clone()).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, node_config).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([0, b.node_id(), c.node_id()]),
        "every joined node is a voter",
    )
    .await?;
    assert!(a.is_leader());

    // See the identical wait in `a_followers_write_reports_local_lag_when_replication_to_it_is_cut`:
    // every node's own startup self-registration (ech0-media#33) should be done before this
    // test's own tight deadline is measured.
    wait_for_registry(
        &a,
        |r| r.get(0).is_some() && r.get(b.node_id()).is_some() && r.get(c.node_id()).is_some(),
        "every node finishes registering",
    )
    .await?;

    faults.isolate(a.node_id(), &[b.node_id(), c.node_id()]);

    let started = std::time::Instant::now();
    let err = tokio::time::timeout(WAIT.expect("cluster waits are bounded"), a.write(set("stale", "value")))
        .await
        .expect("write must return on its own deadline, not hang on an isolated leader")
        .expect_err("an isolated leader cannot commit");
    let WriteError::Indeterminate { request_id } = err else {
        panic!("expected Indeterminate, got {err}");
    };
    // Returning at all proves the point - without its own bound the call would still be waiting
    // when the harness gave up on it - so this only checks the delay is the deadline's, with room
    // for a loaded machine, and not something on the order of that harness wait.
    assert!(
        started.elapsed() < deadline * 10,
        "write took {:?} against a {:?} deadline",
        started.elapsed(),
        deadline
    );

    // Once the network heals, the same request id settles the question: whatever happened to the
    // first attempt, the write takes effect exactly once. Healing can land in the middle of the
    // election the isolation provoked, so one more deadline may still pass without a definite
    // answer - and asking again under the same id, until there is one, is what a caller does.
    faults.heal();
    let revision = write_until_definite(&a, request_id, set("stale", "value")).await?;
    for node in [&a, &b, &c] {
        let view = wait_for_revision(node, revision).await?;
        let kv = view.get("stale").expect("the write took effect");
        assert_eq!("value", text(&kv));
        assert_eq!(kv.create_rev, kv.mod_rev, "written once, never rewritten");
    }

    Ok(())
}

/// A transaction that violates this node's own configured limits must never leave it: no RPC is
/// ever attempted, whether or not this node happens to be the leader.
#[tokio::test(flavor = "multi_thread")]
async fn limits_are_enforced_on_the_calling_node_before_anything_is_sent() -> io::Result<()> {
    let faults = Faults::new();
    let limits = KvLimits {
        max_key_bytes: 4,
        ..KvLimits::default()
    };
    let node_config = EzConfig {
        kv_limits: limits,
        ..config_with_faults(&faults)
    };
    let (addr_a, a, _data_a) = founding_node_with(node_config.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, node_config).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;
    assert!(!b.is_leader());

    // Both nodes' own startup self-registration (ech0-media#33) forwards over this exact same
    // `ez.write` RPC when it runs on a non-leader like `b` - settled here, before the baseline
    // below, so it cannot be mistaken for a request this test's own oversized write made.
    wait_for_registry(
        &a,
        |r| r.get(0).is_some() && r.get(b.node_id()).is_some(),
        "both nodes finish registering",
    )
    .await?;
    let baseline = faults.request_count(b.node_id(), a.node_id(), "rpc/ez.write");

    let err = b.write(set("way-too-long-a-key", "v")).await.expect_err("the key exceeds this node's own limit");
    assert!(
        matches!(err, WriteError::LimitExceeded(LimitError::KeyTooLarge { .. })),
        "{err}"
    );

    assert_eq!(
        baseline,
        faults.request_count(b.node_id(), a.node_id(), "rpc/ez.write"),
        "an oversized transaction must never be forwarded"
    );
    assert_eq!(0, a.view().revision(), "nothing was proposed anywhere");
    assert_eq!(0, b.view().revision());

    Ok(())
}
