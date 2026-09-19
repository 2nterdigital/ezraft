//! Linearizable reads on any node (ech0-media#31): `EzKv::linearizable_view`, ReadIndex only,
//! coalesced on the follower.
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes; the round-count
//! and delay scenarios use the `fault-injection` hook's additive request counters
//! (`Faults::request_count`) to observe how many `ez.read_index` RPCs actually went out, rather
//! than guessing from timing.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::ReadError;
use ezraft::fault::Fault;
use ezraft::fault::Faults;
use ezraft::fault::TrafficClass;

/// Generous enough that a real localhost round trip, and the local catch-up after it, never
/// time out on their own in these tests.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The path every `ez.read_index` RPC travels under - see `Rpc::call`'s `RPC_PATH_PREFIX`.
const READ_INDEX_PATH: &str = "rpc/ez.read_index";

/// After a write is acknowledged on the leader, `linearizable_view` on every follower - and a
/// learner - already contains it. No retry loop in the test: a single call must already see it.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_acknowledged_on_the_leader_is_immediately_visible_on_every_follower_and_a_learner() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;
    let (_, d, _data_d) = joined_learner(&addr_a).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([0, b.node_id(), c.node_id()]),
        "b and c joined as voters",
    )
    .await?;

    write_through_leader(&[&a], set("k1", "v1")).await?;

    for node in [&b, &c, &d] {
        let view = node.linearizable_view(READ_TIMEOUT).await.map_err(|e| io::Error::other(e.to_string()))?;
        assert_eq!(
            Some("v1".to_string()),
            view.get("k1").map(|kv| text(&kv)),
            "node {} must already see the acknowledged write",
            node.node_id()
        );
    }

    Ok(())
}

/// An old leader isolated by a partition fails `linearizable_view` within its timeout, and never
/// returns a view missing the writes the new leader goes on to commit without it.
#[tokio::test(flavor = "multi_thread")]
async fn an_isolated_old_leader_fails_linearizable_view_and_never_serves_stale_data() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;

    let all = BTreeSet::from([a.node_id(), b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    write_through_leader(&[&a], set("k1", "v1")).await?;
    wait_for_applied(&b, &a).await?;
    wait_for_applied(&c, &a).await?;

    let old_leader = a.node_id();
    faults.isolate(old_leader, &[b.node_id(), c.node_id()]);

    let read_on_old_leader = a.linearizable_view(Duration::from_secs(2)).await;
    assert!(
        matches!(read_on_old_leader, Err(ReadError::NoLeader)),
        "an isolated leader must fail, never serve a view: {read_on_old_leader:?}"
    );

    // The remaining voters elect a new leader and keep committing without the old one.
    b.wait_metrics(
        WAIT,
        |m| matches!(m.current_leader, Some(id) if id != old_leader),
        "the remaining voters elect a new leader",
    )
    .await?;
    let new_leader_id = b.metrics().await.current_leader.expect("a new leader was just observed");
    let new_leader = if new_leader_id == b.node_id() { &b } else { &c };
    write_through_leader(&[new_leader], set("k2", "v2")).await?;

    // Even after the rest of the cluster has moved on without it, the isolated old leader still
    // never serves a (necessarily stale) view - it keeps failing outright.
    let still_isolated = a.linearizable_view(Duration::from_millis(200)).await;
    assert!(matches!(still_isolated, Err(ReadError::NoLeader)), "{still_isolated:?}");

    Ok(())
}

/// 100 concurrent `linearizable_view` calls on one follower produce at most 2 `ez.read_index`
/// RPCs to the leader, all succeed, and every result reflects the write acknowledged before the
/// burst started.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reads_on_a_follower_cost_at_most_two_read_index_rounds() -> io::Result<()> {
    // No fault rules are set; `faults` exists only so the round counters are available.
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "b joined as a voter").await?;

    write_through_leader(&[&a], set("k1", "v1")).await?;

    let leader_id = a.node_id();
    let follower_id = b.node_id();

    let mut handles = Vec::new();
    for _ in 0..100 {
        let node = b.clone();
        handles.push(tokio::spawn(async move { node.linearizable_view(READ_TIMEOUT).await }));
    }

    for handle in handles {
        let view = handle.await.expect("task did not panic").map_err(|e| io::Error::other(e.to_string()))?;
        assert_eq!(Some("v1".to_string()), view.get("k1").map(|kv| text(&kv)));
    }

    let rounds = faults.request_count(follower_id, leader_id, READ_INDEX_PATH);
    assert!(rounds >= 1, "expected at least 1 ReadIndex round, got {rounds}");
    assert!(rounds <= 2, "expected at most 2 ReadIndex rounds, got {rounds}");

    Ok(())
}

/// A request that arrives while a round is delayed in flight is served by a later round: this
/// is observable as exactly two counted rounds, not one, and both requests' own views still
/// reflect the write.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_arriving_during_a_delayed_round_is_served_by_a_later_round() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "b joined as a voter").await?;

    write_through_leader(&[&a], set("k1", "v1")).await?;

    let leader_id = a.node_id();
    let follower_id = b.node_id();

    let delay = Duration::from_millis(300);
    faults.set_class(follower_id, leader_id, TrafficClass::Rpc, Fault::Delay(delay));

    let first = {
        let node = b.clone();
        tokio::spawn(async move { node.linearizable_view(READ_TIMEOUT).await })
    };

    // Wait until the first call's round has actually sent its RPC - the fault hook counts a
    // request as it enters the injected delay - rather than guessing how long that takes.
    tokio::time::timeout(READ_TIMEOUT, async {
        while faults.request_count(follower_id, leader_id, READ_INDEX_PATH) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("the first round never sent its RPC"))?;

    let second = {
        let node = b.clone();
        tokio::spawn(async move { node.linearizable_view(READ_TIMEOUT).await })
    };

    let first = first.await.expect("task did not panic").map_err(|e| io::Error::other(e.to_string()))?;
    let second = second.await.expect("task did not panic").map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(Some("v1".to_string()), first.get("k1").map(|kv| text(&kv)));
    assert_eq!(Some("v1".to_string()), second.get("k1").map(|kv| text(&kv)));

    assert_eq!(
        2,
        faults.request_count(follower_id, leader_id, READ_INDEX_PATH),
        "the second call must ride a round issued after it arrived, not the one already in flight"
    );

    Ok(())
}

/// `NoLeader` within the timeout when the cluster has lost its quorum.
#[tokio::test(flavor = "multi_thread")]
async fn no_leader_within_timeout_when_the_cluster_has_lost_quorum() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;

    let ids = [a.node_id(), b.node_id(), c.node_id()];
    wait_for_voters(&a, BTreeSet::from(ids), "every joined node is a voter").await?;

    // Every node loses touch with both others: no majority can ever confirm a leader again.
    for &x in &ids {
        for &y in &ids {
            if x != y {
                faults.set(x, y, Fault::Drop);
            }
        }
    }

    let timeout = Duration::from_secs(2);
    let start = tokio::time::Instant::now();
    let result = b.linearizable_view(timeout).await;
    assert!(matches!(result, Err(ReadError::NoLeader)), "{result:?}");
    assert!(
        start.elapsed() <= timeout + Duration::from_millis(500),
        "linearizable_view must respect its own timeout, took {:?}",
        start.elapsed()
    );

    Ok(())
}

/// `NotMember` on a node that was removed from the cluster.
///
/// A removed node's *own* local membership view is not a reliable signal: the leader stops
/// replicating to it as soon as it is excluded from the new configuration, which can happen
/// before the removed node ever receives the very entry that would tell it so. `NotMember`
/// therefore comes from the leader's own, authoritative answer to the removed node's own
/// `ez.read_index` request (see `leader_read_index`) - checked here right after the leader
/// itself confirms the removal, while `b` can still reach it to ask.
#[tokio::test(flavor = "multi_thread")]
async fn not_member_on_a_node_removed_from_the_cluster() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let b_id = b.node_id();
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "b joined as a voter").await?;

    a.remove_node(b_id).await.map_err(|e| io::Error::other(e.to_string()))?;
    a.wait_metrics(
        WAIT,
        |m| m.membership_config.membership().get_node(&b_id).is_none(),
        "the leader confirms b's removal",
    )
    .await?;

    let result = b.linearizable_view(READ_TIMEOUT).await;
    assert!(
        matches!(result, Err(ReadError::NotMember(id)) if id == b_id),
        "{result:?}"
    );

    Ok(())
}

/// `NotMember` still reaches a removed node once the leader it knew is gone.
///
/// Nothing is replicated to a removed node any more, so its idea of who leads stops at whoever
/// led when it was removed. With that node gone there is nobody left it knows to ask - while the
/// one node that can say "you are not a member", the new leader, is one it has never heard of as
/// a leader. It gets there by asking the other members it remembers, and following their hint.
#[tokio::test(flavor = "multi_thread")]
async fn not_member_on_a_removed_node_once_the_leader_it_knew_is_gone() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;
    let (_, removed, _data_r) = joined_learner(&addr_a).await?;
    let removed_id = removed.node_id();
    let voters = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, voters, "b and c joined as voters").await?;
    // An established member: it has replicated the membership that names it and everyone else,
    // which is also the only address book it will ever have.
    removed
        .wait_metrics(
            WAIT,
            |m| m.membership_config.membership().nodes().count() == 4,
            "the learner has replicated the whole membership",
        )
        .await?;

    a.remove_node(removed_id).await.map_err(|e| io::Error::other(e.to_string()))?;
    for node in [&b, &c] {
        node.wait_metrics(
            WAIT,
            |m| m.committed_membership_config.membership().get_node(&removed_id).is_none(),
            "both surviving voters have committed the removal",
        )
        .await?;
    }

    let old_leader = a.node_id();
    a.shutdown().await?;
    b.wait_metrics(
        WAIT,
        |m| matches!(m.current_leader, Some(id) if id != old_leader),
        "the surviving voters elect a new leader",
    )
    .await?;
    assert_eq!(
        Some(old_leader),
        removed.metrics().await.current_leader,
        "nobody told the removed node about the new leader",
    );

    let result = removed.linearizable_view(READ_TIMEOUT).await;
    assert!(
        matches!(result, Err(ReadError::NotMember(id)) if id == removed_id),
        "{result:?}"
    );

    Ok(())
}

/// `NotCaughtUp` when the follower can reach the leader for the RPC, but replication to it is
/// cut, so its own apply can never advance to the confirmed read position.
#[tokio::test(flavor = "multi_thread")]
async fn not_caught_up_when_replication_is_cut_but_the_leader_answers_the_rpc() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;

    let all = BTreeSet::from([a.node_id(), b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;

    write_through_leader(&[&a], set("k1", "v1")).await?;
    wait_for_applied(&b, &a).await?;

    // Cut only the leader-to-follower direction: replication and heartbeats to `b` stop, but
    // `b`'s own RPC call to the leader (the opposite direction) still gets through and back. The
    // leader still has a quorum through `c`, so it keeps confirming ReadIndex requests normally.
    faults.set(a.node_id(), b.node_id(), Fault::Drop);

    write_through_leader(&[&a], set("k2", "v2")).await?;
    wait_for_applied(&c, &a).await?;

    let result = b.linearizable_view(Duration::from_secs(1)).await;
    assert!(matches!(result, Err(ReadError::NotCaughtUp { .. })), "{result:?}");

    Ok(())
}

/// Dropping half of a concurrent burst mid-flight does not prevent the rest, or a later call,
/// from completing: no waiter can wedge the coalescer for anyone else.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_half_of_a_concurrent_burst_does_not_wedge_the_rest() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "b joined as a voter").await?;

    write_through_leader(&[&a], set("k1", "v1")).await?;

    // Delay the round so every one of these calls is still in flight when half get cancelled.
    faults.set_class(
        b.node_id(),
        a.node_id(),
        TrafficClass::Rpc,
        Fault::Delay(Duration::from_millis(200)),
    );

    let mut handles = Vec::new();
    for _ in 0..10 {
        let node = b.clone();
        handles.push(tokio::spawn(async move { node.linearizable_view(READ_TIMEOUT).await }));
    }

    // Cancel half of them mid-flight.
    for handle in handles.iter().take(5) {
        handle.abort();
    }

    for handle in handles.into_iter().skip(5) {
        let view = handle.await.expect("task did not panic").map_err(|e| io::Error::other(e.to_string()))?;
        assert_eq!(Some("v1".to_string()), view.get("k1").map(|kv| text(&kv)));
    }

    // And the coalescer is not wedged by the cancelled waiters: a later call still completes.
    faults.heal();
    let view = b.linearizable_view(READ_TIMEOUT).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(Some("v1".to_string()), view.get("k1").map(|kv| text(&kv)));

    Ok(())
}
