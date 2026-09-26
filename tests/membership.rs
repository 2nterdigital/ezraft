//! Who is in the cluster and who counts towards a quorum: joining, promotion,
//! demotion and removal
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;

use common::*;
use ezraft::EzKv;

/// Joining plus serving must be all it takes to become a voter, and those
/// voters must keep the cluster alive when the founding leader dies.
#[tokio::test(flavor = "multi_thread")]
async fn join_promotes_to_voter_and_cluster_survives_leader_death() -> io::Result<()> {
    let addr_a = free_addr();
    let data_a = NodeData::new();
    let a = EzKv::create(&addr_a, data_a.open().await?, config()).await?;
    spawn_serve(&a);
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    let addr_b = free_addr();
    let data_b = NodeData::new();
    let b = EzKv::join(&addr_b, &addr_a, data_b.open().await?, config()).await?;
    spawn_serve(&b);

    let addr_c = free_addr();
    let data_c = NodeData::new();
    let c = EzKv::join(&addr_c, &addr_a, data_c.open().await?, config()).await?;
    spawn_serve(&c);

    // Every node must see the final uniform config with all three voters before
    // the leader may die: a joint config still counts the founding node in its
    // old majority, so killing it mid-change would legitimately lose quorum.
    let voters = BTreeSet::from([0, b.node_id(), c.node_id()]);
    for node in [&a, &b, &c] {
        wait_for_voters(node, voters.clone(), "every promoted node is a voter").await?;
    }

    assert_eq!(
        1,
        a.write(set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?
    );

    // A direct read serves from this node's own local view - no consensus round, no log entry.
    assert_eq!(Some("v1".to_string()), a.view().get("k1").map(|kv| text(&kv)));

    // Kill the leader; the two remaining voters still form a quorum.
    assert!(a.is_leader());
    a.shutdown().await?;

    b.wait_metrics(
        WAIT,
        |m| matches!(m.current_leader, Some(id) if id != 0),
        "a surviving node takes over",
    )
    .await?;

    // The new leader accepts writes and still has the data acknowledged before the failover.
    // `write_through_leader` may have committed this on `c` rather than `b`, so wait for `b`'s
    // own local view to reach that revision before reading it - a local `view()` never forwards.
    let revision = write_through_leader(&[&b, &c], set("k2", "v2")).await?;
    wait_for_revision(&b, revision).await?;
    assert_eq!(Some("v1".to_string()), b.view().get("k1").map(|kv| text(&kv)));
    assert_eq!(Some("v2".to_string()), b.view().get("k2").map(|kv| text(&kv)));

    Ok(())
}

/// A node that joins as a learner must replicate the log and stay a learner
/// however long it runs, until it is explicitly promoted.
#[tokio::test(flavor = "multi_thread")]
async fn learner_joins_and_stays_a_learner_until_promoted() -> io::Result<()> {
    let addr_a = free_addr();
    let data_a = NodeData::new();
    let a = EzKv::create(&addr_a, data_a.open().await?, config()).await?;
    spawn_serve(&a);
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    let addr_b = free_addr();
    let data_b = NodeData::new();
    let b = EzKv::join_as_learner(&addr_b, &addr_a, data_b.open().await?, config()).await?;
    spawn_serve(&b);
    let b_id = b.node_id();

    for i in 1..3 {
        a.write(set(&format!("k{}", i), &format!("v{}", i)))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    // This learner must receive the written state and still remain a learner: replication alone
    // never changes its role. The explicit promote below is the only request this test makes to
    // change that role. Anchor the wait to the leader's index because a node that has replicated
    // nothing yet has its own `last_applied` and `last_log_index` both unset.
    let target = a.metrics().await.last_log_index.expect("the leader has written entries");
    b.wait_metrics(
        WAIT,
        |m| m.last_applied.map(|log_id| log_id.index) >= Some(target),
        "learner applied the leader's log",
    )
    .await?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(BTreeSet::from([0]), membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        BTreeSet::from([b_id]),
        membership.learner_ids().collect::<BTreeSet<_>>()
    );

    // A learner is a full replica; it just does not vote.
    assert_eq!(expected_map(1..3), view_map(&b.view()));

    // A node the cluster has never heard of cannot be promoted.
    let unknown = a.promote(b_id + 1000).await.unwrap_err();
    assert!(
        unknown.to_string().contains(&format!("Learner {} not found", b_id + 1000)),
        "unexpected error: {}",
        unknown
    );

    a.promote(b_id).await?;

    let voters = BTreeSet::from([0, b_id]);
    for node in [&a, &b] {
        wait_for_voters(node, voters.clone(), "the promoted learner is a voter everywhere").await?;
    }

    // Promoting again is a no-op rather than an error.
    a.promote(b_id).await?;

    Ok(())
}

/// Demoting a voter must take it out of the quorum without taking it out of
/// the cluster: it keeps receiving the log, and the voters left still commit.
#[tokio::test(flavor = "multi_thread")]
async fn demoted_voter_becomes_a_learner_and_keeps_replicating() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;

    a.demote(c.node_id()).await?;

    let voters = BTreeSet::from([0, b.node_id()]);
    wait_for_voters(&a, voters.clone(), "the demoted node left the voter set").await?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(voters, membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        BTreeSet::from([c.node_id()]),
        membership.learner_ids().collect::<BTreeSet<_>>()
    );

    // This write proves both halves: the two remaining voters are a quorum on
    // their own, and the demoted node is still sent what they commit.
    assert_eq!(
        1,
        a.write(set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?
    );
    wait_for_applied(&c, &a).await?;
    assert_eq!(expected_map(1..2), view_map(&c.view()));

    Ok(())
}

/// Demoting the last voter must be refused: a cluster with no voter can never
/// commit again, not even the change that would give it one back.
#[tokio::test(flavor = "multi_thread")]
async fn demote_refuses_to_empty_the_voter_set() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    let err = a.demote(0).await.unwrap_err();
    assert!(
        err.to_string().contains("new membership cannot be empty"),
        "unexpected error: {}",
        err
    );

    // The refusal must have changed nothing: still one voter, still committing.
    let metrics = a.metrics().await;
    assert_eq!(
        BTreeSet::from([0]),
        metrics.membership_config.membership().voter_ids().collect::<BTreeSet<_>>()
    );
    assert_eq!(
        1,
        a.write(set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?
    );

    Ok(())
}

/// Removing must work on a learner and on a voter alike, and leave nothing of
/// either behind in the membership.
#[tokio::test(flavor = "multi_thread")]
async fn removed_nodes_leave_the_membership() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_learner(&addr_a).await?;

    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;

    // A learner has no quorum to leave, only a node entry to drop; a voter has
    // both, and one change has to do them together.
    a.remove_node(c.node_id()).await?;
    a.remove_node(b.node_id()).await?;

    wait_for_voters(&a, BTreeSet::from([0]), "the removed voter left the voter set").await?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(vec![0], membership.nodes().map(|(id, _)| *id).collect::<Vec<_>>());
    assert!(membership.learner_ids().next().is_none());

    assert_eq!(
        1,
        a.write(set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?
    );

    Ok(())
}
