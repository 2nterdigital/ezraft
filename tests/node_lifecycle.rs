//! Starting a node: what `create`, `join` and `serve` must refuse, report, or finish - and what
//! a restart on the same redb file must recover.
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::EzStorage;

/// `serve` must report a server that cannot start, even for a joining voter whose promotion is
/// still pending. The promotion is waiting for this node to catch up, which it can only do
/// through the server that just failed, so collecting the promotion first would sit on the bind
/// error for the twenty attempts and ninety seconds each it still has to spend.
#[tokio::test(flavor = "multi_thread")]
async fn serve_reports_a_server_that_cannot_bind_instead_of_awaiting_promotion() -> io::Result<()> {
    let (seed, _seed_node, _seed_data) = founding_node().await?;

    // Hold the address the node is about to claim, so its server cannot bind it.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap().to_string();

    let data = NodeData::new();
    let node = EzKv::join(&addr, &seed, data.open().await?, config()).await?;

    // Comfortably below the promotion's own retry budget, and far above what returning a bind
    // error takes.
    let served = tokio::time::timeout(Duration::from_secs(5), node.serve())
        .await
        .map_err(|_| io::Error::other("serve is still waiting for a promotion its server cannot deliver"))?;

    let err = served.expect_err("serve reports the bind failure");
    assert_eq!(
        io::ErrorKind::AddrInUse,
        err.kind(),
        "the error is the bind failure itself: {}",
        err
    );

    Ok(())
}

/// Converting the configuration is the one step of a join that can fail on its arguments alone,
/// so it happens before the cluster is asked for an id or told about this node. A caller that
/// mistyped a duration must not leave a learner behind for someone to remove by hand.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_with_an_unusable_config_leaves_the_cluster_untouched() -> io::Result<()> {
    let (seed, seed_node, _seed_data) = founding_node().await?;

    let unusable = EzConfig {
        heartbeat_interval: Duration::ZERO,
        ..config()
    };
    let data = NodeData::new();
    let err = EzKv::join(free_addr(), &seed, data.open().await?, unusable)
        .await
        .map(|_| ())
        .expect_err("a config the node cannot run with is refused");

    let membership = seed_node.metrics().await.membership_config.membership().clone();
    assert_eq!(
        BTreeSet::from([0]),
        membership.voter_ids().collect::<BTreeSet<_>>(),
        "the voter set is untouched, after: {}",
        err
    );
    assert_eq!(
        BTreeSet::<u64>::new(),
        membership.learner_ids().collect::<BTreeSet<_>>(),
        "no learner was added, after: {}",
        err
    );

    Ok(())
}

/// Joining is two steps - enter the membership as a learner, then be promoted - and a process that
/// dies between them comes back a learner. Restarting repeats neither step: the id is persisted, so
/// the seed is not asked again, and a node is whatever the membership says it is. The promotion is
/// retried the way any learner is promoted, with `promote` on the leader.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_promotion_is_finished_by_promote_not_by_restarting() -> io::Result<()> {
    let (seed, seed_node, _seed_data) = founding_node().await?;

    // A learner in the membership with its id persisted is exactly the state a join whose
    // promotion never landed leaves behind.
    let addr = free_addr();
    let data = NodeData::new();
    let first = EzKv::join_as_learner(&addr, &seed, data.open().await?, config()).await?;
    let node_id = first.node_id();

    // A real restart: the redb file has exactly one opener, so the first instance's core task
    // has to actually stop and release it before the same path can be opened again.
    first.shutdown().await?;
    drop(first);

    // Restarting as a voter join, which is what the caller who wanted a voter would run again.
    let restarted = EzKv::join(&addr, &seed, data.open().await?, config()).await?;
    spawn_serve(&restarted);

    // Being caught up is the only thing a promotion waits for, so a restart that meant to promote
    // would have done it by now.
    seed_node.write(set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?;
    wait_for_applied(&restarted, &seed_node).await?;

    let membership = seed_node.metrics().await.committed_membership_config.membership().clone();
    assert_eq!(BTreeSet::from([0]), membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        BTreeSet::from([node_id]),
        membership.learner_ids().collect::<BTreeSet<_>>(),
        "a restart repeats neither step of the join"
    );

    // And the retry is a plain promotion.
    seed_node.promote(node_id).await?;

    let voters = BTreeSet::from([0, node_id]);
    for node in [&seed_node, &restarted] {
        wait_for_voters(node, voters.clone(), "the retried promotion made it a voter").await?;
    }

    Ok(())
}

/// A voter of a multi-node cluster, shut down and restarted on the same redb file, recovers its
/// vote, log, meta and snapshot from redb without contacting the seed again - and the cluster
/// keeps serving writes throughout, on the voters that never went down. It then serves on its old
/// address again, rejoins as a live participant, catches up, and takes part in commits again.
///
/// This is the storage acceptance scenario in full: `RedbStorage` is what the crash-and-restart
/// promise in `EzStorage`'s contract rests on, not an in-memory stand-in for it. `EzKv::shutdown`
/// now stops the HTTP server it started, not just the Raft engine (wave S1 left this gap), which
/// is what lets this same-process restart bind the same address again - without that, the
/// restarted instance could recover its local storage but never rejoin the network.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_voter_recovers_its_redb_state_and_the_cluster_keeps_serving() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    // Keeps `a` and this node a majority of the three voters throughout, independent of whether
    // the restarted node below ever talks to the network again.
    let (_, c, _data_c) = joined_voter(&addr_a).await?;

    let addr_b = free_addr();
    let data_b = NodeData::new();
    let b = EzKv::join(&addr_b, &addr_a, data_b.open().await?, config()).await?;
    spawn_serve(&b);

    let voters = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, voters, "every joined node is a voter").await?;

    write_through_leader(&[&a], set("k1", "v1")).await?;
    wait_for_applied(&b, &a).await?;

    // A snapshot too, so the restart has to recover a snapshot and the log tail after it, not
    // just a plain log.
    b.trigger_snapshot().await?;
    b.wait_metrics(WAIT, |m| m.snapshot.is_some(), "the follower snapshotted its own state").await?;
    write_through_leader(&[&a], set("k2", "v2")).await?;
    wait_for_applied(&b, &a).await?;

    let node_id = b.node_id();
    let vote_before_restart = b.metrics().await.vote;

    b.shutdown().await?;
    drop(b);

    // The vote must be exactly what was recorded before shutdown - loaded through `RedbStorage`'s
    // own public seam, before any `EzKv` instance exists to move it forward again by campaigning
    // on restart.
    let mut probe = data_b.open().await?;
    let loaded = probe.load().await?;
    assert_eq!(
        Some(vote_before_restart),
        loaded.meta.vote,
        "the vote recovered from redb matches what was persisted before shutdown"
    );
    drop(probe);

    // Same address, same redb file: a real restart of the same node, not a fresh one. It serves
    // again and rejoins the network - `shutdown` releasing the HTTP address is what makes that
    // possible in the same process.
    let restarted = EzKv::join(&addr_b, &addr_a, data_b.open().await?, config()).await?;
    assert_eq!(
        node_id,
        restarted.node_id(),
        "a restart keeps the persisted node id, and does not rejoin as a new one"
    );
    spawn_serve(&restarted);

    wait_for_applied(&restarted, &a).await?;
    assert_eq!(expected_map(1..3), view_map(&restarted.view()));

    // And it takes part in commits again: a write after the restart reaches it like any other
    // voter's.
    write_through_leader(&[&a, &c, &restarted], set("k3", "v3")).await?;
    wait_for_applied(&restarted, &a).await?;
    assert_eq!(Some("v3".to_string()), restarted.view().get("k3").map(|kv| text(&kv)));

    Ok(())
}
