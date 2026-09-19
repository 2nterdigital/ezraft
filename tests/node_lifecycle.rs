//! Starting a node: what `create`, `join` and `serve` must refuse, report, or finish - and what
//! a restart on the same redb file must recover.
//!
//! Every test drives EzRaft through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::EzRaft;
use ezraft::EzStorage;
use ezraft::RedbStorage;

/// `serve` must report a server that cannot start, even for a joining voter whose promotion is
/// still pending. The promotion is waiting for this node to catch up, which it can only do
/// through the server that just failed, so collecting the promotion first would sit on the bind
/// error for the twenty attempts and ninety seconds each it still has to spend.
#[tokio::test(flavor = "multi_thread")]
async fn serve_reports_a_server_that_cannot_bind_instead_of_awaiting_promotion() -> io::Result<()> {
    let (seed, _seed_node) = founding_node().await?;

    // Hold the address the node is about to claim, so its server cannot bind it.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap().to_string();

    let node = EzRaft::join(&addr, &seed, KvSm::default(), node_storage().await?, config()).await?;

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
    let (seed, seed_node) = founding_node().await?;

    let unusable = EzConfig {
        heartbeat_interval: Duration::ZERO,
        ..config()
    };
    let err = EzRaft::join(free_addr(), &seed, KvSm::default(), node_storage().await?, unusable)
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
    let (seed, seed_node) = founding_node().await?;

    // A learner in the membership with its id persisted is exactly the state a join whose
    // promotion never landed leaves behind.
    let addr = free_addr();
    let path = node_data_path();
    let first =
        EzRaft::join_as_learner(&addr, &seed, KvSm::default(), RedbStorage::open(&path).await?, config()).await?;
    let node_id = first.node_id();

    // A real restart: the redb file has exactly one opener, so the first instance's core task
    // has to actually stop and release it before the same path can be opened again.
    first.shutdown().await?;
    drop(first);

    // Restarting as a voter join, which is what the caller who wanted a voter would run again.
    let restarted = EzRaft::join(&addr, &seed, KvSm::default(), RedbStorage::open(&path).await?, config()).await?;
    spawn_serve(&restarted);

    // Being caught up is the only thing a promotion waits for, so a restart that meant to promote
    // would have done it by now.
    seed_node.write(set("k1", "v1")).await?;
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
/// keeps serving writes throughout, on the voters that never went down.
///
/// This is the storage acceptance scenario in full: `RedbStorage` is what the crash-and-restart
/// promise in `EzStorage`'s contract rests on, not an in-memory stand-in for it. A third,
/// never-restarted voter keeps the cluster's majority independent of the restarted node's own
/// networking, because `EzRaft::shutdown` documents that it leaves the HTTP server it started
/// running: this in-process harness has no way to free that port for a second `serve` call
/// without a real process exit, so the restarted node here is checked locally (identity, and the
/// state its own storage recovered) rather than served again on the same address. Its live
/// rejoin as a network participant is the real-process SIGKILL/restart seam's concern
/// (`examples/harness.rs`), not this one.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_voter_recovers_its_redb_state_and_the_cluster_keeps_serving() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    // Keeps `a` and this node a majority of the three voters throughout, independent of whether
    // the restarted node below ever talks to the network again.
    let (_, c) = joined_voter(&addr_a).await?;

    let addr_b = free_addr();
    let path_b = node_data_path();
    let b = EzRaft::join(
        &addr_b,
        &addr_a,
        KvSm::default(),
        RedbStorage::open(&path_b).await?,
        config(),
    )
    .await?;
    spawn_serve(&b);

    let voters = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, voters, "every joined node is a voter").await?;

    a.write(set("k1", "v1")).await?;
    wait_for_applied(&b, &a).await?;

    // A snapshot too, so the restart has to recover a snapshot and the log tail after it, not
    // just a plain log.
    b.trigger_snapshot().await?;
    b.wait_metrics(WAIT, |m| m.snapshot.is_some(), "the follower snapshotted its own state").await?;
    a.write(set("k2", "v2")).await?;
    wait_for_applied(&b, &a).await?;

    let node_id = b.node_id();
    let vote_before_restart = b.metrics().await.vote;

    b.shutdown().await?;
    drop(b);

    // The vote must be exactly what was recorded before shutdown - loaded through `RedbStorage`'s
    // own public seam, before any `EzRaft` instance exists to move it forward again by campaigning
    // on restart.
    let mut probe = RedbStorage::open(&path_b).await?;
    let loaded = EzStorage::<KvSm>::load(&mut probe).await?;
    assert_eq!(
        Some(vote_before_restart),
        loaded.meta.vote,
        "the vote recovered from redb matches what was persisted before shutdown"
    );
    let persisted_last_index = loaded.meta.last_log_id.map(|(_, index)| index);
    drop(probe);

    // Same address, same redb file: a real restart of the same node, not a fresh one. Recovering
    // the snapshot and replaying the log tail after it happens locally at construction - before
    // this node ever talks to the network - so its full state is already there below without any
    // help from the rest of the cluster.
    let restarted = EzRaft::join(
        &addr_b,
        &addr_a,
        KvSm::default(),
        RedbStorage::open(&path_b).await?,
        config(),
    )
    .await?;
    assert_eq!(
        node_id,
        restarted.node_id(),
        "a restart keeps the persisted node id, and does not rejoin as a new one"
    );
    // Replaying the log tail after the snapshot happens on the core task `join` starts, not
    // before it returns - wait for that local catch-up before reading. Pinned to the concrete
    // index the probe above just read from redb, not `m.last_log_index`: metrics start out
    // `last_applied: None` alongside `last_log_index: None`, which would satisfy an
    // equality-with-itself check on the very first, pre-replay report. `>=`, not `==`: this node
    // has no server to receive heartbeats through and the cluster runs without Pre-Vote, so its
    // own election timeout can fire and commit a blank leader-entry past `persisted_last_index`
    // before this wait ever observes the exact value.
    restarted
        .wait_metrics(
            WAIT,
            |m| m.last_applied.map(|log_id| log_id.index) >= persisted_last_index,
            "local log tail replayed",
        )
        .await?;
    assert_eq!(expected_map(1..3), restarted.read(|app| app.data.clone()).await?);
    restarted.shutdown().await?;
    drop(restarted);

    // This cluster runs without Pre-Vote, so the restarted node's own election timeout - never
    // having received a heartbeat, since it was never served over HTTP here - can fire and win a
    // term before the line above shuts it down. Once it is gone, wait for one of the two voters
    // that never went down to lead again, the same way other tests here wait out a leader change.
    let survivors = BTreeSet::from([0, c.node_id()]);
    a.wait_metrics(
        WAIT,
        |m| matches!(m.current_leader, Some(id) if survivors.contains(&id)),
        "a voter that never went down leads again",
    )
    .await?;

    // The cluster - a majority of it never having gone down - keeps serving writes throughout.
    assert_eq!(Response { value: None }, http_write(&addr_a, set("k3", "v3")).await?);
    assert_eq!(
        Response {
            value: Some("v3".into())
        },
        http_write(&addr_a, get("k3")).await?
    );

    Ok(())
}
