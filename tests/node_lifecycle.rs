//! Starting a node: what `create`, `join` and `serve` must refuse, report, or finish - and what
//! a restart on the same redb file must recover.
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::EzStorage;

/// Joining reserves the internal endpoint in its constructor and returns the original bind error
/// before it asks the seed to change membership.
#[tokio::test(flavor = "multi_thread")]
async fn join_reports_an_occupied_listener_during_construction() -> io::Result<()> {
    let (seed, seed_node, _seed_data) = founding_node().await?;

    // Hold the address the constructor must reserve before it asks the seed for an id.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap().to_string();

    let data = NodeData::new();
    let err = match EzKv::join(&addr, &seed, data.open().await?, config()).await {
        Err(err) => err,
        Ok(_) => return Err(io::Error::other("join succeeded with an occupied internal listener")),
    };
    assert_eq!(
        io::ErrorKind::AddrInUse,
        err.kind(),
        "the original bind error is returned: {err}"
    );

    let membership = seed_node.metrics().await.committed_membership_config.membership().clone();
    assert_eq!(BTreeSet::from([0]), membership.voter_ids().collect::<BTreeSet<_>>());
    assert!(membership.learner_ids().next().is_none());
    assert_eq!(vec![0], membership.nodes().map(|(id, _)| *id).collect::<Vec<_>>());

    Ok(())
}

/// Both join APIs must refuse a conflict before touching voters, learners, or their addresses.
#[tokio::test(flavor = "multi_thread")]
async fn an_occupied_internal_listener_leaves_every_committed_member_and_address_unchanged() -> io::Result<()> {
    assert_occupied_join_preserves_membership(ezraft::NodeRole::Learner).await
}

#[tokio::test(flavor = "multi_thread")]
async fn an_occupied_internal_listener_refuses_voter_join_before_membership_changes() -> io::Result<()> {
    assert_occupied_join_preserves_membership(ezraft::NodeRole::Voter).await
}

async fn assert_occupied_join_preserves_membership(role: ezraft::NodeRole) -> io::Result<()> {
    let (seed, seed_node, _seed_data) = founding_node().await?;
    let (_, voter, _voter_data) = joined_voter(&seed).await?;
    let (_, learner, _learner_data) = joined_learner(&seed).await?;
    let voters = BTreeSet::from([0, voter.node_id()]);
    wait_for_voters(&seed_node, voters, "the healthy multi-voter cluster is committed").await?;

    let before = seed_node.metrics().await.committed_membership_config.membership().clone();
    let voters_before = before.voter_ids().collect::<BTreeSet<_>>();
    let learners_before = before.learner_ids().collect::<BTreeSet<_>>();
    let addresses_before =
        before.nodes().map(|(id, node)| (*id, node.internal_addr.clone())).collect::<BTreeMap<_, _>>();
    assert!(learners_before.contains(&learner.node_id()));

    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind_addr = occupied.local_addr().unwrap().to_string();
    let declared_addr = "joiner.internal.invalid:9600";
    let data = NodeData::new();
    let config = EzConfig {
        internal_bind_addr: Some(bind_addr),
        ..config()
    };
    let storage = data.open().await?;
    let result = match role {
        ezraft::NodeRole::Learner => EzKv::join_as_learner(declared_addr, &seed, storage, config).await,
        ezraft::NodeRole::Voter => EzKv::join(declared_addr, &seed, storage, config).await,
    };

    if let Ok(unexpected_node) = result {
        // Keep the baseline failure from leaving an extra learner in this live test cluster.
        seed_node.remove_node(unexpected_node.node_id()).await?;
        unexpected_node.shutdown().await?;
        return Err(io::Error::other(
            "join succeeded while its configured internal listener was occupied",
        ));
    }
    let err = match result {
        Err(err) => err,
        Ok(_) => unreachable!("a successful join was removed above"),
    };
    assert_eq!(
        io::ErrorKind::AddrInUse,
        err.kind(),
        "the original bind error is returned: {err}"
    );

    let after = seed_node.metrics().await.committed_membership_config.membership().clone();
    assert_eq!(voters_before, after.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(learners_before, after.learner_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        addresses_before,
        after.nodes().map(|(id, node)| (*id, node.internal_addr.clone())).collect()
    );

    Ok(())
}

/// A node listens at its local override while the declared DNS address remains in the public
/// handle and the initial membership entry.
#[tokio::test(flavor = "multi_thread")]
async fn create_binds_override_and_keeps_declared_address_for_membership() -> io::Result<()> {
    let local_addr = free_addr();
    let declared_addr = "node-0.internal.invalid:9602";
    let data = NodeData::new();
    let node = EzKv::create(declared_addr, data.open().await?, EzConfig {
        internal_bind_addr: Some(local_addr.clone()),
        ..config()
    })
    .await?;

    assert_eq!(declared_addr, node.addr());
    assert_eq!(
        io::ErrorKind::AddrInUse,
        std::net::TcpListener::bind(&local_addr).unwrap_err().kind(),
        "construction keeps the local listener reserved",
    );

    let serving = tokio::spawn(node.clone().serve());
    wait_until_serving(&local_addr).await?;
    node.wait_metrics(
        WAIT,
        |metrics| {
            metrics
                .committed_membership_config
                .membership()
                .get_node(&node.node_id())
                .is_some_and(|entry| entry.internal_addr == declared_addr)
        },
        "the configured DNS address is committed for peer communication",
    )
    .await?;
    node.shutdown().await?;
    serving.await.map_err(io::Error::other)??;

    let _rebound = std::net::TcpListener::bind(&local_addr)?;
    Ok(())
}

/// A service-factory failure before the first Actix worker is ready leaves the pre-bound port
/// available again after `serve_with` returns.
#[tokio::test(flavor = "multi_thread")]
async fn a_route_factory_failure_releases_the_prebound_listener() -> io::Result<()> {
    let local_addr = free_addr();
    let data = NodeData::new();
    let node = EzKv::create("node-0.internal.invalid:9603", data.open().await?, EzConfig {
        internal_bind_addr: Some(local_addr.clone()),
        ..config()
    })
    .await?;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        node.clone().serve_with(|_| panic!("intentional service-factory failure")),
    )
    .await
    .map_err(|_| io::Error::other("serve_with did not report the failed service factory"))?;
    assert!(result.is_err(), "the service-factory failure is returned to serve_with");

    node.shutdown().await?;
    let _rebound = std::net::TcpListener::bind(&local_addr)?;
    Ok(())
}

/// A failed second serve call has no ownership of the first server and leaves it serving.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_serve_call_does_not_stop_the_existing_server() -> io::Result<()> {
    let local_addr = free_addr();
    let data = NodeData::new();
    let node = EzKv::create("node-0.internal.invalid:9604", data.open().await?, EzConfig {
        internal_bind_addr: Some(local_addr.clone()),
        ..config()
    })
    .await?;

    let first_serve = tokio::spawn(node.clone().serve());
    wait_until_serving(&local_addr).await?;
    let err = node.clone().serve().await.expect_err("only one server owns the pre-bound listener");
    assert_eq!(io::ErrorKind::AlreadyExists, err.kind());
    wait_until_serving(&local_addr).await?;

    node.shutdown().await?;
    first_serve.await.map_err(io::Error::other)??;
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

    // Reopening the persisted node id does not issue a promotion. Applying new leader entries only
    // proves that the restarted learner is receiving the log.
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
/// stops the HTTP server it started, not just the Raft engine, which is what lets this
/// same-process restart bind the same address again - without that, the
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

/// A serve future may be dropped by a thread outside Tokio while its original runtime remains
/// alive. Shutdown must still wait for the server's graceful completion, not just enqueue stop.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_serve_outside_runtime_preserves_shutdown_completion() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();
    let node = EzKv::create(&addr, data.open().await?, config()).await?;
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let handler_release = release.clone();
    let mut serving = Box::pin(node.clone().serve_with(move |routes| {
        let release = handler_release.clone();
        let entered = entered_tx.clone();
        routes.route(
            "/held",
            actix_web::web::get().to(move || {
                let release = release.clone();
                let entered = entered.clone();
                async move {
                    let _ = entered.send(());
                    release.notified().await;
                    actix_web::HttpResponse::Ok().finish()
                }
            }),
        );
    }));
    std::future::poll_fn(|cx| {
        assert!(serving.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    wait_until_serving(&addr).await?;
    let request_addr = addr.clone();
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .get(format!("http://{request_addr}/held"))
            .bearer_auth(config().cluster_token)
            .send()
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered_rx.recv())
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("custom request did not enter"))?;
    std::thread::spawn(move || drop(serving)).join().expect("drop thread completes");
    let closing_node = node.clone();
    let mut closing = tokio::spawn(async move { closing_node.shutdown().await });
    let early = tokio::time::timeout(Duration::from_millis(300), &mut closing).await;
    release.notify_one();
    let response = request.await.map_err(io::Error::other)?.map_err(io::Error::other)?;
    assert!(response.status().is_success());
    assert!(
        early.is_err(),
        "shutdown returned before the held HTTP request finished"
    );
    tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .map_err(io::Error::other)?
        .map_err(io::Error::other)??;
    let _rebound = std::net::TcpListener::bind(&addr)?;
    Ok(())
}
