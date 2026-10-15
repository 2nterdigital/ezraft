//! Local leader eligibility leaves voter membership and replication unchanged.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::EzStorage;
use ezraft::OpenRaftTypes;
use ezraft::ServerState;
use ezraft::fault::Fault;
use ezraft::fault::Faults;
use openraft::raft::TransferLeaderRequest;

fn ineligible(mut config: EzConfig) -> EzConfig {
    config.leader_eligible = false;
    config
}

/// A failed fresh create must leave no identity, membership log, or snapshot behind.
#[tokio::test(flavor = "multi_thread")]
async fn fresh_ineligible_bootstrap_leaves_storage_unassigned() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();
    let err = match EzKv::create(&addr, data.open().await?, ineligible(config())).await {
        Ok(_) => return Err(io::Error::other("an ineligible fresh node created a cluster")),
        Err(err) => err,
    };
    assert_eq!(io::ErrorKind::InvalidInput, err.kind());
    assert!(err.to_string().contains("leader-ineligible"));

    let mut probe = data.open().await?;
    let loaded = probe.load().await?;
    assert_eq!(ezraft::EzMeta::default(), loaded.meta);
    assert!(loaded.snapshot.is_none());
    // Redb reports a missing expected index as NotFound rather than returning an empty range.
    assert_eq!(io::ErrorKind::NotFound, probe.read_logs(0, 1).await.unwrap_err().kind());
    drop(probe);

    // The same address and storage can subsequently bootstrap with the default policy.
    let node = EzKv::create(&addr, data.open().await?, config()).await?;
    node.wait_metrics(WAIT, |m| m.current_leader == Some(0), "eligible retry leads").await?;
    node.shutdown().await?;
    Ok(())
}

/// A crash after saving id 0 but before initialize must not bypass the bootstrap policy.
#[tokio::test(flavor = "multi_thread")]
async fn ineligible_id_only_restart_cannot_initialize_but_eligible_restart_recovers() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();
    let meta = ezraft::EzMeta {
        node_id: Some(0),
        ..ezraft::EzMeta::default()
    };
    let mut storage = data.open().await?;
    storage.persist(ezraft::Persist::Meta(meta.clone())).await?;
    drop(storage);

    let err = match EzKv::create(&addr, data.open().await?, ineligible(config())).await {
        Ok(node) => {
            node.shutdown().await?;
            return Err(io::Error::other("an ineligible ID-only restart initialized a cluster"));
        }
        Err(err) => err,
    };
    assert_eq!(io::ErrorKind::InvalidInput, err.kind());
    assert!(err.to_string().contains("leader-ineligible"));
    let mut probe = data.open().await?;
    let loaded = probe.load().await?;
    assert_eq!(
        meta, loaded.meta,
        "rejected restart must not campaign or persist log metadata"
    );
    assert!(loaded.snapshot.is_none());
    assert_eq!(io::ErrorKind::NotFound, probe.read_logs(0, 1).await.unwrap_err().kind());
    drop(probe);

    // Eligible startup may finish the initialization interrupted by the original crash.
    let node = EzKv::create(&addr, data.open().await?, config()).await?;
    node.wait_metrics(
        WAIT,
        |m| m.current_leader == Some(0) && m.committed_membership_config.log_id().is_some(),
        "eligible ID-only restart finishes bootstrap",
    )
    .await?;
    assert_eq!(0, node.node_id());
    node.shutdown().await?;
    Ok(())
}

/// HK2 needs SZ's vote after HK1 stops; SZ remains a voter and applies new writes.
/// Once both eligible nodes stop, SZ must send no vote requests over several timeouts.
#[tokio::test(flavor = "multi_thread")]
async fn eligible_failover_uses_ineligible_vote_and_replication_without_campaigning() -> io::Result<()> {
    let faults = Faults::new();
    let (_, hk1, _data_hk1) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, hk2, _data_hk2) = joined_voter_with(hk1.addr(), config_with_faults(&faults)).await?;
    let (_, sz, _data_sz) = joined_voter_with(hk1.addr(), ineligible(config_with_faults(&faults))).await?;
    let voters = BTreeSet::from([hk1.node_id(), hk2.node_id(), sz.node_id()]);
    for node in [&hk1, &hk2, &sz] {
        wait_for_voters(node, voters.clone(), "all three remain voters").await?;
    }
    wait_for_applied(&hk2, &hk1).await?;
    wait_for_applied(&sz, &hk1).await?;
    let old_term = hk1.metrics().await.current_term;
    hk1.shutdown().await?;

    hk2.wait_metrics(
        WAIT,
        |m| m.current_leader == Some(hk2.node_id()) && m.current_term > old_term,
        "HK2 wins a timeout election with SZ's vote",
    )
    .await?;
    sz.wait_metrics(WAIT, |m| m.current_leader == Some(hk2.node_id()), "SZ recognizes HK2").await?;
    assert_eq!(hk2.metrics().await.vote, sz.metrics().await.vote);
    assert!(faults.request_count(hk2.node_id(), sz.node_id(), "raft/vote") > 0);
    let revision = hk2.write(set("after-failover", "replicated")).await.map_err(io::Error::other)?;
    let view = wait_for_revision(&sz, revision).await?;
    assert_eq!(
        Some("replicated".to_string()),
        view_map(&view).get("after-failover").cloned()
    );
    wait_for_voters(&sz, voters, "SZ is still a full voter after failover").await?;

    hk2.shutdown().await?;
    let term = sz.metrics().await.current_term;
    // 100 ms heartbeats mean a 600 ms maximum election timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        let metrics = sz.metrics().await;
        assert_eq!(term, metrics.current_term);
        assert!(!matches!(metrics.state, ServerState::Leader | ServerState::Candidate));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for eligible in [hk1.node_id(), hk2.node_id()] {
        assert_eq!(0, faults.request_count(sz.node_id(), eligible, "raft/vote"));
    }
    sz.shutdown().await?;
    Ok(())
}

/// A self-targeted transfer is denied at HTTP ingress; the same voter still processes
/// transfer broadcasts for HK2 and can supply its election quorum.
#[tokio::test(flavor = "multi_thread")]
async fn ineligible_transfer_target_is_denied_but_eligible_transfer_still_works() -> io::Result<()> {
    let faults = Faults::new();
    let (_, hk1, _data_hk1) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, hk2, _data_hk2) = joined_voter_with(hk1.addr(), config_with_faults(&faults)).await?;
    let (_, sz, _data_sz) = joined_voter_with(hk1.addr(), ineligible(config_with_faults(&faults))).await?;
    let voters = BTreeSet::from([hk1.node_id(), hk2.node_id(), sz.node_id()]);
    for node in [&hk1, &hk2, &sz] {
        wait_for_voters(node, voters.clone(), "all three are voters").await?;
        wait_for_applied(node, &hk1).await?;
    }
    let leader = hk1.metrics().await;
    let request = TransferLeaderRequest::<OpenRaftTypes>::new(leader.vote, sz.node_id(), None);
    let response = raw_post(
        sz.addr(),
        "raft/transfer_leader",
        Some(CLUSTER_TOKEN),
        &serde_json::to_value(request)?,
    )
    .await?;
    assert_eq!(reqwest::StatusCode::FORBIDDEN, response.status());
    assert!(!sz.is_leader());
    assert_eq!(leader.current_term, sz.metrics().await.current_term);

    let request = TransferLeaderRequest::<OpenRaftTypes>::new(leader.vote, hk2.node_id(), None);
    let response = raw_post(
        sz.addr(),
        "raft/transfer_leader",
        Some(CLUSTER_TOKEN),
        &serde_json::to_value(request)?,
    )
    .await?;
    assert!(response.status().is_success());
    assert_eq!(
        serde_json::json!({"Ok": {"Ok": null}}),
        response.json::<serde_json::Value>().await.map_err(io::Error::other)?
    );

    // HK2 cannot obtain HK1's vote: the ineligible voter must provide the majority.
    faults.set(hk2.node_id(), hk1.node_id(), Fault::Drop);
    hk1.transfer_leader(hk2.node_id()).await?;
    hk2.wait_metrics(
        WAIT,
        |m| m.current_leader == Some(hk2.node_id()),
        "eligible transfer target takes leadership with SZ's vote",
    )
    .await?;
    let revision = hk2.write(set("after-transfer", "replicated")).await.map_err(io::Error::other)?;
    wait_for_revision(&sz, revision).await?;
    assert!(faults.request_count(hk1.node_id(), sz.node_id(), "raft/transfer_leader") > 0);
    for node in [&hk1, &hk2, &sz] {
        node.shutdown().await?;
    }
    Ok(())
}

/// A committed self-vote normally restores leadership at startup, before any election.
/// Changing the local policy must suppress that path even for persisted node 0 without a seed.
#[tokio::test(flavor = "multi_thread")]
async fn former_leader_restarts_ineligible_without_restoring_leadership() -> io::Result<()> {
    let (addr, leader, data) = founding_node().await?;
    let revision = leader.write(set("persisted", "value")).await.map_err(io::Error::other)?;
    leader.trigger_snapshot().await?;
    leader.wait_metrics(WAIT, |m| m.snapshot.is_some(), "persisted state has a snapshot").await?;
    let vote = leader.metrics().await.vote;
    leader.shutdown().await?;
    drop(leader);

    let restarted = EzKv::create(&addr, data.open().await?, ineligible(config())).await?;
    assert_eq!(0, restarted.node_id());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        let metrics = restarted.metrics().await;
        assert_eq!(vote, metrics.vote);
        assert!(!matches!(metrics.state, ServerState::Leader | ServerState::Candidate));
        assert_ne!(Some(0), metrics.current_leader);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    wait_for_revision(&restarted, revision).await?;
    assert_eq!(
        Some("value".to_string()),
        view_map(&restarted.view()).get("persisted").cloned()
    );
    restarted.shutdown().await?;
    Ok(())
}
