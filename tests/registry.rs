//! The node registry: startup registration, `update_node_meta`, `remove_node`'s cleanup, and
//! surviving a snapshot install or a restart (ech0-media#33)
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;

use bytes::Bytes;
use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;

/// A test config with a distinguishing `external_addr`/`node_meta`, everything else the harness's
/// usual short-heartbeat defaults.
fn config_for(external_addr: &str, meta: &str) -> EzConfig {
    EzConfig {
        external_addr: external_addr.to_string(),
        node_meta: Bytes::copy_from_slice(meta.as_bytes()),
        ..config()
    }
}

/// A new node's `external_addr` and `meta` appear in every node's registry once it has started -
/// registered by the node itself, in the background, without the caller doing anything for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_nodes_external_addr_and_meta_appear_on_every_node() -> io::Result<()> {
    let addr_a = free_addr();
    let data_a = NodeData::new();
    let a = EzKv::create(&addr_a, data_a.open().await?, config_for("client-a:9000", "meta-a")).await?;
    spawn_serve(&a);
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    let addr_b = free_addr();
    let data_b = NodeData::new();
    let b = EzKv::join(
        &addr_b,
        &addr_a,
        data_b.open().await?,
        config_for("client-b:9001", "meta-b"),
    )
    .await?;
    spawn_serve(&b);
    let b_id = b.node_id();

    wait_for_voters(&a, BTreeSet::from([0, b_id]), "the joined node is a voter").await?;

    for node in [&a, &b] {
        let registry = wait_for_registry(
            node,
            |r| r.get(0).is_some() && r.get(b_id).is_some(),
            "both nodes registered",
        )
        .await?;

        let a_info = registry.get(0).expect("just matched Some");
        assert_eq!("client-a:9000", a_info.external_addr);
        assert_eq!(Bytes::from_static(b"meta-a"), a_info.meta);

        let b_info = registry.get(b_id).expect("just matched Some");
        assert_eq!("client-b:9001", b_info.external_addr);
        assert_eq!(Bytes::from_static(b"meta-b"), b_info.meta);
    }

    Ok(())
}

/// `update_node_meta` changes only `meta`, keeps the already-registered `external_addr`, becomes
/// locally readable on every node, and is delivered through `watch_registry` - not merely
/// eventually true of a fresh `registry()` call.
#[tokio::test(flavor = "multi_thread")]
async fn update_node_meta_is_readable_and_delivered_through_the_watch_on_every_node() -> io::Result<()> {
    let addr_a = free_addr();
    let data_a = NodeData::new();
    let a = EzKv::create(&addr_a, data_a.open().await?, config_for("client-a:9000", "meta-a")).await?;
    spawn_serve(&a);
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let b_id = b.node_id();
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "the joined node is a voter").await?;
    // Both nodes' own startup self-registration must be done and settled first: otherwise one of
    // them could still land on the pre-subscribed watch below, and get mistaken for the meta
    // update this test is actually about.
    wait_for_registry(
        &a,
        |r| r.get(0).is_some() && r.get(b_id).is_some(),
        "both nodes registered themselves",
    )
    .await?;

    // Subscribed before the update, so the assertion below proves the change actually arrived
    // through this very stream - not just that a later fresh read happens to agree. Looping on
    // `changed()` rather than trusting the first one guards against any other unrelated change
    // this same channel could in principle also deliver.
    let mut watch_on_b = b.watch_registry();
    watch_on_b.borrow_and_update();

    a.update_node_meta("meta-a-v2").await.map_err(|e| io::Error::other(e.to_string()))?;

    let deadline = tokio::time::Instant::now() + WAIT.expect("bounded");
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::timeout(remaining, watch_on_b.changed())
            .await
            .map_err(|_elapsed| io::Error::other("timed out waiting for the meta update to arrive through the watch"))?
            .map_err(io::Error::other)?;
        if watch_on_b.borrow().get(0).map(|i| i.meta.clone()) == Some(Bytes::from_static(b"meta-a-v2")) {
            break;
        }
    }
    let delivered = watch_on_b.borrow().clone();
    let info = delivered.get(0).expect("node 0 is still registered");
    assert_eq!("client-a:9000", info.external_addr, "the address is unchanged");
    assert_eq!(Bytes::from_static(b"meta-a-v2"), info.meta);

    // And an ordinary local read agrees, on both nodes.
    for node in [&a, &b] {
        let registry = wait_for_registry(
            node,
            |r| r.get(0).map(|i| i.meta) == Some(Bytes::from_static(b"meta-a-v2")),
            "meta update lands",
        )
        .await?;
        assert_eq!("client-a:9000", registry.get(0).unwrap().external_addr);
    }

    Ok(())
}

/// `remove_node` removes the node's registry entry too, on every surviving node - not just its
/// membership entry.
#[tokio::test(flavor = "multi_thread")]
async fn remove_node_deletes_the_registry_entry_on_every_surviving_node() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;

    let removed_id = c.node_id();
    for node in [&a, &b, &c] {
        wait_for_registry(
            node,
            |r| r.get(removed_id).is_some(),
            "the node about to be removed is registered first",
        )
        .await?;
    }

    a.remove_node(removed_id).await?;

    for node in [&a, &b] {
        wait_for_registry(
            node,
            |r| r.get(removed_id).is_none(),
            "the removed node's entry disappears",
        )
        .await?;
    }

    Ok(())
}

/// The registry is identical on a node that caught up entirely through a snapshot install (its
/// own log never carried the entries that produced it - `tests/large_snapshot.rs`'s own
/// technique), and stays identical after that same node is stopped and restarted, re-registering
/// itself with new values on the way back up. Re-registering under the same node id is what
/// startup always does, whatever it last recovered from disk - so this also proves that doing so
/// is harmless rather than, say, producing a second entry.
#[tokio::test(flavor = "multi_thread")]
async fn registry_matches_across_a_snapshot_install_and_a_restart() -> io::Result<()> {
    let addr_a = free_addr();
    let data_a = NodeData::new();
    let a = EzKv::create(&addr_a, data_a.open().await?, config_for("client-a:9000", "meta-a")).await?;
    spawn_serve(&a);
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    // Added as a learner but not yet serving, exactly `tests/large_snapshot.rs`'s technique: once
    // the leader's log covering this point is purged, this node can only ever catch up - the KV
    // and the registry riding in the very same snapshot alike - by installing a whole snapshot.
    let addr_c = free_addr();
    let data_c = NodeData::new();
    let c = EzKv::join_as_learner(
        &addr_c,
        &addr_a,
        data_c.open().await?,
        config_for("client-c:9000", "meta-c"),
    )
    .await?;

    for i in 0..5 {
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

    // `c`'s self-registration cannot land yet: a node only learns who currently leads by
    // *receiving* Raft traffic, and nothing can reach `c` before its own server is serving.
    spawn_serve(&c);
    wait_for_applied(&c, &a).await?;
    wait_for_registry(
        &a,
        |r| r.get(c.node_id()).is_some(),
        "c registers itself once it can hear from the leader",
    )
    .await?;
    // `c`'s own self-registration command still has to replicate back to it after it committed -
    // `wait_for_applied` above only waited for the snapshot-fed catch-up, not this later entry.
    wait_for_registry(
        &c,
        |r| r.get(c.node_id()).is_some(),
        "c's own registry catches up to its own registration",
    )
    .await?;

    let leader_registry = a.registry();
    assert_eq!(
        leader_registry,
        c.registry(),
        "a node that caught up through a snapshot install must have exactly the leader's registry"
    );
    assert_eq!("client-c:9000", leader_registry.get(c.node_id()).unwrap().external_addr);

    // A real restart of `c`: same redb file, a different configured address this time, to prove
    // the restarted node both recovers what a snapshot/log gave it and re-registers on the way
    // back up rather than getting stuck on stale data.
    let node_id = c.node_id();
    c.shutdown().await?;
    drop(c);

    let restarted = EzKv::join(
        &addr_c,
        &addr_a,
        data_c.open().await?,
        config_for("client-c:9001", "meta-c-v2"),
    )
    .await?;
    assert_eq!(node_id, restarted.node_id(), "a restart keeps the persisted node id");
    spawn_serve(&restarted);
    wait_for_applied(&restarted, &a).await?;

    wait_for_registry(
        &a,
        |r| r.get(node_id).map(|info| info.external_addr) == Some("client-c:9001".to_string()),
        "the restarted node's fresh registration lands",
    )
    .await?;
    // As with the snapshot catch-up above: the restarted node's own command still has to
    // replicate back down to it before its own local registry reflects it.
    wait_for_registry(
        &restarted,
        |r| r.get(node_id).map(|info| info.external_addr) == Some("client-c:9001".to_string()),
        "the restarted node's own registry catches up to its own re-registration",
    )
    .await?;

    assert_eq!(
        a.registry(),
        restarted.registry(),
        "the restarted node's registry matches the leader's exactly"
    );
    assert_eq!(
        a.registry().len(),
        restarted.registry().len(),
        "restarting never leaves a duplicate entry behind"
    );

    Ok(())
}
