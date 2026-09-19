//! `EzConfig::snapshot_max_bytes` (ech0-media#26): exceeding the configured maximum is an
//! explicit error, on both the sending and the receiving side, and never silently corrupts a
//! transfer or wedges the cluster - a joiner stuck behind too small a maximum never advances, the
//! rest of the cluster keeps serving regardless, and raising the very same node's own maximum
//! lets it catch up.

mod common;

use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;

/// One 8 KiB value repeated enough times that the exported snapshot is unmistakably larger than
/// every small maximum this file configures, while staying small next to the crate's own
/// generous defaults.
const VALUE_LEN: usize = 8 * 1024;
const ENTRIES: usize = 20;

fn value(i: usize) -> String {
    format!("{i:08}-").chars().cycle().take(VALUE_LEN).collect()
}

fn expected_value_map() -> std::collections::BTreeMap<String, String> {
    (0..ENTRIES).map(|i| (format!("k{i}"), value(i))).collect()
}

/// Write a state, then force every later catch-up onto the snapshot route: no log entry survives
/// for a joiner to replay instead.
async fn write_and_purge_to_snapshot(a: &EzKv) -> io::Result<()> {
    for i in 0..ENTRIES {
        a.write(set(&format!("k{i}"), &value(i))).await.map_err(|e| io::Error::other(e.to_string()))?;
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
    Ok(())
}

/// Bounded wait for `node` to have applied anything at all - used only to prove it does *not*
/// happen within the window, never to wait for it to happen.
async fn never_applies_within(node: &EzKv, window: Duration) -> bool {
    node.wait_metrics(Some(window), |m| m.last_applied.is_some(), "must never apply anything")
        .await
        .is_err()
}

/// A leader configured with a maximum below the state's real size refuses to send the snapshot at
/// all - explicitly, and before ever touching the network - so a joiner that can only be caught
/// up by one never advances. The leader (this cluster's only voter) keeps serving throughout.
#[tokio::test(flavor = "multi_thread")]
async fn a_sender_maximum_below_the_snapshot_refuses_the_transfer() -> io::Result<()> {
    let addr_a = free_addr();
    let data_a = NodeData::new();
    let tiny_sender = EzConfig {
        snapshot_max_bytes: 1024,
        ..config()
    };
    let a = EzKv::create(&addr_a, data_a.open().await?, tiny_sender).await?;
    spawn_serve(&a);
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    write_and_purge_to_snapshot(&a).await?;

    let addr_b = free_addr();
    let data_b = NodeData::new();
    let b = EzKv::join_as_learner(&addr_b, &addr_a, data_b.open().await?, config()).await?;
    spawn_serve(&b);

    assert!(
        never_applies_within(&b, Duration::from_secs(2)).await,
        "a snapshot over the sender's own configured maximum must never install"
    );

    // The cluster - `a` alone is still its only voter - keeps serving despite the stuck joiner.
    let revision = a.write(set("meanwhile", "still-serving")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert!(revision > 0);
    assert_eq!(
        Some("still-serving".to_string()),
        a.view().get("meanwhile").map(|kv| text(&kv))
    );

    Ok(())
}

/// A follower configured with a maximum below the state's real size answers the oversized
/// snapshot body with an explicit failure (`413`, `crate::server`) and never advances; raising the
/// very same node's own maximum and restarting it on the same data catches it up, with nothing
/// about the leader or the snapshot itself having changed.
#[tokio::test(flavor = "multi_thread")]
async fn a_receiver_maximum_below_the_snapshot_fails_until_raised() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    write_and_purge_to_snapshot(&a).await?;

    let addr_b = free_addr();
    let data_b = NodeData::new();
    let tiny_receiver = EzConfig {
        snapshot_max_bytes: 1024,
        ..config()
    };
    let b = EzKv::join_as_learner(&addr_b, &addr_a, data_b.open().await?, tiny_receiver).await?;
    spawn_serve(&b);

    assert!(
        never_applies_within(&b, Duration::from_secs(2)).await,
        "a snapshot over the receiver's own configured maximum must never install"
    );

    // The cluster keeps serving despite the stuck joiner.
    a.write(set("meanwhile", "still-serving")).await.map_err(|e| io::Error::other(e.to_string()))?;

    // Raise the very same node's maximum and restart it on the same data: nothing about the
    // cluster or the snapshot changed, only the configured limit that was rejecting it.
    b.shutdown().await?;
    drop(b);
    let generous_receiver = EzConfig {
        snapshot_max_bytes: 64 * 1024 * 1024,
        ..config()
    };
    let b = EzKv::join_as_learner(&addr_b, &addr_a, data_b.open().await?, generous_receiver).await?;
    spawn_serve(&b);

    wait_for_applied(&b, &a).await?;
    let mut expected = expected_value_map();
    expected.insert("meanwhile".to_string(), "still-serving".to_string());
    assert_eq!(expected, view_map(&b.view()));

    Ok(())
}
