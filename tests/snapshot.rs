//! Snapshots and the log they replace: persistence, restart, replay, catching a
//! lagging node up, purging what a snapshot covers, non-blocking builds, and configuration
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes; a few also
//! shut a node down and read its redb file directly through `RedbStorage`'s own public
//! `load`/`persist` to confirm what actually reached disk, or to patch a stored snapshot's bytes
//! at the codec's documented header offset - never through a private field. None of them decode a
//! snapshot's *body*: the postcard codec's own round-trip, error and version tests live at its
//! unit-test seam (`src/kv/snapshot_codec.rs`), so these assert only what a restart or a lagging
//! joiner observably recovers, and what an unreadable stored snapshot observably does to startup.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::EzSnapshot;
use ezraft::EzStorage;
use ezraft::KvLimits;
use ezraft::Persist;
use ezraft::ReadPolicy;
use ezraft::Txn;

/// A snapshot must land on disk when built, and a restarted node must rebuild
/// the full state from that snapshot plus the log entries after it - values and revision alike.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_survives_restart() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();

    let a = EzKv::create(&addr, data.open().await?, config()).await?;
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "single node leads").await?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i)))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    a.trigger_snapshot().await?;
    a.wait_metrics(WAIT, |m| m.snapshot.is_some(), "snapshot built").await?;

    // More writes after the snapshot: the restart must replay these on top.
    let mut revision_before_restart = 0;
    for i in 10..15 {
        revision_before_restart = a
            .write(set(&format!("k{}", i), &format!("v{}", i)))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    a.shutdown().await?;
    drop(a);

    // A snapshot must actually be on storage - loaded through `RedbStorage`'s own public seam,
    // the same one a restart uses - never a private field.
    {
        let mut probe = data.open().await?;
        let loaded = probe.load().await?;
        let snapshot = loaded.snapshot.expect("snapshot persisted to storage");
        assert!(snapshot.meta.last_log_id.is_some());
    }

    // Restart on the same file with an empty state machine.
    let restarted = EzKv::create(&addr, data.open().await?, config()).await?;
    restarted.wait_metrics(WAIT, |m| m.current_leader == Some(0), "restarted node leads").await?;
    restarted
        .wait_metrics(
            WAIT,
            |m| m.last_applied.map(|log_id| log_id.index) == m.last_log_index,
            "log tail replayed",
        )
        .await?;

    // After the linearizable barrier, a local read must serve every write acknowledged before
    // the shutdown, and the KV's own revision counter must have survived the round trip too.
    restarted.linearizable(ReadPolicy::ReadIndex).await?;
    assert_eq!(revision_before_restart, restarted.view().revision());
    assert_eq!(expected_map(0..15), view_map(&restarted.view()));

    // And the restarted node still accepts writes over the rebuilt state, continuing its
    // revision counter rather than resetting it.
    let next_revision = restarted.write(set("k14", "v14b")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(revision_before_restart + 1, next_revision);
    assert_eq!(
        Some("v14b".to_string()),
        restarted.view().get("k14").map(|kv| text(&kv))
    );

    Ok(())
}

/// A restart must reproduce identical `create_rev`/`mod_rev` bookkeeping for a key the snapshot
/// covers (including one overwritten before the snapshot, so `create_rev` and `mod_rev` differ)
/// and for a key that only exists in the log tail the snapshot never saw.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_reproduces_identical_revisions_for_snapshotted_and_replayed_keys() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();

    let a = EzKv::create(&addr, data.open().await?, config()).await?;
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "single node leads").await?;

    a.write(set("a", "1")).await.map_err(|e| io::Error::other(e.to_string()))?;
    a.write(set("a", "2")).await.map_err(|e| io::Error::other(e.to_string()))?;

    a.trigger_snapshot().await?;
    a.wait_metrics(WAIT, |m| m.snapshot.is_some(), "snapshot built").await?;

    a.write(set("b", "3")).await.map_err(|e| io::Error::other(e.to_string()))?;

    let before = a.view();
    let a_before = before.get("a").expect("key a exists before restart");
    let b_before = before.get("b").expect("key b exists before restart");

    a.shutdown().await?;
    drop(a);

    let restarted = EzKv::create(&addr, data.open().await?, config()).await?;
    restarted.wait_metrics(WAIT, |m| m.current_leader == Some(0), "restarted node leads").await?;
    restarted
        .wait_metrics(
            WAIT,
            |m| m.last_applied.map(|log_id| log_id.index) == m.last_log_index,
            "log tail replayed",
        )
        .await?;

    let after = restarted.view();
    assert_eq!(
        before.revision(),
        after.revision(),
        "the KV's own revision counter must survive the round trip"
    );
    assert_eq!(
        Some(a_before),
        after.get("a"),
        "a key the snapshot covers keeps its create_rev and mod_rev"
    );
    assert_eq!(
        Some(b_before),
        after.get("b"),
        "a key only in the replayed log tail keeps its create_rev and mod_rev too"
    );

    Ok(())
}

/// A node that joins after the leader purged its log can only be brought up by
/// a full snapshot over the network; its promotion waits for that transfer and
/// must still end in a voter with the complete state.
#[tokio::test(flavor = "multi_thread")]
async fn lagging_joiner_catches_up_from_snapshot() -> io::Result<()> {
    let addr_a = free_addr();
    let data_a = NodeData::new();
    let a = EzKv::create(&addr_a, data_a.open().await?, config()).await?;
    spawn_serve(&a);
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i)))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    // Snapshot, then purge every covered entry: whoever joins now cannot be
    // caught up by log replay.
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

    let addr_b = free_addr();
    let data_b = NodeData::new();
    let b = EzKv::join(&addr_b, &addr_a, data_b.open().await?, config()).await?;
    spawn_serve(&b);

    let voters = BTreeSet::from([0, b.node_id()]);
    wait_for_voters(&b, voters, "snapshot-fed joiner promoted to voter").await?;

    // The whole pre-purge state must have arrived through the snapshot.
    assert_eq!(expected_map(0..10), view_map(&b.view()));

    // And the pair keeps working past the transfer.
    write_through_leader(&[&a, &b], set("k10", "v10")).await?;
    wait_for_applied(&b, &a).await?;
    assert_eq!(expected_map(0..11), view_map(&b.view()));

    Ok(())
}

/// Building a snapshot large enough that encoding is not instantaneous must never block `apply`
/// or `view()`: writes proposed right after `trigger_snapshot()` keep being acknowledged, and
/// `view()` keeps answering, for as long as the build is still in flight.
///
/// The assertion is made robust rather than timing-lucky by *comparing measured throughput*, not
/// by racing against a fixed count or a fixed sleep - the machine running this suite may be doing
/// anything from sitting idle to compiling several other test binaries in parallel, so no absolute
/// number of writes-per-millisecond is safe to assume ahead of time. Instead: a background loop
/// counts how many writes it can get acknowledged for as long as the build is in flight (`during`,
/// over `elapsed`, whatever that turns out to be on this run), then the *same* loop runs again for
/// exactly `elapsed` more with no build in flight (`after`) - a self-calibrated baseline of this
/// machine's current write throughput, measured in the same test run instead of assumed. If
/// `build_snapshot` held the state machine's lock across encoding (the bug the non-blocking build
/// requirement exists to prevent), every write proposed after the first would queue behind that
/// same lock and none of them could complete until the build itself finished: `during` would come
/// back near zero regardless of how fast or slow this machine otherwise is, while `after` - the
/// same loop, same machine, moments later - would not.
///
/// The state is reached through *one* transaction with many small `Put`s (raised
/// `kv_limits.max_txn_ops`/`max_txn_bytes` admit it) rather than many small round trips or a few
/// huge values: collecting 600,000 keys into a `Vec<KeyValue>` and postcard-encoding them is
/// genuinely slow (tens to hundreds of milliseconds of allocation-bound CPU work - a far more
/// robust source of a measurable encode than trying to make a `memcpy`-bound large-value payload
/// slow) while setup itself stays a single write.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_build_does_not_block_writes_or_views() -> io::Result<()> {
    const ENTRIES: usize = 600_000;

    let big_state_config = EzConfig {
        kv_limits: KvLimits {
            max_txn_ops: ENTRIES + 10,
            max_txn_bytes: 32 * 1024 * 1024,
            ..KvLimits::default()
        },
        json_body_limit: 128 * 1024 * 1024,
        ..config()
    };
    let (_addr, a, _data) = founding_node_with(big_state_config).await?;

    let mut bulk = Txn::new();
    for i in 0..ENTRIES {
        bulk = bulk.put(format!("bulk{i:010}"), "0123456789");
    }
    a.write(bulk).await.map_err(|e| io::Error::other(e.to_string()))?;
    let revision_before_snapshot = a.view().revision();

    a.trigger_snapshot().await?;

    let build_start = Instant::now();
    let (during_count, wait_result) = writes_while(
        &a,
        "during",
        a.wait_metrics(WAIT, |m| m.snapshot.is_some(), "snapshot built"),
    )
    .await;
    wait_result?;
    let build_elapsed = build_start.elapsed();

    // The self-calibrated baseline: the very same kind of loop, run for exactly as long as the
    // build just took, but now that it has finished.
    let (after_count, ()) = writes_while(&a, "after", tokio::time::sleep(build_elapsed)).await;

    assert!(
        during_count > 0,
        "no write was acknowledged in {build_elapsed:?} while the snapshot was being built - \
         build_snapshot must be holding the state machine's lock across encoding"
    );
    assert!(
        during_count.saturating_mul(4) >= after_count,
        "only {during_count} write(s) landed during the {build_elapsed:?} build, versus \
         {after_count} in the same span immediately afterwards with no build in flight - \
         build_snapshot must be holding the state machine's lock across encoding instead of \
         releasing it before that CPU-bound work runs"
    );
    assert!(
        a.view().revision() > revision_before_snapshot,
        "view() must reflect the writes that landed during the build, proving it kept answering \
         and kept advancing rather than being stuck on a stale value"
    );

    Ok(())
}

/// Run a background loop that writes distinct small keys (named `{prefix}{i}`) and reads
/// `view()` as fast as it can until `until` resolves, then return how many writes it got
/// acknowledged together with `until`'s own result.
///
/// `view()` never awaits and never touches the state machine's lock, so calling it on every
/// iteration proves it keeps answering for the same window the write count proves it for writes.
async fn writes_while<F, T>(node: &EzKv, prefix: &str, until: F) -> (usize, T)
where F: std::future::Future<Output = T> {
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicUsize::new(0));
    let writer = {
        let node = node.clone();
        let stop = stop.clone();
        let acked = acked.clone();
        let prefix = prefix.to_string();
        tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = node.view();
                if node.write(set(&format!("{prefix}{i}"), "v")).await.is_ok() {
                    acked.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        })
    };

    let result = until.await;
    stop.store(true, Ordering::Relaxed);
    writer.await.expect("the writer loop task must not panic");

    (acked.load(Ordering::Relaxed), result)
}

/// A demo-sized write load must drive the whole persist lifecycle on its own: with
/// `snapshot_interval`, `snapshot_log_to_keep` and `replication_lag_threshold` configured
/// together, a snapshot reaches storage and the log entries it covers are deleted, all without a
/// manual trigger - proving the three parameters take effect rather than being silently ignored
/// or re-derived from the interval alone.
#[tokio::test(flavor = "multi_thread")]
async fn automatic_snapshot_purges_old_logs() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();

    let node_config = EzConfig {
        snapshot_interval: 5,
        snapshot_log_to_keep: Some(1),
        replication_lag_threshold: Some(5),
        ..config()
    };
    let a = EzKv::create(&addr, data.open().await?, node_config).await?;
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "single node leads").await?;

    for i in 0..12 {
        a.write(set(&format!("k{}", i), &format!("v{}", i)))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    let metrics = a
        .wait_metrics(
            WAIT,
            |m| m.snapshot.is_some() && m.purged.is_some(),
            "snapshot built and log purged by the interval policy alone",
        )
        .await?;

    a.shutdown().await?;
    drop(a);

    // DeleteLogs must have reached storage: the snapshot itself is there, the purged index
    // itself is gone, and the entry right above it survives. Checked through `RedbStorage`'s own
    // public seam - the same one a restart uses - never a private field.
    let mut probe = data.open().await?;
    let loaded = probe.load().await?;
    let snapshot = loaded.snapshot.expect("snapshot persisted to storage");
    assert!(snapshot.meta.last_log_id.is_some());

    let purged_index = metrics.purged.unwrap().index;
    assert!(
        probe.read_logs(purged_index, purged_index + 1).await.is_err(),
        "the purged index itself must be gone"
    );
    let survivor = probe.read_logs(purged_index + 1, purged_index + 2).await?;
    assert_eq!(1, survivor.len(), "the entry right above the purge point survives");

    Ok(())
}

/// A node caught up by a snapshot that has no log entries after it.
///
/// This is the one path where the purge openraft runs after installing a
/// snapshot moves `last_purged` past a `last_log_id` that no later append comes
/// along to repair. Everywhere else - a joining node, a follower still being
/// replicated to - entries follow the install and hide it. `save_meta` keeps the
/// two ordered; without that the node reads back an inverted pair and openraft
/// refuses to start it, calling the log store corrupted.
///
/// The learner joins and only then starts serving, which is what leaves it with
/// nothing but the snapshot to catch up from: `add_learner` does not wait for
/// the node it adds, so the leader can compact its whole log first.
#[tokio::test(flavor = "multi_thread")]
async fn restart_after_a_snapshot_with_no_log_after_it() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;

    // A learner, so `a` keeps a quorum by itself while this node stays dark.
    let addr_c = free_addr();
    let data_c = NodeData::new();
    let c = EzKv::join_as_learner(&addr_c, &addr_a, data_c.open().await?, config()).await?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i)))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    // Snapshot at `a`'s last entry and purge everything it covers.
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
    assert_eq!(
        Some(snapshot_index),
        a.metrics().await.last_log_index,
        "the snapshot has to cover the whole log, or an append would follow it"
    );

    // Only now can the leader reach it, and only a snapshot can catch it up.
    spawn_serve(&c);
    wait_for_applied(&c, &a).await?;
    assert_eq!(expected_map(0..10), view_map(&c.view()));

    c.shutdown().await?;
    drop(c);

    // The persisted metadata's log positions must be ordered the right way round - checked
    // through `RedbStorage`'s own public load(), never a private field - or the next start
    // reads back an inverted pair and openraft refuses it as a corrupted store.
    let mut probe = data_c.open().await?;
    let loaded = probe.load().await?;
    assert!(
        loaded.meta.last_purged <= loaded.meta.last_log_id,
        "persisted an inverted log range: last_purged={:?} last_log_id={:?}",
        loaded.meta.last_purged,
        loaded.meta.last_log_id
    );
    drop(probe);

    // Opening the store again is the real check: it is where openraft reads
    // both positions back and refuses an inverted pair. A fresh address because
    // this node is only constructed, never served.
    EzKv::join_as_learner(&free_addr(), &addr_a, data_c.open().await?, config()).await?;

    Ok(())
}

/// An inconsistent snapshot parameter combination refuses to start a node outright: openraft's
/// own docs warn that a `replication_lag_threshold` below the snapshot policy's threshold may
/// leave a snapshot unable to catch a lagging follower up, so this must fail at `EzKv::create`
/// with a message naming the problem, never silently accepted.
#[tokio::test(flavor = "multi_thread")]
async fn an_inconsistent_replication_lag_threshold_refuses_to_start_the_node() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();

    let bad_config = EzConfig {
        snapshot_interval: 500,
        replication_lag_threshold: Some(10),
        ..config()
    };

    let err = EzKv::create(&addr, data.open().await?, bad_config)
        .await
        .map(|_| ())
        .expect_err("an inconsistent replication_lag_threshold must be refused");
    assert!(
        err.to_string().contains("replication_lag_threshold") && err.to_string().contains("snapshot_interval"),
        "unexpected error: {err}"
    );

    Ok(())
}

/// A stored snapshot whose header names a format version this build does not understand makes
/// the node fail to start with an explicit error, instead of silently misparsing it. Bytes are
/// patched at the codec's documented header offset - a 4-byte magic, then a little-endian `u16`
/// version at bytes `4..6` - never by reaching into a private field of storage: this is the same
/// technique `src/kv/snapshot_codec.rs`'s own unit tests use, exercised here through the public
/// `RedbStorage`/`EzStorage` seam instead.
#[tokio::test(flavor = "multi_thread")]
async fn a_stored_snapshot_with_an_unknown_version_refuses_to_start_the_node() -> io::Result<()> {
    let addr = free_addr();
    let data = NodeData::new();

    let a = EzKv::create(&addr, data.open().await?, config()).await?;
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "single node leads").await?;
    a.write(set("a", "1")).await.map_err(|e| io::Error::other(e.to_string()))?;
    a.trigger_snapshot().await?;
    a.wait_metrics(WAIT, |m| m.snapshot.is_some(), "snapshot built").await?;
    a.shutdown().await?;
    drop(a);

    // Read the real, valid snapshot bytes back, patch just the version field, and persist the
    // result - through `RedbStorage`'s own public `load`/`persist`, the same seam a restart uses.
    let mut probe = data.open().await?;
    let stored = probe.load().await?.snapshot.expect("snapshot persisted to storage");
    let mut bytes = stored.snapshot.into_inner();
    bytes[4..6].copy_from_slice(&9999u16.to_le_bytes());
    probe
        .persist(Persist::Snapshot(EzSnapshot {
            meta: stored.meta,
            snapshot: Cursor::new(bytes),
        }))
        .await?;
    drop(probe);

    let err = EzKv::create(&addr, data.open().await?, config())
        .await
        .map(|_| ())
        .expect_err("a stored snapshot with an unknown version must refuse to start the node");
    assert!(
        err.to_string().contains("version"),
        "error must name the version problem explicitly, got: {err}"
    );

    Ok(())
}
