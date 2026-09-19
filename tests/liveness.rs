//! The built-in liveness task: peer status from Raft heartbeats, the new-leader grace period,
//! writing only on change, and registry reconciliation (ech0-media#34)
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes; the isolation
//! tests additionally share a [`Faults`] handle, the same way `tests/cluster_state.rs`'s own
//! `last_leader_contact` tests do. Configured thresholds/grace/check interval are short (tens to
//! low hundreds of milliseconds) so these tests finish quickly without ever asserting on a raw
//! sleep - every wait is a condition wait, bounded by [`WAIT`].

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::NodeStatus;
use ezraft::fault::Faults;

/// A short-heartbeat config with liveness thresholds short enough for these tests to run quickly
fn liveness_config() -> EzConfig {
    EzConfig {
        liveness_threshold: Duration::from_millis(200),
        liveness_grace: Duration::from_millis(200),
        liveness_check_interval: Duration::from_millis(30),
        ..config()
    }
}

/// [`liveness_config`], with `faults` attached
fn liveness_config_with_faults(faults: &Faults) -> EzConfig {
    EzConfig {
        faults: Some(faults.clone()),
        ..liveness_config()
    }
}

/// Isolating a voter marks it `Down`, on every surviving node's registry, once its silence has
/// outlasted the threshold; healing the partition brings it back to `Up`.
#[tokio::test(flavor = "multi_thread")]
async fn isolating_a_voter_marks_it_down_after_the_threshold_and_up_after_healing() -> io::Result<()> {
    let faults = Faults::new();
    let cfg = liveness_config_with_faults(&faults);
    let (addr_a, a, _data_a) = founding_node_with(cfg.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, cfg.clone()).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, cfg.clone()).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    let victim = c.node_id();
    wait_for_registry(
        &a,
        |r| r.get(victim).map(|i| i.status) == Some(NodeStatus::Up),
        "victim starts Up",
    )
    .await?;

    faults.isolate(victim, &[a.node_id(), b.node_id()]);

    wait_for_registry(
        &a,
        |r| r.get(victim).map(|i| i.status) == Some(NodeStatus::Down),
        "victim goes Down",
    )
    .await?;
    wait_for_registry(
        &b,
        |r| r.get(victim).map(|i| i.status) == Some(NodeStatus::Down),
        "b sees it too",
    )
    .await?;

    faults.heal();

    wait_for_registry(
        &a,
        |r| r.get(victim).map(|i| i.status) == Some(NodeStatus::Up),
        "victim recovers to Up once healed",
    )
    .await?;

    Ok(())
}

/// Shutting a voter down - not merely partitioning it - marks it `Down` on the leader's registry
/// once the threshold passes, the same way isolation does.
#[tokio::test(flavor = "multi_thread")]
async fn shutting_down_a_voter_marks_it_down_after_the_threshold() -> io::Result<()> {
    let cfg = liveness_config();
    let (addr_a, a, _data_a) = founding_node_with(cfg.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, cfg.clone()).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, cfg.clone()).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;

    let victim = c.node_id();
    wait_for_registry(
        &a,
        |r| r.get(victim).map(|i| i.status) == Some(NodeStatus::Up),
        "victim starts Up",
    )
    .await?;

    c.shutdown().await?;

    wait_for_registry(
        &a,
        |r| r.get(victim).map(|i| i.status) == Some(NodeStatus::Down),
        "the leader marks the stopped voter Down",
    )
    .await?;

    Ok(())
}

/// A new leader judges nobody `Down` until its own grace period has passed, however long that
/// node has already been silent: its heartbeat record starts empty, so a node it has never heard
/// from is indistinguishable, at first, from one it has not heard from *yet*.
///
/// The victim is the old leader itself, shut down. That leaves no earlier leadership around to
/// mark anyone `Down` on its own, long-expired grace - which is what an isolated follower would
/// invite, since the old leader goes on leading, and judging, until the hand-over completes. Any
/// new leadership begins after the instant taken just before the shutdown, so `Down` cannot
/// rightly appear before that instant plus the grace period: a bound that holds however slow the
/// election is. The grace period is far longer than any election here, and the threshold far
/// shorter, so a leader that ignored its grace would be caught well inside it.
#[tokio::test(flavor = "multi_thread")]
async fn no_node_is_marked_down_during_the_new_leader_grace_period() -> io::Result<()> {
    let grace = Duration::from_secs(3);
    let cfg = EzConfig {
        liveness_threshold: Duration::from_millis(100),
        liveness_grace: grace,
        liveness_check_interval: Duration::from_millis(20),
        ..liveness_config()
    };
    let (addr_a, a, _data_a) = founding_node_with(cfg.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, cfg.clone()).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, cfg.clone()).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    wait_for_registry(
        &b,
        |r| r.len() == 3 && r.iter().all(|(_, info)| info.status == NodeStatus::Up),
        "every node is registered and Up before the leader goes away",
    )
    .await?;
    assert!(a.is_leader());

    let victim = a.node_id();
    let before_the_change = tokio::time::Instant::now();
    a.shutdown().await?;

    wait_for_cluster(
        &b,
        |cs| matches!(cs.current_leader, Some(id) if id != victim),
        "the remaining voters elect a new leader",
    )
    .await?;

    wait_for_registry(
        &b,
        |r| r.get(victim).map(|i| i.status) == Some(NodeStatus::Down),
        "the stopped node goes Down under the new leadership",
    )
    .await?;

    let elapsed = before_the_change.elapsed();
    assert!(
        elapsed >= grace,
        "the new leader must not mark anyone Down before its own grace period elapses: {elapsed:?} < {grace:?}",
    );

    Ok(())
}

/// A change of leader is not evidence that anyone recovered. The new leader has heard from
/// nobody yet, so a node the previous leadership left `Down` must stay `Down` - never flip to
/// `Up` for the length of the grace period, which would send every client that consults the
/// registry straight back to a dead node on each failover.
#[tokio::test(flavor = "multi_thread")]
async fn a_down_node_stays_down_across_a_change_of_leader() -> io::Result<()> {
    let grace = Duration::from_secs(2);
    let cfg = EzConfig {
        liveness_threshold: Duration::from_millis(200),
        liveness_grace: grace,
        liveness_check_interval: Duration::from_millis(20),
        ..liveness_config()
    };
    let (addr_a, a, _data_a) = founding_node_with(cfg.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, cfg.clone()).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, cfg.clone()).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());
    wait_for_registry(
        &a,
        |r| r.len() == 3 && r.iter().all(|(_, info)| info.status == NodeStatus::Up),
        "every node is registered and Up before one is stopped",
    )
    .await?;

    let victim = c.node_id();
    c.shutdown().await?;
    for node in [&a, &b] {
        wait_for_registry(
            node,
            |r| r.get(victim).map(|i| i.status) == Some(NodeStatus::Down),
            "the stopped node goes Down under the first leadership",
        )
        .await?;
    }

    // Subscribed before the hand-over, so nothing the new leadership writes can be missed.
    let mut registry = b.watch_registry();
    registry.borrow_and_update();

    a.transfer_leader(b.node_id()).await?;
    wait_for_cluster(
        &b,
        |cs| cs.current_leader == Some(cs.node_id),
        "the hand-over completes",
    )
    .await?;

    // Watch every registry change for longer than the new leader's whole grace period: a wrong
    // `Up` would be written by its very first check and stand until the grace period ended.
    let watch_until = tokio::time::Instant::now() + grace + Duration::from_secs(1);
    loop {
        let status = registry.borrow_and_update().get(victim).map(|i| i.status);
        assert_eq!(
            Some(NodeStatus::Down),
            status,
            "the stopped node must stay Down across the hand-over"
        );

        match tokio::time::timeout_at(watch_until, registry.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) => panic!("the registry watch closed while its node is still running"),
            Err(_) => break,
        }
    }

    Ok(())
}

/// A healthy, unchanged cluster produces no further registry writes at all: the liveness task
/// only ever writes on an actual status change.
#[tokio::test(flavor = "multi_thread")]
async fn status_is_written_only_on_change() -> io::Result<()> {
    // Checked every 30 ms like everywhere else here, but against a threshold no healthy node can
    // plausibly miss: this proves that nothing is written without a change, and a heartbeat that
    // a loaded test machine delays past a 200 ms threshold *is* a change - a real `Down`, and a
    // real `Up` after it.
    let cfg = EzConfig {
        liveness_threshold: Duration::from_secs(5),
        liveness_grace: Duration::from_secs(5),
        ..liveness_config()
    };
    let (addr_a, a, _data_a) = founding_node_with(cfg.clone()).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, cfg.clone()).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, cfg.clone()).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    wait_for_registry(
        &a,
        |r| r.len() == 3 && r.iter().all(|(_, info)| info.status == NodeStatus::Up),
        "every node settles at Up",
    )
    .await?;

    let mut rx = a.watch_registry();
    rx.borrow_and_update();

    // Proving a negative over several liveness-check ticks, bounded well under `WAIT`.
    let outcome = tokio::time::timeout(cfg.liveness_check_interval * 10, rx.changed()).await;
    assert!(
        outcome.is_err(),
        "a healthy, unchanged cluster must not produce further registry writes, got: {outcome:?}"
    );

    Ok(())
}

/// A short, bounded budget for one scan attempt's own waits - long enough for a healthy in-process
/// cluster to converge, short enough that scanning a handful of candidate persist offsets (see
/// [`a_registry_entry_for_a_node_outside_membership_is_reconciled_away`]'s own docs) stays fast
/// when an early one lands short of `remove_node`'s own two writes.
const SCAN_ATTEMPT_BUDGET: Duration = Duration::from_secs(5);

/// One attempt at reproducing "the membership change committed durably, but `remove_node` never
/// got to propose its own follow-up `RemoveNode` command" by crashing exactly `offset` persists
/// after the learner registers, on a freshly built cluster.
///
/// `remove_node` costs exactly two log entries for a learner (a membership entry, then the
/// `RemoveNode` command - no joint-consensus phase, since only a voter-set change needs one) - so
/// `offset` in `1..=4` covers every persist either of those two entries could be armed on ( each
/// log entry costs its own `LogEntry` persist and a following `Meta` persist). Exactly how many
/// *other* persists precede those two - votes, the founding membership, this node's own
/// self-registration, the learner's own join and registration - is not asserted here: real,
/// harmless variance in that count (for instance a single retried command) would otherwise make a
/// single hard-coded offset flaky, which is exactly what scanning a small range avoids without
/// weakening what this test actually proves once the right offset is found.
///
/// Returns `Ok(true)` once `offset` reproduces the scenario and every assertion about it holds;
/// `Ok(false)` if this offset does not land on the boundary this test needs (too early: the
/// membership change itself never became durable; too late: `remove_node` completed both of its
/// own steps normally, so there is nothing stale to reconcile) - the caller retries with a
/// different offset on a fresh cluster, never reusing this one.
async fn try_reconcile_stale_entry_at_offset(offset: usize) -> io::Result<bool> {
    // Threshold, grace and the check interval are all set far longer than one scan attempt runs,
    // so that the built-in task writes nothing this scan does not account for. Threshold and
    // grace: no status can change during an attempt, however slow a heartbeat is under load.
    // Check interval: `tokio::time::interval`'s own first tick fires immediately, not after a
    // full interval, so the built-in task's *first* check (finding nothing stale yet) is harmless
    // - but its *second* check, one interval later, could land on exactly the persist this scan
    // means to crash. A long interval makes that second check impossible within one attempt,
    // while the *restarted* node's own fresh task still reconciles promptly: its first tick fires
    // immediately too.
    let cfg = EzConfig {
        liveness_threshold: Duration::from_secs(60),
        liveness_grace: Duration::from_secs(60),
        liveness_check_interval: Duration::from_secs(60),
        ..config()
    };
    let addr_a = free_addr();
    let data_a = NodeData::new();
    let (storage, crash) = CrashStorage::wrap(data_a.open().await?);
    let a = EzKv::create(&addr_a, storage, cfg.clone()).await?;
    spawn_serve(&a);
    a.wait_metrics(
        Some(SCAN_ATTEMPT_BUDGET),
        |m| m.current_leader == Some(0),
        "founding node leads",
    )
    .await?;

    let (_, d, _data_d) = joined_learner_with(&addr_a, cfg.clone()).await?;
    let learner_id = d.node_id();
    wait_for_registry(&a, |r| r.get(learner_id).is_some(), "the learner registers itself").await?;

    let armed_at = crash.persist_count() + offset;
    crash.arm(CrashPoint::after(armed_at));

    // Captured, not fire-and-forget: telling "the crash interrupted this call" (an `Err`) apart
    // from "both of `remove_node`'s own steps already finished before the armed point fired" (an
    // `Ok`) is exactly how this attempt tells "too early" from "too late".
    let (result_tx, mut result_rx) = tokio::sync::oneshot::channel();
    let victim = a.clone();
    tokio::spawn(async move {
        let _ = result_tx.send(victim.remove_node(learner_id).await);
    });

    let deadline = tokio::time::Instant::now() + SCAN_ATTEMPT_BUDGET;
    let remove_node_outcome = loop {
        if let Ok(outcome) = result_rx.try_recv() {
            break Some(outcome);
        }
        if crash.is_crashed() {
            break None; // crashed, but `remove_node` has not reported back yet - definitely "too late" is ruled out
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false); // the armed point was never reached within this attempt's own budget: offset too large
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    if let Some(Ok(())) = remove_node_outcome {
        return Ok(false); // both of remove_node's own steps already succeeded: offset too large
    }

    a.shutdown().await?;

    // Restart the same founding node from the same, uncorrupted file: `CrashWhen::After` always
    // reaches disk, so this recovers the membership change if it already committed. A restart is
    // a fresh election (a fresh term), and Raft only reports a *previous* term's entries as
    // committed once the new term's own entry commits too - a condition wait, not a one-shot
    // check right after `current_leader` is first set, is what a correct restart assertion needs.
    let storage = data_a.open().await?;
    let restarted = EzKv::create(&addr_a, storage, cfg.clone()).await?;
    spawn_serve(&restarted);
    let settled = restarted
        .wait_metrics(
            Some(SCAN_ATTEMPT_BUDGET),
            |m| {
                m.current_leader == Some(0)
                    && !m.committed_membership_config.membership().learner_ids().any(|id| id == learner_id)
            },
            "the restarted node re-leads, with its own membership already reflecting the pre-crash removal",
        )
        .await;
    if settled.is_err() {
        return Ok(false); // the membership change itself never became durable: offset too small
    }

    // Not asserted: that the registry entry is *still* present immediately after restart - see
    // this function's own module-level docs on why that would itself be racy. The wait below
    // confirms the actual, only claim this test makes: reconciliation converges.
    wait_for_registry(
        &restarted,
        |r| r.get(learner_id).is_none(),
        "the built-in liveness task reconciles the stale entry away",
    )
    .await?;

    Ok(true)
}

/// A registry entry for a node that is no longer in the committed membership - the window
/// `EzKv::remove_node` leaves if a leader crashes between its own two steps - is removed by the
/// leader's built-in liveness task.
///
/// Simulated with `CrashStorage`, scanning a small range of candidate persist offsets (see
/// [`try_reconcile_stale_entry_at_offset`]'s own docs for why a single hard-coded offset would be
/// flaky): arming a crash at the membership entry's own persist reproduces "the membership change
/// is durable, but `remove_node` never got to its follow-up `RemoveNode` command" without needing
/// to race anything - `CrashWhen::After` still writes the entry to disk (a restart from the same
/// file recovers it normally) but tells `remove_node` itself that it failed, exactly as if the
/// process had died a moment later.
#[tokio::test(flavor = "multi_thread")]
async fn a_registry_entry_for_a_node_outside_membership_is_reconciled_away() -> io::Result<()> {
    for offset in 1..=4 {
        if try_reconcile_stale_entry_at_offset(offset).await? {
            return Ok(());
        }
    }
    Err(io::Error::other(
        "no persist offset in 1..=4 reproduced remove_node's own crash-between-its-two-steps window",
    ))
}
