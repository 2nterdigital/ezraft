//! Storage crash injection and power-loss recovery (ech0-media#25)
//!
//! Three layers, cheapest first:
//!
//! - Standalone [`CrashStorage`] behavior, against a bare `RedbStorage`, no cluster involved: a
//!   `Before` crash leaves no trace on disk, an `After` crash's data survives even though the
//!   caller was told it failed, and once crashed every call fails.
//! - A power-loss scan over a fixed sequence of acknowledged writes on a three-voter cluster: for
//!   every persist point the victim (a follower, then the leader with a shorter sequence) can crash
//!   on, restarting it from its redb file must recover every write the client already saw
//!   acknowledged, agree with the other replicas on the resulting state, and the cluster must keep
//!   serving writes.
//! - Vote durability: a crash immediately before and immediately after the persist that records a
//!   vote must never let a restart either invent a vote that never reached disk or lose one that
//!   did.
//!
//! Every crash point is chosen by persist count and kind, never by sleeping; every wait is a
//! bounded condition wait (`WAIT`, or the crash handle's own state) exactly like the rest of this
//! harness.

mod common;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use ezraft::EzEntry;
use ezraft::EzKv;
use ezraft::EzSnapshot;
use ezraft::EzSnapshotMeta;
use ezraft::EzStorage;
use ezraft::EzVote;
use ezraft::KeyValue;
use ezraft::Loaded;
use ezraft::Op;
use ezraft::Persist;
use ezraft::RedbStorage;
use ezraft::Txn;
use ezraft::View;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::StoredMembership;

/// How often a bounded poll re-checks its condition - the same interval the harness's own
/// `write_through_leader`/`wait_for_revision` poll on, kept local here since it is private there.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

// ---------------------------------------------------------------------------------------------
// Layer 1: the CrashStorage primitive on its own, no cluster
// ---------------------------------------------------------------------------------------------

/// A synthetic log entry: `EzEntry`'s payload can't name the crate-private `Command` type from
/// outside it, but a crash point never inspects entry content, only kind and index - `Blank` is
/// exactly as good a log entry as a real transaction for that.
fn blank_entry(term: u64, index: u64) -> EzEntry {
    EzEntry {
        log_id: (term, index),
        payload: EntryPayload::Blank,
    }
}

fn tiny_snapshot(index: u64, data: &[u8]) -> EzSnapshot {
    let last_log_id = LogId::new_term_index(1, index);
    EzSnapshot {
        meta: EzSnapshotMeta {
            last_log_id: Some(last_log_id),
            last_membership: StoredMembership::new(Some(last_log_id), Membership::default()),
        },
        snapshot: Cursor::new(data.to_vec()),
    }
}

#[tokio::test]
async fn a_crash_before_a_persist_leaves_no_trace_and_the_previous_state_intact() -> io::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.redb");

    let (mut storage, handle) = CrashStorage::wrap(RedbStorage::open(&path).await?);
    storage.persist(Persist::LogEntry(blank_entry(1, 1))).await?; // ordinal 1: survives

    handle.arm(CrashPoint::before(2));
    let err = storage
        .persist(Persist::LogEntry(blank_entry(1, 2)))
        .await
        .expect_err("the armed Before point must fail the call");
    assert_eq!(io::ErrorKind::Other, err.kind());
    assert!(handle.is_crashed());
    assert_eq!(
        1,
        handle.persist_count(),
        "the crashed call must not be recorded as having happened"
    );

    drop(storage); // release the redb file, same as a real crash before a restart

    let mut reopened = RedbStorage::open(&path).await?;
    let entries = reopened.read_logs(1, 2).await?;
    assert_eq!(1, entries.len(), "the entry before the crash point is on disk");
    assert!(
        reopened.read_logs(2, 3).await.is_err(),
        "the crashed entry must never have reached disk"
    );

    Ok(())
}

#[tokio::test]
async fn a_crash_after_a_persist_still_reaches_disk_but_reports_failure_to_the_caller() -> io::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.redb");

    let (mut storage, handle) = CrashStorage::wrap(RedbStorage::open(&path).await?);
    handle.arm(CrashPoint::after(1));

    let err = storage
        .persist(Persist::LogEntry(blank_entry(1, 1)))
        .await
        .expect_err("the armed After point must still fail the call");
    assert_eq!(io::ErrorKind::Other, err.kind());
    assert!(handle.is_crashed());
    assert_eq!(
        1,
        handle.persist_count(),
        "an After crash's whole point is that the call did happen, durably"
    );

    drop(storage);

    let mut reopened = RedbStorage::open(&path).await?;
    let entries = reopened.read_logs(1, 2).await?;
    assert_eq!(
        1,
        entries.len(),
        "the entry the caller was told failed is on disk anyway"
    );

    Ok(())
}

#[tokio::test]
async fn once_crashed_every_further_call_fails_persist_and_read_alike() -> io::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.redb");

    let (mut storage, handle) = CrashStorage::wrap(RedbStorage::open(&path).await?);
    storage.persist(Persist::LogEntry(blank_entry(1, 1))).await?;

    handle.arm(CrashPoint::before(2));
    let _ = storage.persist(Persist::LogEntry(blank_entry(1, 2))).await;
    assert!(handle.is_crashed());

    assert!(storage.load().await.is_err(), "load must fail once crashed");
    assert!(
        storage.read_logs(1, 2).await.is_err(),
        "read_logs must fail once crashed"
    );
    assert!(
        storage.persist(Persist::LogEntry(blank_entry(1, 3))).await.is_err(),
        "persist must keep failing once crashed"
    );

    Ok(())
}

#[tokio::test]
async fn history_records_every_persist_kind_in_call_order() -> io::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.redb");

    let (mut storage, handle) = CrashStorage::wrap(RedbStorage::open(&path).await?);
    storage.persist(Persist::LogEntry(blank_entry(1, 1))).await?;
    storage.persist(Persist::LogEntry(blank_entry(1, 2))).await?;
    storage.persist(Persist::DeleteLogs { from: 5, to: 10 }).await?;
    storage.persist(Persist::Snapshot(tiny_snapshot(2, b"snap"))).await?;

    assert_eq!(
        vec![
            PersistRecord::LogEntry { index: 1 },
            PersistRecord::LogEntry { index: 2 },
            PersistRecord::DeleteLogs { from: 5, to: 10 },
            PersistRecord::Snapshot,
        ],
        handle.history(),
    );
    assert_eq!(4, handle.persist_count());
    assert!(!handle.is_crashed());

    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Layer 2 & 3 shared harness: a three-voter cluster with one node wrapped in a CrashStorage
// ---------------------------------------------------------------------------------------------

/// Which of the three voters a trial wraps in a [`CrashStorage`]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Victim {
    /// Slot 0: the founding node, which leads for this whole scenario (nothing here ever
    /// transfers leadership deliberately)
    Leader,
    /// Slot 1: a node that joined as a voter and never leads
    Follower,
}

/// One live three-voter cluster for a crash-point trial: the founding leader plus two joined
/// voters, one of which (`victim`) is wrapped in a [`CrashStorage`], and every `NodeData` the
/// caller must keep alive so a restart can reopen the same file.
struct Trial {
    nodes: [EzKv; 3],
    data: [NodeData; 3],
    victim: usize,
    handle: CrashHandle,
}

impl Trial {
    fn all(&self) -> [&EzKv; 3] {
        [&self.nodes[0], &self.nodes[1], &self.nodes[2]]
    }

    fn victim_node(&self) -> &EzKv {
        &self.nodes[self.victim]
    }

    /// The one voter no trial ever crashes: [`Victim`] only ever names slot 0 or slot 1
    fn survivor(&self) -> &EzKv {
        &self.nodes[2]
    }

    /// Stop the victim and start a fresh, plain `RedbStorage` instance on the same file, at the
    /// same address - the crash-and-restart this whole ticket is about. Only valid to call once
    /// the victim has actually crashed: this does not itself do any power-loss simulation.
    async fn restart_victim(&mut self) -> io::Result<()> {
        let addr = self.nodes[self.victim].addr().to_string();
        let seed = self.nodes[0].addr().to_string();

        self.nodes[self.victim].shutdown().await?;

        let storage = self.data[self.victim].open().await?;
        let restarted = if self.victim == 0 {
            EzKv::create(&addr, storage, config()).await?
        } else {
            EzKv::join(&addr, &seed, storage, config()).await?
        };
        spawn_serve(&restarted);
        self.nodes[self.victim] = restarted;
        Ok(())
    }
}

/// Build a three-voter cluster where `victim` names which slot is wrapped in a [`CrashStorage`],
/// converged (every node a committed voter) and idle - nothing has been armed yet.
async fn build_trial(victim: Victim) -> io::Result<Trial> {
    let cfg = config();

    let (addr_a, a, data_a, handle_a) = if victim == Victim::Leader {
        let data = NodeData::new();
        let addr = free_addr();
        let (storage, handle) = CrashStorage::wrap(data.open().await?);
        let node = EzKv::create(&addr, storage, cfg.clone()).await?;
        spawn_serve(&node);
        node.wait_metrics(
            WAIT,
            |m| m.current_leader == Some(0) && m.committed_membership_config.log_id().is_some(),
            "founding node leads, with its own membership committed",
        )
        .await?;
        (addr, node, data, Some(handle))
    } else {
        let (addr, node, data) = founding_node_with(cfg.clone()).await?;
        (addr, node, data, None)
    };

    let (b, data_b, handle_b) = if victim == Victim::Follower {
        let data = NodeData::new();
        let addr = free_addr();
        let (storage, handle) = CrashStorage::wrap(data.open().await?);
        let node = EzKv::join(&addr, &addr_a, storage, cfg.clone()).await?;
        spawn_serve(&node);
        (node, data, Some(handle))
    } else {
        let (_, node, data) = joined_voter_with(&addr_a, cfg.clone()).await?;
        (node, data, None)
    };

    let (_, c, data_c) = joined_voter_with(&addr_a, cfg.clone()).await?;

    let voters = BTreeSet::from([a.node_id(), b.node_id(), c.node_id()]);
    wait_for_voters(&a, voters, "every joined node is a voter").await?;

    let handle = handle_a.or(handle_b).expect("exactly one slot is wrapped in a CrashStorage");
    let victim_index = if victim == Victim::Leader { 0 } else { 1 };

    Ok(Trial {
        nodes: [a, b, c],
        data: [data_a, data_b, data_c],
        victim: victim_index,
        handle,
    })
}

/// Write through the one voter no trial ever crashes
///
/// `write` works on any node: it forwards to whoever leads and retries under the same request id
/// across the victim leader's crash and the election that follows, so every write here takes effect
/// exactly once. Trying each node in turn instead would spend a whole write deadline on the crashed
/// victim, and re-issuing the transaction elsewhere under a new request id is not exactly-once.
async fn write_through_survivor(trial: &Trial, txn: Txn) -> io::Result<u64> {
    trial.survivor().write(txn).await.map_err(|e| io::Error::other(e.to_string()))
}

/// This node's whole state, keyed by key - `View::prefix` already carries `create_rev`/`mod_rev`,
/// which is exactly the agreement a restarted replica must reach with the others.
fn full_state(view: &View) -> BTreeMap<String, KeyValue> {
    view.prefix("").map(|kv| (kv.key.clone(), kv)).collect()
}

/// The `(key, value)` a single-`Put` transaction writes - every write this file's scenarios use.
fn put_kv(txn: &Txn) -> (String, Bytes) {
    match txn.ops.as_slice() {
        [Op::Put { key, value }] => (key.clone(), value.clone()),
        other => panic!("expected a single-Put transaction, got {other:?}"),
    }
}

fn write_sequence(n: usize) -> Vec<Txn> {
    (0..n).map(|i| set(&format!("k{i}"), &format!("v{i}"))).collect()
}

/// How many persists the victim performs over `writes`, discovered by actually running the
/// sequence once, unarmed, and waiting for the victim to fully catch up - not guessed, since
/// batching under replication lag can change the count.
async fn discover_persist_range(victim: Victim, writes: &[Txn]) -> io::Result<usize> {
    let trial = build_trial(victim).await?;
    let baseline = trial.handle.persist_count();

    for txn in writes {
        write_through_leader(&trial.all(), txn.clone()).await?;
    }
    wait_for_applied(trial.victim_node(), &trial.nodes[0]).await?;

    let range = trial.handle.persist_count() - baseline;
    for node in trial.all() {
        let _ = node.shutdown().await;
    }
    Ok(range)
}

/// Wait until the victim has caught up to `trial.nodes[0]`'s log, or has crashed trying -
/// whichever happens first.
///
/// A write's `Ok` only proves the node it was called on, and a quorum, persisted and applied it.
/// For a follower victim, replication to it is a background process the caller of
/// `write` never waits on, so checking `is_crashed()` right after the write loop would race an
/// armed point that has not been reached yet. `trial.nodes[0]` is always the reference: for a
/// follower victim it is the leader that never crashes in this scenario, so its log is the true
/// target; for a leader victim it *is* the victim, so the wait is a trivial no-op or resolves
/// through the crash branch instantly.
async fn wait_for_victim_to_settle(trial: &Trial) -> io::Result<()> {
    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster waits are bounded");
    let target = trial.nodes[0].metrics().await.last_log_index;

    loop {
        if trial.handle.is_crashed() {
            return Ok(());
        }
        let applied = trial.victim_node().metrics().await.last_applied.map(|log_id| log_id.index);
        if applied >= target {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other(
                "the victim neither caught up nor crashed within the wait budget",
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Run `writes` acknowledged one at a time, with a crash armed at `point` on the victim; if it
/// actually fires, restart the victim and keep writing past it. Asserts every acknowledged write
/// survived, with every replica (the restarted victim included) agreeing on the resulting state.
/// Returns whether the crash actually fired, so the caller can confirm the scan covered
/// something real.
///
/// `point.at` is relative to the write sequence, not the victim's whole lifetime: `build_trial`
/// itself already persists several times getting the victim to a converged voter (its own node id,
/// then the membership entries that add and promote it), so ordinal 1 of the *victim's* history is
/// long gone by the time this function can arm anything. Every trial re-measures its own baseline,
/// the persist count right after setup, and arms `baseline + point.at`, the same reference
/// `discover_persist_range` used to turn its raw count into a range in the first place.
async fn run_crash_point(victim: Victim, writes: &[Txn], point: CrashPoint) -> io::Result<bool> {
    let mut trial = build_trial(victim).await?;
    let baseline = trial.handle.persist_count();
    trial.handle.arm(CrashPoint {
        at: baseline + point.at,
        when: point.when,
    });

    let mut acknowledged: Vec<(String, Bytes, u64)> = Vec::new();
    for txn in writes {
        let revision = write_through_survivor(&trial, txn.clone()).await?;
        let (key, value) = put_kv(txn);
        acknowledged.push((key, value, revision));
    }
    wait_for_victim_to_settle(&trial).await?;

    let fired = trial.handle.is_crashed();
    if fired {
        trial.restart_victim().await?;

        let tail = set("after-crash", "ok");
        let revision = write_through_survivor(&trial, tail.clone()).await?;
        let (key, value) = put_kv(&tail);
        acknowledged.push((key, value, revision));
    }

    let final_revision = acknowledged.iter().map(|(_, _, r)| *r).max().expect("at least one write");
    for node in trial.all() {
        wait_for_revision(node, final_revision).await?;
    }

    let reference = full_state(&trial.nodes[0].view());
    for node in trial.all() {
        assert_eq!(
            reference,
            full_state(&node.view()),
            "node {} disagrees with node {} after crash point {:?} (fired: {})",
            node.node_id(),
            trial.nodes[0].node_id(),
            point,
            fired,
        );
    }
    for (key, value, _) in &acknowledged {
        assert_eq!(
            Some(value.clone()),
            reference.get(key).map(|kv| kv.value.clone()),
            "acknowledged write to {key} did not survive crash point {point:?}"
        );
    }

    for node in trial.all() {
        let _ = node.shutdown().await;
    }

    Ok(fired)
}

// ---------------------------------------------------------------------------------------------
// Layer 2: the power-loss scan
// ---------------------------------------------------------------------------------------------

/// Every acknowledged write, and full replica agreement, survives a crash at any persist point on
/// a follower - the whole range the dry run observes, both `Before` and `After`.
#[tokio::test(flavor = "multi_thread")]
async fn every_acknowledged_write_survives_a_crash_at_any_point_on_a_follower() -> io::Result<()> {
    let writes = write_sequence(3);
    let range = discover_persist_range(Victim::Follower, &writes).await?;
    assert!(
        range > 0,
        "the write sequence must produce at least one persist to scan"
    );

    let mut fired = 0;
    for at in 1..=range {
        for when in [CrashWhen::Before, CrashWhen::After] {
            if run_crash_point(Victim::Follower, &writes, CrashPoint { at, when }).await? {
                fired += 1;
            }
        }
    }
    assert!(
        fired > 0,
        "no scanned point on the follower actually crashed - the range may be wrong"
    );

    Ok(())
}

/// The same scan with the leader as the victim, over a shorter sequence: a crash here also moves
/// leadership, so every trial costs an extra election, and the ticket allows a smaller sequence
/// for this role. See the delivery report for exactly what this covers.
#[tokio::test(flavor = "multi_thread")]
async fn every_acknowledged_write_survives_a_crash_at_any_point_on_the_leader() -> io::Result<()> {
    let writes = write_sequence(2);
    let range = discover_persist_range(Victim::Leader, &writes).await?;
    assert!(
        range > 0,
        "the write sequence must produce at least one persist to scan"
    );

    let mut fired = 0;
    for at in 1..=range {
        for when in [CrashWhen::Before, CrashWhen::After] {
            if run_crash_point(Victim::Leader, &writes, CrashPoint { at, when }).await? {
                fired += 1;
            }
        }
    }
    assert!(
        fired > 0,
        "no scanned point on the leader actually crashed - the range may be wrong"
    );

    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Layer 3: vote durability
// ---------------------------------------------------------------------------------------------

/// Poll `handle.is_crashed()` until it fires, bounded by `WAIT` - a condition wait on the crash
/// handle's own state, not a fixed sleep.
async fn wait_until_crashed(handle: &CrashHandle) -> io::Result<()> {
    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster waits are bounded");
    loop {
        if handle.is_crashed() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other("the armed crash point never fired"));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Poll `handle`'s history, from `baseline` on, until some `Meta` persist there has recorded a
/// vote; return that persist's ordinal relative to `baseline` (1-based), bounded by `WAIT`.
///
/// Not the first vote in the *whole* history: a follower already records a vote for the founding
/// leader the moment it accepts that leader's first `AppendEntries` during setup - openraft's vote
/// doubles as "who I currently follow", not only "who I voted for in an election" - so `baseline`
/// must be measured right before the caller triggers the election under test, and this looks only
/// at what happens after it.
async fn wait_for_vote_after(handle: &CrashHandle, baseline: usize) -> io::Result<usize> {
    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster waits are bounded");
    loop {
        let history = handle.history();
        if let Some(pos) = history
            .get(baseline..)
            .and_then(|since| since.iter().position(|r| matches!(r, PersistRecord::Meta { vote: Some(_) })))
        {
            return Ok(pos + 1);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other(
                "no persist recorded a vote after the election was triggered",
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The vote carried by the last `Meta` persist in `history[..upto]`, or `None` if none of them
/// ever touched it (a fresh node that has neither voted nor accepted a leader yet)
fn vote_as_of(history: &[PersistRecord], upto: usize) -> Option<EzVote> {
    history[..upto]
        .iter()
        .rev()
        .find_map(|r| match r {
            PersistRecord::Meta { vote } => Some(*vote),
            _ => None,
        })
        .flatten()
}

/// At most one leader per term, sampled from every live node's own metrics: the cluster-wide half
/// of "a vote is never granted twice in a term" - the storage layer's job (proved directly, below,
/// through what actually reached disk) is what gives openraft's own vote arbitration correct data
/// to work with.
async fn assert_single_leader_per_term(nodes: &[&EzKv]) {
    let mut leader_by_term: HashMap<u64, u64> = HashMap::new();
    for node in nodes {
        let metrics = node.metrics().await;
        let Some(leader) = metrics.current_leader else { continue };
        let term = metrics.vote.leader_id.term;

        match leader_by_term.get(&term) {
            Some(&existing) => assert_eq!(existing, leader, "term {term} has two different leaders reported"),
            None => {
                leader_by_term.insert(term, leader);
            }
        }
    }
}

/// Discover, relative to a freshly measured baseline, how many persists after `trigger().elect()`
/// the victim's own vote-recording persist lands at - not an absolute ordinal, for the same reason
/// the write-sequence scan measures a range rather than reusing raw counts (`build_trial` itself
/// already persists getting the victim to a converged voter, and that setup's length is not
/// perfectly fixed run to run). A fresh trial later reproduces the same delta from *its own*
/// baseline, measured the same way, right before it triggers the same election.
async fn discover_vote_persist_delta() -> io::Result<usize> {
    let trial = build_trial(Victim::Follower).await?;
    let baseline = trial.handle.persist_count();

    trial.victim_node().inner().trigger().elect(false).await.map_err(io::Error::other)?;
    let delta = wait_for_vote_after(&trial.handle, baseline).await?;

    for node in trial.all() {
        let _ = node.shutdown().await;
    }
    Ok(delta)
}

/// Force a fresh victim to campaign with a crash armed `delta` persists after its own baseline
/// (the persist that records its vote for this election), `when`, then prove directly through
/// `RedbStorage`'s own `load()` - the same seam `tests/node_lifecycle.rs`'s restart tests use -
/// what a restart actually recovers: `Before` leaves the vote exactly as it was before this
/// election; `After` recovers exactly the new vote that reached disk, even though the caller was
/// told the persist failed. The cluster then keeps serving past it.
async fn run_vote_crash(when: CrashWhen, delta: usize) -> io::Result<()> {
    let mut trial = build_trial(Victim::Follower).await?;
    let victim_addr = trial.victim_node().addr().to_string();
    let seed_addr = trial.nodes[0].addr().to_string();
    let baseline = trial.handle.persist_count();
    let vote_before = vote_as_of(&trial.handle.history(), baseline);

    trial.handle.arm(CrashPoint {
        at: baseline + delta,
        when,
    });
    // A caller-side error from `elect` itself is expected once storage starts failing; the crash
    // handle, not this call's own result, is the thing under test.
    let _ = trial.victim_node().inner().trigger().elect(false).await;
    wait_until_crashed(&trial.handle).await?;

    let history = trial.handle.history();
    let vote_after_wrapper = vote_as_of(&history, history.len());

    trial.nodes[trial.victim].shutdown().await?;

    let mut probe = trial.data[trial.victim].open().await?;
    let loaded: Loaded = probe.load().await?;
    drop(probe);

    match when {
        CrashWhen::Before => {
            assert_eq!(
                vote_before, vote_after_wrapper,
                "a Before crash must never let a new vote reach the wrapper's own history"
            );
            assert_eq!(
                vote_before, loaded.meta.vote,
                "a vote that crashed before reaching disk must leave the previous vote untouched"
            );
        }
        CrashWhen::After => {
            assert_ne!(
                vote_before, vote_after_wrapper,
                "an After crash's whole point is that a new vote did reach disk"
            );
            assert_eq!(
                vote_after_wrapper, loaded.meta.vote,
                "the vote persisted right before the crash must still be on disk after it"
            );
        }
    }

    let restarted = EzKv::join(
        &victim_addr,
        &seed_addr,
        trial.data[trial.victim].open().await?,
        config(),
    )
    .await?;
    spawn_serve(&restarted);
    trial.nodes[trial.victim] = restarted;

    wait_for_applied(trial.victim_node(), &trial.nodes[0]).await?;
    let revision = write_through_survivor(&trial, set("after-vote-crash", "ok")).await?;
    for node in trial.all() {
        wait_for_revision(node, revision).await?;
    }

    assert_single_leader_per_term(&trial.all()).await;

    for node in trial.all() {
        let _ = node.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_vote_crashed_just_after_its_persist_survives_a_restart() -> io::Result<()> {
    let delta = discover_vote_persist_delta().await?;
    run_vote_crash(CrashWhen::After, delta).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_vote_crashed_just_before_its_persist_leaves_the_previous_vote_on_restart() -> io::Result<()> {
    let delta = discover_vote_persist_delta().await?;
    run_vote_crash(CrashWhen::Before, delta).await
}
