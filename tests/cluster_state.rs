//! Cluster observation: `EzKv::cluster()` and `EzKv::last_leader_contact()` (ech0-media#33)
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes; the
//! `last_leader_contact` tests additionally share a [`Faults`] handle, the same way
//! `tests/partition.rs` does.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::ServerState;
use ezraft::fault::Faults;

/// The founding node's own `cluster()` settles on exactly the fields a single-node cluster
/// implies: leading, still a member, the sole voter, and its own id as the current leader.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_reports_correct_fields_on_the_founding_leader() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    let state = wait_for_cluster(
        &a,
        |c| c.role == ServerState::Leader && c.is_member && c.current_leader == Some(0),
        "the founding node settles as leader",
    )
    .await?;

    assert_eq!(0, state.node_id);
    assert!(
        state.term >= 1,
        "a node that won an election has campaigned at least once"
    );
    assert_eq!(BTreeSet::from([0]), state.voters);
    assert!(state.learners.is_empty());

    Ok(())
}

/// A learner join and its later promotion both show up on every node's `cluster()`: the learner
/// moves from `learners` to `voters`, and its own role moves from `Learner` to `Follower`.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_tracks_a_learner_join_and_its_promotion_to_voter() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_learner(&addr_a).await?;
    let b_id = b.node_id();

    wait_for_cluster(&a, |c| c.learners.contains(&b_id), "the leader sees the new learner").await?;

    // `ServerState::Learner` is also openraft's default before a node has committed its own
    // membership entry at all, so `role == Learner` alone matches too early; `is_member` is what
    // tells "not yet joined" and "joined, as a learner" apart.
    let learner_state = wait_for_cluster(
        &b,
        |c| c.role == ServerState::Learner && c.is_member,
        "the learner sees its own committed membership",
    )
    .await?;
    assert!(
        learner_state.voters.contains(&0),
        "the founding voter is visible to a learner too"
    );

    // Wait for the learner to apply a real log entry before explicitly promoting it; this checks
    // replication without treating catch-up as an automatic promotion gate.
    a.write(set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?;
    let target = a.metrics().await.last_log_index.expect("the leader has written entries");
    b.wait_metrics(
        WAIT,
        |m| m.last_applied.map(|log_id| log_id.index) >= Some(target),
        "learner applied the leader's log",
    )
    .await?;

    a.promote(b_id).await?;

    wait_for_cluster(
        &a,
        |c| c.voters.contains(&b_id) && !c.learners.contains(&b_id),
        "the leader sees the promotion",
    )
    .await?;
    wait_for_cluster(
        &b,
        |c| c.role == ServerState::Follower && c.voters.contains(&b_id),
        "the promoted node sees its own new role",
    )
    .await?;

    Ok(())
}

/// A leadership transfer shows up on both ends: the old leader becomes a follower naming the new
/// one, and the new leader reports itself as leader.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_reflects_leadership_transfer_on_both_old_and_new_leader() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    wait_for_applied(&c, &a).await?;

    let target = c.node_id();
    a.transfer_leader(target).await?;

    let new_leader_state =
        wait_for_cluster(&c, |cs| cs.role == ServerState::Leader, "the named node becomes leader").await?;
    assert_eq!(Some(target), new_leader_state.current_leader);

    let old_leader_state = wait_for_cluster(
        &a,
        |cs| cs.role != ServerState::Leader && cs.current_leader == Some(target),
        "the old leader steps down and names the new one",
    )
    .await?;
    assert_eq!(ServerState::Follower, old_leader_state.role);

    Ok(())
}

/// Killing the leader shows up on the survivors: a new leader emerges, at a higher term, and
/// every surviving node's `cluster()` names it.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_reflects_a_leader_shutdown_and_the_ensuing_election() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(a.addr()).await?;
    let (_, c, _data_c) = joined_voter(a.addr()).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    let old_term = a.metrics().await.current_term;

    a.shutdown().await?;

    let survivor_state = wait_for_cluster(
        &b,
        |cs| matches!(cs.current_leader, Some(id) if id != 0) && cs.term > old_term,
        "a surviving voter sees the new leader at a higher term",
    )
    .await?;
    let new_leader_id = survivor_state.current_leader.expect("just matched Some(..)");
    let new_leader = if new_leader_id == b.node_id() { &b } else { &c };

    wait_for_cluster(
        new_leader,
        |cs| cs.role == ServerState::Leader,
        "the new leader sees itself leading",
    )
    .await?;

    Ok(())
}

/// Removing a node flips `is_member` to `false` on that very node's own `cluster()`, and drops it
/// from every other node's voter set.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_flips_is_member_and_drops_the_voter_on_removal() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let (_, c, _data_c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    let removed_id = c.node_id();

    a.remove_node(removed_id).await?;

    let removed_state = wait_for_cluster(&c, |cs| !cs.is_member, "the removed node sees its own removal").await?;
    assert!(!removed_state.voters.contains(&removed_id));

    wait_for_cluster(
        &a,
        |cs| !cs.voters.contains(&removed_id) && !cs.learners.contains(&removed_id),
        "the leader's own cluster state drops the removed node",
    )
    .await?;
    wait_for_cluster(
        &b,
        |cs| !cs.voters.contains(&removed_id),
        "the other survivor's cluster state drops the removed node",
    )
    .await?;

    Ok(())
}

/// `cluster()` must stay silent through a stretch of ordinary writes: nothing about role, term,
/// current leader, membership or voter/learner sets changes when only the KV changes.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_sends_nothing_for_a_stretch_of_ordinary_writes() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;
    wait_for_cluster(&a, |cs| cs.role == ServerState::Leader, "leader settles first").await?;

    let mut rx = a.cluster();
    // The wait above already consumed every earlier change; a fresh `borrow_and_update` marks
    // the leader's own settled state seen, so `changed()` below can only fire on something new.
    rx.borrow_and_update();

    for i in 0..10 {
        a.write(set(&format!("k{i}"), &format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    // Proving a negative: bounded well above the heartbeat interval so a wrongful update would
    // have every chance to arrive, but far under the harness's overall `WAIT` budget.
    let outcome = tokio::time::timeout(Duration::from_millis(500), rx.changed()).await;
    assert!(
        outcome.is_err(),
        "cluster() must not send an update for ordinary writes, got: {:?}",
        outcome
    );

    Ok(())
}

/// `last_leader_contact()` is always zero on the leader itself.
#[tokio::test(flavor = "multi_thread")]
async fn last_leader_contact_is_zero_on_the_leader() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;
    assert_eq!(Some(Duration::ZERO), a.last_leader_contact());

    // Still zero after this node has proposed and applied a write - nothing about being the
    // leader that just committed something changes the answer.
    a.write(set("k1", "v1")).await.map_err(|e| io::Error::other(e.to_string()))?;
    assert_eq!(Some(Duration::ZERO), a.last_leader_contact());

    Ok(())
}

/// A follower isolated from the leader watches `last_leader_contact()` climb - proven by sampling
/// it before and after a condition wait that itself needs real time to elapse (the isolated node
/// campaigning on its own) - and sees it drop back down once healed.
#[tokio::test(flavor = "multi_thread")]
async fn last_leader_contact_grows_while_isolated_and_drops_back_after_healing() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    // `b` has definitely heard from the leader by the time it is a committed voter.
    let sample_before = b.last_leader_contact().expect("a voter has heard from the leader");

    let term_before_isolation = b.metrics().await.current_term;
    faults.isolate(b.node_id(), &[a.node_id(), c.node_id()]);

    // A condition wait that itself needs real time to elapse: cut off from everyone, `b` cannot
    // hear a heartbeat and so times out and campaigns on its own, which takes at least one
    // election timeout - a genuine event to wait for, not a raw sleep.
    b.wait_metrics(
        WAIT,
        |m| m.current_term > term_before_isolation,
        "the isolated follower campaigns on its own",
    )
    .await?;

    let sample_after = b.last_leader_contact().expect("still recorded from before isolation");
    assert!(
        sample_after > sample_before,
        "last_leader_contact must keep growing while isolated: {:?} then {:?}",
        sample_before,
        sample_after
    );

    // `a` and `c` still form a quorum throughout, so the cluster commits fine while `b` is cut
    // off - proving isolation did not simply stall the whole cluster.
    write_through_leader(&[&a, &c], set("k1", "v1")).await?;

    faults.heal();

    // No stream to await here (there is no `watch::Receiver` for a plain `Duration`), so this
    // polls the same way `tests/common::wait_for_revision` does for a value with no event to
    // subscribe to.
    let deadline = tokio::time::Instant::now() + WAIT.expect("bounded");
    loop {
        if let Some(d) = b.last_leader_contact()
            && d < Duration::from_millis(500)
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::other(
                "last_leader_contact never dropped back down after healing",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    Ok(())
}
