//! Network fault injection (ech0-media#23): drop, delay, and partition requests between chosen
//! node pairs, and heal them again.
//!
//! Every test drives EzRaft through its public API and real HTTP between nodes; the only
//! difference from the other cluster tests is that these nodes share a [`Faults`] handle.

mod common;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::fault::Fault;
use ezraft::fault::Faults;

/// Dropping both directions to one follower stops its applied index from advancing, while the
/// cluster still commits through the other follower; healing lets it catch up.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_both_directions_to_a_follower_stalls_it_until_healed() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let (_, c) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;

    let all = BTreeSet::from([a.node_id(), b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    // Cut B off from the leader in both directions; the cluster still has a quorum through C.
    faults.set(a.node_id(), b.node_id(), Fault::Drop);
    faults.set(b.node_id(), a.node_id(), Fault::Drop);

    a.write(set("k1", "v1")).await?;
    wait_for_applied(&c, &a).await?;

    // B cannot have received a request that was dropped in both directions before it was ever
    // sent: this is a fact about what already happened, not a race to observe in time.
    let target = a.metrics().await.last_log_index.expect("the leader has written an entry");
    let stalled = b.metrics().await.last_applied.map(|log_id| log_id.index);
    assert!(
        stalled < Some(target),
        "a follower cut off from the leader must not have applied the write it missed: {:?} < Some({})",
        stalled,
        target
    );

    faults.clear(a.node_id(), b.node_id());
    faults.clear(b.node_id(), a.node_id());
    wait_for_applied(&b, &a).await?;

    assert_eq!(Some("v1".to_string()), b.read(|app| app.data.get("k1").cloned()).await?);

    Ok(())
}

/// A delay well under the append RPC's timeout slows replication down without breaking it: the
/// delayed follower still catches up, inside the harness's bounded wait.
#[tokio::test(flavor = "multi_thread")]
async fn a_delay_under_the_rpc_timeout_slows_replication_without_breaking_it() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let (_, c) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;

    let all = BTreeSet::from([a.node_id(), b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;

    // The harness heartbeat is 100ms, so the append RPC's soft timeout is 75ms (see
    // `Network::request`); 20ms is comfortably inside that budget.
    faults.set(a.node_id(), b.node_id(), Fault::Delay(Duration::from_millis(20)));
    faults.set(b.node_id(), a.node_id(), Fault::Delay(Duration::from_millis(20)));

    a.write(set("k1", "v1")).await?;
    wait_for_applied(&b, &a).await?;

    assert_eq!(Some("v1".to_string()), b.read(|app| app.data.get("k1").cloned()).await?);

    Ok(())
}

/// Isolating the leader from both followers: they elect a new leader at a higher term without
/// it, a write on the isolated old leader never takes effect, writes through the new leader
/// commit, and healing the network converges every node on the same leader and the same state -
/// with the old leader's uncommitted write present nowhere.
#[tokio::test(flavor = "multi_thread")]
async fn isolating_the_leader_elects_a_new_one_and_drops_its_uncommitted_write() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a) = founding_node_with(config_with_faults(&faults)).await?;
    let (addr_b, b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let (addr_c, c) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;

    let all = BTreeSet::from([a.node_id(), b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    let old_leader = a.node_id();
    let old_term = a.metrics().await.current_term;

    // Cut the leader off from both followers; they still form a quorum between themselves.
    faults.isolate(old_leader, &[b.node_id(), c.node_id()]);

    // The isolated leader cannot commit. Bound the attempt: a leader that still has a moment of
    // lease left proposes an entry it can then never get a quorum for, and openraft's
    // `client_write` has no timeout of its own - without this bound that write would hang the
    // test rather than fail it. Either outcome (a fast, empty `ForwardToLeader` once the lease
    // lapses, or this timeout) proves the same thing: the write never takes effect.
    let write_on_old_leader = tokio::time::timeout(Duration::from_secs(1), a.write(set("stale", "value"))).await;
    assert!(
        !matches!(write_on_old_leader, Ok(Ok(_))),
        "a write on the isolated leader must not succeed: {:?}",
        write_on_old_leader
    );

    // The two remaining voters elect one of themselves, at a higher term.
    b.wait_metrics(
        WAIT,
        |m| matches!(m.current_leader, Some(id) if id != old_leader) && m.current_term > old_term,
        "the remaining voters elect a new leader at a higher term",
    )
    .await?;
    let new_leader_id = b.metrics().await.current_leader.expect("a new leader was just observed");
    let (new_leader, new_leader_addr) = if new_leader_id == b.node_id() {
        (&b, &addr_b)
    } else {
        (&c, &addr_c)
    };

    // Writes through the new leader commit normally.
    assert_eq!(
        Response { value: None },
        http_write(new_leader_addr, set("k1", "v1")).await?
    );

    // Heal the network: every node converges on the same leader.
    faults.heal();
    for node in [&a, &b, &c] {
        node.wait_metrics(
            WAIT,
            |m| m.current_leader == Some(new_leader_id),
            "every node converges on the new leader",
        )
        .await?;
    }

    // And every node applies exactly what the new leader committed.
    for node in [&a, &b, &c] {
        if node.node_id() != new_leader_id {
            wait_for_applied(node, new_leader).await?;
        }
    }

    let expected = BTreeMap::from([("k1".to_string(), "v1".to_string())]);
    for node in [&a, &b, &c] {
        let data = node.read(|app| app.data.clone()).await?;
        assert_eq!(
            expected,
            data,
            "node {} must apply exactly the committed write, and nothing the isolated leader attempted",
            node.node_id()
        );
    }

    Ok(())
}
