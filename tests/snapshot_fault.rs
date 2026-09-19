//! Traffic-class connection pools under the network fault hook (ech0-media#26,
//! `crate::transport::fault`): delaying only *snapshot* traffic to a lagging follower must not
//! delay the heartbeats keeping it (and the cluster) from ever calling an election it does not
//! need.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::fault::Fault;
use ezraft::fault::Faults;
use ezraft::fault::TrafficClass;

/// A voter whose *replication* traffic (and only that traffic class) is cut off falls behind a
/// purged log exactly as if it were unreachable, while it keeps hearing heartbeats over the
/// separate heartbeat connection pool - so it never calls an election on its own. Once the
/// snapshot the leader now needs to send it is itself delayed (a different class again), no
/// voter's term may change for as long as that delay lasts; once it elapses, the transfer
/// installs.
#[tokio::test(flavor = "multi_thread")]
async fn heartbeats_keep_flowing_while_snapshot_traffic_to_a_lagging_follower_is_delayed() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let (_, c, _data_c) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([a.node_id(), b.node_id(), c.node_id()]),
        "every joined node is a voter",
    )
    .await?;

    // `b`'s replication traffic only: it keeps hearing heartbeats over their own connection pool
    // (so it never calls an election on its own), but its log never advances - `a` and `c` alone
    // still form a majority of the three voters, so writes keep committing without it.
    faults.set_class(a.node_id(), b.node_id(), TrafficClass::Replication, Fault::Drop);

    // The snapshot delay is set now, before `b` ever needs one, rather than right before checking
    // term stability below: setting it only after confirming the purge would leave a window where
    // the leader could already have sent (and `b` already installed) an undelayed snapshot,
    // making the test not actually exercise a slow transfer. Well past an election timeout, while
    // heartbeats to `b` keep flowing unaffected (a different traffic class, a different pool).
    let election_timeout = Duration::from_millis(600); // harness config(): 100ms heartbeat x 6
    let delay = election_timeout * 3;
    faults.set_class(a.node_id(), b.node_id(), TrafficClass::Snapshot, Fault::Delay(delay));

    for i in 0..20 {
        a.write(set(&format!("k{i}"), &format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    wait_for_applied(&c, &a).await?;

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

    // `b` needs a whole snapshot now - openraft cannot replay a log the leader no longer has -
    // and the delay rule set above is already in place for it.
    let term_before = a.metrics().await.current_term;

    // While the snapshot transfer to `b` is stuck in the delay, no voter's term may change.
    // `wait_metrics` with a bound comfortably inside the delay is the condition-based way to
    // assert a negative: if the term ever changed early, this would observe it immediately
    // instead of only at the end of a fixed sleep.
    for node in [&a, &b, &c] {
        let changed = node
            .wait_metrics(
                Some(delay - Duration::from_millis(150)),
                |m| m.current_term > term_before,
                "term must not change while snapshot traffic is delayed",
            )
            .await;
        assert!(
            changed.is_err(),
            "node {} changed term while only snapshot traffic to a lagging follower was delayed",
            node.node_id()
        );
    }

    // The delay elapses and the snapshot installs.
    wait_for_applied(&b, &a).await?;
    assert_eq!(expected_map(0..20), view_map(&b.view()));
    assert_eq!(
        term_before,
        a.metrics().await.current_term,
        "no election happened at any point during the delayed transfer"
    );

    Ok(())
}
