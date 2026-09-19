//! SIGKILL a follower and restart it on the same data directory: it must catch up with the
//! cluster, and a watch client reconnecting to it with its last-seen revision must get either a
//! `Reset` followed by increments or gap-free increments - folding whatever it receives on top of
//! what it already had must equal the final state.

mod api_client;
mod harness;

use std::collections::BTreeMap;
use std::io;
use std::time::Duration;
use std::time::Instant;

use api_client::*;
use harness::*;

/// The prefix this test watches; `"ready"` (below) deliberately does not match it, so the
/// watch's own folded state never has to account for the readiness write.
const PREFIX: &str = "k";

#[tokio::test(flavor = "multi_thread")]
async fn sigkill_follower_then_restart_catches_up_and_watch_resumes_gap_free() -> io::Result<()> {
    let example = Example::build("kvstore")?;

    let addr_pairs = [free_addr_pair(), free_addr_pair(), free_addr_pair()];
    let mut nodes = vec![
        example.spawn(&addr_pairs[0].0, &addr_pairs[0].1, None)?,
        example.spawn(&addr_pairs[1].0, &addr_pairs[1].1, Some(&addr_pairs[0].0))?,
        example.spawn(&addr_pairs[2].0, &addr_pairs[2].1, Some(&addr_pairs[0].0))?,
    ];
    let addrs: Vec<String> = addr_pairs.iter().map(|(addr, _)| addr.clone()).collect();
    let apis: Vec<String> = addr_pairs.iter().map(|(_, api)| api.clone()).collect();
    let api_refs: Vec<&str> = apis.iter().map(String::as_str).collect();

    // The readiness write also establishes the cluster; its key deliberately falls outside
    // `PREFIX` so it never appears in the watch this test folds.
    write(&api_refs, put("ready", "1"), Instant::now() + WAIT).await?;

    let ids = node_ids(&apis, Instant::now() + WAIT).await?;
    let leader_status = wait_for_status(&apis[0], Instant::now() + WAIT, |s| s["leader_id"].is_u64()).await?;
    let leader_id = leader_status["leader_id"].as_u64().expect("checked by the predicate above");
    let follower_index = ids
        .iter()
        .position(|id| *id != leader_id)
        .expect("a 3-node cluster always has at least one non-leader");

    // Subscribe on the follower we are about to kill, before killing it: a fresh subscription
    // (no `after`) always opens with a `Reset` carrying the current view.
    let mut watch = WatchClient::connect(&apis[follower_index], PREFIX, None).await?;
    let mut fold = Fold::default();
    let opening = watch
        .next_line()
        .await
        .ok_or_else(|| io::Error::other("watch stream ended before delivering anything"))??;
    assert!(
        matches!(opening, WatchLine::Reset { .. }),
        "a subscription with no `after` must open with a reset"
    );
    assert!(fold.state().is_empty(), "no key under {PREFIX:?} exists yet");
    fold.apply(&opening);
    let mut last_seen_revision = opening.revision();

    // Three writes the follower is still alive to see - one `next_line` read per write, since
    // nothing else is writing to this prefix concurrently.
    for i in 0..3 {
        write(
            &api_refs,
            put(&format!("k{i}"), &format!("v{i}")),
            Instant::now() + WAIT,
        )
        .await?;
        let line = tokio::time::timeout(WAIT, watch.next_line())
            .await
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("watch stream ended before delivering a write"))??;
        last_seen_revision = line.revision();
        fold.apply(&line);
    }
    assert_eq!(
        expected_state(0..3),
        *fold.state(),
        "the follower's watch did not deliver the pre-kill writes correctly"
    );

    let follower_pid = nodes[follower_index].pid();
    nodes[follower_index].kill()?;
    drop(watch); // the connection is already dead; drop it explicitly for clarity
    assert!(
        !pid_is_alive(follower_pid),
        "the killed follower's process ({follower_pid}) is still alive"
    );

    // More writes while the follower is down - these are what it has to catch up on.
    for i in 3..6 {
        write(
            &api_refs,
            put(&format!("k{i}"), &format!("v{i}")),
            Instant::now() + WAIT,
        )
        .await?;
    }

    // Restart it on the very same address and (because the example derives its data directory
    // from `--addr`, under this same work directory) the very same data directory. `seed: None`:
    // a node with a persisted id ignores it anyway (see `Example::spawn`'s own docs).
    nodes[follower_index] = example.spawn(&addrs[follower_index], &apis[follower_index], None)?;

    // It catches up: every key written so far, including while it was down, reads back on it.
    for i in 0..6 {
        wait_for_value(
            &apis[follower_index],
            &format!("k{i}"),
            Some(&format!("v{i}")),
            Instant::now() + WAIT,
        )
        .await?;
    }
    wait_for_value(&apis[follower_index], "ready", Some("1"), Instant::now() + WAIT).await?;

    // Reconnect a watch to the restarted node, resuming from the last revision the earlier
    // connection saw. Either a `Reset` or gap-free increments is acceptable (see this file's own
    // doc comment); what must hold is that folding everything received on top of `fold`'s
    // existing state reaches exactly the expected final state.
    let mut resumed = WatchClient::connect(&apis[follower_index], PREFIX, Some(last_seen_revision)).await?;
    let target = wait_for_status(&apis[follower_index], Instant::now() + WAIT, |_| true).await?["revision"]
        .as_u64()
        .expect("`/status` always carries a revision");

    let mut saw_reset_on_resume = false;
    let resume_deadline = Instant::now() + WAIT;
    loop {
        if Instant::now() >= resume_deadline {
            return Err(io::Error::other(format!(
                "watch resume never reached revision {target}; folded so far: {:?}",
                fold.state()
            )));
        }
        let Some(line) =
            tokio::time::timeout(Duration::from_secs(5), resumed.next_line()).await.map_err(io::Error::other)?
        else {
            break;
        };
        let line = line?;
        if matches!(line, WatchLine::Reset { .. }) {
            saw_reset_on_resume = true;
        }
        let revision = line.revision();
        fold.apply(&line);
        if revision >= target {
            break;
        }
    }

    assert_eq!(
        expected_state(0..6),
        *fold.state(),
        "folding the resumed watch did not reach the final state"
    );
    println!(
        "watch resume after restart: {}",
        if saw_reset_on_resume {
            "delivered a Reset"
        } else {
            "delivered gap-free increments"
        }
    );

    let pids: Vec<u32> = nodes.iter().map(Node::pid).collect();
    drop(nodes);
    for pid in pids {
        assert!(!pid_is_alive(pid), "process {pid} outlived the test");
    }

    example.cleanup()?;
    Ok(())
}

/// The expected `k<i>` -> `v<i>` state for `i` in `range`, matching what the test above writes
fn expected_state(range: std::ops::Range<u32>) -> BTreeMap<String, String> {
    range.map(|i| (format!("k{i}"), format!("v{i}"))).collect()
}
