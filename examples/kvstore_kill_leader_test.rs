//! SIGKILL the leader while a writer keeps writing: every write acknowledged to the client must
//! survive on the two remaining nodes, a new leader must be elected, and writes must keep
//! succeeding past the kill.

mod api_client;
mod harness;

use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use api_client::*;
use harness::*;

/// How many sequential writes the background writer attempts across the whole test. Large enough
/// that some land before the kill and some after, small enough that the test does not spend long
/// beyond what the kill and the election themselves need.
const WRITE_COUNT: u32 = 40;

/// The writer waits for at least this many acknowledged writes before the kill is triggered, so
/// the kill lands mid-stream rather than before the cluster has even started serving.
const MIN_ACKED_BEFORE_KILL: usize = 5;

#[tokio::test(flavor = "multi_thread")]
async fn sigkill_leader_while_writing_elects_a_new_leader_and_keeps_serving() -> io::Result<()> {
    let example = Example::build("kvstore")?;

    let addr_pairs = [free_addr_pair(), free_addr_pair(), free_addr_pair()];
    let mut nodes = vec![
        example.spawn(&addr_pairs[0].0, &addr_pairs[0].1, None)?,
        example.spawn(&addr_pairs[1].0, &addr_pairs[1].1, Some(&addr_pairs[0].0))?,
        example.spawn(&addr_pairs[2].0, &addr_pairs[2].1, Some(&addr_pairs[0].0))?,
    ];
    let apis: Vec<String> = addr_pairs.iter().map(|(_, api)| api.clone()).collect();
    let api_refs: Vec<&str> = apis.iter().map(String::as_str).collect();

    // `node_ids[i]` is `nodes[i]`'s own id, learned once every node is up and serving.
    let node_ids = node_ids(&apis, Instant::now() + WAIT).await?;

    // A background writer, sharing nothing with the test but an `Arc<Mutex<..>>` list of every
    // write it got a *definite* `Success` for. `write`'s own request-id reuse (see
    // `api_client::write`) is what makes an `Indeterminate` outcome safe to retry instead of a
    // second, distinct write - so every entry recorded here really happened exactly once.
    let acked: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let writer = {
        let apis = apis.clone();
        let acked = acked.clone();
        tokio::spawn(async move {
            let addrs: Vec<&str> = apis.iter().map(String::as_str).collect();
            for i in 0..WRITE_COUNT {
                let key = format!("k{i}");
                let value = format!("v{i}");
                match write(&addrs, put(&key, &value), Instant::now() + WAIT).await? {
                    TxOutcome::Success { .. } => acked.lock().unwrap().push((key, value)),
                    TxOutcome::Conflict(body) => {
                        return Err(io::Error::other(format!("unexpected conflict on {key}: {body}")));
                    }
                }
                // Pacing only, never synchronization: nothing below depends on this timing.
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            io::Result::Ok(())
        })
    };

    // Wait for a handful of acknowledged writes, then kill whichever node is leading right now.
    let kill_deadline = Instant::now() + WAIT;
    while acked.lock().unwrap().len() < MIN_ACKED_BEFORE_KILL {
        if Instant::now() >= kill_deadline {
            return Err(io::Error::other(
                "writer never produced enough acknowledged writes to proceed",
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    let acked_before_kill = acked.lock().unwrap().len();

    let leader_status = wait_for_status(&apis[0], Instant::now() + WAIT, |s| s["leader_id"].is_u64()).await?;
    let leader_id = leader_status["leader_id"].as_u64().expect("checked by the predicate above");
    let leader_index = node_ids.iter().position(|id| *id == leader_id).ok_or_else(|| {
        io::Error::other(format!(
            "leader id {leader_id} is not one of this test's own three nodes {node_ids:?}"
        ))
    })?;

    let leader_pid = nodes[leader_index].pid();
    nodes[leader_index].kill()?;

    // The writer keeps going against the two survivors (and the now-dead leader's address, which
    // simply fails to connect) until it has attempted every write.
    writer.await.map_err(io::Error::other)??;
    let acked_final = acked.lock().unwrap().clone();
    assert!(
        acked_final.len() > acked_before_kill,
        "no write succeeded after the leader was killed: {acked_before_kill} acked before, {} after",
        acked_final.len()
    );

    // Every acknowledged write survives on both remaining nodes.
    let survivor_apis: Vec<&str> =
        api_refs.iter().copied().enumerate().filter(|(i, _)| *i != leader_index).map(|(_, a)| a).collect();
    for (key, value) in &acked_final {
        for api in &survivor_apis {
            wait_for_value(api, key, Some(value), Instant::now() + WAIT).await?;
        }
    }

    // A new leader took over - some node other than the one just killed.
    let new_leader_status = wait_for_status(survivor_apis[0], Instant::now() + WAIT, |s| {
        s["leader_id"].as_u64().is_some_and(|id| id != leader_id)
    })
    .await?;
    assert_ne!(Some(leader_id), new_leader_status["leader_id"].as_u64());

    // Every child process this test owned is gone - the killed one explicitly, the rest via drop.
    let remaining_pids: Vec<u32> =
        nodes.iter().enumerate().filter(|(i, _)| *i != leader_index).map(|(_, n)| n.pid()).collect();
    drop(nodes);
    assert!(
        !pid_is_alive(leader_pid),
        "the killed leader's process ({leader_pid}) is still alive"
    );
    for pid in remaining_pids {
        assert!(!pid_is_alive(pid), "process {pid} outlived the test");
    }

    example.cleanup()?;
    Ok(())
}
