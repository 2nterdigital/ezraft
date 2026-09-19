//! End-to-end test of the kvstore example: spawn real `kvstore` processes,
//! form a three-node cluster, and drive it purely over its external API - the
//! same commands the example's doc header shows, minus the terminals.

mod api_client;
mod harness;

use std::io;
use std::time::Instant;

use api_client::*;
use harness::*;

#[tokio::test(flavor = "multi_thread")]
async fn three_node_kvstore_cluster_writes_and_reads() -> io::Result<()> {
    let example = Example::build("kvstore")?;

    let (addr_a, api_a) = free_addr_pair();
    let a = example.spawn(&addr_a, &api_a, None)?;

    // The first write doubles as the readiness wait: it only succeeds once the
    // founding node serves HTTP and leads. A brand-new cluster's first
    // transaction always lands at revision 1.
    let deadline = Instant::now() + WAIT;
    match write(&[&api_a], put("k1", "v1"), deadline).await? {
        TxOutcome::Success { revision, .. } => assert_eq!(1, revision),
        other => panic!("expected the first write to succeed, got {other:?}"),
    }

    let (addr_b, api_b) = free_addr_pair();
    let (addr_c, api_c) = free_addr_pair();
    let b = example.spawn(&addr_b, &api_b, Some(&addr_a))?;
    let c = example.spawn(&addr_c, &api_c, Some(&addr_a))?;

    // Replication must deliver the pre-join write to both joiners.
    wait_for_value(&api_b, "k1", Some("v1"), Instant::now() + WAIT).await?;
    wait_for_value(&api_c, "k1", Some("v1"), Instant::now() + WAIT).await?;

    // Any node accepts a write (forwarding to the leader is the foundation's own job), so every
    // write below is tried against all three addresses.
    let apis = [api_a.as_str(), api_b.as_str(), api_c.as_str()];
    write(&apis, put("k2", "v2"), Instant::now() + WAIT).await?;
    write(&apis, delete("k1"), Instant::now() + WAIT).await?;
    write(&apis, put("k2", "v2b"), Instant::now() + WAIT).await?;

    // Every node converges on the exact final state: the overwritten key reads
    // back everywhere, and the deleted key answers with no entry at all.
    for api in &apis {
        wait_for_value(api, "k2", Some("v2b"), Instant::now() + WAIT).await?;
        wait_for_value(api, "k1", None, Instant::now() + WAIT).await?;
    }

    // A conflicting condition is reported as a conflict, not silently ignored or applied.
    let conflicting = serde_json::json!({
        "conditions": [{"ModRevEq": {"key": "k2", "mod_rev": 999}}],
        "ops": [{"Put": {"key": "k2", "value": b64("nope")}}],
    });
    match write(&apis, conflicting, Instant::now() + WAIT).await? {
        TxOutcome::Conflict(body) => {
            assert_eq!(
                Some("conflict"),
                body["error"].as_str(),
                "unexpected conflict body: {body}"
            );
        }
        other => panic!("expected the mismatched condition to conflict, got {other:?}"),
    }
    // The conflict changed nothing.
    wait_for_value(&api_a, "k2", Some("v2b"), Instant::now() + WAIT).await?;

    // One read answering with every match, which is the shape a point lookup cannot express.
    let prefixed: Vec<ezraft::KeyValue> = reqwest::Client::new()
        .get(format!("http://{api_a}/view"))
        .query(&[("prefix", "k")])
        .send()
        .await
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)?;
    assert_eq!(1, prefixed.len(), "unexpected /view?prefix= body: {prefixed:?}");
    assert_eq!("k2", prefixed[0].key);
    assert_eq!("v2b", text(&prefixed[0].value));

    let pids = [a.pid(), b.pid(), c.pid()];
    drop(a);
    drop(b);
    drop(c);
    for pid in pids {
        assert!(!pid_is_alive(pid), "process {pid} outlived the test");
    }
    example.cleanup()?;
    Ok(())
}
