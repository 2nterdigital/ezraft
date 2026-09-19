//! End-to-end test of the kvstore example: spawn real `kvstore` processes,
//! form a three-node cluster, and drive it purely over HTTP - the same
//! commands the example's doc header shows, minus the terminals.

mod harness;

use std::io;
use std::time::Instant;

use harness::*;
use serde_json::Value;
use serde_json::json;

/// POST `req` to `/api/write` on whichever of `addrs` currently accepts it.
///
/// Only the leader does - `EzKv::write` has no forwarding (ech0-media#30) - so this tries every
/// address in turn and retries the whole round while the cluster is still forming or between an
/// election and the next write.
async fn write(addrs: &[&str], req: Value) -> io::Result<Value> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + WAIT;
    loop {
        for addr in addrs {
            if let Ok(resp) = client.post(format!("http://{}/api/write", addr)).json(&req).send().await
                && resp.status().is_success()
            {
                return resp.json().await.map_err(io::Error::other);
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "no node in {:?} accepted {:?} within {:?}",
                addrs, req, WAIT
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Poll a node's `/api/read` endpoint until `key` serves exactly `expected`,
/// where `None` is a key the node does not hold - the app answers that with no
/// entry. Getting there proves the state reached that node's own state machine.
async fn wait_for_key(addr: &str, key: &str, expected: Option<&str>) -> io::Result<()> {
    let client = reqwest::Client::new();
    let want = expected.map_or_else(|| json!({}), |value| json!({ key: value }));
    let deadline = Instant::now() + WAIT;
    let mut last = None;
    loop {
        // An error is the node's HTTP server still binding; retry until the deadline.
        let get = json!({"op": "Get", "key": key});
        if let Ok(resp) = client.post(format!("http://{}/api/read", addr)).json(&get).send().await {
            let status = resp.status();
            if !status.is_success() {
                return Err(io::Error::other(format!(
                    "read {} from {} failed: {}",
                    key, addr, status
                )));
            }

            let entries: Value = resp.json().await.map_err(io::Error::other)?;
            if entries == want {
                return Ok(());
            }
            last = Some(entries);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "{} never served {:?} for {}, last seen: {:?}",
                addr, expected, key, last
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_node_kvstore_cluster_writes_and_reads() -> io::Result<()> {
    let example = Example::build("kvstore")?;

    let addr_a = free_addr();
    let a = example.spawn(&addr_a, None)?;

    // The first write doubles as the readiness wait: it only succeeds once the
    // founding node serves HTTP and leads. A brand-new cluster's first
    // transaction always lands at revision 1.
    assert_eq!(
        json!({"revision": 1}),
        write(&[&addr_a], json!({"op": "Set", "key": "k1", "value": "v1"})).await?
    );

    let addr_b = free_addr();
    let addr_c = free_addr();
    let b = example.spawn(&addr_b, Some(&addr_a))?;
    let c = example.spawn(&addr_c, Some(&addr_a))?;

    // Replication must deliver the pre-join write to both joiners.
    wait_for_key(&addr_b, "k1", Some("v1")).await?;
    wait_for_key(&addr_c, "k1", Some("v1")).await?;

    // Only the leader accepts a write, and which node that is may have changed
    // since the first write (joining two more voters can trigger an election),
    // so every write below is tried against all three addresses.
    let addrs = [addr_a.as_str(), addr_b.as_str(), addr_c.as_str()];
    write(&addrs, json!({"op": "Set", "key": "k2", "value": "v2"})).await?;
    write(&addrs, json!({"op": "Delete", "key": "k1"})).await?;
    write(&addrs, json!({"op": "Set", "key": "k2", "value": "v2b"})).await?;

    // Every node converges on the exact final state: the overwritten key reads
    // back everywhere, and the deleted key answers with no entry at all.
    for addr in &addrs {
        wait_for_key(addr, "k2", Some("v2b")).await?;
        wait_for_key(addr, "k1", None).await?;
    }

    // One read answering with every match, which is the shape a key-at-a-time
    // request cannot express.
    let prefixed: Value = reqwest::Client::new()
        .post(format!("http://{}/api/read", addr_a))
        .json(&json!({"op": "Prefix", "prefix": "k"}))
        .send()
        .await
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)?;
    assert_eq!(json!({"k2": "v2b"}), prefixed);

    drop(a);
    drop(b);
    drop(c);
    example.cleanup()?;
    Ok(())
}
