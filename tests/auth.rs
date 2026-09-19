//! Cluster-token authentication for the internal port (ech0-media#29): a request without the
//! right `Authorization: Bearer <token>` is turned away before any handler runs, on every kind of
//! route the internal port serves - Raft, admin, and RPC alike - and a node whose token does not
//! match the rest of the cluster cannot join it.
//!
//! Every test drives `EzKv` through its public API, and drives the raw HTTP protocol directly
//! (`tests/common::raw_post`) where the point is exactly what an unauthenticated request sees.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::RpcError;
use serde_json::json;

/// A cluster token guaranteed not to be [`CLUSTER_TOKEN`]
const WRONG_TOKEN: &str = "wrong-cluster-token";

/// Every Raft and admin `POST` route answers `401` to a missing or wrong token, before any
/// handler runs: the body is empty, never the substantive JSON any of these handlers would
/// otherwise answer with (a `VoteResponse`, a `Redirect`, a fresh node id, ...).
#[tokio::test(flavor = "multi_thread")]
async fn raft_and_admin_post_routes_reject_missing_and_wrong_tokens() -> io::Result<()> {
    let (addr_a, _a, _data_a) = founding_node().await?;
    wait_until_serving(&addr_a).await?;

    let routes: [(&str, serde_json::Value); 6] = [
        ("raft/append", json!({})),
        ("raft/vote", json!({})),
        ("raft/snapshot", json!({})),
        ("raft/transfer_leader", json!({})),
        ("api/node_id", json!({})),
        ("api/membership", json!({"op": "Remove", "node_id": 999})),
    ];

    for (path, body) in routes {
        for token in [None, Some(WRONG_TOKEN)] {
            let resp = raw_post(&addr_a, path, token, &body).await?;
            assert_eq!(
                reqwest::StatusCode::UNAUTHORIZED,
                resp.status(),
                "{path} with token {token:?}"
            );
            let text = resp.text().await.map_err(io::Error::other)?;
            assert!(text.is_empty(), "{path} with token {token:?} leaked a body: {text}");
        }
    }

    Ok(())
}

/// `GET /api/metrics` answers `401` the same way, missing or wrong token alike - and the same
/// request with the right token does succeed, so the `401`s above are not a sign the route
/// itself is broken.
#[tokio::test(flavor = "multi_thread")]
async fn metrics_route_rejects_missing_and_wrong_tokens_but_serves_the_right_one() -> io::Result<()> {
    let (addr_a, _a, _data_a) = founding_node().await?;
    wait_until_serving(&addr_a).await?;
    let client = reqwest::Client::new();

    for token in [None, Some(WRONG_TOKEN)] {
        let mut req = client.get(format!("http://{addr_a}/api/metrics"));
        if let Some(token) = token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(io::Error::other)?;
        assert_eq!(reqwest::StatusCode::UNAUTHORIZED, resp.status(), "token {token:?}");
    }

    let resp = client
        .get(format!("http://{addr_a}/api/metrics"))
        .bearer_auth(CLUSTER_TOKEN)
        .send()
        .await
        .map_err(io::Error::other)?;
    assert!(
        resp.status().is_success(),
        "the right token must still work: {}",
        resp.status()
    );

    Ok(())
}

/// `POST /api/membership` with the wrong token changes nothing: the membership before and after
/// is byte-for-byte identical, which is what "the handler never ran" means for this route.
#[tokio::test(flavor = "multi_thread")]
async fn membership_route_with_wrong_token_changes_nothing() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    wait_until_serving(&addr_a).await?;
    let before = a.metrics().await.membership_config.membership().clone();

    let resp = raw_post(
        &addr_a,
        "api/membership",
        Some(WRONG_TOKEN),
        &json!({"op": "Add", "node_id": 7, "addr": "127.0.0.1:1"}),
    )
    .await?;
    assert_eq!(reqwest::StatusCode::UNAUTHORIZED, resp.status());

    let after = a.metrics().await.membership_config.membership().clone();
    assert_eq!(
        before, after,
        "a request with the wrong token must not change membership"
    );

    Ok(())
}

/// A raw call to the RPC route with a missing or wrong token never reaches the registered
/// handler - a call counter stays at zero - and the same request with the right token does reach
/// it, so the zero above is not an artifact of a route that never dispatches at all.
#[tokio::test(flavor = "multi_thread")]
async fn rpc_route_rejects_missing_and_wrong_tokens_without_invoking_the_handler() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    wait_until_serving(&addr_a).await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    a.rpc()
        .register::<(), (), std::convert::Infallible, _, _>("count", move |()| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .expect("registering a fresh name succeeds");

    for token in [None, Some(WRONG_TOKEN)] {
        let resp = raw_post(&addr_a, "rpc/count", token, &json!(null)).await?;
        assert_eq!(reqwest::StatusCode::UNAUTHORIZED, resp.status(), "token {token:?}");
    }
    assert_eq!(0, calls.load(Ordering::SeqCst), "the handler must not have run");

    let resp = raw_post(&addr_a, "rpc/count", Some(CLUSTER_TOKEN), &json!(null)).await?;
    assert!(
        resp.status().is_success(),
        "the right token must still reach the handler"
    );
    assert_eq!(1, calls.load(Ordering::SeqCst));

    Ok(())
}

/// A node configured with a different cluster token cannot join: the seed refuses its admin
/// requests with a clear, immediate error (never retried away as if it were transient), and the
/// cluster's membership is left untouched.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_with_a_different_token_cannot_join() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let before = a.metrics().await.membership_config.membership().clone();

    let addr_b = free_addr();
    let data_b = NodeData::new();
    let mismatched = EzConfig {
        cluster_token: WRONG_TOKEN.to_string(),
        ..config()
    };
    let err = match EzKv::join(&addr_b, &addr_a, data_b.open().await?, mismatched).await {
        Ok(_) => panic!("a node with a different cluster token must not be allowed to join"),
        Err(e) => e,
    };
    assert_eq!(
        io::ErrorKind::PermissionDenied,
        err.kind(),
        "a token mismatch is a permission error, not a generic one: {err}"
    );

    let after = a.metrics().await.membership_config.membership().clone();
    assert_eq!(before, after, "a rejected join must not have changed membership");

    Ok(())
}

/// `rpc.call` against a peer whose cluster token no longer matches this node's answers
/// `Unauthorized`, not `Unreachable` or a generic `Remote` error.
///
/// The scenario: a voter restarted with a different token than the rest of the cluster - the one
/// way a member's token can legitimately drift, since restarting reuses the persisted node id and
/// never re-contacts the seed (`EzKv::join`'s own doc comment), so it never goes through the join
/// handshake the test above shows a mismatched token cannot pass.
#[tokio::test(flavor = "multi_thread")]
async fn rpc_call_against_a_peer_with_a_mismatched_token_is_unauthorized() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (addr_b, b, data_b) = joined_voter(&addr_a).await?;
    let b_id = b.node_id();
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "b joined as a voter").await?;

    b.shutdown().await?;
    drop(b);

    let mismatched = EzConfig {
        cluster_token: WRONG_TOKEN.to_string(),
        ..config()
    };
    let b2 = EzKv::join(&addr_b, &addr_a, data_b.open().await?, mismatched).await?;
    assert_eq!(b_id, b2.node_id(), "a restart keeps the persisted node id");
    spawn_serve(&b2);

    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster waits are bounded");
    loop {
        match a.rpc().call::<(), ()>(b_id, "whatever", (), Duration::from_millis(200)).await {
            Err(RpcError::Unauthorized(id)) => {
                assert_eq!(b_id, id);
                return Ok(());
            }
            // B's server may not have finished (re)binding the address yet; that transient
            // window is not what this test is about.
            Err(RpcError::Unreachable { .. }) if tokio::time::Instant::now() < deadline => continue,
            other => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "gave up waiting for Unauthorized, last result: {other:?}"
                );
            }
        }
    }
}
