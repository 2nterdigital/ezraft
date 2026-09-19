//! Typed inter-node RPC (ech0-media#29): `kv.rpc()`'s `register`/`call`, addressed by node id
//! through the current membership, with the crate's full error taxonomy and no automatic retry.
//!
//! Every test drives `EzKv` through its public API and real HTTP between nodes; the fault
//! scenarios use the `fault-injection` hook (`tests/common/faults.rs`), never timing luck.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use ezraft::RpcError;
use ezraft::RpcRegisterError;
use ezraft::fault::Fault;
use ezraft::fault::Faults;

/// Generous enough that a real localhost round trip never times out on its own
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// A node calling a handler registered on another node by id gets the typed result back.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_calls_a_handler_registered_on_another_node_and_gets_a_typed_result() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "b joined as a voter").await?;

    b.rpc()
        .register::<u64, u64, std::convert::Infallible, _, _>("double", |req: u64| async move { Ok(req * 2) })
        .expect("registering a fresh name succeeds");

    let result = a.rpc().call::<u64, u64>(b.node_id(), "double", 21, RPC_TIMEOUT).await;
    assert_eq!(42, result.expect("the typed result comes back"));

    Ok(())
}

/// Calling this node's own id works the same as calling any other, with no HTTP round trip.
#[tokio::test(flavor = "multi_thread")]
async fn calling_this_nodes_own_id_works_the_same_as_calling_a_peer() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    a.rpc()
        .register::<u64, u64, std::convert::Infallible, _, _>("double", |req: u64| async move { Ok(req * 2) })
        .expect("registering a fresh name succeeds");

    let result = a.rpc().call::<u64, u64>(a.node_id(), "double", 21, RPC_TIMEOUT).await;
    assert_eq!(42, result.expect("a local call returns the typed result too"));

    Ok(())
}

/// A handler's own error travels back as `Remote`, with a message that names it.
#[tokio::test(flavor = "multi_thread")]
async fn a_handler_error_comes_back_as_remote() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "b joined as a voter").await?;

    b.rpc()
        .register::<(), (), String, _, _>("fails", |()| async move { Err("boom".to_string()) })
        .expect("registering a fresh name succeeds");

    let err = a.rpc().call::<(), ()>(b.node_id(), "fails", (), RPC_TIMEOUT).await.unwrap_err();
    match err {
        RpcError::Remote(msg) => assert!(msg.contains("boom"), "{msg}"),
        other => panic!("expected Remote, got {other:?}"),
    }

    Ok(())
}

/// Calling a name nobody registered answers `Remote`, naming the unknown name.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_name_is_remote() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "b joined as a voter").await?;

    let err = a.rpc().call::<(), ()>(b.node_id(), "no-such-name", (), RPC_TIMEOUT).await.unwrap_err();
    match err {
        RpcError::Remote(msg) => assert!(msg.contains("no-such-name"), "{msg}"),
        other => panic!("expected Remote, got {other:?}"),
    }

    Ok(())
}

/// Registering a name under the `ez.` prefix is refused: it is reserved for the foundation's own
/// later handlers.
#[tokio::test(flavor = "multi_thread")]
async fn registering_an_ez_prefixed_name_is_refused() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    let err = a
        .rpc()
        .register::<(), (), std::convert::Infallible, _, _>("ez.internal", |()| async move { Ok(()) })
        .unwrap_err();
    assert!(matches!(err, RpcRegisterError::Reserved(name) if name == "ez.internal"));

    Ok(())
}

/// Registering the same name twice through the public API is refused, not silently replaced.
#[tokio::test(flavor = "multi_thread")]
async fn registering_the_same_name_twice_is_refused() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;

    a.rpc()
        .register::<(), (), std::convert::Infallible, _, _>("once", |()| async move { Ok(()) })
        .expect("the first registration succeeds");
    let err = a
        .rpc()
        .register::<(), (), std::convert::Infallible, _, _>("once", |()| async move { Ok(()) })
        .unwrap_err();
    assert!(matches!(err, RpcRegisterError::AlreadyRegistered(name) if name == "once"));

    Ok(())
}

/// A target that has been removed from the membership answers `NotMember`.
#[tokio::test(flavor = "multi_thread")]
async fn a_target_removed_from_membership_is_not_member() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let b_id = b.node_id();
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "b joined as a voter").await?;

    a.remove_node(b_id).await.map_err(|e| io::Error::other(e.to_string()))?;
    a.wait_metrics(
        WAIT,
        |m| m.membership_config.membership().get_node(&b_id).is_none(),
        "b left the membership",
    )
    .await?;

    let err = a.rpc().call::<(), ()>(b_id, "whatever", (), RPC_TIMEOUT).await.unwrap_err();
    assert!(matches!(err, RpcError::NotMember(id) if id == b_id));

    Ok(())
}

/// A member whose process is gone (shut down) answers `Unreachable`.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_whose_process_is_gone_is_unreachable() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;
    let (_, b, _data_b) = joined_voter(&addr_a).await?;
    let b_id = b.node_id();
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "b joined as a voter").await?;

    b.shutdown().await.map_err(|e| io::Error::other(e.to_string()))?;

    let err = a.rpc().call::<(), ()>(b_id, "whatever", (), RPC_TIMEOUT).await.unwrap_err();
    assert!(
        matches!(err, RpcError::Unreachable { node_id, .. } if node_id == b_id),
        "{err:?}"
    );

    Ok(())
}

/// An injected delay past the call's own timeout answers `Timeout`, distinct from `Unreachable`.
#[tokio::test(flavor = "multi_thread")]
async fn an_injected_delay_past_the_timeout_is_a_timeout() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let b_id = b.node_id();
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "b joined as a voter").await?;

    b.rpc()
        .register::<(), (), std::convert::Infallible, _, _>("slow", |()| async move { Ok(()) })
        .expect("registering a fresh name succeeds");

    let short_timeout = Duration::from_millis(20);
    faults.set(a.node_id(), b_id, Fault::Delay(short_timeout * 5));

    let err = a.rpc().call::<(), ()>(b_id, "slow", (), short_timeout).await.unwrap_err();
    assert!(
        matches!(err, RpcError::Timeout { node_id, .. } if node_id == b_id),
        "{err:?}"
    );

    Ok(())
}

/// No automatic retry, ever: a handler whose response is dropped (the request itself still
/// reaches it and runs) is invoked exactly once, not retried after the caller sees a failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_response_means_the_handler_ran_exactly_once_with_no_retry() -> io::Result<()> {
    let faults = Faults::new();
    let (addr_a, a, _data_a) = founding_node_with(config_with_faults(&faults)).await?;
    let (_, b, _data_b) = joined_voter_with(&addr_a, config_with_faults(&faults)).await?;
    let b_id = b.node_id();
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "b joined as a voter").await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    b.rpc()
        .register::<(), (), std::convert::Infallible, _, _>("count", move |()| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .expect("registering a fresh name succeeds");

    faults.set(a.node_id(), b_id, Fault::DropResponse);

    let result = a.rpc().call::<(), ()>(b_id, "count", (), RPC_TIMEOUT).await;
    assert!(result.is_err(), "a dropped response is a failure to the caller");
    assert_eq!(
        1,
        calls.load(Ordering::SeqCst),
        "the handler ran exactly once, with no retry"
    );

    Ok(())
}
