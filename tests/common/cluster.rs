//! Helpers for driving and asserting on `cluster()`/`watch_registry()` streams: see
//! `tests/cluster_state.rs`, `tests/registry.rs`.
//!
//! Additive to `tests/common/mod.rs`, the same way `tests/common/watch.rs` is for the KV's own
//! watch streams: both wait for a predicate to hold by driving the stream's own change
//! notification (`tokio::sync::watch::Receiver::changed`), never by polling.

use std::io;

use ezraft::ClusterState;
use ezraft::EzKv;
use ezraft::Registry;
use tokio::sync::watch;

use super::WAIT;

/// Wait until `node`'s [`EzKv::cluster`] stream reports a [`ClusterState`] matching `predicate`
pub async fn wait_for_cluster<F>(node: &EzKv, predicate: F, reason: &str) -> io::Result<ClusterState>
where F: Fn(&ClusterState) -> bool {
    wait_for_watch(&mut node.cluster(), predicate, reason).await
}

/// Wait until `node`'s [`EzKv::watch_registry`] stream reports a [`Registry`] matching `predicate`
pub async fn wait_for_registry<F>(node: &EzKv, predicate: F, reason: &str) -> io::Result<Registry>
where F: Fn(&Registry) -> bool {
    wait_for_watch(&mut node.watch_registry(), predicate, reason).await
}

/// The condition wait both helpers above share: check the stream's current value first (so a
/// predicate already satisfied returns immediately), then wait for the next change and check
/// again, bounded by [`WAIT`] overall.
async fn wait_for_watch<T, F>(rx: &mut watch::Receiver<T>, predicate: F, reason: &str) -> io::Result<T>
where
    T: Clone,
    F: Fn(&T) -> bool,
{
    let deadline = tokio::time::Instant::now() + WAIT.expect("cluster/registry waits are bounded");

    loop {
        let current = rx.borrow_and_update().clone();
        if predicate(&current) {
            return Ok(current);
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::other(format!(
                "never satisfied within the wait budget: {reason}"
            )));
        }

        match tokio::time::timeout(remaining, rx.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) => return Err(io::Error::other(format!("the stream ended before: {reason}"))),
            Err(_elapsed) => return Err(io::Error::other(format!("timed out waiting for: {reason}"))),
        }
    }
}
