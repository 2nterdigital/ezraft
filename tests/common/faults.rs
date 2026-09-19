//! Cluster helpers for the `fault-injection` hook: build a cluster whose nodes share one
//! [`Faults`] handle, so a test can drop, delay, or partition traffic between them and heal it
//! again.
//!
//! Additive to `tests/common/mod.rs`: every helper here is a `_with`-suffixed sibling of one
//! there, taking an explicit [`EzConfig`] instead of building the harness's own.

use std::io;

use ezraft::EzConfig;
use ezraft::EzRaft;
use ezraft::fault::Faults;

use super::KvSm;
use super::config;
use super::free_addr;
use super::node_storage;
use super::spawn_serve;

/// The harness's usual test config (short heartbeat), with `faults` attached
///
/// Every node built with the config this returns shares `faults`: pass the same handle to every
/// `*_with` call that makes up one test cluster.
pub fn config_with_faults(faults: &Faults) -> EzConfig {
    EzConfig {
        faults: Some(faults.clone()),
        ..config()
    }
}

/// A founding node, serving and leading, built with an explicit config
///
/// The `_with` sibling of [`super::founding_node`], for a cluster that needs
/// [`config_with_faults`] instead of the plain test config.
pub async fn founding_node_with(config: EzConfig) -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::create(&addr, KvSm::default(), node_storage().await?, config).await?;
    spawn_serve(&node);

    // See `super::founding_node`: leading is not enough, the founding membership must be
    // committed too.
    node.wait_metrics(
        super::WAIT,
        |m| m.current_leader == Some(0) && m.committed_membership_config.log_id().is_some(),
        "founding node leads, with its own membership committed",
    )
    .await?;
    Ok((addr, node))
}

/// A node that joined asking to be a voter, serving, built with an explicit config
///
/// The `_with` sibling of [`super::joined_voter`].
pub async fn joined_voter_with(seed: &str, config: EzConfig) -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::join(&addr, seed, KvSm::default(), node_storage().await?, config).await?;
    spawn_serve(&node);
    Ok((addr, node))
}

/// A node that joined as a learner, serving, built with an explicit config
///
/// The `_with` sibling of [`super::joined_learner`].
pub async fn joined_learner_with(seed: &str, config: EzConfig) -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::join_as_learner(&addr, seed, KvSm::default(), node_storage().await?, config).await?;
    spawn_serve(&node);
    Ok((addr, node))
}
