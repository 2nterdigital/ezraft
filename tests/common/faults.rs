//! Cluster helpers for the `fault-injection` hook: build a cluster whose nodes share one
//! [`Faults`] handle, so a test can drop, delay, or partition traffic between them and heal it
//! again.
//!
//! Additive to `tests/common/mod.rs`: node-building itself is not duplicated here - the fault
//! handle is just a field on the `EzConfig` every `*_with` builder in `mod.rs` already takes.

use ezraft::EzConfig;
use ezraft::fault::Faults;

use super::config;

/// The harness's usual test config (short heartbeat), with `faults` attached
///
/// Every node built with the config this returns shares `faults`: pass the same handle to every
/// `founding_node_with`/`joined_voter_with`/`joined_learner_with` call that makes up one test
/// cluster.
pub fn config_with_faults(faults: &Faults) -> EzConfig {
    EzConfig {
        faults: Some(faults.clone()),
        ..config()
    }
}
