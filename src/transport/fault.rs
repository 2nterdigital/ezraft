//! The `fault-injection` hook: an in-process test can drop, delay, or hide the response of
//! requests between chosen directed node pairs, and heal them again.
//!
//! This module exists only when the `fault-injection` cargo feature is enabled. A normal build
//! has none of it: no [`Fault`] or [`Faults`] type, no `faults` field on
//! [`EzConfig`](crate::EzConfig), no branch in `Transport::send`.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use reqwest::Client;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Transport;
use super::TransportError;

/// One fault a directed node pair can be given
///
/// A rule for `(from, to)` affects only requests `from` sends `to`; the opposite direction needs
/// its own rule, which is what [`Faults::partition`] and [`Faults::isolate`] set up for both
/// directions at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The request is never sent; the caller sees the same error as an unreachable peer.
    Drop,
    /// Wait this long before sending, inside the caller's timeout budget: a delay at least as
    /// long as that budget is reported as a timeout, and the peer never sees the request.
    Delay(Duration),
    /// Send the request and let the peer process it, but discard the response and report a
    /// failure to the caller - the caller's view of the outcome is genuinely unknown.
    DropResponse,
}

/// A cheap, `Clone`-able handle onto one test cluster's fault rules
///
/// Every node of one in-process test cluster shares one `Faults`. It is never process-global:
/// test binaries run tests in parallel, and each test's cluster needs its own independent rules.
/// Rules are keyed by directed node-id pair and take effect immediately for requests sent after
/// they are set.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    rules: Arc<Mutex<HashMap<(u64, u64), Fault>>>,
}

impl Faults {
    /// A handle with no rules set
    pub fn new() -> Self {
        Self::default()
    }

    /// Give requests from `from` to `to` `fault`, replacing any rule already set for that pair
    pub fn set(&self, from: u64, to: u64, fault: Fault) {
        self.rules.lock().unwrap().insert((from, to), fault);
    }

    /// Remove the rule for `from -> to`, if any; requests between them go through normally again
    pub fn clear(&self, from: u64, to: u64) {
        self.rules.lock().unwrap().remove(&(from, to));
    }

    /// Drop every request between the two groups, in both directions
    pub fn partition(&self, group_a: &[u64], group_b: &[u64]) {
        for &a in group_a {
            for &b in group_b {
                self.set(a, b, Fault::Drop);
                self.set(b, a, Fault::Drop);
            }
        }
    }

    /// Drop every request between `node` and each of `others`, in both directions
    pub fn isolate(&self, node: u64, others: &[u64]) {
        self.partition(&[node], others);
    }

    /// Clear every rule; every pair goes through normally again
    pub fn heal(&self) {
        self.rules.lock().unwrap().clear();
    }

    /// The rule for `from -> to`, if any
    fn get(&self, from: u64, to: u64) -> Option<Fault> {
        self.rules.lock().unwrap().get(&(from, to)).copied()
    }
}

impl Transport {
    /// The fault rule for a request this node is about to send `target`, if any
    pub(super) fn fault_for(&self, target: u64) -> Option<Fault> {
        self.faults.as_ref().and_then(|faults| faults.get(self.from, target))
    }

    /// Apply `fault` instead of - or, for [`Fault::DropResponse`], in addition to - the normal
    /// send `Transport::send`/`Transport::send_rpc` would otherwise make
    ///
    /// `client` is whichever pool the caller (`send` or `send_rpc`) was about to use, so a
    /// faulted request that ends up sent "for real" (`Delay`, `DropResponse`) still goes out on
    /// the right traffic class.
    pub(super) async fn send_faulty<Req, Resp, Err>(
        &self,
        client: &Client,
        fault: Fault,
        target: u64,
        addr: &str,
        path: impl Display,
        timeout: Duration,
        req: Req,
    ) -> Result<Result<Resp, Err>, TransportError>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        match fault {
            Fault::Drop => Err(TransportError::Unreachable(format!(
                "fault-injection: dropped {} -> {target}",
                self.from
            ))),

            Fault::Delay(delay) if delay >= timeout => {
                // The delay alone exhausts the caller's budget, so the peer never sees the
                // request: reported as `Timeout`, not `Unreachable` - the peer is not the
                // problem here, the caller's own budget ran out. openraft does not need the
                // difference (`crate::network` maps both to one `RPCError::Unreachable`), but
                // `crate::rpc::RpcError` does.
                tokio::time::sleep(timeout).await;
                Err(TransportError::Timeout(format!(
                    "fault-injection: delay {delay:?} at or past the {timeout:?} timeout {} -> {target}",
                    self.from
                )))
            }
            Fault::Delay(delay) => {
                tokio::time::sleep(delay).await;
                self.send_now(client, target, addr, path, timeout - delay, req).await
            }

            Fault::DropResponse => {
                // The peer applies the request; only this side's view of the outcome is lost.
                let _ = self.send_now::<Req, Resp, Err>(client, target, addr, path, timeout, req).await;
                Err(TransportError::Other(format!(
                    "fault-injection: response dropped {} -> {target}",
                    self.from
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use openraft::error::Infallible;

    use super::*;

    #[test]
    fn set_clear_round_trip() {
        let faults = Faults::new();
        assert_eq!(None, faults.get(1, 2));

        faults.set(1, 2, Fault::Drop);
        assert_eq!(Some(Fault::Drop), faults.get(1, 2));

        faults.clear(1, 2);
        assert_eq!(None, faults.get(1, 2));
    }

    #[test]
    fn rules_are_directed() {
        let faults = Faults::new();
        faults.set(1, 2, Fault::Drop);
        assert_eq!(None, faults.get(2, 1), "a rule for 1 -> 2 must not affect 2 -> 1");
    }

    #[test]
    fn partition_drops_both_directions_between_every_cross_pair_only() {
        let faults = Faults::new();
        faults.partition(&[1, 2], &[3, 4]);

        for (a, b) in [(1, 3), (1, 4), (2, 3), (2, 4), (3, 1), (4, 1), (3, 2), (4, 2)] {
            assert_eq!(Some(Fault::Drop), faults.get(a, b), "{a} -> {b}");
        }
        assert_eq!(None, faults.get(1, 2), "partition does not touch pairs within a group");
        assert_eq!(None, faults.get(3, 4), "partition does not touch pairs within a group");
    }

    #[test]
    fn isolate_is_a_partition_of_one_node_against_the_rest() {
        let faults = Faults::new();
        faults.isolate(1, &[2, 3]);

        for (a, b) in [(1, 2), (2, 1), (1, 3), (3, 1)] {
            assert_eq!(Some(Fault::Drop), faults.get(a, b), "{a} -> {b}");
        }
        assert_eq!(
            None,
            faults.get(2, 3),
            "isolate does not touch the others between themselves"
        );
    }

    #[test]
    fn heal_clears_every_rule() {
        let faults = Faults::new();
        faults.partition(&[1], &[2, 3]);

        faults.heal();

        assert_eq!(None, faults.get(1, 2));
        assert_eq!(None, faults.get(1, 3));
    }

    #[tokio::test]
    async fn a_dropped_request_is_unreachable_without_being_sent() {
        let faults = Faults::new();
        faults.set(1, 2, Fault::Drop);
        let transport = Transport::new(1, "test-token").unwrap().with_faults(Some(faults));

        // Nothing is listening at this address: a real send would itself fail, but a dropped
        // request must fail before it ever gets that far.
        let err = transport
            .send::<_, (), Infallible>(2, "127.0.0.1:1", "x", Duration::from_secs(5), ())
            .await
            .expect_err("a dropped request is reported as a failure");
        assert!(matches!(err, TransportError::Unreachable(_)), "{err}");
    }

    #[tokio::test]
    async fn a_delay_at_or_past_the_timeout_is_reported_as_a_timeout() {
        let faults = Faults::new();
        let delay = Duration::from_millis(50);
        faults.set(1, 2, Fault::Delay(delay));
        let transport = Transport::new(1, "test-token").unwrap().with_faults(Some(faults));

        let start = std::time::Instant::now();
        let err = transport
            .send::<_, (), Infallible>(2, "127.0.0.1:1", "x", delay, ())
            .await
            .expect_err("a delay at least as long as the timeout is a timeout");
        assert!(matches!(err, TransportError::Timeout(_)), "{err}");
        assert!(start.elapsed() >= delay);
    }

    #[tokio::test]
    async fn a_dropped_response_is_reported_as_a_failure() {
        let faults = Faults::new();
        faults.set(1, 2, Fault::DropResponse);
        let transport = Transport::new(1, "test-token").unwrap().with_faults(Some(faults));

        let err = transport
            .send::<_, (), Infallible>(2, "127.0.0.1:1", "x", Duration::from_millis(50), ())
            .await
            .expect_err("a dropped response is reported as a failure");
        assert!(matches!(err, TransportError::Other(_)), "{err}");
    }
}
