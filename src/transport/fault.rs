//! The `fault-injection` hook: an in-process test can drop, delay, or hide the response of
//! requests between chosen directed node pairs - for one traffic class or for all of them - and
//! heal them again.
//!
//! This module exists only when the `fault-injection` cargo feature is enabled. A normal build
//! has none of it: no [`Fault`] or [`Faults`] type, no `faults` field on
//! [`EzConfig`](crate::EzConfig), no branch in `Transport::send`/`Transport::send_raw`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

// Re-exported (not just used) so a test only needs `ezraft::fault::{Faults, Fault,
// TrafficClass}`: `TrafficClass` itself is declared in the parent, non-feature-gated module
// (`crate::transport`) because production code needs it with or without this feature; this is
// the one place it becomes reachable outside the crate.
pub use super::TrafficClass;
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

/// A rule's key: a directed node-id pair, optionally narrowed to one [`TrafficClass`] - `None`
/// means "every traffic class" (see [`Faults::rules`]'s own doc comment)
type RuleKey = (u64, u64, Option<TrafficClass>);

/// A cheap, `Clone`-able handle onto one test cluster's fault rules
///
/// Every node of one in-process test cluster shares one `Faults`. It is never process-global:
/// test binaries run tests in parallel, and each test's cluster needs its own independent rules.
/// Rules are keyed by directed node-id pair, optionally narrowed to one [`TrafficClass`], and
/// take effect immediately for requests sent after they are set.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// `None` in a [`RuleKey`]'s third element means "every traffic class" - what [`Self::set`]
    /// (and so `partition`/`isolate`/`heal`) has always meant, kept exactly as it was so every
    /// test written before traffic classes existed keeps working unchanged. `Some(class)` entries
    /// (`Self::set_class`) are additive and are checked first.
    rules: Arc<Mutex<HashMap<RuleKey, Fault>>>,
}

impl Faults {
    /// A handle with no rules set
    pub fn new() -> Self {
        Self::default()
    }

    /// Give every request from `from` to `to`, of any traffic class, `fault`, replacing any
    /// all-classes rule already set for that pair
    ///
    /// A class-specific rule set with [`Self::set_class`] for the same pair still takes
    /// precedence, for its own class, over this one.
    pub fn set(&self, from: u64, to: u64, fault: Fault) {
        self.rules.lock().unwrap().insert((from, to, None), fault);
    }

    /// Remove the all-classes rule set with [`Self::set`], if any
    ///
    /// Any class-specific rule for the same pair (`Self::set_class`) is untouched.
    pub fn clear(&self, from: u64, to: u64) {
        self.rules.lock().unwrap().remove(&(from, to, None));
    }

    /// Give requests of one traffic class from `from` to `to` `fault`
    ///
    /// Checked before, and independently of, any all-classes rule [`Self::set`] has for the same
    /// pair - which is what lets a test delay only a peer's snapshot traffic while its heartbeats
    /// (a different class) keep flowing normally.
    pub fn set_class(&self, from: u64, to: u64, class: TrafficClass, fault: Fault) {
        self.rules.lock().unwrap().insert((from, to, Some(class)), fault);
    }

    /// Remove a class-specific rule set with [`Self::set_class`]
    ///
    /// An all-classes rule ([`Self::set`]) for the same pair, if any, still applies to that class
    /// afterwards.
    pub fn clear_class(&self, from: u64, to: u64, class: TrafficClass) {
        self.rules.lock().unwrap().remove(&(from, to, Some(class)));
    }

    /// Drop every request between the two groups, in both directions, for every traffic class
    pub fn partition(&self, group_a: &[u64], group_b: &[u64]) {
        for &a in group_a {
            for &b in group_b {
                self.set(a, b, Fault::Drop);
                self.set(b, a, Fault::Drop);
            }
        }
    }

    /// Drop every request between `node` and each of `others`, in both directions, for every
    /// traffic class
    pub fn isolate(&self, node: u64, others: &[u64]) {
        self.partition(&[node], others);
    }

    /// Clear every rule, all-classes and class-specific alike; every pair goes through normally
    /// again
    pub fn heal(&self) {
        self.rules.lock().unwrap().clear();
    }

    /// The rule for `from -> to`'s `class` traffic, if any
    ///
    /// A class-specific rule ([`Self::set_class`]) wins over the pair's all-classes rule
    /// ([`Self::set`]) when both are set.
    fn get(&self, from: u64, to: u64, class: TrafficClass) -> Option<Fault> {
        let rules = self.rules.lock().unwrap();
        rules.get(&(from, to, Some(class))).or_else(|| rules.get(&(from, to, None))).copied()
    }
}

impl Transport {
    /// The fault rule for a request of `class` this node is about to send `target`, if any
    pub(super) fn fault_for(&self, target: u64, class: TrafficClass) -> Option<Fault> {
        self.faults.as_ref().and_then(|faults| faults.get(self.from(), target, class))
    }

    /// Apply `fault` instead of - or, for [`Fault::DropResponse`], in addition to - the real send
    /// `send_now` performs
    ///
    /// Generic over the real send itself (`send_now`, a closure taking the - possibly
    /// delay-reduced - remaining timeout) rather than over the request/response types directly,
    /// so this one function serves both `Transport::send`'s JSON path and
    /// `Transport::send_raw`'s raw-body path, on whichever traffic class's pool the caller chose.
    pub(super) async fn send_faulty<F, Fut, Resp, Err>(
        &self,
        fault: Fault,
        target: u64,
        timeout: Duration,
        send_now: F,
    ) -> Result<Result<Resp, Err>, TransportError>
    where
        F: FnOnce(Duration) -> Fut,
        Fut: Future<Output = Result<Result<Resp, Err>, TransportError>>,
    {
        match fault {
            Fault::Drop => Err(TransportError::Unreachable(format!(
                "fault-injection: dropped {} -> {target}",
                self.from()
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
                    self.from()
                )))
            }
            Fault::Delay(delay) => {
                tokio::time::sleep(delay).await;
                send_now(timeout - delay).await
            }

            Fault::DropResponse => {
                // The peer applies the request; only this side's view of the outcome is lost.
                let _ = send_now(timeout).await;
                Err(TransportError::Other(format!(
                    "fault-injection: response dropped {} -> {target}",
                    self.from()
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
        assert_eq!(None, faults.get(1, 2, TrafficClass::Replication));

        faults.set(1, 2, Fault::Drop);
        assert_eq!(Some(Fault::Drop), faults.get(1, 2, TrafficClass::Replication));

        faults.clear(1, 2);
        assert_eq!(None, faults.get(1, 2, TrafficClass::Replication));
    }

    #[test]
    fn rules_are_directed() {
        let faults = Faults::new();
        faults.set(1, 2, Fault::Drop);
        assert_eq!(
            None,
            faults.get(2, 1, TrafficClass::Replication),
            "a rule for 1 -> 2 must not affect 2 -> 1"
        );
    }

    #[test]
    fn an_all_classes_rule_applies_to_every_traffic_class() {
        let faults = Faults::new();
        faults.set(1, 2, Fault::Drop);

        for class in [
            TrafficClass::Heartbeat,
            TrafficClass::Replication,
            TrafficClass::Snapshot,
        ] {
            assert_eq!(Some(Fault::Drop), faults.get(1, 2, class), "{class:?}");
        }
    }

    #[test]
    fn a_class_specific_rule_overrides_the_all_classes_rule_for_that_class_only() {
        let faults = Faults::new();
        faults.set(1, 2, Fault::Drop);
        faults.set_class(1, 2, TrafficClass::Snapshot, Fault::DropResponse);

        assert_eq!(
            Some(Fault::DropResponse),
            faults.get(1, 2, TrafficClass::Snapshot),
            "the class-specific rule wins for its own class"
        );
        assert_eq!(
            Some(Fault::Drop),
            faults.get(1, 2, TrafficClass::Heartbeat),
            "every other class still sees the all-classes rule"
        );
        assert_eq!(
            Some(Fault::Drop),
            faults.get(1, 2, TrafficClass::Replication),
            "every other class still sees the all-classes rule"
        );
    }

    #[test]
    fn a_class_specific_rule_applies_with_no_all_classes_rule_set() {
        let faults = Faults::new();
        faults.set_class(1, 2, TrafficClass::Snapshot, Fault::Drop);

        assert_eq!(Some(Fault::Drop), faults.get(1, 2, TrafficClass::Snapshot));
        assert_eq!(
            None,
            faults.get(1, 2, TrafficClass::Heartbeat),
            "a class-specific rule must not leak into other classes"
        );
    }

    #[test]
    fn clear_class_only_removes_the_class_specific_rule() {
        let faults = Faults::new();
        faults.set(1, 2, Fault::Drop);
        faults.set_class(1, 2, TrafficClass::Snapshot, Fault::DropResponse);

        faults.clear_class(1, 2, TrafficClass::Snapshot);

        assert_eq!(
            Some(Fault::Drop),
            faults.get(1, 2, TrafficClass::Snapshot),
            "the all-classes rule still applies once the class-specific one is cleared"
        );
    }

    #[test]
    fn partition_drops_both_directions_between_every_cross_pair_only() {
        let faults = Faults::new();
        faults.partition(&[1, 2], &[3, 4]);

        for (a, b) in [(1, 3), (1, 4), (2, 3), (2, 4), (3, 1), (4, 1), (3, 2), (4, 2)] {
            assert_eq!(
                Some(Fault::Drop),
                faults.get(a, b, TrafficClass::Replication),
                "{a} -> {b}"
            );
        }
        assert_eq!(
            None,
            faults.get(1, 2, TrafficClass::Replication),
            "partition does not touch pairs within a group"
        );
        assert_eq!(
            None,
            faults.get(3, 4, TrafficClass::Replication),
            "partition does not touch pairs within a group"
        );
    }

    #[test]
    fn isolate_is_a_partition_of_one_node_against_the_rest() {
        let faults = Faults::new();
        faults.isolate(1, &[2, 3]);

        for (a, b) in [(1, 2), (2, 1), (1, 3), (3, 1)] {
            assert_eq!(
                Some(Fault::Drop),
                faults.get(a, b, TrafficClass::Replication),
                "{a} -> {b}"
            );
        }
        assert_eq!(
            None,
            faults.get(2, 3, TrafficClass::Replication),
            "isolate does not touch the others between themselves"
        );
    }

    #[test]
    fn heal_clears_every_rule() {
        let faults = Faults::new();
        faults.partition(&[1], &[2, 3]);
        faults.set_class(1, 2, TrafficClass::Snapshot, Fault::Delay(Duration::from_secs(1)));

        faults.heal();

        assert_eq!(None, faults.get(1, 2, TrafficClass::Replication));
        assert_eq!(None, faults.get(1, 3, TrafficClass::Replication));
        assert_eq!(None, faults.get(1, 2, TrafficClass::Snapshot));
    }

    #[tokio::test]
    async fn a_dropped_request_is_unreachable_without_being_sent() {
        let faults = Faults::new();
        faults.set(1, 2, Fault::Drop);
        let transport = Transport::new(1, "test-token").unwrap().with_faults(Some(faults));

        // Nothing is listening at this address: a real send would itself fail, but a dropped
        // request must fail before it ever gets that far.
        let err = transport
            .send::<_, (), Infallible>(
                TrafficClass::Replication,
                2,
                "127.0.0.1:1",
                "x",
                Duration::from_secs(5),
                (),
            )
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
            .send::<_, (), Infallible>(TrafficClass::Replication, 2, "127.0.0.1:1", "x", delay, ())
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
            .send::<_, (), Infallible>(
                TrafficClass::Replication,
                2,
                "127.0.0.1:1",
                "x",
                Duration::from_millis(50),
                (),
            )
            .await
            .expect_err("a dropped response is reported as a failure");
        assert!(matches!(err, TransportError::Other(_)), "{err}");
    }

    #[tokio::test]
    async fn a_class_specific_delay_leaves_other_classes_unaffected() {
        let faults = Faults::new();
        faults.set_class(1, 2, TrafficClass::Snapshot, Fault::Drop);
        let transport = Transport::new(1, "test-token").unwrap().with_faults(Some(faults));

        let snapshot_err = transport
            .send::<_, (), Infallible>(
                TrafficClass::Snapshot,
                2,
                "127.0.0.1:1",
                "x",
                Duration::from_secs(5),
                (),
            )
            .await
            .expect_err("snapshot traffic is dropped by the class-specific rule");
        assert!(matches!(snapshot_err, TransportError::Unreachable(_)), "{snapshot_err}");

        // Heartbeat traffic to the same peer has no rule at all, so it goes through normally -
        // "normally" here means it actually tries to connect, which fails because nothing is
        // listening, but that failure comes from the real send, not the fault hook.
        let heartbeat_err = transport
            .send::<_, (), Infallible>(
                TrafficClass::Heartbeat,
                2,
                "127.0.0.1:1",
                "x",
                Duration::from_millis(200),
                (),
            )
            .await
            .expect_err("nothing is listening at this address");
        assert!(
            matches!(heartbeat_err, TransportError::Unreachable(_)),
            "{heartbeat_err}"
        );
    }
}
