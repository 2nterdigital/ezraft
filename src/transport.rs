//! Outbound HTTP: the single choke point every request this node sends to another node goes
//! through - Raft append/vote/snapshot/transfer-leader today, inter-node RPC and forwarded
//! writes later.
//!
//! [`crate::network`] adapts this to openraft's network traits and maps [`TransportError`] to
//! openraft's own error types; this module only knows how to reach a peer, attach the cluster
//! token, and, in test builds, how to make that reach fail on purpose.

use std::collections::HashSet;
use std::fmt::Display;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use reqwest::Client;
use reqwest::StatusCode;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[cfg(feature = "fault-injection")]
pub mod fault;

// Brought into local scope so `Transport`'s own field and methods below can name it plainly.
// `src/lib.rs` re-exports the whole `fault` module - not these names - as the public
// `ezraft::fault`.
#[cfg(feature = "fault-injection")]
use fault::Faults;

/// What went wrong sending a request
///
/// Distinct from anything the peer's own handler returned - that travels as the `Result` a
/// successful send still has to decode. [`crate::network::Network`] maps [`Self::Unreachable`]
/// and [`Self::Timeout`] to openraft's `RPCError::Unreachable` (openraft does not distinguish the
/// two: "the peer could not be reached, or did not answer in time" either way) and
/// [`Self::Unauthorized`]/[`Self::Other`] to `RPCError::Network`. Inter-node RPC
/// (`crate::rpc::Rpc::call`) keeps `Unreachable` and `Timeout` apart, and reports `Unauthorized`
/// as its own [`crate::rpc::RpcError::Unauthorized`], because a caller deciding whether to retry
/// needs to know which of the three actually happened.
#[derive(Debug)]
pub(crate) enum TransportError {
    /// The peer could not be reached at all: connection refused, DNS failure, or - in test
    /// builds - a [`fault::Fault::Drop`] or a [`fault::Fault::Delay`] that alone exhausts the
    /// caller's timeout budget before ever sending anything.
    Unreachable(String),
    /// The request was sent, but no answer came back before the caller's timeout elapsed.
    Timeout(String),
    /// The peer answered `401 Unauthorized`: the presented cluster token did not match its own.
    Unauthorized(String),
    /// Anything else: a non-success HTTP status, or a body that would not decode.
    Other(String),
}

impl Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Unreachable(msg)
            | TransportError::Timeout(msg)
            | TransportError::Unauthorized(msg)
            | TransportError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for TransportError {}

/// The one outbound path every node uses to reach another
///
/// Knows this node's own id (`from`) and the cluster token every request carries as
/// `Authorization: Bearer <token>`, so a request can be addressed, authenticated and, under
/// `fault-injection`, matched against a directed `(from, to)` rule. Cheap to `Clone`: the HTTP
/// clients, the token and the fault handle are already-shared handles, so every
/// [`Network`](crate::network::Network) and every [`crate::rpc::Rpc`] a node hands out clones the
/// same one.
///
/// Raft traffic (`Self::send`, used by [`crate::network`]) and inter-node RPC (`Self::send_rpc`,
/// used by [`crate::rpc`]) go through two separate `reqwest::Client`s - and so two separate
/// connection pools - so a burst of application RPC calls can never make a heartbeat wait behind
/// one of them.
#[derive(Clone)]
pub(crate) struct Transport {
    from: u64,
    token: Arc<str>,
    /// Raft traffic: append/vote/snapshot/transfer-leader
    client: Client,
    /// Inter-node RPC (`crate::rpc`) - never shares a pool with `client`
    rpc_client: Client,
    /// Peers that have already been logged for rejecting this node's token, so a misconfigured
    /// peer does not fill the log with the same warning on every heartbeat
    warned_unauthorized: Arc<Mutex<HashSet<u64>>>,
    #[cfg(feature = "fault-injection")]
    faults: Option<Faults>,
}

impl Transport {
    /// Build the transport for the node whose id is `from`, authenticating as `token`
    ///
    /// One client per traffic class for the whole node, each shared by every peer it talks to on
    /// that class, so each keeps one connection pool instead of opening one per peer.
    pub(crate) fn new(from: u64, token: impl Into<Arc<str>>) -> Result<Self, io::Error> {
        let client = Client::builder().no_proxy().build().map_err(io::Error::other)?;
        let rpc_client = Client::builder().no_proxy().build().map_err(io::Error::other)?;

        Ok(Self {
            from,
            token: token.into(),
            client,
            rpc_client,
            warned_unauthorized: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(feature = "fault-injection")]
            faults: None,
        })
    }

    /// Attach the test cluster's shared fault-injection handle
    #[cfg(feature = "fault-injection")]
    pub(crate) fn with_faults(mut self, faults: Option<Faults>) -> Self {
        self.faults = faults;
        self
    }

    /// Send `req` to `target` at `addr` over the Raft connection pool, and decode the peer's
    /// `Result<Resp, Err>`
    ///
    /// `timeout` is the caller's whole budget for the round trip: it bounds the HTTP request
    /// and, under `fault-injection`, any injected delay too - a delay that alone would exhaust
    /// the budget is reported exactly like a timeout, without the request ever reaching the peer.
    pub(crate) async fn send<Req, Resp, Err>(
        &self,
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
        #[cfg(feature = "fault-injection")]
        if let Some(fault) = self.fault_for(target) {
            return self.send_faulty(&self.client, fault, target, addr, path, timeout, req).await;
        }

        self.send_now(&self.client, target, addr, path, timeout, req).await
    }

    /// Send `req` to `target` at `addr` over the RPC-only connection pool, and decode the peer's
    /// `Result<Resp, Err>`
    ///
    /// Identical contract to [`Self::send`] - same token, same fault hook - kept as its own
    /// method only so RPC traffic (`crate::rpc::Rpc::call`) never contends with Raft traffic
    /// (heartbeats, replication, snapshots) for a connection.
    pub(crate) async fn send_rpc<Req, Resp, Err>(
        &self,
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
        #[cfg(feature = "fault-injection")]
        if let Some(fault) = self.fault_for(target) {
            return self.send_faulty(&self.rpc_client, fault, target, addr, path, timeout, req).await;
        }

        self.send_now(&self.rpc_client, target, addr, path, timeout, req).await
    }

    /// The actual HTTP round trip over `client`, with no fault check
    ///
    /// [`Self::send`]/[`Self::send_rpc`] are the only entry points; under `fault-injection` this
    /// is also what a `Delay` or `DropResponse` fault calls once it has decided a request goes
    /// out for real, on whichever client the caller was using.
    async fn send_now<Req, Resp, Err>(
        &self,
        client: &Client,
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
        let url = format!("http://{addr}/{path}");
        let from = self.from;

        let resp = client
            .post(url.clone())
            .timeout(timeout)
            .bearer_auth(self.token.as_ref())
            .json(&req)
            .send()
            .await
            .map_err(|e| {
            // Distinguished so `crate::rpc::RpcError` can tell a peer that never answered
            // (`Unreachable`) from one that answered too slowly (`Timeout`); openraft itself
            // does not need the difference and maps both back to one `RPCError::Unreachable`.
            if e.is_timeout() {
                TransportError::Timeout(format!("node {from} -> node {target} ({url}): {e}"))
            } else if e.is_connect() {
                TransportError::Unreachable(format!("node {from} -> node {target} ({url}): {e}"))
            } else {
                TransportError::Other(format!("node {from} -> node {target} ({url}): {e}"))
            }
        })?;

        // A mismatched token is reported before the body is even read: the 401 response never
        // carries anything but an empty body (`crate::auth`), and nothing about the request that
        // failed - credentials included - belongs in the message either.
        if resp.status() == StatusCode::UNAUTHORIZED {
            self.warn_unauthorized_once(target);
            return Err(TransportError::Unauthorized(format!(
                "node {from} -> node {target} ({url}): rejected - cluster token mismatch"
            )));
        }

        // The body is a serialized `Result` only on success; any other status carries a
        // plain-text reason that would otherwise surface as an opaque deserialization failure.
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(TransportError::Other(format!(
                "node {from} -> node {target} ({url}) responded {status}: {body}"
            )));
        }

        resp.json()
            .await
            .map_err(|e| TransportError::Other(format!("node {from} -> node {target} ({url}): {e}")))
    }

    /// Log once, at `warn`, that `target` rejected this node's cluster token
    ///
    /// A token mismatch recurs on every retry - every heartbeat, for a misconfigured peer - so
    /// this logs it exactly once per peer per process rather than flooding the log; the token
    /// itself is never part of the message, only the two node ids.
    fn warn_unauthorized_once(&self, target: u64) {
        let mut warned = self.warned_unauthorized.lock().unwrap();
        if warned.insert(target) {
            tracing::warn!(
                from = self.from,
                to = target,
                "node {target} rejected a request from node {} with 401 Unauthorized - \
                 the two nodes do not share the same cluster token",
                self.from
            );
        }
    }
}
