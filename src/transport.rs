//! Outbound HTTP: the single choke point every request this node sends to another node goes
//! through - Raft append/vote/snapshot/transfer-leader today, inter-node RPC and forwarded
//! writes later.
//!
//! [`crate::network`] adapts this to openraft's network traits and maps [`TransportError`] to
//! openraft's own error types; this module only knows how to reach a peer and, in test builds,
//! how to make that reach fail on purpose. It also knows nothing about the *shape* of a Raft
//! request - not even the raw-binary snapshot body: [`Transport::send_raw`] takes an
//! already-encoded byte body, and framing it (`crate::network::encode_snapshot_request`) is
//! [`crate::network`]'s job, same as the JSON `Req` type [`Transport::send`] serializes.

use std::fmt::Display;
use std::io;
use std::time::Duration;

use reqwest::Client;
use reqwest::RequestBuilder;
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
/// successful send still has to decode. [`crate::network::Network`] maps `Unreachable` to
/// openraft's `RPCError::Unreachable` (the peer could not be reached, or did not answer in time -
/// exactly what a connect failure or a request timeout meant before this type existed, and now
/// also what a dropped request means) and `Other` to `RPCError::Network`.
#[derive(Debug)]
pub(crate) enum TransportError {
    Unreachable(String),
    Other(String),
}

impl Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Unreachable(msg) | TransportError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for TransportError {}

/// Which kind of outbound traffic a request is, so [`Transport`] can route it to its own
/// connection pool: a large snapshot or append batch must never sit in front of a heartbeat
/// (ech0-media#26).
///
/// One [`reqwest::Client`] per variant, handed out through openraft's `new_heartbeat_client`/
/// `new_snapshot_client` factory hooks (`crate::network::EzNetworkFactory`); `new_client` (the
/// one every other request goes through, replication included) keeps mapping to
/// [`Self::Replication`]. Peer ticket #29 adds an `Rpc` variant here for inter-node RPC,
/// alongside the bearer token - keep additions to this type and to [`Transport`] itself small and
/// localized so the two merge cleanly.
///
/// `pub` (not `pub(crate)`) only so the `fault-injection`-gated [`fault::Faults::set_class`] can
/// name it from outside the crate (integration tests); nothing reaches it from outside without
/// that feature, because [`crate::transport`] itself is a private module.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TrafficClass {
    /// Leader heartbeats, and the ReadIndex leadership probes openraft sends over the same
    /// client: must never queue behind a large snapshot or append batch, or a healthy cluster
    /// could call an election it does not need.
    Heartbeat,
    /// Ordinary log replication - `append_entries`, `vote`, `transfer_leader` - whatever
    /// openraft's default network client (`new_client`) is asked to carry.
    Replication,
    /// Whole-snapshot transfer: large and long-running, and must never block replication or
    /// heartbeats to the same peer.
    Snapshot,
}

/// The one outbound path every node uses to reach another
///
/// Knows this node's own id (`from`), so a request can be addressed and, under
/// `fault-injection`, matched against a directed `(from, to)` rule. Cheap to `Clone`: the HTTP
/// clients and the fault handle are already-shared handles, so every
/// [`Network`](crate::network::Network) a factory hands out clones the same one.
#[derive(Clone)]
pub(crate) struct Transport {
    from: u64,
    /// One pool per [`TrafficClass`] (ech0-media#26): sharing a single `reqwest::Client` across
    /// all three would put every request behind the same HTTP/1.1 connections, so a large
    /// snapshot or append batch could still head-of-line-block a heartbeat despite openraft
    /// asking for a dedicated client.
    heartbeat_client: Client,
    replication_client: Client,
    snapshot_client: Client,
    #[cfg(feature = "fault-injection")]
    faults: Option<Faults>,
}

impl Transport {
    /// Build the transport for the node whose id is `from`
    ///
    /// One client per [`TrafficClass`], each shared by every peer this node talks to for that
    /// class, so each keeps one connection pool instead of opening one per peer.
    pub(crate) fn new(from: u64) -> Result<Self, io::Error> {
        let build_client = || Client::builder().no_proxy().build().map_err(io::Error::other);

        Ok(Self {
            from,
            heartbeat_client: build_client()?,
            replication_client: build_client()?,
            snapshot_client: build_client()?,
            #[cfg(feature = "fault-injection")]
            faults: None,
        })
    }

    /// This node's own id, for tracing (`crate::network::Network::full_snapshot`) - nothing else
    /// needs it, so it stays a crate-internal accessor rather than a public one.
    pub(crate) fn from(&self) -> u64 {
        self.from
    }

    /// Attach the test cluster's shared fault-injection handle
    #[cfg(feature = "fault-injection")]
    pub(crate) fn with_faults(mut self, faults: Option<Faults>) -> Self {
        self.faults = faults;
        self
    }

    /// This traffic class's own connection pool
    fn client(&self, class: TrafficClass) -> &Client {
        match class {
            TrafficClass::Heartbeat => &self.heartbeat_client,
            TrafficClass::Replication => &self.replication_client,
            TrafficClass::Snapshot => &self.snapshot_client,
        }
    }

    /// Send `req` as JSON to `target` at `addr`, and decode the peer's `Result<Resp, Err>`
    ///
    /// `timeout` is the caller's whole budget for the round trip: it bounds the HTTP request
    /// and, under `fault-injection`, any injected delay too - a delay that alone would exhaust
    /// the budget is reported exactly like a timeout, without the request ever reaching the peer.
    pub(crate) async fn send<Req, Resp, Err>(
        &self,
        class: TrafficClass,
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
        if let Some(fault) = self.fault_for(target, class) {
            return self
                .send_faulty(fault, target, timeout, |t| {
                    self.send_now(class, target, addr, path, t, req)
                })
                .await;
        }

        self.send_now(class, target, addr, path, timeout, req).await
    }

    /// Send an already-framed raw body to `target` at `addr`, and decode the peer's
    /// `Result<Resp, Err>` JSON response
    ///
    /// The one exception to "everything is JSON" (ech0-media#26): a raw-binary snapshot body is
    /// framed by the caller (`crate::network::encode_snapshot_request`) before it ever reaches
    /// here, because this module has no notion of what a snapshot header looks like. Subject to
    /// the exact same fault-injection and timeout handling as [`Self::send`].
    pub(crate) async fn send_raw<Resp, Err>(
        &self,
        class: TrafficClass,
        target: u64,
        addr: &str,
        path: impl Display,
        timeout: Duration,
        body: Vec<u8>,
    ) -> Result<Result<Resp, Err>, TransportError>
    where
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        #[cfg(feature = "fault-injection")]
        if let Some(fault) = self.fault_for(target, class) {
            return self
                .send_faulty(fault, target, timeout, |t| {
                    self.send_raw_now(class, target, addr, path, t, body)
                })
                .await;
        }

        self.send_raw_now(class, target, addr, path, timeout, body).await
    }

    /// The actual JSON HTTP round trip, with no fault check
    ///
    /// [`Self::send`] is the only entry point; under `fault-injection` it is also what a
    /// `Delay` or `DropResponse` fault calls once it has decided a request goes out for real.
    async fn send_now<Req, Resp, Err>(
        &self,
        class: TrafficClass,
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
        let request = self.client(class).post(url.clone()).timeout(timeout).json(&req);

        self.execute(target, url, request).await
    }

    /// The actual raw-body HTTP round trip, with no fault check - [`Self::send_raw`]'s
    /// counterpart to [`Self::send_now`]
    async fn send_raw_now<Resp, Err>(
        &self,
        class: TrafficClass,
        target: u64,
        addr: &str,
        path: impl Display,
        timeout: Duration,
        body: Vec<u8>,
    ) -> Result<Result<Resp, Err>, TransportError>
    where
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let url = format!("http://{addr}/{path}");
        let request = self
            .client(class)
            .post(url.clone())
            .timeout(timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body);

        self.execute(target, url, request).await
    }

    /// Send a built request, and decode the peer's `Result<Resp, Err>` - the tail end both
    /// [`Self::send_now`] and [`Self::send_raw_now`] share, since neither differs in how it
    /// handles the response, only in how it builds the request.
    async fn execute<Resp, Err>(
        &self,
        target: u64,
        url: String,
        request: RequestBuilder,
    ) -> Result<Result<Resp, Err>, TransportError>
    where
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let from = self.from;

        let resp = request.send().await.map_err(|e| {
            // A peer that does not answer in time is treated like one that cannot be reached, so
            // replication backs off instead of hammering it.
            if e.is_connect() || e.is_timeout() {
                TransportError::Unreachable(format!("node {from} -> node {target} ({url}): {e}"))
            } else {
                TransportError::Other(format!("node {from} -> node {target} ({url}): {e}"))
            }
        })?;

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
}
