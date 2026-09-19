//! Outbound HTTP: the single choke point every request this node sends to another node goes
//! through - Raft append/vote/snapshot/transfer-leader today, inter-node RPC and forwarded
//! writes later.
//!
//! [`crate::network`] adapts this to openraft's network traits and maps [`TransportError`] to
//! openraft's own error types; this module only knows how to reach a peer and, in test builds,
//! how to make that reach fail on purpose.

use std::fmt::Display;
use std::io;
use std::time::Duration;

use reqwest::Client;
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

/// The one outbound path every node uses to reach another
///
/// Knows this node's own id (`from`), so a request can be addressed and, under
/// `fault-injection`, matched against a directed `(from, to)` rule. Cheap to `Clone`: the HTTP
/// client and the fault handle are already-shared handles, so every
/// [`Network`](crate::network::Network) a factory hands out clones the same one.
#[derive(Clone)]
pub(crate) struct Transport {
    from: u64,
    client: Client,
    #[cfg(feature = "fault-injection")]
    faults: Option<Faults>,
}

impl Transport {
    /// Build the transport for the node whose id is `from`
    ///
    /// One client for the whole node, shared by every peer it talks to, so it keeps one
    /// connection pool instead of opening one per peer.
    pub(crate) fn new(from: u64) -> Result<Self, io::Error> {
        let client = Client::builder().no_proxy().build().map_err(io::Error::other)?;

        Ok(Self {
            from,
            client,
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

    /// Send `req` to `target` at `addr`, and decode the peer's `Result<Resp, Err>`
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
            return self.send_faulty(fault, target, addr, path, timeout, req).await;
        }

        self.send_now(target, addr, path, timeout, req).await
    }

    /// The actual HTTP round trip, with no fault check
    ///
    /// [`Self::send`] is the only entry point; under `fault-injection` it is also what a
    /// `Delay` or `DropResponse` fault calls once it has decided a request goes out for real.
    async fn send_now<Req, Resp, Err>(
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
        let url = format!("http://{addr}/{path}");
        let from = self.from;

        let resp = self.client.post(url.clone()).timeout(timeout).json(&req).send().await.map_err(|e| {
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
