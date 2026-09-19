//! Typed inter-node RPC: register a named handler on this node, call it on another by node id
//!
//! `kv.rpc()` hands out the facade this module defines, [`Rpc`]: JSON over one fixed path prefix
//! on the internal port, addressed by node id through the current membership, with no actix type
//! anywhere in its public surface (ech0-media#15). Traffic goes through the
//! same `Transport` Raft traffic does, so it carries the same cluster token
//! and, in test builds, sees the same fault-injection rules - on its own connection pool, so a
//! burst of RPC calls can never make a heartbeat wait behind one.
//!
//! No automatic retry happens anywhere in this module: a handler is not assumed idempotent, so
//! retrying on [`RpcError::Timeout`]/[`RpcError::Unreachable`] is left to the caller, who knows
//! whether its own request is safe to repeat. Dropping the future [`Rpc::call`] returns cancels
//! only this side's wait - the remote handler, once started, runs to completion regardless.

mod error;
mod registry;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

pub use error::RpcError;
pub use error::RpcRegisterError;
use openraft::async_runtime::WatchReceiver;
use openraft::error::Infallible;
use registry::Outcome;
use registry::Registry;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::raft::ORRaft;
use crate::transport::TrafficClass;
use crate::transport::Transport;
use crate::transport::TransportError;

/// The fixed path prefix every RPC route is mounted under, on the internal port: `POST
/// /<prefix>/<name>`. [`crate::server::bind`] mounts the route there; [`Rpc::call`] builds its
/// outbound requests at the same path.
pub(crate) const RPC_PATH_PREFIX: &str = "rpc";

/// The name prefix reserved for the foundation's own handlers: forwarded writes (ech0-media#30),
/// ReadIndex (ech0-media#31), and any other internal machinery later tickets add on this same
/// mechanism. [`Rpc::register`] refuses a name starting with it;
/// [`Rpc::register_internal`] is the crate-only entry point that accepts one.
const RESERVED_PREFIX: &str = "ez.";

/// The typed inter-node RPC facade: `kv.rpc()`
///
/// Cheap to `Clone`: every field is already a cheap-clone handle (an `Arc`, or - like
/// `Transport` and the openraft handle - a type that is internally one).
#[derive(Clone)]
pub struct Rpc {
    node_id: u64,
    raft: ORRaft,
    transport: Transport,
    registry: Arc<Registry>,
}

impl Rpc {
    /// Build the facade for the node whose id is `node_id`
    ///
    /// `raft` is read only for this node's own current membership, to resolve a target node id
    /// to its internal address and to answer [`RpcError::NotMember`]. `transport` is the same
    /// choke point [`crate::network::EzNetworkFactory`] sends Raft traffic through:
    /// sharing it is what gives RPC traffic the same cluster token and the same
    /// fault-injection rules Raft traffic gets.
    pub(crate) fn new(node_id: u64, raft: ORRaft, transport: Transport) -> Self {
        Self {
            node_id,
            raft,
            transport,
            registry: Arc::new(Registry::default()),
        }
    }

    /// Register a handler for `name`
    ///
    /// `handler` is an async function `Req -> Result<Resp, E>`. A node calling this name on this
    /// node by id (`Rpc::call`) gets `Resp` back typed on success; a handler error travels back
    /// as [`RpcError::Remote`], carrying `E`'s `Display`. Registration works before and after
    /// `serve` starts, from any clone of this handle.
    ///
    /// Registering the same name twice is refused rather than replacing the first registration
    /// (see [`RpcRegisterError::AlreadyRegistered`]): two independent registrations racing for
    /// one name is far more likely a bug than an intended hot-swap.
    ///
    /// # Errors
    ///
    /// [`RpcRegisterError::Reserved`] if `name` starts with `ez.` - that prefix is reserved for
    /// the foundation's own handlers. [`RpcRegisterError::AlreadyRegistered`] if a handler is
    /// already registered under `name`.
    pub fn register<Req, Resp, E, F, Fut>(&self, name: impl Into<String>, handler: F) -> Result<(), RpcRegisterError>
    where
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize + Send + 'static,
        E: std::fmt::Display + Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, E>> + Send + 'static,
    {
        let name = name.into();
        if name.starts_with(RESERVED_PREFIX) {
            return Err(RpcRegisterError::Reserved(name));
        }
        self.register_any(name, handler)
    }

    /// Register a handler under a name in the foundation's own reserved `ez.` prefix
    ///
    /// The crate-internal counterpart to [`Self::register`], for later tickets (forwarded writes
    /// ech0-media#30, ReadIndex ech0-media#31) to install their own handlers on this same
    /// mechanism. This milestone builds only the reservation and this entry point - nothing in
    /// the crate calls it yet.
    #[allow(
        dead_code,
        reason = "wired up by ech0-media#30's forwarded-write and #31's ReadIndex handlers"
    )]
    pub(crate) fn register_internal<Req, Resp, E, F, Fut>(
        &self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<(), RpcRegisterError>
    where
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize + Send + 'static,
        E: std::fmt::Display + Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, E>> + Send + 'static,
    {
        self.register_any(name.into(), handler)
    }

    fn register_any<Req, Resp, E, F, Fut>(&self, name: String, handler: F) -> Result<(), RpcRegisterError>
    where
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize + Send + 'static,
        E: std::fmt::Display + Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, E>> + Send + 'static,
    {
        if self.registry.register(name.clone(), handler) {
            Ok(())
        } else {
            Err(RpcRegisterError::AlreadyRegistered(name))
        }
    }

    /// Call the handler registered as `name` on the node `node_id`, and wait up to `timeout`
    ///
    /// Addressed by node id: the address comes from this node's own current membership. Calling
    /// this node's own id works the same as calling any other - it dispatches straight into the
    /// local registry, with no HTTP round trip and no token check (a node trusts itself), but the
    /// same `timeout` and the same [`RpcError`] taxonomy apply either way, so a caller never needs
    /// a special case for it.
    ///
    /// No automatic retry happens here, ever. Dropping the returned future cancels only this
    /// side's wait; a remote handler that has already started keeps running to completion.
    ///
    /// # Errors
    ///
    /// See [`RpcError`].
    pub async fn call<Req, Resp>(&self, node_id: u64, name: &str, req: Req, timeout: Duration) -> Result<Resp, RpcError>
    where
        Req: Serialize + Send + 'static,
        Resp: Serialize + DeserializeOwned + Send + 'static,
    {
        if node_id == self.node_id {
            return self.call_local(name, req, timeout).await;
        }

        let addr = self.member_addr(node_id)?;
        let path = format!("{RPC_PATH_PREFIX}/{name}");

        let sent = self
            .transport
            .send::<Req, Outcome<Resp>, Infallible>(TrafficClass::Rpc, node_id, &addr, path, timeout, req)
            .await;

        match sent {
            Ok(Ok(outcome)) => outcome.into_rpc_result(name),
            // `Infallible` has no variants: the server side never actually answers this shape.
            Ok(Err(never)) => match never {},
            Err(TransportError::Unreachable(_)) => Err(RpcError::Unreachable {
                node_id,
                name: name.to_string(),
            }),
            Err(TransportError::Timeout(_)) => Err(RpcError::Timeout {
                node_id,
                name: name.to_string(),
            }),
            Err(TransportError::Unauthorized(_)) => Err(RpcError::Unauthorized(node_id)),
            Err(TransportError::Other(msg)) => Err(RpcError::Remote(format!("'{name}': {msg}"))),
        }
    }

    /// [`Self::call`] for `node_id == self.node_id`
    async fn call_local<Req, Resp>(&self, name: &str, req: Req, timeout: Duration) -> Result<Resp, RpcError>
    where
        Req: Serialize + Send + 'static,
        Resp: DeserializeOwned + Send + 'static,
    {
        let body =
            serde_json::to_vec(&req).map_err(|e| RpcError::Remote(format!("'{name}': request did not encode: {e}")))?;

        let value = tokio::time::timeout(timeout, self.registry.dispatch(name, body)).await.map_err(|_elapsed| {
            RpcError::Timeout {
                node_id: self.node_id,
                name: name.to_string(),
            }
        })?;

        let outcome: Outcome<Resp> = serde_json::from_value(value)
            .map_err(|e| RpcError::Remote(format!("'{name}': response did not decode: {e}")))?;

        outcome.into_rpc_result(name)
    }

    /// This node's own current view of `node_id`'s internal address, or [`RpcError::NotMember`]
    fn member_addr(&self, node_id: u64) -> Result<String, RpcError> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics
            .membership_config
            .membership()
            .get_node(&node_id)
            .map(|node| node.internal_addr.clone())
            .ok_or(RpcError::NotMember(node_id))
    }

    /// Dispatch one inbound RPC call to whatever this node has registered under `name`
    ///
    /// Used by [`crate::server`]'s `/rpc/{name}` route, which owns turning the returned JSON
    /// value into an HTTP response; this method itself never fails, an unknown name included -
    /// see [`registry::Registry::dispatch`].
    pub(crate) async fn dispatch(&self, name: &str, body: Vec<u8>) -> serde_json::Value {
        self.registry.dispatch(name, body).await
    }
}
