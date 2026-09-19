//! The registry: type-erased handlers keyed by name, and the wire envelope a dispatch answers in
//!
//! [`register`](Registry::register) is called with `Req`/`Resp`/error types fully known, and
//! builds a closure that captures them; only the closure's *signature* is erased to raw bytes in
//! and a JSON value out, never what it does internally. This is what lets one `HashMap` hold
//! handlers of unrelated types, and what lets [`crate::server`]'s HTTP route dispatch by name
//! without ever naming a concrete `Req`/`Resp` itself.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::RpcError;

/// A dispatchable handler, after `Req`/`Resp` have been erased
///
/// `Arc`, not `Box`: [`Registry::dispatch`] clones this handle out from under the registry's lock
/// before calling it, so a slow handler never blocks another dispatch - or a new registration -
/// from looking itself up.
type BoxedHandler = Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = serde_json::Value> + Send>> + Send + Sync>;

/// What a dispatch answers, wire-compatible regardless of which concrete `Resp` a name was
/// registered with
///
/// A name nobody registered, a request that would not decode, and a handler's own error all look
/// the same on the wire (an externally-tagged JSON object, the default `derive`), because the
/// caller looked the name up dynamically and cannot statically know which of the three happened.
#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Outcome<T> {
    /// The handler ran and returned this
    Ok(T),
    /// No handler is registered under the name that was called
    UnknownName,
    /// The request body did not decode as this name's `Req`
    BadRequest(String),
    /// The handler ran and returned its own error
    HandlerError(String),
}

impl<T> Outcome<T> {
    /// Turn a dispatch outcome into what [`crate::rpc::Rpc::call`] hands back to its caller
    ///
    /// `name` is folded into the message of every non-`Ok` case so a caller reading a bare
    /// [`RpcError::Remote`] still knows which call it came from.
    pub(super) fn into_rpc_result(self, name: &str) -> Result<T, RpcError> {
        match self {
            Outcome::Ok(value) => Ok(value),
            Outcome::UnknownName => Err(RpcError::Remote(format!("no RPC handler is registered as '{name}'"))),
            Outcome::BadRequest(msg) => Err(RpcError::Remote(format!(
                "'{name}': request or response did not decode: {msg}"
            ))),
            Outcome::HandlerError(msg) => Err(RpcError::Remote(format!("'{name}': {msg}"))),
        }
    }
}

/// One node's table of registered RPC names
///
/// `RwLock`, not an async lock: every critical section here is a plain map operation with no
/// `.await` inside it, so a `std::sync` lock held only across that operation is the simpler and
/// cheaper choice - a lock must never be held across an `.await`, and this one never is.
#[derive(Default)]
pub(super) struct Registry {
    handlers: RwLock<HashMap<String, BoxedHandler>>,
}

impl Registry {
    /// Register `handler` under `name`, unless a handler is already registered there
    ///
    /// Returns `true` on success, `false` if `name` was already taken - registering a name twice
    /// is a caller error, not a silent replacement (see [`RpcRegisterError::AlreadyRegistered`]
    /// at the call site).
    ///
    /// [`RpcRegisterError::AlreadyRegistered`]: super::RpcRegisterError::AlreadyRegistered
    pub(super) fn register<Req, Resp, E, F, Fut>(&self, name: String, handler: F) -> bool
    where
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize + Send + 'static,
        E: std::fmt::Display + Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, E>> + Send + 'static,
    {
        // `Arc`, not a plain move: the outer closure is called once per inbound request (it is an
        // `Fn`, stored and invoked through `BoxedHandler`), so each call needs its own handle on
        // `handler` rather than consuming the one the outer closure owns.
        let handler = Arc::new(handler);
        let boxed: BoxedHandler = Arc::new(move |body: Vec<u8>| {
            let decoded = serde_json::from_slice::<Req>(&body);
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let outcome = match decoded {
                    Err(e) => Outcome::<Resp>::BadRequest(e.to_string()),
                    Ok(req) => match handler(req).await {
                        Ok(resp) => Outcome::Ok(resp),
                        Err(e) => Outcome::HandlerError(e.to_string()),
                    },
                };
                serde_json::to_value(&outcome).unwrap_or_else(|e| {
                    // `Resp: Serialize` failing to actually serialize is a bug in the handler's
                    // own type, not something a caller can act on beyond seeing that encoding
                    // broke - there is no `Resp` value left to report otherwise.
                    serde_json::to_value(Outcome::<()>::HandlerError(format!("response encoding failed: {e}")))
                        .expect("Outcome<()> always encodes")
                })
            })
        });

        let mut handlers = self.handlers.write().unwrap();
        if handlers.contains_key(&name) {
            return false;
        }
        handlers.insert(name, boxed);
        true
    }

    /// Dispatch one inbound call to whatever is registered under `name`, if anything
    ///
    /// Always answers - an unknown name is not a rejection, it is an [`Outcome::UnknownName`] the
    /// caller decodes on its own side.
    pub(super) async fn dispatch(&self, name: &str, body: Vec<u8>) -> serde_json::Value {
        let handler = {
            let handlers = self.handlers.read().unwrap();
            handlers.get(name).cloned()
        };

        match handler {
            Some(handler) => handler(body).await,
            None => serde_json::to_value(Outcome::<()>::UnknownName).expect("Outcome<()> always encodes"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_registered_handler_answers_ok() {
        let registry = Registry::default();
        assert!(registry.register::<String, String, std::convert::Infallible, _, _>(
            "echo".to_string(),
            |req: String| async move { Ok(req) },
        ));

        let body = serde_json::to_vec(&"hello".to_string()).unwrap();
        let value = registry.dispatch("echo", body).await;
        let outcome: Outcome<String> = serde_json::from_value(value).unwrap();
        assert_eq!("hello".to_string(), outcome.into_rpc_result("echo").unwrap());
    }

    #[tokio::test]
    async fn dispatching_an_unregistered_name_answers_unknown_name() {
        let registry = Registry::default();

        let value = registry.dispatch("missing", Vec::new()).await;
        let outcome: Outcome<()> = serde_json::from_value(value).unwrap();
        let err = outcome.into_rpc_result("missing").unwrap_err();
        assert!(matches!(err, RpcError::Remote(_)));
        assert!(err.to_string().contains("no RPC handler"));
    }

    #[tokio::test]
    async fn registering_the_same_name_twice_is_refused() {
        let registry = Registry::default();
        assert!(
            registry
                .register::<(), (), std::convert::Infallible, _, _>("name".to_string(), |_: ()| async move { Ok(()) },)
        );
        assert!(
            !registry
                .register::<(), (), std::convert::Infallible, _, _>("name".to_string(), |_: ()| async move { Ok(()) },)
        );
    }

    #[tokio::test]
    async fn a_request_that_does_not_decode_answers_bad_request() {
        let registry = Registry::default();
        assert!(registry.register::<u64, u64, std::convert::Infallible, _, _>(
            "needs_a_number".to_string(),
            |req: u64| async move { Ok(req) },
        ));

        let value = registry.dispatch("needs_a_number", b"\"not a number\"".to_vec()).await;
        let outcome: Outcome<u64> = serde_json::from_value(value).unwrap();
        let err = outcome.into_rpc_result("needs_a_number").unwrap_err();
        assert!(err.to_string().contains("did not decode"), "{err}");
    }

    #[tokio::test]
    async fn a_handler_error_answers_handler_error() {
        let registry = Registry::default();
        assert!(
            registry
                .register::<(), (), String, _, _>("fails".to_string(), |_: ()| async move { Err("boom".to_string()) })
        );

        let value = registry.dispatch("fails", serde_json::to_vec(&()).unwrap()).await;
        let outcome: Outcome<()> = serde_json::from_value(value).unwrap();
        let err = outcome.into_rpc_result("fails").unwrap_err();
        assert_eq!("'fails': boom", err.to_string());
    }
}
