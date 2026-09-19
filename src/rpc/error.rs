//! Why a call through [`crate::rpc::Rpc`] did not produce a typed result

/// Why [`crate::rpc::Rpc::call`] did not return the handler's typed result
///
/// No automatic retry happens anywhere in this crate for any of these
/// (ech0-media#15): a handler is not assumed idempotent, so retrying is the caller's
/// decision to make, informed by which of these it got.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// `timeout` elapsed before an answer came back
    ///
    /// The remote handler may still be running: dropping this call's future only cancels the
    /// local side, and cancelling this call does not tell the peer to stop.
    #[error("RPC call to node {node_id} ('{name}') timed out")]
    Timeout {
        /// The node id the call was addressed to
        node_id: u64,
        /// The name that was called
        name: String,
    },

    /// `node_id` could not be reached: a connection failure, or - in test builds - a dropped
    /// request
    #[error("RPC call to node {node_id} ('{name}') found it unreachable")]
    Unreachable {
        /// The node id the call was addressed to
        node_id: u64,
        /// The name that was called
        name: String,
    },

    /// `node_id` is not in this node's current membership
    #[error("node {0} is not a member of this cluster")]
    NotMember(u64),

    /// `node_id` answered `401 Unauthorized`: its cluster token does not match this node's
    #[error("node {0} rejected the cluster token")]
    Unauthorized(u64),

    /// The target was reached and authenticated, but the call still did not produce a typed
    /// result: no handler is registered under this name, the request or the response would not
    /// decode, or the handler itself returned an error. The message says which.
    #[error("{0}")]
    Remote(String),
}

/// Why [`crate::rpc::Rpc::register`] refused to register a handler
#[derive(Debug, thiserror::Error)]
pub enum RpcRegisterError {
    /// The `ez.` prefix is reserved for the foundation's own handlers (forwarded writes
    /// ech0-media#30, ReadIndex ech0-media#31, and other internal machinery later tickets add on
    /// this same mechanism)
    #[error("RPC name '{0}' is reserved: the 'ez.' prefix is for the foundation's own handlers")]
    Reserved(String),

    /// A handler is already registered under this name
    ///
    /// Registering a name twice is refused rather than silently replacing the first handler: a
    /// second `register` call for the same name is far more likely to be a bug (two features
    /// picked the same name) than an intentional hot-swap.
    #[error("an RPC handler is already registered as '{0}'")]
    AlreadyRegistered(String),
}
