//! The Raft log's request and response types
//!
//! One state machine (the KV) means one command shape. Kept as an enum, not a bare [`Txn`],
//! because later tickets add a request id for idempotency (ech0-media#30) and registry commands
//! (ech0-media#33) as further variants.
//!
//! `command` is a private module: [`Command`] and [`CommandOutcome`] are `pub` only so
//! [`crate::entry::EzEntry`] - which must stay public, since [`crate::EzStorage`] hands it to
//! implementors - type-checks without a `private_interfaces` warning. Neither name is
//! re-exported from the crate root, so nothing outside this crate can name or construct them.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::kv::Conflict;
use crate::kv::Txn;

/// The Raft log's request type: today, exactly one KV transaction
///
/// Plain serde derives throughout, so a positional format (postcard) can still round-trip a
/// logged entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// A conditional, all-or-nothing KV transaction
    Txn(Txn),
}

impl fmt::Display for Command {
    /// A short summary that never prints a key or a value - openraft logs every request it
    /// processes, and [`Txn`]'s ops carry arbitrary caller-controlled bytes. Delegates to
    /// `Txn`'s own `Display`, which already keeps that promise.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Txn(txn) => write!(f, "{txn}"),
        }
    }
}

/// What applying one [`Command`] produced, as the writer hears it
///
/// Today this is always [`Command::Txn`]'s own result: the failing condition, or the revision the
/// transaction committed at. The change group itself goes to the publisher, not back to the
/// writer - a writer knows what it wrote, and this is also what a forwarded write's answer carries
/// over the network (ech0-media#30).
pub type CommandOutcome = Result<u64, Conflict>;
