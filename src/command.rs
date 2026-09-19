//! The Raft log's request and response types
//!
//! One state machine (the KV) means one command shape, but not only one *kind* of command:
//! [`Command`] wraps whatever the log entry actually does (`body`) together with the two fields
//! every command needs for idempotency, regardless of `body` - a [`RequestId`] and the ledger
//! capacity its proposer was configured with (ech0-media#30). [`CommandBody::Txn`] is the first
//! and, this milestone, only variant; the registry commands of ech0-media#33 add further variants
//! to `CommandBody` and reuse `Command`'s two idempotency fields as-is, rather than growing a
//! second forwarding-and-ledger path.
//!
//! `command` is a private module: [`Command`], [`CommandBody`] and [`CommandOutcome`] are `pub`
//! only so [`crate::entry::EzEntry`] - which must stay public, since [`crate::EzStorage`] hands it
//! to implementors - type-checks without a `private_interfaces` warning. None of the three names
//! is re-exported from the crate root, so nothing outside this crate can name or construct them.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::kv::Conflict;
use crate::kv::RequestId;
use crate::kv::Txn;

/// The Raft log's request type: an idempotency-tracked command
///
/// Plain serde derives throughout, so a positional format (postcard) can still round-trip a
/// logged entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    /// The idempotency ledger's key for this command - the same value on every retry of the same
    /// logical write, however many times it ends up proposed.
    pub request_id: RequestId,
    /// The idempotency ledger's FIFO eviction bound, stamped in by whichever node actually
    /// proposed this command from *its own* `EzConfig::ledger_capacity` - never read back out of
    /// this node's own configuration when the command is applied. See
    /// `crate::kv::ledger`'s module docs for why that is what keeps eviction a pure function of
    /// the log.
    pub ledger_capacity: u64,
    /// What this command actually does
    pub body: CommandBody,
}

/// What one [`Command`] does, once its idempotency check has passed
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandBody {
    /// A conditional, all-or-nothing KV transaction
    Txn(Txn),
}

impl fmt::Display for Command {
    /// A short summary that never prints a key or a value - openraft logs every request it
    /// processes, and [`Txn`]'s ops carry arbitrary caller-controlled bytes. Delegates to
    /// `Txn`'s own `Display`, which already keeps that promise.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.body {
            CommandBody::Txn(txn) => write!(f, "{} (request {})", txn, self.request_id),
        }
    }
}

/// What applying one [`CommandBody::Txn`] produced, as the writer hears it
///
/// Today this is always the transaction's own result: the failing condition, or the revision the
/// transaction committed at. The change group itself goes to the publisher, not back to the
/// writer - a writer knows what it wrote, and this is also what a forwarded write's answer carries
/// over the network (ech0-media#30). It doubles as the idempotency ledger's own recorded value
/// (`crate::kv::ledger::TxnOutcome`): the two are the exact same shape by construction.
pub type CommandOutcome = Result<u64, Conflict>;
