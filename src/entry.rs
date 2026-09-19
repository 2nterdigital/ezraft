//! The Raft log entry

use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::entry::RaftEntry;
use openraft::entry::RaftPayload;
use openraft::log_id::RaftLogId;
use openraft::vote::leader_id_std::CommittedLeaderId;

use crate::command::Command;
use crate::type_config::Node;

/// Log ID type (term, index)
///
/// A tuple that implements `RaftLogId` via OpenRaft's blanket implementation.
pub type EzLogId = (u64, u64);

/// Committed leader ID: the term of the leader that proposed a log entry
pub type EzCommittedLeaderId = CommittedLeaderId<u64>;

/// Entry payload with the foundation's node id and node types
type EzEntryPayload = EntryPayload<Command, u64, Node>;

/// A Raft log entry with the foundation's simplified log ID type
///
/// Wraps the entry's log ID (term, index) and payload. This is the native entry type
/// [`crate::EzStorage`] persists and reads back.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EzEntry {
    /// Log ID (term, index)
    pub log_id: EzLogId,

    /// Entry payload (Normal request, Blank, or Membership change)
    pub payload: EzEntryPayload,
}

// Manually implement Display: openraft has no blanket impl, and this is what its own logs use.
impl std::fmt::Display for EzEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EzEntry(log_id: ({}, {}), payload: {:?})",
            self.log_id.0, self.log_id.1, self.payload
        )
    }
}

// Implement RaftPayload trait
impl RaftPayload<u64, Node> for EzEntry {
    fn get_membership(&self) -> Option<Membership<u64, Node>> {
        self.payload.get_membership()
    }
}

// Implement openraft::RaftEntry trait so EzEntry works with OpenRaft
impl RaftEntry for EzEntry {
    type CommittedLeaderId = EzCommittedLeaderId;
    type D = Command;
    type NodeId = u64;
    type Node = Node;

    fn new(log_id: LogId<EzCommittedLeaderId>, payload: EzEntryPayload) -> Self {
        Self {
            log_id: log_id.to_type(),
            payload,
        }
    }

    fn log_id_parts(&self) -> (&EzCommittedLeaderId, u64) {
        RaftLogId::log_id_parts(&self.log_id)
    }

    fn set_log_id(&mut self, new: LogId<EzCommittedLeaderId>) {
        self.log_id = new.to_type();
    }
}
