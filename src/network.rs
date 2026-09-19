//! HTTP network layer for EzRaft
//!
//! This module provides the built-in HTTP networking that connects Raft nodes.
//! Users don't need to implement anything - the framework handles all RPC communication.
//!
//! The actual outbound send - and, in test builds, the fault-injection hook - live in
//! [`crate::transport`]; this module adapts that to openraft's network traits and maps
//! [`crate::transport`]'s errors to openraft's own.
//!
//! # Connection pools (ech0-media#26)
//!
//! [`EzNetworkFactory`] implements openraft's three network-client factory hooks -
//! `new_client`, `new_heartbeat_client`, `new_snapshot_client` - each tagging the [`Network`] it
//! hands back with its own [`TrafficClass`], so heartbeats (and the ReadIndex probes that reuse
//! the heartbeat client), ordinary replication, and snapshot transfer each get their own
//! `reqwest` connection pool inside the shared [`Transport`]. A large snapshot or append batch
//! then never sits in front of a heartbeat on an HTTP/1.1 connection.
//!
//! # Snapshot transfer
//!
//! The snapshot itself travels as a raw `application/octet-stream` body, not JSON: riding inside
//! JSON would serialize the snapshot bytes as an array of numbers (roughly 4x on the wire, see
//! [`SnapshotHeader`]'s own doc comment for why a header or a JSON body were both worse fits).
//! [`EzConfig::snapshot_max_bytes`](crate::EzConfig::snapshot_max_bytes) bounds it on both ends:
//! [`Network::full_snapshot`] refuses to send a snapshot over its own configured maximum, and
//! `crate::server::EzServer::handle_snapshot`'s `actix_web::web::PayloadConfig` answers a body
//! over the receiving node's own maximum with `413` before this module ever sees it.

use std::fmt::Display;
use std::future::Future;

use openraft::OptionalSend;
use openraft::error::Infallible;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::network::v2::RaftNetworkV2;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::VoteOf;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::snapshot::EzSnapshotData;
use crate::snapshot::EzSnapshotMeta;
use crate::transport::TrafficClass;
use crate::transport::Transport;
use crate::transport::TransportError;
use crate::type_config::EzVote;
use crate::type_config::Node;
use crate::type_config::OpenRaftTypes;

/// Type alias for OpenRaft types
type C = OpenRaftTypes;

/// HTTP network factory
///
/// Creates the per-peer [`Network`] openraft asks for, one per Raft [`TrafficClass`]
/// (ech0-media#26). Every one of them shares one [`Transport`] - and so its per-class connection
/// pools, the cluster token, and, in test builds, one fault-injection handle. Implements
/// OpenRaft's `RaftNetworkFactory` trait.
pub struct EzNetworkFactory {
    transport: Transport,
    /// The largest snapshot this node will ever attempt to send - see
    /// [`Network::full_snapshot`].
    snapshot_max_bytes: usize,
}

impl EzNetworkFactory {
    /// Build the network factory around an already-configured [`Transport`]
    ///
    /// The caller ([`crate::raft::EzKv::new`]) builds one `Transport` per node and shares it with
    /// this factory and with [`crate::rpc::Rpc`] alike: both send through the same choke point, so
    /// both carry the same cluster token and see the same fault-injection rules.
    pub(crate) fn new(transport: Transport, snapshot_max_bytes: usize) -> Self {
        Self {
            transport,
            snapshot_max_bytes,
        }
    }

    /// Build the [`Network`] client for `target`, tagged with `class`
    fn build(&self, target: u64, node: &Node, class: TrafficClass) -> Network {
        Network {
            addr: node.internal_addr.clone(),
            target,
            transport: self.transport.clone(),
            class,
            snapshot_max_bytes: self.snapshot_max_bytes,
        }
    }
}

impl RaftNetworkFactory<C> for EzNetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: u64, node: &Node) -> Self::Network {
        self.build(target, node, TrafficClass::Replication)
    }

    /// A client dedicated to leader heartbeats and the ReadIndex leadership probes that reuse it:
    /// its own connection pool, so replication or a snapshot transfer to the same peer can never
    /// delay one.
    async fn new_heartbeat_client(&mut self, target: u64, node: &Node) -> Self::Network {
        self.build(target, node, TrafficClass::Heartbeat)
    }

    /// A client dedicated to whole-snapshot transfer: its own connection pool, so a large
    /// transfer can never delay a heartbeat or ordinary replication to the same peer.
    async fn new_snapshot_client(&mut self, target: u64, node: &Node) -> Self::Network {
        self.build(target, node, TrafficClass::Snapshot)
    }
}

/// HTTP network client for a single Raft node, tagged with the [`TrafficClass`] the factory
/// method that created it stands for
pub struct Network {
    addr: String,
    target: u64,
    transport: Transport,
    class: TrafficClass,
    snapshot_max_bytes: usize,
}

impl Network {
    /// Send a JSON request to this client's target node, and decode the peer's
    /// `Result<Resp, Err>`
    ///
    /// The request is given the timeout budget openraft computed for it: it is dropped at
    /// `soft_ttl` so this side reports the failure before openraft abandons the call itself.
    async fn request<Req, Resp, Err, Cfg>(
        &mut self,
        uri: impl Display,
        req: Req,
        option: &RPCOption,
    ) -> Result<Result<Resp, Err>, RPCError<Cfg>>
    where
        Cfg: openraft::RaftTypeConfig,
        Req: Serialize,
        Resp: DeserializeOwned,
        Err: DeserializeOwned,
    {
        self.transport
            .send(self.class, self.target, &self.addr, uri, option.soft_ttl(), req)
            .await
            .map_err(|e| to_rpc_error(&e))
    }

    /// [`Self::request`]'s counterpart for the raw-binary snapshot body: `body` is already framed
    /// ([`encode_snapshot_request`]) by the time it gets here.
    async fn request_raw<Resp, Err, Cfg>(
        &mut self,
        uri: impl Display,
        body: Vec<u8>,
        option: &RPCOption,
    ) -> Result<Result<Resp, Err>, RPCError<Cfg>>
    where
        Cfg: openraft::RaftTypeConfig,
        Resp: DeserializeOwned,
        Err: DeserializeOwned,
    {
        self.transport
            .send_raw(self.class, self.target, &self.addr, uri, option.soft_ttl(), body)
            .await
            .map_err(|e| to_rpc_error(&e))
    }
}

/// What openraft hears when a request could not be delivered: always `Unreachable`
///
/// `Unreachable` is the one answer that makes openraft wait before it tries again, and waiting
/// is right whatever went wrong - a peer that cannot be reached, one that does not answer in
/// time, one that rejects this node's cluster token, one that refuses the request's size, or, in
/// test builds, one a fault dropped the request to. None of those is fixed by retrying at once,
/// and alpha.33 does exactly that on anything else: its snapshot sender backs off on
/// `Unreachable` only, and on a `Network` or `Timeout` error resets the backoff and sends the
/// whole snapshot again immediately, in a loop. `crate::rpc` tells these causes apart for its
/// callers; the Raft path has no use for the distinction.
fn to_rpc_error<Cfg>(e: &TransportError) -> RPCError<Cfg>
where Cfg: openraft::RaftTypeConfig {
    RPCError::Unreachable(Unreachable::new(e))
}

/// Implement RaftNetworkV2 for HTTP transport
impl RaftNetworkV2<C> for Network {
    type SnapshotData = EzSnapshotData;

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        let res = self.request::<_, _, Infallible, C>("raft/append", req, &option).await?;
        Ok(res.unwrap())
    }

    /// Send the whole snapshot in one request, as a raw binary body with a small length-prefixed
    /// JSON header (ech0-media#26); the receiving side hands the decoded snapshot to
    /// [`Raft::install_full_snapshot`](openraft::Raft::install_full_snapshot).
    ///
    /// Refuses outright, before sending anything, if `snapshot` is larger than this node's own
    /// [`EzConfig::snapshot_max_bytes`](crate::EzConfig::snapshot_max_bytes): logged, and
    /// reported to openraft as `Unreachable`, so replication backs off and retries rather than
    /// spinning or wedging ([`to_rpc_error`] says why nothing else will do). Raising the
    /// configured maximum (and this attempt eventually being retried) is what lets a lagging
    /// node catch up.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let last_log_id = snapshot.meta.last_log_id;
        let data = snapshot.snapshot.into_inner();
        let size_bytes = data.len();
        let from = self.transport.from();
        let target = self.target;

        if size_bytes > self.snapshot_max_bytes {
            tracing::warn!(
                from,
                target,
                last_log_id = ?last_log_id,
                size_bytes,
                max_bytes = self.snapshot_max_bytes,
                outcome = "refused",
                "refusing to send a snapshot larger than the configured maximum",
            );
            return Err(StreamingError::Unreachable(Unreachable::from_string(format!(
                "snapshot is {size_bytes} bytes, exceeding the configured {}-byte maximum",
                self.snapshot_max_bytes
            ))));
        }

        let header = SnapshotHeader {
            vote,
            meta: snapshot.meta,
        };
        let body = encode_snapshot_request(&header, &data).map_err(|e| {
            StreamingError::Unreachable(Unreachable::from_string(format!("encoding the snapshot header: {e}")))
        })?;

        let start = std::time::Instant::now();
        tokio::pin!(cancel);

        let result: Result<SnapshotResponse<C>, StreamingError<C>> = tokio::select! {
            closed = &mut cancel => Err(StreamingError::Closed(closed)),
            res = self.request_raw::<_, Infallible, C>("raft/snapshot", body, &option) => {
                res.map(|r| r.unwrap()).map_err(StreamingError::from)
            }
        };

        tracing::info!(
            from,
            target,
            last_log_id = ?last_log_id,
            size_bytes,
            elapsed_ms = start.elapsed().as_millis(),
            outcome = match &result {
                Ok(_) => "ok",
                Err(StreamingError::Closed(_)) => "closed",
                Err(_) => "failed",
            },
            "sent a snapshot",
        );

        result
    }

    async fn vote(&mut self, req: VoteRequest<C>, option: RPCOption) -> Result<VoteResponse<C>, RPCError<C>> {
        let res = self.request::<_, _, Infallible, C>("raft/vote", req, &option).await?;
        Ok(res.unwrap())
    }

    /// Hand leadership to the node openraft picked
    ///
    /// This has a default implementation that reports "not implemented", which would leave every
    /// transfer failing silently - including the one a leader makes on its way out after being
    /// removed from the voter set, which is how a demoted leader gives the cluster a new one
    /// promptly instead of leaving it to time out and elect.
    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<C>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<C>, RPCError<C>> {
        let res = self.request::<_, _, Infallible, C>("raft/transfer_leader", req, &option).await?;
        Ok(res.unwrap())
    }
}

/// The small metadata that travels ahead of the snapshot bytes on `/raft/snapshot`
/// (ech0-media#26)
///
/// The vote and [`EzSnapshotMeta`] are always small (no user data) and are the only metadata the
/// receiving side needs; the snapshot bytes are the only part that can be large. Three ways to
/// carry them were on the table:
///
/// - **Inside JSON**, as this route did before this ticket: the `Vec<u8>` snapshot serializes as a
///   JSON array of numbers, inflating roughly 4x, on top of running into the same body-size limit
///   as every other Raft request.
/// - **An HTTP header**: avoids that inflation, but header values are bound by (implementation- and
///   proxy-dependent) per-header size limits well below what a large cluster's membership could
///   need, and must be visible ASCII - not guaranteed for an operator-supplied node address.
/// - **A length-prefixed JSON preamble ahead of the raw body** (what this module does): no size
///   limit smaller than the request body's own, no encoding restriction on the header's content,
///   and no ambiguity about where the header ends and the snapshot begins.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct SnapshotHeader {
    pub vote: EzVote,
    pub meta: EzSnapshotMeta,
}

/// The length prefix ahead of the JSON header, in bytes
const SNAPSHOT_HEADER_LEN_PREFIX: usize = 4;

/// Frame `header` and the raw snapshot `body` for the wire: a 4-byte big-endian length, the
/// header as JSON, then `body` untouched
fn encode_snapshot_request(header: &SnapshotHeader, body: &[u8]) -> Result<Vec<u8>, serde_json::Error> {
    let header_json = serde_json::to_vec(header)?;

    let mut framed = Vec::with_capacity(SNAPSHOT_HEADER_LEN_PREFIX + header_json.len() + body.len());
    framed.extend_from_slice(&(header_json.len() as u32).to_be_bytes());
    framed.extend_from_slice(&header_json);
    framed.extend_from_slice(body);
    Ok(framed)
}

/// Split a `/raft/snapshot` request body back into its header and the snapshot bytes
///
/// Strict on purpose: a corrupted transfer or a bug on the sending side must come back as a
/// `400` naming what was wrong, never a panic - `bytes` is untrusted input straight off the wire.
pub(crate) fn decode_snapshot_request(bytes: &[u8]) -> Result<(SnapshotHeader, &[u8]), SnapshotFramingError> {
    if bytes.len() < SNAPSHOT_HEADER_LEN_PREFIX {
        return Err(SnapshotFramingError::Truncated);
    }

    let (len_prefix, rest) = bytes.split_at(SNAPSHOT_HEADER_LEN_PREFIX);
    let header_len = u32::from_be_bytes([len_prefix[0], len_prefix[1], len_prefix[2], len_prefix[3]]) as usize;

    if header_len > rest.len() {
        return Err(SnapshotFramingError::HeaderLongerThanBody {
            header_len,
            body_len: rest.len(),
        });
    }

    let (header_json, data) = rest.split_at(header_len);
    let header = serde_json::from_slice(header_json).map_err(|e| SnapshotFramingError::InvalidHeader(e.to_string()))?;
    Ok((header, data))
}

/// Why [`decode_snapshot_request`] could not make sense of a `/raft/snapshot` request body
#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotFramingError {
    /// The body is shorter than the length prefix alone
    #[error("snapshot request body is shorter than its 4-byte header-length prefix")]
    Truncated,
    /// The length prefix names more bytes than the body has left
    #[error("snapshot request's header-length prefix ({header_len}) exceeds the {body_len} bytes left in the body")]
    HeaderLongerThanBody { header_len: usize, body_len: usize },
    /// The header's bytes are not valid JSON, or not a [`SnapshotHeader`]
    #[error("snapshot request header is not valid JSON: {0}")]
    InvalidHeader(String),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use openraft::Membership;
    use openraft::Snapshot;
    use openraft::StoredMembership;

    use super::*;

    fn header() -> SnapshotHeader {
        SnapshotHeader {
            vote: EzVote::new(1, 0),
            meta: EzSnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::new(None, Membership::default()),
            },
        }
    }

    /// Whatever kept a request from being delivered, openraft must hear `Unreachable`: it is
    /// the only answer alpha.33's snapshot sender backs off on.
    #[test]
    fn every_transport_failure_is_reported_as_unreachable() {
        let failures = [
            TransportError::Unreachable("connection refused".to_string()),
            TransportError::Timeout("no answer".to_string()),
            TransportError::Unauthorized("rejected the cluster token".to_string()),
            TransportError::Other("413 Payload Too Large".to_string()),
        ];
        for failure in failures {
            let reported: RPCError<C> = to_rpc_error(&failure);
            assert!(
                matches!(reported, RPCError::Unreachable(_)),
                "{failure} was reported as {reported}"
            );
        }
    }

    /// A snapshot over this node's own maximum is never sent, and is refused the same way: as
    /// `Unreachable`, so the refusal is retried after a backoff rather than in a tight loop that
    /// re-reads the whole snapshot each time round.
    #[tokio::test]
    async fn an_oversized_snapshot_is_refused_as_unreachable() {
        let transport = Transport::new(1, "test-token").unwrap();
        let node = Node {
            internal_addr: "127.0.0.1:1".to_string(),
        };
        let mut network = EzNetworkFactory::new(transport, 8).build(2, &node, TrafficClass::Snapshot);

        let snapshot = Snapshot {
            meta: header().meta,
            snapshot: std::io::Cursor::new(vec![0u8; 9]),
        };
        let refused = network
            .full_snapshot(
                EzVote::new(1, 1),
                snapshot,
                std::future::pending(),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .expect_err("nine bytes exceed an eight-byte maximum");

        assert!(
            matches!(refused, StreamingError::Unreachable(_)),
            "refused as {refused}"
        );
    }

    #[test]
    fn encode_then_decode_round_trips_the_header_and_body() {
        let body = b"snapshot bytes, not JSON".to_vec();

        let framed = encode_snapshot_request(&header(), &body).unwrap();
        let (decoded_header, decoded_body) = decode_snapshot_request(&framed).unwrap();

        assert_eq!(header().vote, decoded_header.vote);
        assert_eq!(body, decoded_body);
    }

    #[test]
    fn a_body_shorter_than_the_length_prefix_is_truncated() {
        let err = decode_snapshot_request(&[0, 1]).unwrap_err();
        assert!(matches!(err, SnapshotFramingError::Truncated), "{err}");
    }

    #[test]
    fn a_length_prefix_past_the_body_is_rejected() {
        let mut bytes = 100u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(b"too short");

        let err = decode_snapshot_request(&bytes).unwrap_err();
        assert!(
            matches!(err, SnapshotFramingError::HeaderLongerThanBody { .. }),
            "{err}"
        );
    }

    #[test]
    fn invalid_header_json_is_rejected() {
        let mut bytes = 4u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(b"nope");

        let err = decode_snapshot_request(&bytes).unwrap_err();
        assert!(matches!(err, SnapshotFramingError::InvalidHeader(_)), "{err}");
    }

    #[test]
    fn an_empty_body_after_the_header_decodes_as_an_empty_snapshot() {
        let framed = encode_snapshot_request(&header(), &[]).unwrap();
        let (_, data) = decode_snapshot_request(&framed).unwrap();
        assert!(data.is_empty());
    }
}
