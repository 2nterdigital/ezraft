//! HTTP network layer for EzRaft
//!
//! This module provides the built-in HTTP networking that connects Raft nodes.
//! Users don't need to implement anything - the framework handles all RPC communication.
//!
//! The actual outbound send - and, in test builds, the fault-injection hook - live in
//! [`crate::transport`]; this module adapts that to openraft's network traits and maps
//! [`crate::transport`]'s errors to openraft's own.
//!
//! # Snapshot transfer
//!
//! All RPCs, snapshot included, are plain request/response JSON over HTTP: a snapshot is
//! small enough at this crate's scale to travel as one POST, which spares the protocol any
//! chunking, resume, or transfer state - a failed transfer is simply sent again.
//!
//! The accepted cost: snapshot bytes ride in JSON, where they serialize as an array of
//! numbers (roughly 4x on the wire). If snapshots outgrow that, `full_snapshot` is the place
//! to switch to e.g. a raw-body streaming POST.

use std::fmt::Display;
use std::future::Future;

use openraft::OptionalSend;
use openraft::error::Infallible;
use openraft::error::NetworkError;
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
use crate::transport::Transport;
use crate::transport::TransportError;
use crate::type_config::EzVote;
use crate::type_config::Node;
use crate::type_config::OpenRaftTypes;

/// Type alias for OpenRaft types
type C = OpenRaftTypes;

/// HTTP network factory
///
/// Creates the per-peer [`Network`] openraft asks for. Every one of them shares one
/// [`Transport`] - and so one connection pool, one cluster token, and, in test builds, one
/// fault-injection handle. Implements OpenRaft's `RaftNetworkFactory` trait.
pub struct EzNetworkFactory {
    transport: Transport,
}

impl EzNetworkFactory {
    /// Build the network factory around an already-configured [`Transport`]
    ///
    /// The caller ([`crate::raft::EzKv::new`]) builds one `Transport` per node and shares it with
    /// this factory and with [`crate::rpc::Rpc`] alike (DESIGN.md §4): both send through the same
    /// choke point, so both carry the same cluster token and see the same fault-injection rules.
    pub(crate) fn new(transport: Transport) -> Self {
        Self { transport }
    }
}

impl RaftNetworkFactory<C> for EzNetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: u64, node: &Node) -> Self::Network {
        Network {
            addr: node.internal_addr.clone(),
            target,
            transport: self.transport.clone(),
        }
    }
}

/// HTTP network client for a single Raft node
pub struct Network {
    addr: String,
    target: u64,
    transport: Transport,
}

impl Network {
    /// Send a request to this client's target node, and decode the peer's `Result<Resp, Err>`
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
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        self.transport
            .send(self.target, &self.addr, uri, option.soft_ttl(), req)
            .await
            .map_err(|e| match &e {
                // A peer that does not answer in time, one that cannot be reached at all, or -
                // in test builds - one a fault dropped the request to, are all the same to
                // openraft: "the peer could not be reached, or did not answer in time", so
                // replication backs off instead of hammering it. `crate::rpc` keeps these two
                // apart; the Raft path here does not need to.
                TransportError::Unreachable(_) | TransportError::Timeout(_) => {
                    RPCError::Unreachable(Unreachable::new(&e))
                }
                TransportError::Unauthorized(_) | TransportError::Other(_) => RPCError::Network(NetworkError::new(&e)),
            })
    }
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

    /// Send the whole snapshot in one request; the receiving side hands it to
    /// [`Raft::install_full_snapshot`](openraft::Raft::install_full_snapshot).
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let req = SnapshotTransfer {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot.into_inner(),
        };
        tokio::pin!(cancel);

        tokio::select! {
            closed = &mut cancel => Err(StreamingError::Closed(closed)),
            res = self.request::<_, _, Infallible, C>("raft/snapshot", req, &option) => Ok(res?.unwrap()),
        }
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

/// Wire format of the one-shot snapshot POST to `/raft/snapshot`
///
/// Carries exactly what [`Raft::install_full_snapshot`](openraft::Raft::install_full_snapshot)
/// needs on the receiving side.
#[derive(serde::Deserialize, serde::Serialize)]
pub(crate) struct SnapshotTransfer {
    pub vote: EzVote,
    pub meta: EzSnapshotMeta,
    pub data: Vec<u8>,
}
