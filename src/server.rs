//! HTTP server: `/raft/*` and the admin API
//!
//! This module provides the HTTP server that handles:
//! - Internal Raft RPC (append entries, vote, install snapshot, transfer leader)
//! - Admin API (joining, node roles, removal, metrics)
//!
//! `/api/write` and `/api/read` - a sample write/read API typed by the user's own application -
//! do not survive `EzApp` becoming an internal implementation detail (ech0-media#27b); any-node
//! writes and reads are `EzKv::write`/`EzKv::view` on the public handle now, and forwarding a
//! write to the leader over HTTP is ech0-media#30.

use std::io::Cursor;

use actix_web::App;
use actix_web::HttpServer;
use actix_web::dev::Server;
use actix_web::web;
use actix_web::web::Data;
use actix_web::web::ServiceConfig;
use openraft::Snapshot;
use openraft::errors::Infallible;
use openraft::errors::decompose::DecomposeResult;
use openraft::raft;
use openraft::raft::SnapshotResponse;

use crate::admin::MembershipChange;
use crate::admin::Redirect;
use crate::network::SnapshotTransfer;
use crate::node_role::NodeRole;
use crate::raft::EzKv;
use crate::type_config::OpenRaftTypes;

/// Type alias for OpenRaft types
type C = OpenRaftTypes;

/// HTTP server wrapper: `/raft/*` and the admin routes
///
/// This is both a working server and a sample to copy: these routes are production-shaped, but
/// an application's own routes (client writes and reads) are not this crate's to shape - see
/// [`crate::raft::EzKv::serve_with`] to add them, and `examples/kvstore.rs` for a worked example.
pub struct EzServer {
    raft: EzKv,
}

impl EzServer {
    pub fn new(raft: EzKv) -> Self {
        Self { raft }
    }

    /// Raft append entries RPC handler
    ///
    /// The body is the `Result` the peer's [`crate::network::Network`] expects; only a
    /// [`Fatal`](openraft::errors::Fatal) error becomes an HTTP error status.
    async fn handle_append(
        req: web::Json<raft::AppendEntriesRequest<C>>,
        ez: Data<Self>,
    ) -> Result<web::Json<Result<raft::AppendEntriesResponse<C>, Infallible>>, actix_web::Error> {
        let resp = ez
            .raft
            .inner()
            .append_entries(req.into_inner())
            .await
            .decompose()
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("append_entries failed: {}", e)))?;

        Ok(web::Json(resp))
    }

    /// Raft vote RPC handler
    async fn handle_vote(
        req: web::Json<raft::VoteRequest<C>>,
        ez: Data<Self>,
    ) -> Result<web::Json<Result<raft::VoteResponse<C>, Infallible>>, actix_web::Error> {
        let resp = ez
            .raft
            .inner()
            .vote(req.into_inner())
            .await
            .decompose()
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("vote failed: {}", e)))?;

        Ok(web::Json(resp))
    }

    /// Raft transfer leadership RPC handler
    ///
    /// A leader hands leadership over rather than stopping and leaving the cluster to notice its
    /// silence: an election costs a timeout, a transfer costs a round trip. A leader removed from
    /// the cluster does this on its way out - one merely demoted keeps leading.
    async fn handle_transfer_leader(
        req: web::Json<raft::TransferLeaderRequest<C>>,
        ez: Data<Self>,
    ) -> Result<web::Json<Result<raft::TransferLeaderResponse<C>, Infallible>>, actix_web::Error> {
        let resp = ez
            .raft
            .inner()
            .handle_transfer_leader(req.into_inner())
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("transfer_leader failed: {}", e)))?;

        Ok(web::Json(Ok(resp)))
    }

    /// Raft install snapshot RPC handler
    ///
    /// A leader falls back to this when a follower lags behind the purged log. The snapshot
    /// arrives whole in a single request.
    async fn handle_snapshot(
        req: web::Json<SnapshotTransfer>,
        ez: Data<Self>,
    ) -> Result<web::Json<Result<SnapshotResponse<C>, Infallible>>, actix_web::Error> {
        let SnapshotTransfer { vote, meta, data } = req.into_inner();
        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(data),
        };

        let resp = ez
            .raft
            .inner()
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("install_snapshot failed: {}", e)))?;

        Ok(web::Json(Ok(resp)))
    }

    /// Metrics API handler
    async fn handle_metrics(ez: Data<Self>) -> Result<web::Json<openraft::RaftMetrics<C>>, actix_web::Error> {
        let metrics = ez.raft.metrics().await;
        Ok(web::Json(metrics))
    }

    /// Node id API handler
    ///
    /// Hands out an id no other node has held, taken from the index of a blank log entry: the log
    /// is the one counter every node already agrees on. Ids are therefore unique but not
    /// consecutive.
    ///
    /// This does not add anything to the cluster - [`Self::handle_membership`] does that - so a
    /// node that takes an id and dies costs the cluster one unused number.
    async fn handle_node_id(ez: Data<Self>) -> Result<web::Json<Redirect<u64>>, actix_web::Error> {
        let leader = match Self::leader_or_redirect(&ez).await {
            Ok(leader) => leader,
            Err(redirect) => return Ok(web::Json(Err(redirect))),
        };

        let write_result = leader
            .inner()
            .write_blank()
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("node id write failed: {}", e)))?;

        Ok(web::Json(Ok(write_result.log_id.index)))
    }

    /// Membership API handler
    ///
    /// One endpoint for every change to who is in the cluster, because they are one decision made
    /// in stages: whether a node is a member at all, and whether it counts towards a quorum.
    ///
    /// Adding is separate from making a voter for a reason. A node added straight to the voter set
    /// would be counted in the new configuration's quorum before it could answer anything, and the
    /// change would wait forever on its own acknowledgement.
    async fn handle_membership(
        req: web::Json<MembershipChange>,
        ez: Data<Self>,
    ) -> Result<web::Json<Redirect<()>>, actix_web::Error> {
        let leader = match Self::leader_or_redirect(&ez).await {
            Ok(leader) => leader,
            Err(redirect) => return Ok(web::Json(Err(redirect))),
        };

        let change = req.into_inner();

        let changed = match &change {
            MembershipChange::Add { node_id, addr } => leader.add_learner(*node_id, addr.clone()).await,
            MembershipChange::SetRole { node_id, role } => match role {
                NodeRole::Voter => leader.promote(*node_id).await,
                NodeRole::Learner => leader.demote(*node_id).await,
            },
            MembershipChange::Remove { node_id } => leader.remove_node(*node_id).await,
        };

        changed.map_err(|e| actix_web::error::ErrorInternalServerError(format!("{:?} failed: {}", change, e)))?;

        Ok(web::Json(Ok(())))
    }

    /// This node's [`EzKv`] if it leads, or where to ask instead
    ///
    /// Membership is the leader's to change, so every admin handler starts here. The redirect is
    /// `None` when this node knows of no leader, which is a moment to wait out rather than a
    /// failure. Both answers come from one reading of the metrics, so they cannot disagree.
    async fn leader_or_redirect(ez: &Data<Self>) -> Result<&EzKv, Option<String>> {
        let metrics = ez.raft.metrics().await;

        if metrics.current_leader == Some(metrics.id) {
            return Ok(&ez.raft);
        }

        Err(metrics
            .current_leader
            .and_then(|leader_id| metrics.membership_config.membership().get_node(&leader_id).map(|n| n.addr.clone())))
    }
}

/// Bind the node's HTTP server, running `configure` on top of the standard routes
///
/// Binding is synchronous (it can fail immediately, e.g. `AddrInUse`); running the accept loop
/// is not, which is why this hands back the still-unawaited [`Server`] rather than awaiting it
/// itself - the caller ([`EzKv::serve_with`](crate::raft::EzKv::serve_with)) needs the `Server`'s
/// own `handle()` before the accept loop starts, so [`EzKv::shutdown`](crate::raft::EzKv::shutdown)
/// can stop it later.
pub(crate) fn bind<F>(raft: EzKv, configure: F) -> std::io::Result<Server>
where F: Fn(&mut ServiceConfig) + Clone + Send + 'static {
    let addr = raft.addr().to_string();
    let server_data = Data::new(EzServer::new(raft));

    let server = HttpServer::new(move || {
        let configure = configure.clone();
        App::new()
            .app_data(server_data.clone())
            // Raft internal RPC
            .route("/raft/append", web::post().to(EzServer::handle_append))
            .route("/raft/vote", web::post().to(EzServer::handle_vote))
            .route("/raft/snapshot", web::post().to(EzServer::handle_snapshot))
            .route(
                "/raft/transfer_leader",
                web::post().to(EzServer::handle_transfer_leader),
            )
            // Admin API
            .route("/api/node_id", web::post().to(EzServer::handle_node_id))
            .route("/api/membership", web::post().to(EzServer::handle_membership))
            .route("/api/metrics", web::get().to(EzServer::handle_metrics))
            .configure(configure)
    })
    .bind(&addr)?;

    Ok(server.run())
}
