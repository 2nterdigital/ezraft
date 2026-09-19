//! This example's own external API: the one thing a client ever talks to
//!
//! Bound to `--api-addr`, entirely separate from the foundation's internal port
//! (`EzKv::serve`/`serve_with`, `--addr`) - no route here exists on the internal port, and no
//! route of the internal port (`/raft/*`, `/api/*`, `/rpc/*`) is reachable through this one.
//! Every handler is a thin wrapper over `EzKv`'s public API: this module *is* the worked example
//! of using it from an application's own HTTP layer.
//!
//! Values travel as base64 text in JSON everywhere on this API, matching the crate's own
//! convention for [`ezraft::Op::Put`] and [`ezraft::KeyValue`] - `POST /tx`'s body is exactly
//! [`ezraft::Txn`]'s own JSON shape (`{"conditions": [...], "ops": [...]}`, each `Put`'s `value`
//! base64) plus one extra field, and every response that carries a value reuses
//! [`ezraft::KeyValue`] or [`ezraft::Change`] unchanged, so a client decodes the same way in every
//! direction.
//!
//! # Routes
//!
//! | Route | Reads | Answers |
//! | --- | --- | --- |
//! | `POST /tx` | a transaction (see [`TxnRequest`]) | `200` [`TxnResponse`], or an error below |
//! | `GET /view?key=` or `?prefix=` | the local view ([`EzKv::view`]) | `200` a [`ezraft::KeyValue`] or `[ezraft::KeyValue]`, `404` if the key is absent |
//! | `GET /linearizable?key=`/`?prefix=`/`&timeout_ms=` | [`EzKv::linearizable_view`] | same as `/view`, or an error below |
//! | `GET /status` | [`EzKv::metrics`], [`EzKv::view`] | `200` [`StatusResponse`] |
//! | `GET /watch?prefix=&after=` | [`EzKv::watch`]/[`EzKv::view_and_watch`] | `200`, newline-delimited JSON, chunked, never ends on its own |
//!
//! `POST /tx` errors ([`WriteError`](ezraft::WriteError), via [`write_error_response`]):
//!
//! | Case | Status | Body |
//! | --- | --- | --- |
//! | `LimitExceeded` | `422` | `{"error": "limit_exceeded", "detail": <LimitError>}` |
//! | `Conflict` | `409` | `{"error": "conflict", "detail": <Conflict>}` |
//! | `Indeterminate` | `504` | `{"error": "indeterminate", "request_id": <id>}` |
//! | `LocalLag` | `503` | `{"error": "local_lag", "revision": <u64>}` |
//!
//! `GET /linearizable` errors ([`ReadError`](ezraft::ReadError), via [`read_error_response`]):
//!
//! | Case | Status | Body |
//! | --- | --- | --- |
//! | `NoLeader` | `503` | `{"error": "no_leader"}` |
//! | `NotMember` | `410` | `{"error": "not_member", "node_id": <u64>}` |
//! | `NotCaughtUp` | `504` | `{"error": "not_caught_up", "read_index": <u64>, "applied_index": <u64\|null>}` |
//!
//! `GET /watch` lines, one JSON object per line:
//!
//! ```text
//! {"type": "reset", "revision": <u64>, "cause": <see below>, "entries": [<ezraft::KeyValue>, ...]}
//! {"type": "changes", "revision": <u64>, "items": [<ezraft::Change>, ...]}
//! {"type": "progress", "revision": <u64>}
//! ```
//!
//! `entries` is already filtered to the subscribed prefix. Without `after`, the stream always
//! opens with a `"reset"` line built from [`EzKv::view_and_watch`] (`"cause": {"kind":
//! "initial"}`), so a fresh client gets a full listing before any increment. With `after`, the
//! crate's own [`EzKv::watch`]`(prefix, After(rev))` is used directly: either gap-free
//! `"changes"`/`"progress"` lines continuing right after `rev`, or a real `"reset"` (crate's own
//! [`ezraft::ResetCause`]) if `rev` had already fallen out of the retained window:
//! `{"kind": "resume_too_old", "requested": <u64>, "window_floor": <u64>}`,
//! `{"kind": "subscriber_too_slow"}`, or `{"kind": "snapshot_installed"}`.
//!
//! No route here checks the cluster token: it protects the internal port only. A real deployment
//! authenticates its own client-facing API however fits its clients (LiveKit-style JWTs for
//! ech0-media, something else elsewhere) - this reference example demonstrates the foundation,
//! not that choice, so the external port is left open.

use std::io;
use std::pin::Pin;
use std::time::Duration;

use actix_web::App;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use actix_web::Responder;
use actix_web::dev::Server;
use actix_web::web;
use ezraft::EzKv;
use ezraft::KeyValue;
use ezraft::ReadError;
use ezraft::RequestId;
use ezraft::ResetCause;
use ezraft::Txn;
use ezraft::View;
use ezraft::WatchStart;
use ezraft::WatchUpdate;
use ezraft::WriteError;
use futures::Stream;
use futures::StreamExt;
use futures::stream;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

/// Body of `POST /tx`: a transaction, plus an optional caller-chosen idempotency key
///
/// `#[serde(flatten)]` is safe here only because this type is this HTTP route's own wire shape,
/// never a type the crate's postcard-based, positional log/snapshot formats have to round-trip.
#[derive(Deserialize)]
struct TxnRequest {
    /// Reuse this to retry a specific earlier attempt - see [`TxnResponse::request_id`] and the
    /// `Indeterminate` case in [`write_error_response`]. Omit it for a fresh write; the ledger
    /// only recognizes a reused id when `txn` is byte-for-byte the same as the first attempt.
    #[serde(default)]
    request_id: Option<RequestId>,
    #[serde(flatten)]
    txn: Txn,
}

/// What `POST /tx` answers on success
#[derive(Serialize)]
struct TxnResponse {
    revision: u64,
    /// Worth saving: this is the `request_id` a later `POST /tx` with the identical `txn` must
    /// send to resolve an `Indeterminate` outcome instead of risking a second, different write.
    request_id: RequestId,
}

/// `POST /tx`: submit a transaction, on any node - forwarding to the leader, if needed, is the
/// foundation's own job ([`EzKv::write_with_id`]).
async fn handle_tx(node: web::Data<EzKv>, body: web::Json<TxnRequest>) -> HttpResponse {
    let TxnRequest { request_id, txn } = body.into_inner();
    let request_id = request_id.unwrap_or_default();

    match node.write_with_id(request_id, txn).await {
        Ok(revision) => HttpResponse::Ok().json(TxnResponse { revision, request_id }),
        Err(err) => write_error_response(&err),
    }
}

/// Map [`WriteError`] to a distinct status and body - see this module's own doc comment for the
/// full table.
fn write_error_response(err: &WriteError) -> HttpResponse {
    match err {
        // Syntactically valid JSON, semantically rejected: 422, not 400 - the same distinction
        // `Txn::validate` itself draws by running before anything is ever proposed.
        WriteError::LimitExceeded(limit_err) => {
            HttpResponse::UnprocessableEntity().json(json!({"error": "limit_exceeded", "detail": limit_err}))
        }
        WriteError::Conflict(conflict) => {
            HttpResponse::Conflict().json(json!({"error": "conflict", "detail": conflict}))
        }
        // The write's own deadline elapsed with no definite answer - the same "ran out of time
        // waiting for a definite answer" shape as `ReadError::NotCaughtUp`, so both map to 504.
        WriteError::Indeterminate { request_id } => {
            HttpResponse::GatewayTimeout().json(json!({"error": "indeterminate", "request_id": request_id}))
        }
        // Committed, just not applied on this node yet: nothing about the request was wrong, so
        // 503 (retry shortly) rather than a 4xx.
        WriteError::LocalLag(lag) => {
            HttpResponse::ServiceUnavailable().json(json!({"error": "local_lag", "revision": lag.revision}))
        }
    }
}

/// Query for `GET /view` and `GET /linearizable`: exactly one of `key` or `prefix`
#[derive(Deserialize)]
struct ViewQuery {
    key: Option<String>,
    prefix: Option<String>,
}

/// Answer `query` from `view`: a point lookup (`?key=`) or a prefix scan (`?prefix=`)
fn view_response(view: &View, query: &ViewQuery) -> HttpResponse {
    match (&query.key, &query.prefix) {
        (Some(key), None) => match view.get(key) {
            Some(kv) => HttpResponse::Ok().json(kv),
            None => HttpResponse::NotFound().json(json!({"error": "not_found", "key": key})),
        },
        (None, Some(prefix)) => HttpResponse::Ok().json(view.prefix(prefix).collect::<Vec<KeyValue>>()),
        _ => HttpResponse::BadRequest().json(json!({
            "error": "bad_request",
            "message": "pass exactly one of `key` or `prefix`",
        })),
    }
}

/// `GET /view`: a point lookup or prefix scan from this node's own local view - no consensus
/// round, and it already reflects every write this node has itself applied.
async fn handle_view(node: web::Data<EzKv>, query: web::Query<ViewQuery>) -> HttpResponse {
    view_response(&node.view(), &query)
}

/// Query for `GET /linearizable`: [`ViewQuery`] plus a timeout budget
#[derive(Deserialize)]
struct LinearizableQuery {
    key: Option<String>,
    prefix: Option<String>,
    #[serde(default = "default_linearizable_timeout_ms")]
    timeout_ms: u64,
}

/// A reasonable default budget for a call that, unlike `/view`, may cross the network to confirm
/// leadership - generous enough to ride out a single election, not so long a stuck request holds
/// a connection open indefinitely.
fn default_linearizable_timeout_ms() -> u64 {
    5_000
}

/// `GET /linearizable`: a point lookup or prefix scan through [`EzKv::linearizable_view`] -
/// reflects every write acknowledged anywhere before this call started, even one this node has
/// not applied yet.
async fn handle_linearizable(node: web::Data<EzKv>, query: web::Query<LinearizableQuery>) -> HttpResponse {
    let timeout = Duration::from_millis(query.timeout_ms);

    match node.linearizable_view(timeout).await {
        Ok(view) => view_response(&view, &ViewQuery {
            key: query.key.clone(),
            prefix: query.prefix.clone(),
        }),
        Err(err) => read_error_response(&err),
    }
}

/// Map [`ReadError`] to a distinct status and body - see this module's own doc comment for the
/// full table.
fn read_error_response(err: &ReadError) -> HttpResponse {
    match err {
        ReadError::NoLeader => HttpResponse::ServiceUnavailable().json(json!({"error": "no_leader"})),
        // This node's own authoritative removal from the cluster: 410 (gone for good), not a
        // 503 - retrying against this node cannot ever succeed.
        ReadError::NotMember(node_id) => HttpResponse::Gone().json(json!({"error": "not_member", "node_id": node_id})),
        // Confirmed committed, just not applied here within the timeout - the same "ran out of
        // time" shape as `WriteError::Indeterminate`, so also 504.
        ReadError::NotCaughtUp {
            read_index,
            applied_index,
        } => HttpResponse::GatewayTimeout().json(json!({
            "error": "not_caught_up",
            "read_index": read_index,
            "applied_index": applied_index,
        })),
    }
}

/// `GET /status`: node id, leadership, revision, and - until ech0-media#33's `EzKv::cluster()`
/// lands - an approximation of cluster membership read through [`EzKv::metrics`]
#[derive(Serialize)]
struct StatusResponse {
    node_id: u64,
    is_leader: bool,
    revision: u64,
    role: String,
    term: u64,
    leader_id: Option<u64>,
    voters: Vec<u64>,
    learners: Vec<u64>,
}

/// `GET /status`
async fn handle_status(node: web::Data<EzKv>) -> HttpResponse {
    let metrics = node.metrics().await;
    let membership = metrics.membership_config.membership();

    // WIRING POINT (ech0-media#33): once `EzKv::cluster()` exists, replace this whole handler
    // body with a read of its `ClusterState` (role, term, leader, voters/learners, whether this
    // node is still a member) - push-updated rather than polled through openraft's raw
    // `RaftMetrics` the way this approximation has to, absent that ticket.
    HttpResponse::Ok().json(StatusResponse {
        node_id: node.node_id(),
        is_leader: node.is_leader(),
        revision: node.view().revision(),
        role: format!("{:?}", metrics.state),
        term: metrics.current_term,
        leader_id: metrics.current_leader,
        voters: membership.voter_ids().collect(),
        learners: membership.learner_ids().collect(),
    })
}

/// Query for `GET /watch`: the prefix to subscribe to, and an optional resume revision
#[derive(Deserialize)]
struct WatchQuery {
    #[serde(default)]
    prefix: String,
    after: Option<u64>,
}

/// `GET /watch`: a streamed, newline-delimited JSON subscription to `prefix`'s changes - see this
/// module's own doc comment for the exact line shapes.
async fn handle_watch(node: web::Data<EzKv>, query: web::Query<WatchQuery>) -> impl Responder {
    let prefix = query.prefix.clone();

    let lines: Pin<Box<dyn Stream<Item = String> + Send>> = match query.after {
        Some(after) => node
            .watch(prefix.clone(), WatchStart::After(after))
            .map(move |update| update_line(update, &prefix))
            .boxed(),
        None => {
            let (view, updates) = node.view_and_watch(prefix.clone());
            let initial = reset_line(view.revision(), view.prefix(&prefix), json!({"kind": "initial"}));
            stream::iter(std::iter::once(initial))
                .chain(updates.map(move |update| update_line(update, &prefix)))
                .boxed()
        }
    };

    let body = lines.map(|line| Ok::<_, actix_web::Error>(web::Bytes::from(format!("{line}\n"))));

    HttpResponse::Ok().content_type("application/x-ndjson").streaming(body)
}

/// Build one `"reset"` line: `revision`, `cause`, and `entries` already filtered to the
/// subscribed prefix
fn reset_line(revision: u64, entries: impl Iterator<Item = KeyValue>, cause: serde_json::Value) -> String {
    json!({
        "type": "reset",
        "revision": revision,
        "cause": cause,
        "entries": entries.collect::<Vec<KeyValue>>(),
    })
    .to_string()
}

/// Build one line for a live [`WatchUpdate`]
fn update_line(update: WatchUpdate, prefix: &str) -> String {
    match update {
        WatchUpdate::Changes(changes) => json!({
            "type": "changes",
            "revision": changes.revision,
            "items": changes.items,
        })
        .to_string(),
        WatchUpdate::Reset { view, cause } => {
            reset_line(view.revision(), view.prefix(prefix), reset_cause_json(&cause))
        }
        WatchUpdate::Progress { revision } => json!({"type": "progress", "revision": revision}).to_string(),
    }
}

/// The crate's own [`ResetCause`] as JSON - it carries no `Serialize` of its own (it is not a
/// wire type inside the crate), so this route hand-writes the mapping once, here.
fn reset_cause_json(cause: &ResetCause) -> serde_json::Value {
    match cause {
        ResetCause::ResumeTooOld {
            requested,
            window_floor,
        } => json!({
            "kind": "resume_too_old",
            "requested": requested,
            "window_floor": window_floor,
        }),
        ResetCause::SubscriberTooSlow => json!({"kind": "subscriber_too_slow"}),
        ResetCause::SnapshotInstalled => json!({"kind": "snapshot_installed"}),
    }
}

/// Bind this example's external API to `addr`
///
/// Synchronous: binding can fail immediately (e.g. `AddrInUse`), so `main` gets that failure
/// right away rather than after `await`ing an accept loop that never started. Running the accept
/// loop is what the returned [`Server`] does once `main` awaits it, alongside the internal port's
/// own (`node.clone().serve()`).
pub(crate) fn bind(node: EzKv, addr: impl AsRef<str>) -> io::Result<Server> {
    let node_data = web::Data::new(node);

    let server = HttpServer::new(move || {
        App::new()
            .app_data(node_data.clone())
            .route("/tx", web::post().to(handle_tx))
            .route("/view", web::get().to(handle_view))
            .route("/linearizable", web::get().to(handle_linearizable))
            .route("/status", web::get().to(handle_status))
            .route("/watch", web::get().to(handle_watch))
    })
    .bind(addr.as_ref())?;

    Ok(server.run())
}
