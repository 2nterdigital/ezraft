//! A voter join defers its promotion until its public internal HTTP server is serving.
//!
//! The fake seed below is only an HTTP system boundary: it counts delivered membership requests
//! and can hold a response. A separate real-cluster test checks that cancelling local serving
//! does not remove a voter whose membership change has already committed.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actix_web::App;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use actix_web::dev::ServerHandle;
use actix_web::web;
use common::NodeData;
use common::config;
use common::founding_node;
use common::free_addr;
use common::joined_voter;
use common::wait_for_voters;
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::admin::MembershipChange;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const CLUSTER_TOKEN: &str = "promotion-lifecycle-test-token";
const REQUEST_WAIT: Duration = Duration::from_secs(5);
const NO_RETRY_WINDOW: Duration = Duration::from_millis(700);

#[derive(Clone, Copy)]
enum PromotionReply {
    Success,
    NoLeader,
    InternalError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObservedChange {
    Add,
    SetRole,
    Remove,
}

#[derive(Clone)]
struct FakeSeedState {
    changes: Arc<Mutex<Vec<ObservedChange>>>,
    promotion_requests: Arc<AtomicUsize>,
    promotion_started: mpsc::UnboundedSender<u64>,
    promotion_gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    promotion_reply: PromotionReply,
}

struct FakeSeed {
    addr: String,
    changes: Arc<Mutex<Vec<ObservedChange>>>,
    promotion_requests: Arc<AtomicUsize>,
    promotion_started: mpsc::UnboundedReceiver<u64>,
    release_promotion: Option<oneshot::Sender<()>>,
    server_handle: Option<ServerHandle>,
    server: Option<JoinHandle<io::Result<()>>>,
}

impl FakeSeed {
    fn start(hold_first_promotion: bool, promotion_reply: PromotionReply) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?.to_string();
        let changes = Arc::new(Mutex::new(Vec::new()));
        let promotion_requests = Arc::new(AtomicUsize::new(0));
        let (promotion_started, promotion_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = oneshot::channel();
        let promotion_gate = Arc::new(Mutex::new(hold_first_promotion.then_some(release_rx)));
        let state = FakeSeedState {
            changes: Arc::clone(&changes),
            promotion_requests: Arc::clone(&promotion_requests),
            promotion_started,
            promotion_gate: Arc::clone(&promotion_gate),
            promotion_reply,
        };
        let server = HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(state.clone()))
                .route("/api/node_id", web::post().to(fake_node_id))
                .route("/api/membership", web::post().to(fake_membership))
        })
        .disable_signals()
        .workers(1)
        .listen(listener)?
        .run();
        let server_handle = server.handle();

        Ok(Self {
            addr,
            changes,
            promotion_requests,
            promotion_started: promotion_rx,
            release_promotion: hold_first_promotion.then_some(release_tx),
            server_handle: Some(server_handle),
            server: Some(tokio::spawn(server)),
        })
    }

    async fn next_promotion(&mut self) -> io::Result<u64> {
        timeout(REQUEST_WAIT, self.promotion_started.recv())
            .await
            .map_err(|_| io::Error::other("fake seed did not receive a promotion request"))?
            .ok_or_else(|| io::Error::other("fake seed promotion request channel closed"))
    }

    fn release_promotion(&mut self) {
        if let Some(release) = self.release_promotion.take() {
            let _ = release.send(());
        }
    }

    fn request_count(&self) -> usize {
        self.promotion_requests.load(Ordering::SeqCst)
    }

    fn changes(&self) -> Vec<ObservedChange> {
        self.changes.lock().expect("fake seed history is not poisoned").clone()
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.release_promotion();
        if let Some(handle) = self.server_handle.take() {
            handle.stop(false).await;
        }
        if let Some(server) = self.server.take() {
            server.await.map_err(io::Error::other)??;
        }
        Ok(())
    }
}

impl Drop for FakeSeed {
    fn drop(&mut self) {
        self.release_promotion();
        if let (Some(handle), Some(server)) = (self.server_handle.take(), self.server.take()) {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    handle.stop(false).await;
                    let _ = server.await;
                });
            } else {
                server.abort();
            }
        }
    }
}

async fn fake_node_id() -> HttpResponse {
    HttpResponse::Ok().json(Ok::<u64, Option<String>>(1))
}

async fn fake_membership(state: web::Data<FakeSeedState>, change: web::Json<MembershipChange>) -> HttpResponse {
    match change.into_inner() {
        MembershipChange::Add { .. } => {
            state.changes.lock().expect("fake seed history is not poisoned").push(ObservedChange::Add);
            HttpResponse::Ok().json(Ok::<(), Option<String>>(()))
        }
        MembershipChange::SetRole { node_id, role: _ } => {
            state.changes.lock().expect("fake seed history is not poisoned").push(ObservedChange::SetRole);
            state.promotion_requests.fetch_add(1, Ordering::SeqCst);
            let _ = state.promotion_started.send(node_id);
            let gate = state.promotion_gate.lock().expect("fake seed gate is not poisoned").take();
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            match state.promotion_reply {
                PromotionReply::Success => HttpResponse::Ok().json(Ok::<(), Option<String>>(())),
                PromotionReply::NoLeader => HttpResponse::Ok().json(Err::<(), Option<String>>(None)),
                PromotionReply::InternalError => HttpResponse::InternalServerError().finish(),
            }
        }
        MembershipChange::Remove { .. } => {
            state.changes.lock().expect("fake seed history is not poisoned").push(ObservedChange::Remove);
            HttpResponse::Ok().json(Ok::<(), Option<String>>(()))
        }
    }
}

fn fake_join_config() -> EzConfig {
    EzConfig {
        cluster_token: CLUSTER_TOKEN.to_string(),
        ..config()
    }
}

async fn wait_for_port_rebound(addr: &str) -> io::Result<TcpListener> {
    let deadline = tokio::time::Instant::now() + REQUEST_WAIT;
    loop {
        match TcpListener::bind(addr) {
            Ok(listener) => return Ok(listener),
            Err(err) if err.kind() == io::ErrorKind::AddrInUse && tokio::time::Instant::now() < deadline => {
                tokio::task::yield_now().await;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn join_fake_seed(seed: &FakeSeed) -> io::Result<(EzKv, NodeData, String)> {
    let addr = free_addr();
    let data = NodeData::new();
    let node = EzKv::join(&addr, &seed.addr, data.open().await?, fake_join_config()).await?;
    Ok((node, data, addr))
}

async fn assert_no_promotion(receiver: &mut mpsc::UnboundedReceiver<u64>, window: Duration) -> io::Result<()> {
    match timeout(window, receiver.recv()).await {
        Err(_) => Ok(()),
        Ok(Some(node_id)) => Err(io::Error::other(format!(
            "unexpected promotion request for node {node_id}"
        ))),
        Ok(None) => Err(io::Error::other("promotion request channel closed unexpectedly")),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unserved_joiner_stays_unpromoted_and_shutdown_releases_its_listener() -> io::Result<()> {
    let mut seed = FakeSeed::start(false, PromotionReply::NoLeader)?;
    let (node, _data, addr) = join_fake_seed(&seed).await?;

    assert_eq!(
        io::ErrorKind::AddrInUse,
        TcpListener::bind(&addr).unwrap_err().kind(),
        "the constructor holds the internal listener before serve",
    );
    assert_no_promotion(&mut seed.promotion_started, NO_RETRY_WINDOW).await?;
    assert_eq!(0, seed.request_count());
    assert_eq!(vec![ObservedChange::Add], seed.changes());

    node.shutdown().await?;
    let _rebound = TcpListener::bind(&addr)?;
    assert_no_promotion(&mut seed.promotion_started, NO_RETRY_WINDOW).await?;
    assert_eq!(0, seed.request_count(), "shutdown does not start a promotion or retry");
    seed.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn serving_starts_promotion_after_the_internal_http_server_is_ready() -> io::Result<()> {
    let mut seed = FakeSeed::start(false, PromotionReply::Success)?;
    let (node, _data, addr) = join_fake_seed(&seed).await?;
    assert_no_promotion(&mut seed.promotion_started, Duration::from_millis(100)).await?;

    let serving = tokio::spawn(node.clone().serve());
    let promoted_id = seed.next_promotion().await?;
    assert_eq!(node.node_id(), promoted_id);

    let response = reqwest::Client::new()
        .get(format!("http://{addr}/api/metrics"))
        .bearer_auth(CLUSTER_TOKEN)
        .send()
        .await
        .map_err(io::Error::other)?;
    assert!(
        response.status().is_success(),
        "the internal server is serving before promotion: {}",
        response.status()
    );

    node.shutdown().await?;
    serving.await.map_err(io::Error::other)??;
    let _rebound = TcpListener::bind(&addr)?;
    assert_eq!(1, seed.request_count());
    seed.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_serve_stops_local_promotion_retries_and_releases_the_listener() -> io::Result<()> {
    let mut seed = FakeSeed::start(true, PromotionReply::InternalError)?;
    let (node, _data, addr) = join_fake_seed(&seed).await?;
    let serving = tokio::spawn(node.clone().serve());
    assert_eq!(node.node_id(), seed.next_promotion().await?);

    serving.abort();
    assert!(serving.await.expect_err("serve future was cancelled").is_cancelled());
    let _rebound = wait_for_port_rebound(&addr).await?;
    node.shutdown().await?;

    seed.release_promotion();
    assert_no_promotion(&mut seed.promotion_started, NO_RETRY_WINDOW).await?;
    assert_eq!(1, seed.request_count(), "no local promotion retry followed shutdown");
    seed.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_during_promotion_cancels_retries_and_releases_the_listener() -> io::Result<()> {
    let mut seed = FakeSeed::start(true, PromotionReply::InternalError)?;
    let (node, _data, addr) = join_fake_seed(&seed).await?;
    let serving = tokio::spawn(node.clone().serve());
    assert_eq!(node.node_id(), seed.next_promotion().await?);

    node.shutdown().await?;
    serving.await.map_err(io::Error::other)??;
    let _rebound = TcpListener::bind(&addr)?;

    seed.release_promotion();
    assert_no_promotion(&mut seed.promotion_started, NO_RETRY_WINDOW).await?;
    assert_eq!(1, seed.request_count(), "shutdown prevents local promotion retries");
    seed.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_shutdown_calls_share_promotion_and_listener_cleanup() -> io::Result<()> {
    let mut seed = FakeSeed::start(true, PromotionReply::InternalError)?;
    let (node, _data, addr) = join_fake_seed(&seed).await?;
    let serving = tokio::spawn(node.clone().serve());
    assert_eq!(node.node_id(), seed.next_promotion().await?);

    let (first, second) = tokio::join!(node.shutdown(), node.shutdown());
    first?;
    second?;
    serving.await.map_err(io::Error::other)??;
    let _rebound = TcpListener::bind(&addr)?;

    seed.release_promotion();
    assert_no_promotion(&mut seed.promotion_started, NO_RETRY_WINDOW).await?;
    assert_eq!(1, seed.request_count(), "concurrent shutdowns do not retry promotion");
    seed.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_local_serve_does_not_remove_a_committed_cluster_voter() -> io::Result<()> {
    let (seed_addr, seed, _seed_data) = founding_node().await?;
    let (_, second, _second_data) = joined_voter(&seed_addr).await?;
    let mut voters = BTreeSet::from([seed.node_id(), second.node_id()]);
    wait_for_voters(&seed, voters.clone(), "the initial two-voter cluster is uniform").await?;

    let addr = free_addr();
    let data = NodeData::new();
    let third = EzKv::join(&addr, &seed_addr, data.open().await?, common::config()).await?;
    let third_id = third.node_id();
    voters.insert(third_id);
    let serving = tokio::spawn(third.clone().serve());
    wait_for_voters(&seed, voters.clone(), "the serving joiner is committed as a voter").await?;

    serving.abort();
    assert!(serving.await.expect_err("serve future was cancelled").is_cancelled());
    third.shutdown().await?;
    let _rebound = TcpListener::bind(&addr)?;

    wait_for_voters(&seed, voters, "local cancellation does not remove the committed voter").await?;
    second.shutdown().await?;
    seed.shutdown().await?;
    Ok(())
}
