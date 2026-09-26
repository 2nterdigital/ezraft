//! Ownership of the pre-bound listener, HTTP server, and post-start join promotion.

use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

use actix_web::dev::ServerHandle;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::EzKv;
use crate::admin::AdminClient;
use crate::admin::MembershipChange;
use crate::node_role::NodeRole;

pub(super) struct PromotionRequest {
    admin: AdminClient,
    node_id: u64,
}

impl PromotionRequest {
    pub(super) fn new(admin: AdminClient, node_id: u64) -> Self {
        Self { admin, node_id }
    }
}

#[derive(Clone)]
pub(super) struct NodeLifecycle {
    runtime: tokio::runtime::Handle,
    state: Arc<Mutex<State>>,
}

struct State {
    listener: Option<TcpListener>,
    pending_promotion: Option<PromotionRequest>,
    server_handle: Option<ServerHandle>,
    server_task: Option<JoinHandle<()>>,
    promotion_task: Option<JoinHandle<()>>,
    close_done: watch::Sender<bool>,
    started: bool,
    closed: bool,
}

struct Resources {
    listener: Option<TcpListener>,
    pending_promotion: Option<PromotionRequest>,
    server_handle: Option<ServerHandle>,
    server_task: Option<JoinHandle<()>>,
    promotion_task: Option<JoinHandle<()>>,
}

struct StartError {
    error: io::Error,
    close_lifecycle: bool,
}

impl NodeLifecycle {
    pub(super) fn new(listener: TcpListener, pending_promotion: Option<PromotionRequest>) -> Self {
        let (close_done, _) = watch::channel(false);
        Self {
            runtime: tokio::runtime::Handle::current(),
            state: Arc::new(Mutex::new(State {
                listener: Some(listener),
                pending_promotion,
                server_handle: None,
                server_task: None,
                promotion_task: None,
                close_done,
                started: false,
                closed: false,
            })),
        }
    }

    fn start_server<F>(
        &self,
        raft: EzKv,
        configure: F,
    ) -> Result<(oneshot::Receiver<()>, oneshot::Receiver<io::Result<()>>), StartError>
    where
        F: Fn(&mut actix_web::web::ServiceConfig) + Clone + Send + 'static,
    {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(StartError {
                error: io::Error::new(io::ErrorKind::BrokenPipe, "EzKv is shut down"),
                close_lifecycle: true,
            });
        }
        if state.started {
            return Err(StartError {
                error: io::Error::new(io::ErrorKind::AlreadyExists, "EzKv::serve may only be started once"),
                close_lifecycle: false,
            });
        }

        state.started = true;
        let listener = state.listener.take().ok_or_else(|| StartError {
            error: io::Error::new(io::ErrorKind::BrokenPipe, "the internal listener is unavailable"),
            close_lifecycle: true,
        })?;
        let server = match crate::server::bind(raft, listener, configure) {
            Ok(server) => server,
            Err(error) => {
                return Err(StartError {
                    error,
                    close_lifecycle: true,
                });
            }
        };

        let server_handle = server.handle();
        let (ready_tx, ready_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            let mut server = Box::pin(server);
            let mut ready_tx = Some(ready_tx);
            let result = poll_fn(|cx| {
                let result = server.as_mut().poll(cx);
                if matches!(result, Poll::Pending)
                    && let Some(ready_tx) = ready_tx.take()
                {
                    // actix-server 2.9.5's first poll starts the acceptor and waits for each
                    // worker's service factory initialization before it returns Pending.
                    let _ = ready_tx.send(());
                }
                result
            })
            .await;
            let _ = result_tx.send(result);
        });

        state.server_handle = Some(server_handle);
        state.server_task = Some(server_task);
        Ok((ready_rx, result_rx))
    }

    fn start_promotion(&self) -> Option<oneshot::Receiver<io::Result<()>>> {
        let mut state = self.state.lock().unwrap();
        if state.closed || !state.started {
            return None;
        }

        let request = state.pending_promotion.take()?;
        let (result_tx, result_rx) = oneshot::channel();
        let promotion_task = tokio::spawn(async move {
            let change = MembershipChange::SetRole {
                node_id: request.node_id,
                role: NodeRole::Voter,
            };
            let _ = result_tx.send(request.admin.membership(change).await);
        });
        state.promotion_task = Some(promotion_task);
        Some(result_rx)
    }

    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    fn begin_close(&self) -> (Option<Resources>, watch::Receiver<bool>, watch::Sender<bool>) {
        let mut state = self.state.lock().unwrap();
        let done_rx = state.close_done.subscribe();
        let done_tx = state.close_done.clone();
        if state.closed {
            return (None, done_rx, done_tx);
        }
        state.closed = true;
        if let Some(task) = &state.promotion_task {
            task.abort();
        }
        let resources = Resources {
            listener: state.listener.take(),
            pending_promotion: state.pending_promotion.take(),
            server_handle: state.server_handle.take(),
            server_task: state.server_task.take(),
            promotion_task: state.promotion_task.take(),
        };
        (Some(resources), done_rx, done_tx)
    }

    async fn close(&self) {
        let (resources, mut done_rx, done_tx) = self.begin_close();
        if let Some(resources) = resources {
            self.runtime.spawn(async move {
                close_resources(resources).await;
                done_tx.send_replace(true);
            });
        }
        wait_for_close(&mut done_rx).await;
    }

    fn close_detached(&self) {
        let (resources, _done_rx, done_tx) = self.begin_close();
        if let Some(resources) = resources {
            if let Some(task) = &resources.promotion_task {
                task.abort();
            }
            let stop = resources.server_handle.as_ref().map(|handle| handle.stop(true));
            // A Send serve future may be dropped on a thread outside Tokio. Cleanup still
            // belongs to the live runtime that owns the node, and completion is published only
            // after its server and promotion tasks have actually finished.
            self.runtime.spawn(async move {
                close_resources_after_stop(resources, stop).await;
                done_tx.send_replace(true);
            });
        }
    }
}

async fn close_resources(resources: Resources) {
    if let Some(task) = &resources.promotion_task {
        task.abort();
    }
    let stop = resources.server_handle.as_ref().map(|handle| handle.stop(true));
    close_resources_after_stop(resources, stop).await;
}

async fn close_resources_after_stop(resources: Resources, stop: Option<impl std::future::Future<Output = ()>>) {
    // Release a listener that was reserved but never handed to Actix.
    drop(resources.listener);
    drop(resources.pending_promotion);

    if let Some(stop) = stop {
        stop.await;
    }
    if let Some(task) = resources.promotion_task {
        let _ = task.await;
    }
    if let Some(task) = resources.server_task {
        let _ = task.await;
    }
}

async fn wait_for_close(done_rx: &mut watch::Receiver<bool>) {
    while !*done_rx.borrow_and_update() {
        if done_rx.changed().await.is_err() {
            break;
        }
    }
}

struct ServeGuard {
    lifecycle: NodeLifecycle,
    armed: bool,
}

impl Drop for ServeGuard {
    fn drop(&mut self) {
        if self.armed {
            self.lifecycle.close_detached();
        }
    }
}

fn server_result(result: Result<io::Result<()>, oneshot::error::RecvError>) -> io::Result<()> {
    match result {
        Ok(result) => result,
        Err(err) => Err(io::Error::other(format!("internal HTTP server task ended: {err}"))),
    }
}

async fn run_started_server(
    lifecycle: &NodeLifecycle,
    mut ready: oneshot::Receiver<()>,
    mut server_result_rx: oneshot::Receiver<io::Result<()>>,
) -> io::Result<()> {
    tokio::select! {
        result = &mut server_result_rx => return server_result(result),
        started = &mut ready => {
            if started.is_err() {
                return server_result(server_result_rx.await);
            }
        }
    }

    if let Some(mut promotion_result_rx) = lifecycle.start_promotion() {
        tokio::select! {
            server = &mut server_result_rx => server_result(server),
            promotion = &mut promotion_result_rx => match promotion {
                Ok(Ok(())) => server_result(server_result_rx.await),
                Ok(Err(err)) => Err(err),
                Err(_) if lifecycle.is_closed() => Ok(()),
                Err(err) => Err(io::Error::other(format!("promotion task ended: {err}"))),
            },
        }
    } else {
        server_result(server_result_rx.await)
    }
}

impl EzKv {
    /// Stop this node's Raft engine and HTTP server, and wait for local lifecycle work to finish.
    ///
    /// Releases the listener acquired during construction, including when `serve` was never
    /// called. The internal HTTP server does not handle process signals; the embedding
    /// application owns them and calls this after its own drain and cleanup are complete.
    pub async fn shutdown(&self) -> Result<(), io::Error> {
        if let Some(handle) = self.self_registration.lock().unwrap().take() {
            handle.abort();
        }

        self.lifecycle.close().await;
        self.raft.shutdown().await.map_err(io::Error::other)?;
        self.watch.close_watch();
        Ok(())
    }

    /// Start the internal HTTP server and wait until it stops.
    ///
    /// A joining voter starts its promotion only after Actix has initialized the HTTP workers.
    /// The server keeps handling Raft RPC while that membership request waits to complete.
    pub async fn serve(self) -> Result<(), io::Error> {
        self.serve_with(|_| {}).await
    }

    /// Start the internal HTTP server with application-specific routes.
    ///
    /// Standard routes, cluster-token authentication, and the configured body limits wrap the
    /// custom routes. Process signals remain the embedding application's responsibility.
    pub async fn serve_with<F>(self, configure: F) -> Result<(), io::Error>
    where F: Fn(&mut actix_web::web::ServiceConfig) + Clone + Send + 'static {
        let (ready, server_result_rx) = match self.lifecycle.start_server(self.clone(), configure) {
            Ok(start) => start,
            Err(start_error) => {
                if start_error.close_lifecycle {
                    self.lifecycle.close().await;
                }
                return Err(start_error.error);
            }
        };
        let mut guard = ServeGuard {
            lifecycle: self.lifecycle.clone(),
            armed: true,
        };
        let result = run_started_server(&self.lifecycle, ready, server_result_rx).await;
        self.lifecycle.close().await;
        guard.armed = false;
        result
    }
}
