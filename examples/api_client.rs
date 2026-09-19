//! A small `reqwest`-based client for `kvstore`'s external API (`examples/kvstore/api.rs`),
//! shared by every `*_test.rs` file that drives it. Kept separate from `harness.rs`, which owns
//! process lifecycle only and knows nothing about HTTP.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io;
use std::time::Instant;

use bytes::Bytes;
use ezraft::Change;
use ezraft::KeyValue;
use ezraft::RequestId;
use reqwest::StatusCode;
use serde_json::Value;
use serde_json::json;

use crate::harness::POLL_INTERVAL;

/// Base64-encode `text` the way `POST /tx` expects a `Put`'s value (matching
/// `ezraft::Op::Put`'s own JSON convention)
pub fn b64(text: &str) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, text)
}

/// A one-`Put` transaction body for `POST /tx`
///
/// `conditions` is required by `ezraft::Txn`'s own `Deserialize` even when empty - there is no
/// `#[serde(default)]` on it, so a body that omits the key fails to deserialize rather than
/// defaulting to no conditions.
pub fn put(key: &str, value: &str) -> Value {
    json!({"conditions": [], "ops": [{"Put": {"key": key, "value": b64(value)}}]})
}

/// A one-`Delete` transaction body for `POST /tx` - see [`put`] on why `conditions` is required
pub fn delete(key: &str) -> Value {
    json!({"conditions": [], "ops": [{"Delete": {"key": key}}]})
}

/// A definite outcome [`write`] resolved
#[derive(Debug, Clone)]
pub enum TxOutcome {
    Success { revision: u64, request_id: RequestId },
    Conflict(Value),
}

/// Submit `txn` (a `POST /tx` body, e.g. from [`put`]/[`delete`]) against whichever of `addrs`
/// currently accepts it, until a *definite* outcome or `deadline`.
///
/// Generates one [`RequestId`] up front and resends the exact same id on every retry - whether
/// the previous attempt came back `Indeterminate`, or the connection to whichever node it hit
/// simply failed (indistinguishable, from here, from a response that never arrived) - which is
/// how a caller resolves an indeterminate write without risking running it twice: the foundation's
/// idempotency ledger recognizes the id and returns the first real outcome instead of executing
/// `txn` again. This is this example's own demonstration of that pattern, not part of the crate.
pub async fn write(addrs: &[&str], mut txn: Value, deadline: Instant) -> io::Result<TxOutcome> {
    let client = reqwest::Client::new();
    let request_id = RequestId::new();
    txn["request_id"] = json!(request_id);

    loop {
        for addr in addrs {
            if let Ok(resp) = client.post(format!("http://{addr}/tx")).json(&txn).send().await {
                match resp.status() {
                    StatusCode::OK => {
                        let body: Value = resp.json().await.map_err(io::Error::other)?;
                        let revision = body["revision"]
                            .as_u64()
                            .ok_or_else(|| io::Error::other(format!("no revision in /tx response: {body}")))?;
                        return Ok(TxOutcome::Success { revision, request_id });
                    }
                    StatusCode::CONFLICT => {
                        return Ok(TxOutcome::Conflict(resp.json().await.map_err(io::Error::other)?));
                    }
                    // `Indeterminate` (504), `LocalLag` (503), or anything else transient: retry,
                    // with the same request id, against any address - including this one again,
                    // once it is a node that can answer.
                    _ => {}
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "no node in {addrs:?} produced a definite outcome for request {request_id} within the deadline"
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The text a `Bytes` value decodes to, for the plain UTF-8 text this test suite always writes
pub fn text(value: &Bytes) -> String {
    String::from_utf8_lossy(value).into_owned()
}

/// Poll `addr`'s local view (`GET /view?key=`) until `key` reads back exactly `expected`
/// (`None` for "absent"), or `deadline` passes. Tolerates the node not serving yet.
pub async fn wait_for_value(addr: &str, key: &str, expected: Option<&str>, deadline: Instant) -> io::Result<()> {
    let client = reqwest::Client::new();
    let mut last: Option<String> = None;
    loop {
        if let Ok(resp) = client.get(format!("http://{addr}/view")).query(&[("key", key)]).send().await {
            match resp.status() {
                StatusCode::NOT_FOUND => {
                    if expected.is_none() {
                        return Ok(());
                    }
                    last = None;
                }
                StatusCode::OK => {
                    let kv: KeyValue = resp.json().await.map_err(io::Error::other)?;
                    let value = text(&kv.value);
                    if Some(value.as_str()) == expected {
                        return Ok(());
                    }
                    last = Some(value);
                }
                status => {
                    return Err(io::Error::other(format!(
                        "GET /view?key={key} on {addr} failed: {status}"
                    )));
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "{addr} never served {expected:?} for {key}, last seen: {last:?}"
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// One `GET /status` reading
pub async fn status(addr: &str) -> io::Result<Value> {
    reqwest::Client::new()
        .get(format!("http://{addr}/status"))
        .send()
        .await
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)
}

/// Poll `addr`'s `GET /status` until `predicate` accepts a reading, or `deadline` passes
pub async fn wait_for_status<F>(addr: &str, deadline: Instant, mut predicate: F) -> io::Result<Value>
where F: FnMut(&Value) -> bool {
    let mut last = None;
    loop {
        if let Ok(value) = status(addr).await {
            if predicate(&value) {
                return Ok(value);
            }
            last = Some(value);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "{addr} never satisfied the status predicate within the deadline, last seen: {last:?}"
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Each of `apis`' own `node_id`, in the same order - lets a test recover "which spawned node is
/// this" from a `/status` reading elsewhere (e.g. `leader_id`) without threading node ids through
/// spawn order by hand.
pub async fn node_ids(apis: &[String], deadline: Instant) -> io::Result<Vec<u64>> {
    let mut ids = Vec::with_capacity(apis.len());
    for api in apis {
        let value = wait_for_status(api, deadline, |_| true).await?;
        let id = value["node_id"]
            .as_u64()
            .ok_or_else(|| io::Error::other(format!("{api}'s /status carried no node_id: {value}")))?;
        ids.push(id);
    }
    Ok(ids)
}

/// One line `GET /watch` delivered, decoded - see `examples/kvstore/api.rs`'s own doc comment for
/// the wire shape.
#[derive(Debug, Clone)]
pub enum WatchLine {
    Reset {
        revision: u64,
        cause: Value,
        entries: Vec<KeyValue>,
    },
    Changes {
        revision: u64,
        items: Vec<Change>,
    },
    Progress {
        revision: u64,
    },
}

impl WatchLine {
    fn parse(line: &str) -> io::Result<Self> {
        let value: Value = serde_json::from_str(line).map_err(io::Error::other)?;
        let revision = || {
            value["revision"]
                .as_u64()
                .ok_or_else(|| io::Error::other(format!("watch line missing revision: {line}")))
        };
        match value["type"].as_str() {
            Some("reset") => Ok(WatchLine::Reset {
                revision: revision()?,
                cause: value["cause"].clone(),
                entries: serde_json::from_value(value["entries"].clone()).map_err(io::Error::other)?,
            }),
            Some("changes") => Ok(WatchLine::Changes {
                revision: revision()?,
                items: serde_json::from_value(value["items"].clone()).map_err(io::Error::other)?,
            }),
            Some("progress") => Ok(WatchLine::Progress { revision: revision()? }),
            other => Err(io::Error::other(format!("unknown watch line type {other:?}: {line}"))),
        }
    }

    /// This line's revision, whichever variant it is
    pub fn revision(&self) -> u64 {
        match self {
            WatchLine::Reset { revision, .. }
            | WatchLine::Changes { revision, .. }
            | WatchLine::Progress { revision } => *revision,
        }
    }
}

/// A live `GET /watch` subscription, read one line at a time
pub struct WatchClient {
    resp: reqwest::Response,
    buf: String,
}

impl WatchClient {
    /// Connect to `addr`'s `GET /watch?prefix=<prefix>` - `&after=<rev>` too, when given
    pub async fn connect(addr: &str, prefix: &str, after: Option<u64>) -> io::Result<Self> {
        let mut url = format!("http://{addr}/watch?prefix={prefix}");
        if let Some(after) = after {
            url.push_str(&format!("&after={after}"));
        }

        let resp = reqwest::Client::new().get(url).send().await.map_err(io::Error::other)?;
        if !resp.status().is_success() {
            return Err(io::Error::other(format!(
                "GET /watch on {addr} failed: {}",
                resp.status()
            )));
        }

        Ok(Self {
            resp,
            buf: String::new(),
        })
    }

    /// The next line, or `None` once the stream ends - including the connection simply dropping,
    /// e.g. because this node was just SIGKILLed. `chunk()` needs no extra `reqwest` feature,
    /// unlike `bytes_stream()`.
    pub async fn next_line(&mut self) -> Option<io::Result<WatchLine>> {
        loop {
            if let Some(pos) = self.buf.find('\n') {
                let line: String = self.buf.drain(..=pos).collect();
                return Some(WatchLine::parse(line.trim_end()));
            }
            match self.resp.chunk().await {
                Ok(Some(chunk)) => self.buf.push_str(&String::from_utf8_lossy(&chunk)),
                Ok(None) => return None,
                Err(_) => return None,
            }
        }
    }
}

/// Folds a sequence of [`WatchLine`]s into the key/value state they describe, restricted to
/// whatever prefix the subscription was for.
///
/// Deduplicates by each key's `mod_rev` (a `Delete`'s own "new" revision is the group's
/// `revision`, since [`ezraft::Change::Delete`] carries only the value from *before* the
/// deletion): a change at or below the revision already folded in for that key is a replay,
/// ignored rather than applied again - see `EzKv::watch`'s own docs on when a resumed
/// subscription can repeat one.
#[derive(Default, Debug)]
pub struct Fold {
    state: BTreeMap<String, String>,
    seen_rev: BTreeMap<String, u64>,
}

impl Fold {
    pub fn state(&self) -> &BTreeMap<String, String> {
        &self.state
    }

    pub fn apply(&mut self, line: &WatchLine) {
        match line {
            WatchLine::Reset { entries, .. } => {
                self.state.clear();
                self.seen_rev.clear();
                for entry in entries {
                    self.state.insert(entry.key.clone(), text(&entry.value));
                    self.seen_rev.insert(entry.key.clone(), entry.mod_rev);
                }
            }
            WatchLine::Changes { revision, items } => {
                for item in items {
                    match item {
                        Change::Put {
                            key, value, mod_rev, ..
                        } => self.apply_one(key, Some(text(value)), *mod_rev),
                        Change::Delete { key, prev } => self.apply_one(key, None, (*revision).max(prev.mod_rev)),
                    }
                }
            }
            WatchLine::Progress { .. } => {}
        }
    }

    fn apply_one(&mut self, key: &str, value: Option<String>, revision: u64) {
        let last = self.seen_rev.get(key).copied().unwrap_or(0);
        if revision <= last {
            return;
        }
        self.seen_rev.insert(key.to_string(), revision);
        match value {
            Some(value) => {
                self.state.insert(key.to_string(), value);
            }
            None => {
                self.state.remove(key);
            }
        }
    }
}
