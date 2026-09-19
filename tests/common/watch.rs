//! Helpers for driving and asserting on `watch`/`view_and_watch` streams (ech0-media#32).
//!
//! Additive to `tests/common/mod.rs`: node-building itself is not duplicated here.

use std::collections::BTreeMap;

use ezraft::Change;
use ezraft::Changes;
use ezraft::View;
use ezraft::WatchStream;
use ezraft::WatchUpdate;
use futures::StreamExt;

use super::WAIT;
use super::text;

/// Every key and value under `prefix` in `view`, decoded as UTF-8 text - the prefix-scoped
/// counterpart to `view_map`, for comparing a watch subscription's folded state against the
/// replicated state it is supposed to track.
pub fn view_map_prefix(view: &View, prefix: &str) -> BTreeMap<String, String> {
    view.prefix(prefix).map(|kv| (kv.key.clone(), text(&kv))).collect()
}

/// Apply one `Changes` group's items, in order, to `map` - the fold rule every watch test's
/// consumer uses to keep its own map in sync with what a subscription delivers
pub fn fold_changes(map: &mut BTreeMap<String, String>, changes: &Changes) {
    for change in &changes.items {
        match change {
            Change::Put { key, value, .. } => {
                map.insert(key.clone(), String::from_utf8_lossy(value).into_owned());
            }
            Change::Delete { key, .. } => {
                map.remove(key);
            }
        }
    }
}

/// The key a `Put` or `Delete` touched, regardless of which it is - for tests that only care
/// which keys a group's items name, not their values
pub fn change_key(change: &Change) -> &str {
    match change {
        Change::Put { key, .. } | Change::Delete { key, .. } => key,
    }
}

/// Wait for a [`WatchStream`]'s next item, bounded by the harness's usual wait budget
///
/// A stream ends only when its node shuts down, so on a running node a bare `.next().await` with
/// no bound would hang a test that mistakenly expects an item that never arrives.
pub async fn next_update(stream: &mut WatchStream) -> WatchUpdate {
    tokio::time::timeout(WAIT.expect("cluster waits are bounded"), stream.next())
        .await
        .expect("a WatchStream must deliver its next item within the wait budget")
        .expect("a WatchStream ends only when its node shuts down")
}
