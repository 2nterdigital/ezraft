//! Raw-binary snapshot transfer (ech0-media#26): a state well over 2 MiB - past the actix
//! defaults (2 MiB JSON, 256 KiB raw body) the old JSON-array-of-numbers encoding and the
//! unconfigured payload limit used to fail on - installs on a node that lagged behind a purged
//! log and on a node that joins fresh after the purge, both ending with exactly the leader's view.

mod common;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;

use common::*;
use ezraft::EzKv;
use ezraft::KeyValue;
use ezraft::View;

/// One 64 KiB value (`kv_limits.max_value_bytes`'s own default) repeated enough times that the
/// exported state is comfortably in the 4-8 MiB range this ticket's acceptance criteria call for.
const VALUE_LEN: usize = 64 * 1024;
const ENTRIES: usize = 80;

/// A value with enough distinct content that a truncated or corrupted transfer would not
/// accidentally read back as correct.
fn big_value(i: usize) -> String {
    format!("{i:08}-").chars().cycle().take(VALUE_LEN).collect()
}

/// Every key, value and revision bookkeeping in `view`, keyed by key - the full `KeyValue`
/// (`create_rev`/`mod_rev` included), not just the text `tests/common::view_map` compares.
fn full_view(view: &View) -> BTreeMap<String, KeyValue> {
    view.prefix("").map(|kv| (kv.key.clone(), kv)).collect()
}

/// A state larger than 2 MiB installs, over the raw-binary snapshot route, on both a node that
/// lagged behind a purged log and a node that joins fresh after the purge - and both end up with
/// exactly the leader's view.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_over_2_mib_installs_on_a_lagging_node_and_a_fresh_joiner() -> io::Result<()> {
    let (addr_a, a, _data_a) = founding_node().await?;

    // A learner added before the purge, but not yet serving: it can only be caught up by the
    // whole snapshot the leader builds after it falls behind (`add_learner` does not wait for the
    // node it adds, so the leader can compact its whole log first - the same technique
    // `tests/snapshot.rs::restart_after_a_snapshot_with_no_log_after_it` uses).
    let addr_c = free_addr();
    let data_c = NodeData::new();
    let c = EzKv::join_as_learner(&addr_c, &addr_a, data_c.open().await?, config()).await?;

    for i in 0..ENTRIES {
        a.write(set(&format!("k{i}"), &big_value(i))).await.map_err(|e| io::Error::other(e.to_string()))?;
    }

    a.trigger_snapshot().await?;
    let snapshot_index =
        a.wait_metrics(WAIT, |m| m.snapshot.is_some(), "snapshot built").await?.snapshot.unwrap().index;
    a.purge_log(snapshot_index).await?;
    a.wait_metrics(
        WAIT,
        |m| m.purged.map(|log_id| log_id.index) == Some(snapshot_index),
        "log purged up to the snapshot",
    )
    .await?;

    // Only now does the lagging learner start serving, and only a whole snapshot transfer can
    // catch it up: openraft cannot replay a log the leader no longer has.
    spawn_serve(&c);
    wait_for_applied(&c, &a).await?;
    let leader_view = full_view(&a.view());
    assert_eq!(ENTRIES, leader_view.len());
    assert_eq!(
        leader_view,
        full_view(&c.view()),
        "the lagging node's view - keys, values and every create_rev/mod_rev - must match the leader's exactly"
    );
    assert_eq!(
        a.view().revision(),
        c.view().revision(),
        "the KV's own revision counter must match too"
    );

    // A node that joins fresh, after the purge, is caught up the same way.
    let addr_b = free_addr();
    let data_b = NodeData::new();
    let b = EzKv::join(&addr_b, &addr_a, data_b.open().await?, config()).await?;
    spawn_serve(&b);
    wait_for_voters(
        &b,
        BTreeSet::from([a.node_id(), b.node_id()]),
        "snapshot-fed fresh joiner promoted to voter",
    )
    .await?;
    assert_eq!(
        leader_view,
        full_view(&b.view()),
        "the fresh joiner's view - keys, values and every create_rev/mod_rev - must match the leader's exactly"
    );
    assert_eq!(
        a.view().revision(),
        b.view().revision(),
        "the KV's own revision counter must match too"
    );

    // And the trio keeps working past the transfer: the same later write reaches every node.
    write_through_leader(&[&a, &b], set("after", "value")).await?;
    wait_for_applied(&b, &a).await?;
    wait_for_applied(&c, &a).await?;
    assert_eq!(Some("value".to_string()), b.view().get("after").map(|kv| text(&kv)));
    assert_eq!(Some("value".to_string()), c.view().get("after").map(|kv| text(&kv)));

    Ok(())
}
