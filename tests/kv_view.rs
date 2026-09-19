//! `EzKv::view` acceptance tests (ech0-media#27): concurrent readers never see half of a
//! multi-key transaction, and a view held across many later writes neither blocks them nor sees
//! them.
//!
//! Every test drives `EzKv` through its public API.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use common::*;
use ezraft::Txn;

/// How many keys the invariant in `concurrent_readers_never_see_half_of_a_multi_key_transaction`
/// spans
const KEYS: usize = 5;

/// A transaction that sets every one of `KEYS` keys to the same "generation" number - the
/// invariant a concurrent reader must never see broken.
fn generation_txn(generation: u64) -> Txn {
    let mut txn = Txn::new();
    for i in 0..KEYS {
        txn = txn.put(format!("gen/{i}"), generation.to_string());
    }
    txn
}

/// Concurrent reader tasks polling `view()` in a tight loop must never see a multi-key
/// transaction half-applied: every key this writer touches always carries the same generation
/// number, in every one of the transactions it commits.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_readers_never_see_half_of_a_multi_key_transaction() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;
    a.write(generation_txn(0)).await.map_err(|e| io::Error::other(e.to_string()))?;

    const GENERATIONS: u64 = 300;

    // Readers poll for as long as the writer is writing, not for a fixed number of rounds: a
    // writer bound by one fsync per transaction outlasts any fixed count by orders of magnitude,
    // and a reader that finished early would have watched almost none of the transactions.
    let writing = Arc::new(AtomicBool::new(true));

    let writer = {
        let a = a.clone();
        let writing = writing.clone();
        tokio::spawn(async move {
            for generation in 1..=GENERATIONS {
                a.write(generation_txn(generation)).await.unwrap();
            }
            writing.store(false, Ordering::Release);
        })
    };

    let spawn_reader = || {
        let a = a.clone();
        let writing = writing.clone();
        tokio::spawn(async move {
            let mut violations: Vec<Vec<String>> = Vec::new();
            let mut generations_seen = BTreeSet::new();
            while writing.load(Ordering::Acquire) {
                let view = a.view();
                let values: Vec<String> =
                    (0..KEYS).map(|i| view.get(&format!("gen/{i}")).map(|kv| text(&kv)).unwrap_or_default()).collect();
                if values.iter().any(|v| v != &values[0]) {
                    violations.push(values);
                } else {
                    generations_seen.insert(values[0].clone());
                }
                tokio::task::yield_now().await;
            }
            (violations, generations_seen.len())
        })
    };
    let reader_a = spawn_reader();
    let reader_b = spawn_reader();

    writer.await.expect("writer task panicked");
    let (violations_a, seen_a) = reader_a.await.expect("reader task panicked");
    let (violations_b, seen_b) = reader_b.await.expect("reader task panicked");

    assert!(
        violations_a.is_empty(),
        "reader saw inconsistent generations: {:?}",
        violations_a
    );
    assert!(
        violations_b.is_empty(),
        "reader saw inconsistent generations: {:?}",
        violations_b
    );

    // The readers really did watch the transactions go by, rather than finishing before them.
    assert!(
        seen_a > 1 && seen_b > 1,
        "readers must observe several generations to prove anything: {} and {}",
        seen_a,
        seen_b
    );

    Ok(())
}

/// A `View` taken before a long run of writes must be frozen at its own revision - neither
/// advancing nor seeing anything past it - and holding it must not stall those writes: the state
/// machine's `apply` never touches a reader's view once it has been handed out.
///
/// Each write is bounded by the harness's own wait budget rather than a hand-picked duration:
/// that proves `apply` keeps making progress while an old view is held, without coupling the
/// assertion to how fast this machine's storage happens to fsync (`RedbStorage` does one or more
/// real fsyncs per write, whose latency varies a great deal across machines and filesystems).
#[tokio::test(flavor = "multi_thread")]
async fn a_view_held_across_many_later_writes_neither_blocks_them_nor_sees_them() -> io::Result<()> {
    let (_, a, _data_a) = founding_node().await?;
    a.write(set("held", "before")).await.map_err(|e| io::Error::other(e.to_string()))?;

    let held = a.view();

    const WRITES: usize = 50;
    let per_write_budget = WAIT.expect("cluster waits are bounded");
    for i in 0..WRITES {
        tokio::time::timeout(per_write_budget, a.write(set(&format!("k{i}"), &format!("v{i}"))))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "write {i} did not complete within {:?} while an old view was held",
                    per_write_budget
                )
            })
            .map_err(|e| io::Error::other(e.to_string()))?;
    }

    // The held view is exactly what it was when taken.
    assert_eq!(1, held.revision());
    assert_eq!(Some("before".to_string()), held.get("held").map(|kv| text(&kv)));
    assert_eq!(None, held.get("k0"), "the held view predates every write below");
    assert_eq!(0, held.prefix("k").count());

    // The live state, meanwhile, reflects every one of them.
    let current = a.view();
    assert_eq!(1 + WRITES as u64, current.revision());
    for i in 0..WRITES {
        assert_eq!(Some(format!("v{i}")), current.get(&format!("k{i}")).map(|kv| text(&kv)));
    }

    Ok(())
}
