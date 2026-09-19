//! `watch` `Progress` acceptance tests (ech0-media#32): a quiet subscriber still hears the
//! revision advance, at no more than the configured rate, and never ahead of a `Changes` it still
//! has coming.

mod common;

use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::Txn;
use ezraft::WatchLimits;
use ezraft::WatchStart;
use ezraft::WatchUpdate;
use futures::StreamExt;

/// A subscriber to a prefix nobody writes still eventually learns the node's revision through
/// `Progress`, driven purely by writes elsewhere.
#[tokio::test(flavor = "multi_thread")]
async fn progress_eventually_reaches_the_current_revision_on_a_quiet_prefix() -> io::Result<()> {
    let quick_progress = EzConfig {
        watch_limits: WatchLimits {
            progress_interval: Duration::from_millis(50),
            ..WatchLimits::default()
        },
        ..config()
    };
    let (_, a, _data) = founding_node_with(quick_progress).await?;
    let mut stream = a.watch("quiet/", WatchStart::Now);

    for i in 0..10 {
        a.write(Txn::new().put(format!("noisy/{i}"), format!("v{i}")))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    let target_revision = a.view().revision();

    loop {
        match next_update(&mut stream).await {
            WatchUpdate::Progress { revision } => {
                if revision >= target_revision {
                    break;
                }
            }
            other => panic!("a prefix nobody writes must never see anything but Progress, got {other:?}"),
        }
    }

    Ok(())
}

/// `Progress` never fires more often than the configured interval, even under continuous
/// non-matching writes.
#[tokio::test(flavor = "multi_thread")]
async fn progress_never_fires_more_often_than_the_configured_interval() -> io::Result<()> {
    let progress_interval = Duration::from_millis(80);
    let quick_progress = EzConfig {
        watch_limits: WatchLimits {
            progress_interval,
            ..WatchLimits::default()
        },
        ..config()
    };
    let (_, a, _data) = founding_node_with(quick_progress).await?;
    let mut stream = a.watch("quiet/", WatchStart::Now);

    // Keep the revision continuously advancing elsewhere for the whole observation window, so
    // `Progress` has something new to report at every opportunity - a single write would report
    // once and then have nothing left to say, which would not exercise the rate limit at all.
    let writer = {
        let a = a.clone();
        tokio::spawn(async move {
            let mut i = 0u64;
            while a.write(Txn::new().put(format!("noisy/{i}"), format!("v{i}"))).await.is_ok() {
                i += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };

    let mut arrivals = Vec::new();
    let window_end = tokio::time::Instant::now() + Duration::from_millis(500);
    loop {
        let remaining = window_end.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(WatchUpdate::Progress { .. })) => arrivals.push(tokio::time::Instant::now()),
            Ok(Some(other)) => panic!("a prefix nobody writes must never see anything but Progress, got {other:?}"),
            Ok(None) => panic!("a WatchStream never ends on its own"),
            Err(_elapsed) => break,
        }
    }
    writer.abort();

    assert!(
        arrivals.len() >= 2,
        "must observe at least two Progress updates to check their spacing, got {}",
        arrivals.len()
    );
    for pair in arrivals.windows(2) {
        let gap = pair[1] - pair[0];
        // A little slack for scheduling jitter between the actual enqueue and this test sampling
        // `Instant::now()` after `.next().await` returns - never enough to hide a real violation
        // (which would show up as a gap far short of the configured interval, not a few ms off).
        assert!(
            gap + Duration::from_millis(15) >= progress_interval,
            "Progress fired more often than the configured interval: gap {gap:?}, interval {progress_interval:?}"
        );
    }

    Ok(())
}

/// The ordering constraint, tested directly: interleaving matching and non-matching writes (with
/// enough spacing for `Progress` to fire between them), no `Progress` revision this subscriber
/// receives is ever at or beyond the revision of a `Changes` that arrives after it - a consumer
/// that persisted that `Progress` as its resume cursor and crashed would never silently skip that
/// change.
#[tokio::test(flavor = "multi_thread")]
async fn progress_never_races_ahead_of_a_later_delivered_change() -> io::Result<()> {
    let progress_interval = Duration::from_millis(30);
    let quick_progress = EzConfig {
        watch_limits: WatchLimits {
            progress_interval,
            ..WatchLimits::default()
        },
        ..config()
    };
    let (_, a, _data) = founding_node_with(quick_progress).await?;
    let mut stream = a.watch("match/", WatchStart::Now);

    for i in 0..15u32 {
        if i % 3 == 0 {
            a.write(Txn::new().put(format!("match/{i}"), format!("v{i}")))
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;
        } else {
            a.write(Txn::new().put(format!("other/{i}"), format!("v{i}")))
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;
            // Give the progress task room to fire between non-matching writes.
            tokio::time::sleep(progress_interval).await;
        }
    }

    let mut max_progress_seen: Option<u64> = None;
    let mut violation: Option<(u64, u64)> = None;
    let mut changes_seen = 0;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(WatchUpdate::Changes(changes))) => {
                changes_seen += 1;
                if let Some(seen) = max_progress_seen
                    && changes.revision <= seen
                {
                    violation = Some((seen, changes.revision));
                }
            }
            Ok(Some(WatchUpdate::Progress { revision })) => {
                max_progress_seen = Some(max_progress_seen.map_or(revision, |seen| seen.max(revision)));
            }
            Ok(Some(other @ WatchUpdate::Reset { .. })) => panic!("unexpected {other:?}"),
            Ok(None) => panic!("a WatchStream never ends on its own"),
            Err(_elapsed) => break,
        }
    }

    assert!(
        violation.is_none(),
        "a Progress raced ahead of a later Changes: {violation:?}"
    );
    assert_eq!(
        5, changes_seen,
        "every matching write (i % 3 == 0, for i in 0..15) must still be delivered"
    );

    Ok(())
}
