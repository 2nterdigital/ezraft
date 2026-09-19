//! A crash-injecting `EzStorage` wrapper: "loses power" at a chosen persist point.
//!
//! [`CrashStorage`] wraps a real [`RedbStorage`]. It counts every call to
//! [`EzStorage::persist`], starting at 1, and records each one's [`PersistRecord`] - its kind and,
//! for a `Meta` write, the vote it carried - in the order it durably happened. A [`CrashHandle`]
//! (cheap to `Clone`, shared with the wrapped storage) lets a test:
//!
//! - watch that history to find *which* persist recorded a vote, rather than guessing an index (see
//!   `tests/crash_recovery.rs`'s vote-durability scenarios), and
//! - [`CrashHandle::arm`] a crash at a chosen persist's ordinal, [`CrashWhen::Before`] it would
//!   reach disk or [`CrashWhen::After`] it already has.
//!
//! Once the armed point fires, [`CrashHandle::is_crashed`] turns permanently true and every
//! further `persist`/`read_logs`/`load` call on that instance fails - "loses power" - without
//! touching the file again. The test is then the one that stops the node: `EzKv::shutdown` joins
//! openraft's core task (which owns the last handle to this storage) before returning, exactly the
//! release `tests/node_lifecycle.rs`'s own restart tests already depend on, so a fresh, plain
//! `RedbStorage` can reopen the same file - `RedbStorage::open`'s own bounded retry
//! (`src/storage/redb_storage.rs`) tolerates the brief `DatabaseAlreadyOpen` race while that
//! release finishes.

use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use ezraft::EzEntry;
use ezraft::EzStorage;
use ezraft::EzVote;
use ezraft::Loaded;
use ezraft::Persist;
use ezraft::RedbStorage;

/// One [`Persist`] call's kind and the detail a test needs to place a crash on it deliberately
///
/// Carries no log or snapshot payload beyond what identifies the call - a test does not need to
/// decode a command to find "the vote write" or "the fifth log entry".
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PersistRecord {
    /// A metadata write, with the vote it carried (`None` before any election ever touched it)
    Meta { vote: Option<EzVote> },
    /// A log entry write, at this index
    LogEntry { index: u64 },
    /// A full snapshot replacing the previous one
    Snapshot,
    /// A deletion of the half-open index range `[from, to)`
    DeleteLogs { from: u64, to: u64 },
}

impl From<&Persist> for PersistRecord {
    fn from(op: &Persist) -> Self {
        match op {
            Persist::Meta(meta) => PersistRecord::Meta { vote: meta.vote },
            Persist::LogEntry(entry) => PersistRecord::LogEntry { index: entry.log_id.1 },
            Persist::Snapshot(_) => PersistRecord::Snapshot,
            Persist::DeleteLogs { from, to } => PersistRecord::DeleteLogs { from: *from, to: *to },
        }
    }
}

/// When a [`CrashPoint`] fires, relative to the persist it names
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashWhen {
    /// The call never reaches the inner storage - as if the machine had died a moment earlier.
    /// Whatever was already durable is all a restart will find; this call leaves no trace.
    Before,
    /// The call reaches the inner storage and commits - the data really is durable - but the
    /// caller (openraft) is told it failed anyway, as if the machine died a moment later: after
    /// the disk write, before anything downstream learned of it.
    After,
}

/// A persist's ordinal (1-based, in call order) and when to crash relative to it
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CrashPoint {
    /// Which persist call to crash around, counting every call to [`EzStorage::persist`] on this
    /// instance from 1
    pub at: usize,
    /// Whether the crash lands before or after that call reaches disk
    pub when: CrashWhen,
}

impl CrashPoint {
    /// Crash before the `at`-th persist call reaches disk
    pub fn before(at: usize) -> Self {
        Self {
            at,
            when: CrashWhen::Before,
        }
    }

    /// Crash after the `at`-th persist call has reached disk, but before its caller learns of it
    pub fn after(at: usize) -> Self {
        Self {
            at,
            when: CrashWhen::After,
        }
    }
}

/// State shared between a [`CrashStorage`] and every [`CrashHandle`] cloned from it
#[derive(Default)]
struct Shared {
    /// Every persist that has actually reached disk, in call order
    history: Vec<PersistRecord>,
    /// The crash to fire, if any has been armed
    armed: Option<CrashPoint>,
    /// Whether the armed crash has already fired
    crashed: bool,
}

/// A shared, cheap-to-`Clone` handle onto one [`CrashStorage`]'s recorded history and crash state
///
/// Kept by the test while the [`CrashStorage`] itself is moved into `EzKv::create`/`join`.
#[derive(Clone)]
pub struct CrashHandle {
    shared: Arc<Mutex<Shared>>,
}

impl CrashHandle {
    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().expect("crash handle state mutex poisoned")
    }

    /// Every persist this instance has completed so far, in call order
    ///
    /// Only calls that actually reached disk: a [`CrashWhen::Before`] crash's own call is never
    /// durable and never appears here, but everything before it does.
    pub fn history(&self) -> Vec<PersistRecord> {
        self.lock().history.clone()
    }

    /// How many persist calls have completed so far - `history().len()`, without the clone
    pub fn persist_count(&self) -> usize {
        self.lock().history.len()
    }

    /// Arm a crash at `point`
    ///
    /// Takes effect for the next call whose ordinal matches; a point already passed when armed
    /// never fires (arm before the sequence that would reach it, not after).
    pub fn arm(&self, point: CrashPoint) {
        self.lock().armed = Some(point);
    }

    /// Whether the armed crash has already fired
    pub fn is_crashed(&self) -> bool {
        self.lock().crashed
    }
}

/// The error every call reports once the crash has fired: no persist reaches disk and no read
/// finds anything, exactly as if the process had died.
fn power_lost() -> io::Error {
    io::Error::other("simulated power loss: this storage instance is gone")
}

/// A real [`RedbStorage`] wrapped to "lose power" at a chosen persist
pub struct CrashStorage {
    inner: RedbStorage,
    handle: CrashHandle,
}

impl CrashStorage {
    /// Wrap `inner`, returning the storage to hand to `EzKv::create`/`join` and the handle the
    /// test keeps to arm and observe it
    pub fn wrap(inner: RedbStorage) -> (Self, CrashHandle) {
        let handle = CrashHandle {
            shared: Arc::new(Mutex::new(Shared::default())),
        };
        (
            Self {
                inner,
                handle: handle.clone(),
            },
            handle,
        )
    }
}

#[async_trait::async_trait]
impl EzStorage for CrashStorage {
    async fn load(&mut self) -> Result<Loaded, io::Error> {
        if self.handle.is_crashed() {
            return Err(power_lost());
        }
        self.inner.load().await
    }

    async fn persist(&mut self, op: Persist) -> Result<(), io::Error> {
        // One lock for the gate: whether this call is already crashed-out, its own ordinal, and
        // whether that ordinal is the armed `Before` point - decided before the (possibly slow)
        // inner call, and never held across it.
        let (ordinal, crash_before) = {
            let shared = self.lock_shared();
            if shared.crashed {
                return Err(power_lost());
            }
            let ordinal = shared.history.len() + 1;
            let crash_before = shared.armed.is_some_and(|p| p.at == ordinal && p.when == CrashWhen::Before);
            (ordinal, crash_before)
        };

        if crash_before {
            self.lock_shared().crashed = true;
            return Err(power_lost());
        }

        let record = PersistRecord::from(&op);
        self.inner.persist(op).await?;

        let crash_after = {
            let mut shared = self.lock_shared();
            shared.history.push(record);
            let crash_after = shared.armed.is_some_and(|p| p.at == ordinal && p.when == CrashWhen::After);
            if crash_after {
                shared.crashed = true;
            }
            crash_after
        };

        if crash_after { Err(power_lost()) } else { Ok(()) }
    }

    async fn read_logs(&mut self, start: u64, end: u64) -> Result<Vec<EzEntry>, io::Error> {
        if self.handle.is_crashed() {
            return Err(power_lost());
        }
        self.inner.read_logs(start, end).await
    }
}

impl CrashStorage {
    fn lock_shared(&self) -> MutexGuard<'_, Shared> {
        self.handle.lock()
    }
}
