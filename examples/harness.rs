//! The harness the `*_test.rs` end-to-end tests share: build an example, run its
//! nodes as real child processes, and clean up after a passing test.
//!
//! Each test file is its own binary and uses a subset of this, so unused items
//! here are expected rather than dead.

#![allow(dead_code)]

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;

/// Upper bound for every wait in these tests
pub const WAIT: Duration = Duration::from_secs(30);

pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// An example's child process, killed when the test ends, pass or fail.
pub struct Node {
    child: Child,
}

impl Node {
    /// This node's OS process id, for logging or for [`pid_is_alive`] to check on later.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// SIGKILL this node right now (`Child::kill`, which sends `SIGKILL` on Unix), without
    /// waiting for the test to end.
    ///
    /// Distinct from [`Drop`]: a SIGKILL test kills one node mid-test and keeps driving the rest
    /// of the cluster, so the kill has to happen on the test's own schedule. Reaps the process
    /// (`wait`, not `try_wait`) so a killed node never lingers as a zombie - a just-killed child
    /// exits immediately, so this does not block.
    pub fn kill(&mut self) -> io::Result<()> {
        self.child.kill()?;
        self.child.wait()?;
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Whether the OS still has a process at `pid`
///
/// Unix only, like [`Node::kill`]'s own `SIGKILL` (`Child::kill`) - used to prove a killed or
/// dropped node actually left no process behind, not merely that this handle stopped tracking it.
/// Sends signal `0` (`kill -0`), which delivers no signal and only checks the pid exists; a zero
/// exit status means it still does. A pid the OS has already recycled for an unrelated process
/// would be a false positive, but the window for that on a test machine, checked immediately
/// after a `kill`+`wait`, is not one these tests hit in practice.
#[cfg(unix)]
pub fn pid_is_alive(pid: u32) -> bool {
    // A dead pid is the expected, common case here (this is mostly called to confirm a kill
    // worked), and `kill -0` on one prints "No such process" to stderr on top of the exit status
    // this only cares about - discarded so a passing test's output stays quiet.
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// A built example binary and the scratch directory its nodes run in.
pub struct Example {
    bin: PathBuf,
    work_dir: PathBuf,
    node_args: Vec<String>,
}

impl Example {
    /// Build example `name` and create the work directory its nodes share.
    ///
    /// The test cannot assume the example is already built: `cargo test --tests`,
    /// which `cargo llvm-cov` runs, builds test targets only. Nor can it derive the
    /// path from its own executable, because cargo has more than one target
    /// directory layout. Cargo offers no `CARGO_BIN_EXE_*` for examples, so ask it.
    pub fn build(name: &str) -> io::Result<Self> {
        let cargo = std::env::var_os("CARGO").expect("cargo sets CARGO for the tests it runs");
        let out = Command::new(cargo)
            .args(["build", "--package", "ezraft", "--example", name])
            .arg("--message-format=json-render-diagnostics")
            .stderr(Stdio::inherit())
            .output()
            .expect("failed to run cargo");
        assert!(out.status.success(), "building the {} example failed", name);

        let rendered = String::from_utf8(out.stdout).expect("cargo emits UTF-8");
        let artifact = rendered
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|msg| msg["reason"] == "compiler-artifact" && msg["target"]["name"] == name)
            .unwrap_or_else(|| panic!("cargo reported no artifact for the {} example", name));
        let executable = artifact["executable"]
            .as_str()
            .unwrap_or_else(|| panic!("cargo reported no executable for the {} example", name));

        let work_dir = std::env::temp_dir().join(format!("ezraft-{}-{}", name, std::process::id()));
        std::fs::create_dir_all(&work_dir)?;

        Ok(Self {
            bin: PathBuf::from(executable),
            work_dir,
            node_args: Vec::new(),
        })
    }

    /// Extra command line arguments, passed to every node this example spawns.
    pub fn node_args(mut self, args: &[&str]) -> Self {
        self.node_args = args.iter().map(|arg| arg.to_string()).collect();
        self
    }

    /// Spawn one node: `addr` is its internal port (Raft + RPC), `api_addr` the separate external
    /// port this example's own client API listens on. Its `./data/<addr>` dir and log land in the
    /// work directory.
    ///
    /// `seed` is another node's internal-port address to join through, or `None` to found a new
    /// cluster. Restarting a node that already has an id (same `addr`, same work directory, so the
    /// same data directory) ignores `seed` on its own - pass `None` for a restart, since the
    /// example itself already does the same (see `examples/kvstore.rs`).
    pub fn spawn(&self, addr: &str, api_addr: &str, seed: Option<&str>) -> io::Result<Node> {
        let log_name = format!("node-{}.log", addr.replace(':', "-"));
        let log = File::create(self.work_dir.join(log_name))?;
        let mut cmd = Command::new(&self.bin);
        cmd.current_dir(&self.work_dir)
            .args(["--addr", addr, "--api-addr", api_addr])
            .args(&self.node_args)
            .stdout(log.try_clone()?)
            .stderr(log)
            .stdin(Stdio::null());
        if let Some(seed) = seed {
            cmd.args(["--seed", seed]);
        }
        Ok(Node { child: cmd.spawn()? })
    }

    /// Remove the work directory. Call this only on success, so that a failure
    /// leaves the data dirs and logs behind for debugging.
    pub fn cleanup(&self) -> io::Result<()> {
        std::fs::remove_dir_all(&self.work_dir)
    }
}

/// Grab a free port; the listener is dropped so the child process can bind it.
pub fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

/// A fresh `(internal, external)` address pair - every node this example spawns needs both.
pub fn free_addr_pair() -> (String, String) {
    (free_addr(), free_addr())
}
