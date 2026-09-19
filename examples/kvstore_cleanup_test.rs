//! Every child process this harness spawns is cleaned up when a test ends, pass or fail. The
//! SIGKILL tests already prove this on the happy path (they pass); this file proves the harness'
//! `Drop` still runs, and still kills its child, when the test that owned it panics.

mod harness;

use std::io;
use std::panic;
use std::time::Instant;

use harness::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_test_still_kills_its_child_process() -> io::Result<()> {
    let example = Example::build("kvstore")?;
    let (addr, api_addr) = free_addr_pair();
    let node = example.spawn(&addr, &api_addr, None)?;
    let pid = node.pid();

    // `catch_unwind` requires its closure to be `UnwindSafe`; `AssertUnwindSafe` is correct here
    // because nothing after the panic reads the node's state - only whether its OS process is
    // still around, checked below through `pid`, captured before the closure ever runs.
    let panicked = panic::catch_unwind(panic::AssertUnwindSafe(move || {
        let _node = node; // moved in, so unwinding drops it right here
        panic!("intentional panic: proves Drop still runs during unwinding");
    }))
    .is_err();
    assert!(panicked, "the inner closure was supposed to panic");

    // Unwinding already ran `Node::drop` (kill + reap) by the time `catch_unwind` returned; poll
    // briefly rather than assume it, since some platforms reap a process asynchronously relative
    // to `wait()` returning.
    let deadline = Instant::now() + WAIT;
    loop {
        if !pid_is_alive(pid) {
            break;
        }
        if Instant::now() >= deadline {
            panic!("process {pid} outlived the panicking test that owned it");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    example.cleanup()?;
    Ok(())
}
