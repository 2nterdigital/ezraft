<img src="logo.png" alt="EzRaft" width="320">

> **This is the 2nterdigital fork** of [drmingdrmer/ezraft](https://github.com/drmingdrmer/ezraft),
> extended into the foundation (底座) of [ech0-media](https://github.com/2nterdigital/ech0-media).
> The trunk is `dev`; `main` mirrors the original and is never committed to. See
> [FORK.md](FORK.md) for the baseline, the full list of changes (each with its source ticket), and
> how to depend on this fork from another repository.

[![License](https://img.shields.io/crates/l/ezraft.svg)](https://github.com/drmingdrmer/ezraft#license)

Run a replicated key-value store on several machines at once, all holding the same state, so the
service survives losing some of them - that is [Raft](https://raft.github.io/), and this fork of
[OpenRaft](https://github.com/databendlabs/openraft)'s `ezraft` example project turns it into the
foundation ech0-media's distributed media cluster builds on: replicated state (`EzKv`), any-node
reads and writes, change subscriptions, typed inter-node RPC, cluster observation and a node
registry, and production storage (`RedbStorage`). It is not published to crates.io and not meant
to be reused outside ech0-media - see [FORK.md](FORK.md) for why.

- **`EzKv::write(txn)`**: a conditional, all-or-nothing transaction - `Put`, `Delete`,
  `DeletePrefix` ops, gated on conditions like "this key's `mod_rev` is still `R`" - callable on
  *any* node: the leader proposes it directly, any other node forwards it and retries under one
  request id until a definite outcome or a deadline.
- **`EzKv::view()`**: a lock-free, point-in-time read of the whole key space, answered from local
  memory - no consensus round, no log entry, and it already reflects any write you just made on
  this node. `EzKv::linearizable_view()` reads what has definitely been committed instead, on any
  node, when that stronger guarantee is worth a round trip.
- **`EzKv::watch(prefix, start)`**: a resumable, gap-free subscription to a key prefix's changes.
- **Storage included**: `RedbStorage` persists to disk (redb) out of the box. Writing your own
  `EzStorage` is three methods.
- **Networking included**: every node serves the Raft RPCs its peers need, an authenticated admin
  API, and typed inter-node RPC (`EzKv::rpc()`) for calling one node from another by id. There is
  no transport to write.

## Example

A minimal node on `EzKv`, whole:

```rust,no_run
use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::RedbStorage;
use ezraft::Txn;

/// One binary, run once per machine: `kvstore <own-addr> [addr of a node already in it]`
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().expect("usage: kvstore <addr> [seed_addr]");
    let seed = args.next();

    let storage = RedbStorage::open(format!("./data/{addr}.redb")).await?;
    // Every node on the same internal port needs the same cluster token; it has no
    // default, so a node refuses to start without one.
    let config = EzConfig {
        cluster_token: "change-me-to-a-real-secret".to_string(),
        ..EzConfig::default()
    };

    // The first node creates the cluster; every other one joins through a node
    // already in it and is given an id, the membership, and the log so far.
    let node = match seed {
        None => EzKv::create(addr, storage, config).await?,
        Some(seed) => EzKv::join(addr, seed, storage, config).await?,
    };

    // Write a key on any node - the leader proposes it, another node forwards it -
    // then read it back from this node's own local view.
    node.write(Txn::new().put("hello", "world")).await.map_err(std::io::Error::other)?;
    println!("{:?}", node.view().get("hello"));

    // The Raft RPCs peers need, plus the admin API. Runs until shutdown.
    node.serve().await
}
```

Three of those form a cluster that survives losing any one of them:

```bash
kvstore 127.0.0.1:8080                  # creates it
kvstore 127.0.0.1:8081 127.0.0.1:8080   # joins
kvstore 127.0.0.1:8082 127.0.0.1:8080   # joins
```

## Drive it

The example above is a minimal sketch; `examples/kvstore.rs` is the full worked reference,
including a **separate, small external HTTP API** it builds on top of `EzKv` for its own
client traffic (`examples/kvstore/api.rs`) - nothing on the internal port a client should ever
talk to directly. Run it (`cargo run --example kvstore`, three times, one `--seed`-ed off the
first) and drive that external API:

```bash
# A transaction: any node accepts it and forwards to the leader if needed.
curl -X POST 127.0.0.1:9080/tx -H 'Content-Type: application/json' \
    -d '{"conditions": [], "ops": [{"Put": {"key": "hello", "value": "d29ybGQ="}}]}'
# {"revision":1,"request_id":"..."}

# Read it back from this node's own local view - no consensus round.
curl '127.0.0.1:9082/view?key=hello'

# This node's view of the cluster and the node registry.
curl 127.0.0.1:9080/status
```

Those routes are the example's own (`examples/kvstore/api.rs` documents each one's request and
response); the crate ships none. See the [guide](docs/guide.md) for the foundation's full public
API and error semantics.

## Status

This fork is under active development as the foundation of
[ech0-media](https://github.com/2nterdigital/ech0-media); it tracks that project's milestones,
not the upstream `ezraft`'s own API-design goals. It is **not published** and not meant to be
used outside ech0-media - see [FORK.md](FORK.md) for the baseline, the complete list of changes
relative to the original, and how another repository depends on it during joint development.

## Documentation

- [Guide](docs/guide.md) - starting a cluster, the full `EzKv` API and its error semantics, and
  the conventions ech0-media's own upstream code follows on top of this crate
- [FORK.md](FORK.md) - the baseline, every change relative to the original (with its source
  ticket), and the verification and upgrade procedures
- API documentation: `cargo doc --open` (not published to docs.rs - see [FORK.md](FORK.md))
- [`examples/kvstore.rs`](examples/kvstore.rs) - a complete key-value service, the reference this
  guide's own examples are drawn from
- [CHANGELOG](CHANGELOG.md)

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
