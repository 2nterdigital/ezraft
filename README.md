<img src="logo.png" alt="EzRaft" width="320">

> **This is the 2nterdigital fork** of [drmingdrmer/ezraft](https://github.com/drmingdrmer/ezraft),
> extended into the foundation of [ech0-media](https://github.com/2nterdigital/ech0-media). The
> trunk is `dev`; `main` mirrors the original. See [FORK.md](FORK.md) for the baseline, the list of
> changes, and how to depend on it. The text below is still the original's and is replaced as the
> fork's own API lands.

[![CI](https://github.com/drmingdrmer/ezraft/actions/workflows/ci.yaml/badge.svg)](https://github.com/drmingdrmer/ezraft/actions/workflows/ci.yaml)
[![Crates.io](https://img.shields.io/crates/v/ezraft.svg)](https://crates.io/crates/ezraft)
[![Docs](https://docs.rs/ezraft/badge.svg)](https://docs.rs/ezraft)
[![License](https://img.shields.io/crates/l/ezraft.svg)](https://github.com/drmingdrmer/ezraft#license)

A beginner-friendly Raft consensus framework built on
[OpenRaft](https://github.com/databendlabs/openraft).

Run a replicated key-value store on several machines at once, all holding the same
state, so the service survives losing some of them. That is [Raft](https://raft.github.io/);
EzRaft builds in the state model - `EzKv`, a single flat key space with conditional
transactions and lock-free local views - and reduces persistence to one trait,
`EzStorage`, that puts bytes on disk. Elections, replication, membership, snapshots and
transport are internal.

- **`EzKv::write(txn)`**: a conditional, all-or-nothing transaction - `Put`, `Delete`,
  `DeletePrefix` ops, gated on conditions like "this key's `mod_rev` is still `R`" - that
  only ever succeeds on the current leader.
- **`EzKv::view()`**: a lock-free, point-in-time read of the whole key space, answered
  from local memory - no consensus round, no log entry, and it already reflects any
  write you just made on this node.
- **Storage included**: `RedbStorage` persists to disk (redb) out of the box. Writing your
  own `EzStorage` is three methods.
- **Networking included**: every node serves the Raft RPCs its peers need and the admin
  API. There is no transport to write.

## Example

A replicated key-value service, whole:

```rust
/// One binary, run once per machine: `kvstore <own-addr> [addr of a node already in it]`
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().expect("usage: kvstore <addr> [seed_addr]");
    let seed = args.next();

    let storage = RedbStorage::open(format!("./data/{addr}.redb")).await?;
    let config = EzConfig::default();

    // The first node creates the cluster; every other one joins through a node in it
    // and is given an id, the membership, and the log so far.
    let node = match seed {
        None => EzKv::create(addr, storage, config).await?,
        Some(seed) => EzKv::join(addr, seed, storage, config).await?,
    };

    // Write a key, then read it back from this node's own local view.
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

`examples/kvstore.rs` adds its own small `POST /api/write`/`POST /api/read` routes on
top of `EzKv::serve_with` - these are a sample to copy and rewrite, not part of the
crate:

```bash
# Only the leader accepts a write; any-node forwarding is a later ticket.
curl -X POST 127.0.0.1:8080/api/write -H 'Content-Type: application/json' \
    -d '{"op": "Set", "key": "hello", "value": "world"}'
# {"revision":1}

# A read is answered from that node's own local view: no consensus round, no log entry.
curl -X POST 127.0.0.1:8082/api/read -H 'Content-Type: application/json' \
    -d '{"op": "Get", "key": "hello"}'
# {"hello":"world"}  - from a node you never wrote to

curl 127.0.0.1:8080/api/metrics   # leader, term, log index, membership
```

## Status

**Experimental.** The API is the thing being searched for, so it changes until the
crate stabilizes. A real service built on it is the feedback the search needs most -
pin the version, and read the CHANGELOG before upgrading.

**Next phase: Stable API.** Once the design exploration matures, EzRaft will provide a
stable API with well-considered abstractions - exposing what users need while hiding
unnecessary complexity.

## Documentation

- [Guide](docs/guide.md) - run a cluster, write a service against it, and the API,
  configuration and HTTP endpoints that make it up
- [API documentation](https://docs.rs/ezraft)
- [`examples/kvstore.rs`](examples/kvstore.rs) - a complete key-value service
- [CHANGELOG](CHANGELOG.md)

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
