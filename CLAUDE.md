# EzRaft Development Guidelines

## This is a fork

This repository is the 2nterdigital fork of `drmingdrmer/ezraft`: the **foundation** (底座) that
[ech0-media](https://github.com/2nterdigital/ech0-media) builds on. Read [FORK.md](FORK.md) first - it
records the baseline, every change relative to the original (each with its source ticket), the
verification commands, and the rules for syncing with the original and upgrading openraft.

- `dev` is the trunk; `main` mirrors the original and is never committed to.
- Work comes from the implementation tickets under
  [ech0-media#21](https://github.com/2nterdigital/ech0-media/issues/21). Branch from `dev`, merge back
  to `dev`, and register the change in FORK.md's change list.
- openraft stays pinned with `=`; upgrading it is its own deliberate change (FORK.md).
- The crate is not publishable (`publish = false`) and must stay that way.

## Design Philosophy

The section below is the original's, updated as the fork lands its own replacements (the bundled
file storage, the sample app and its write/read routes, the examples, the guide); whoever lands a
replacement deletes the sample it replaces and updates the statements here that it invalidates.

Originally EzRaft gave a simple API with extensibility for building Raft apps, where the user
implementing `EzStorage` (persistence) and `EzApp` (business logic) was the product. ech0-media#27
built the foundation's own state model - `EzKv`, a replicated key-value store with conditional
transactions and lock-free views (`src/kv.rs` + `src/kv/`), wired into the node by `EzKv` in
`src/raft.rs` and `src/write.rs` - in place of `EzApp`, which is no longer public. `EzStorage`
(persistence) remains the one trait a user implements.

- `RedbStorage` (`src/storage/redb_storage.rs`) ships as the production `EzStorage` implementation: one redb database per node, three tables (log, meta, snapshot), one `Durability::Immediate` write transaction per `persist`. It doubles as the worked example of the trait. `EzStorage` stays the extension point for anything it does not fit.
- DX improvements simplify the contract the user implements (fewer operations, fewer inexplicable bounds, better docs) rather than adding more shipped components.
- Blanket impls derived from capabilities the user's own type declares (for example snapshots via `Serialize`) are API simplification and are welcome.
- `EzServer` (`src/server.rs`) has the same "sample, not the API" shape: its `/raft/*` and admin routes are production-shaped; an application's own client-facing routes (`examples/kvstore.rs`'s `POST /api/write`/`POST /api/read`) are a sample to copy and rewrite, added on top of `EzKv::serve_with` rather than shipped by the crate.
