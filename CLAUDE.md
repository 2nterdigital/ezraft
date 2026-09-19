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

The section below is the original's. The fork replaces its samples as the tickets land (the bundled
file storage, `/api/write`, `/api/read`, the examples, the guide); whoever lands a replacement deletes
the sample it replaces and updates the statements here that it invalidates.

EzRaft exists to give a simple API with extensibility for building Raft apps: the user implementing `EzStorage` (persistence) and `EzApp` (business logic) is still the product.

- `RedbStorage` (`src/storage/redb_storage.rs`) ships as the production `EzStorage` implementation: one redb database per node, three tables (log, meta, snapshot), one `Durability::Immediate` write transaction per `persist`. It doubles as the worked example of the trait. `EzStorage` stays the extension point for anything it does not fit.
- DX improvements simplify the contract the user implements (fewer operations, fewer inexplicable bounds, better docs) rather than adding more shipped components.
- Blanket impls derived from capabilities the user's own type declares (for example snapshots via `Serialize`) are API simplification and are welcome.
- `EzServer` (`src/server.rs`) has the same "sample, not the API" shape: its `/raft/*` and admin routes are production-shaped; `POST /api/write` and `POST /api/read` are a sample to copy and rewrite.
