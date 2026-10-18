# Changelog

## 0.7.0 – 2026-10-18

pgdo 0.7.0 reworks how processes share a cluster, so that a data directory
can be used safely from containers and VMs, e.g. bind-mounted into Docker. It
also slims pgdo down: fewer commands, fewer dependencies, and a smaller API.
Almost every part of the library API has changed.

### Highlights

- **Sessions.** `cluster.session(&[])?` creates and starts a cluster as
  needed and holds it open; when the last session ends, the cluster is
  stopped, or destroyed with `Finish::Destroy`. The CLI uses sessions too.
- **Safe sharing across paths, containers and VMs.** Sessions coordinate
  through `pgdo.lock` in the data directory, not through a file in `$TMPDIR`,
  so processes that reach a cluster by different paths, or from a container,
  now see each other. A running cluster's owner is recorded in `pgdo.owner`,
  with a heartbeat:
  - A session in another PID namespace on the same kernel (e.g. a container)
    joins the running server through its socket, rather than starting a
    second server.
  - A session on another kernel (e.g. macOS host ↔ Linux VM) refuses to use
    the cluster. `pgdo --force` (or `Takeover::IfStale`) takes it over only
    if its owner shows no sign of life for 30 seconds. **Danger:** if the
    server there is in fact still running, this will corrupt the cluster.
  - `postmaster.pid` is verified before pgdo trusts or signals the PID in it.
- **Usable from async code.** pgdo no longer uses SQLx, Tokio or the
  `postgres` crate. A small internal client handles pgdo's own queries and
  blocks on ordinary socket I/O, so it no longer panics inside a Tokio
  runtime.
- **Ctrl-C** while a session is starting stops any server it started and
  releases what it claimed; see `pgdo::cluster::session::interrupt`.
- **Clearer errors.** The CLI prints the error, its causes, and a hint where
  there is one, including hints from the PostgreSQL server.

### Breaking changes: requirements and on-disk layout

- **PostgreSQL 15 or later** is required, for clusters and runtimes alike
  (`pgdo::runtime::MINIMUM_VERSION`). Upgrade older clusters with
  `pg_upgrade`, or keep using pgdo 0.6.
- **The CLI's default data directory is now `pgdo.cluster`**, not `cluster`.
  Pass `-D cluster` (or set `PGDATA`) to keep using an existing one.
- Data directories gain `pgdo.lock` and, while running, `pgdo.owner`. New
  clusters are created only in empty directories (`pgdo.*` files excepted).
  Existing clusters need no migration.
- **Clusters always listen on port 5432**, whatever `port` is configured
  (`pgdo::cluster::PORT`). They listen only on a Unix socket in the data
  directory, so this does not clash with other servers. `PGPORT` is set for
  commands run by `pgdo exec` and `pgdo shell`.

### Breaking changes: CLI

- Removed `backup`, `restore`, `clone` and `backup:tools`. Backup will return,
  built on PostgreSQL's `basic_archive`.
- Added `--force`; see above.

### Breaking changes: library

- **Coordination:** removed the `coordinate` module, `cluster::resource`,
  `cluster::run` and `ClusterGuard`, in favour of `cluster::Session`, `Finish`
  and `Takeover`. `State` moved to `cluster::State`.
- **Clients:** removed `cluster::sqlx`, `Cluster::pool`, `Cluster::connect`
  and `Cluster::socket_dir`. Connect with any client library through the
  socket in `Cluster::datadir`, or with `Cluster::url`. `cluster::config`
  functions take `&Cluster` instead of a client.
- **Errors:** dropped miette. Errors are plain `thiserror` types, with hints
  from `ClusterError::help` and `ClientError::help`. Messages are lowercase
  fragments, per the Rust API Guidelines.
- **Visibility:** `pgdo::lock` and `pgdo::cluster::client` are private.
  `ClientError` and `ServerError` are re-exported from `pgdo::cluster`. Many
  other items that were never meant to be public are now private.
- **Removed:** `pgdo::cluster::backup` and `pgdo::util::percent`.
- `StrategyLike` no longer requires `RefUnwindSafe`.

Pull requests: #227, #228, #229, #230, #231, #232, #233, #234.

## 0.6.0 – 2026-07-13

- Upgrade to SQLx 0.9, which is re-exported as `pgdo::cluster::sqlx`.
- Upgrade other dependencies.

## 0.5.7 – 2026-05-17

- Test with PostgreSQL 18.

## 0.5.6 – 2026-04-18

- Fix Clippy and documentation warnings.

## 0.5.5 – 2026-03-24

- Group Dependabot updates, with a cooldown.

## 0.5.4 – 2025-08-31

- Install PostgreSQL 17 in CI and the debug container, and use it by default
  there.

## 0.5.3 – 2025-08-30

- Retry `pg_ctl init` and `Cluster::start` with exponential backoff and
  jitter, and log more usefully when retrying.
- Fix the spelling of `DATABASE_TEMPLATE1`.
- Upgrade dependencies.

## 0.5.2 – 2025-04-23

- Replace `lazy_static` with `LazyLock`, and fix Clippy warnings for Rust
  1.84.

## 0.5.1 – 2024-12-11

- Upgrade dependencies, and fix Clippy warnings for Rust 1.83.

## 0.5.0 – 2024-08-27

- **Breaking:** split the `coordinate::resource::Faceted` trait into
  `FacetFree`, `FacetShared` and `FacetExclusive`, drop the lifetime
  parameter from resources, and rename their `inner` to `subject`.
- Upgrade dependencies.

## 0.4.3 – 2024-07-23

- Disable default features of SQLx and Tokio.
- Support shell-quote 0.7.
- Upgrade dependencies.

## 0.4.2 – 2024-04-08

- Determine a more useful status for a cluster.
- Upgrade dependencies.

## 0.4.1 – 2023-12-06

- `ClusterGuard` can create a cluster that does not exist.

## 0.4.0 – 2023-12-06

- Set `DATABASE_URL` in `Cluster::shell` and `Cluster::exec`.
- Add `coordinate::guard` and the `ClusterGuard` type alias, and use a guard
  in `cluster::run`.
- `restore` shows progress and colour, manages the cluster as a resource,
  and disables archiving in the restored cluster.
- **Breaking:** `backup --backup-into <DIR>` is now `backup --into <DIR>`.
- **Breaking:** remove `prelude`, and rename `control` to `subject`.
- Report errors with miette instead of color-eyre.
- `percent` rounds to nearest rather than up.

## 0.3.1 – 2023-10-11

- Mark the crates as alpha in their READMEs.

## 0.3.0 – 2023-10-11

- Introduce a more general constraint system for runtime strategies;
  `Strategy` is now an enum.
- Add `--runtime-default` to `shell` and `exec`, and runtime selection to
  `runtimes`.
- Run `shell` when no command is given.

## 0.2.0 – 2023-10-09

- First tagged release.
