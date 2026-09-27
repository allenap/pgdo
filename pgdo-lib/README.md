<p style="font-size: 2rem; margin-bottom: 0.75rem">⚠️ ALPHA QUALITY ⚠️</p>

This project is in the early stages of development. It is far from feature
complete. It likely contains many bugs and inconsistencies. Documentation is
limited or non-existent. It will change in ways that break backwards
compatibility. **It is not ready for production use**.

That said, if you want to try it out, please do! But bear in mind that it is
being updated frequently, at least at the time I'm writing this, and you should
expect to update it frequently too. **Please check out known issues and file
new ones [here](https://github.com/allenap/pgdo/issues)**.

Thanks! Gavin.

---

# pgdo-lib

[![pgdo CI](https://github.com/allenap/pgdo/actions/workflows/build.yml/badge.svg)](https://github.com/allenap/pgdo/actions/workflows/build.yml)

A [Rust](https://www.rust-lang.org/) library for creating standalone PostgreSQL
clusters and databases with a focus on convenience and rapid prototyping – such
as one sees using SQLite. Scaling down the developer experience to meet
individuals working to build something new, build something rapidly, is a key
goal of this project.

It inherits code from the [rust-postgresfixture][] project but deviates from
that project's goals and design. Way back, we can trace this tool's origins to
ideas in the Python [postgresfixture][] library which saw heavy use in
[MAAS](https://maas.io/). That was (and is) a useful tool when experimenting
with PostgreSQL. For example we could use it to bring up a cluster to run a
development server. However, it came into its own in MAAS's test suites, and was
key to [making MAAS's test suites faster][maas-faster-tests].

[rust-postgresfixture]: https://github.com/allenap/rust-postgresfixture
[postgresfixture]: https://pypi.python.org/pypi/postgresfixture
[maas-faster-tests]: https://allenap.me/post/the-way-to-run-tests-quickly-in-maas

## Command-line application

There is a [`pgdo`](../pgdo-cli) command-line application that uses this
library. That may be the easiest way to see how pgdo works.

It's published as **pgdo-cli** on [Crates.io](https://crates.io/crates/pgdo-cli)
and [Lib.rs](https://lib.rs/crates/pgdo-cli).

## Use as a library

The essential functionality in this crate is in the `Cluster` struct and its
implementation. This covers the logic you need to create, run, and destroy
PostgreSQL clusters of any version that is supported upstream, from PostgreSQL
15 onwards (see `pgdo::runtime::MINIMUM_VERSION`).

```rust
use pgdo::{
  cluster::{Cluster, ClusterError, State},
  runtime::{self, strategy::StrategyLike},
};
for runtime in runtime::strategy::Strategy::default().runtimes() {
  let datadir = tempfile::tempdir()?;
  let cluster = Cluster::new(&datadir, runtime)?;
  cluster.start(&[])?;
  assert_eq!(cluster.databases()?, vec!["postgres", "template0", "template1"]);
  assert_eq!(cluster.createdb("example")?, State::Modified);
  // Connect with your client library of choice via `cluster.datadir`, or
  // with a URL from `cluster.url("example")`, e.g.:
  let url = cluster.url("example")?.expect("datadir is not valid UTF-8");
  assert_eq!(url.scheme(), "postgresql");
  cluster.stop()?;
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

**However**, you will usually want to use a [`Session`][`cluster::Session`]
instead: `cluster.session(&[])?` creates and starts the cluster as necessary,
and stops it – or destroys it – when the session ends. Sessions use locking so
that multiple processes can safely share a single on-demand cluster; the cluster
is stopped only when the last session ends.

To connect with a different client library, such as SQLx, use
[`Cluster::datadir`][`cluster::Cluster::datadir`] or
[`Cluster::url`][`cluster::Cluster::url`].

## Contributing

If you feel the urge to hack on this code, here's how to get started:

- [Install Cargo][install-cargo],
- Clone this repository,
- Build it: `cargo build`.

[install-cargo]: https://crates.io/install

### Running the tests

After installing the source (see above) run tests with: `cargo test`.

Most tests use pgdo's platform-specific knowledge to test against all of the
PostgreSQL runtimes that are installed. When writing new tests, try to mimic the
pattern in preexisting tests to ensure that those tests are getting the broadest
coverage. Specifically this means:

- Install multiple versions of PostgreSQL on your machine. Read on for
  platform-specific notes.

- [`runtime::strategy::Strategy::default()`] may be able to automatically find
  those installed runtimes – this is the function used by tests.

- If pgdo's platform-specific knowledge doesn't cover your platform, have a go
  at adding to it. [`runtime::strategy::RuntimesOnPlatform`] is a good place to
  start.

#### Debian & Ubuntu

From <https://wiki.postgresql.org/wiki/Apt>:

```shellsession
$ sudo apt-get install -y postgresql-common
$ sudo sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
$ sudo apt-get install -y postgresql-{15,16,17,18}  # Adjust as necessary.
```

#### macOS

Using [Homebrew](https://brew.sh/):

```shellsession
$ brew install postgresql  # Latest version.
$ brew install postgresql@{15,16,17,18}  # Adjust as necessary.
```

### Releasing new versions of pgdo-cli and pgdo-lib

The packages in this workspace are released together, with the same version
number, and they must be uploaded in a certain order.

1. Bump the version in the top-level (workspace) `Cargo.toml`.
2. In `pgdo-cli/Cargo.toml`, update the dependency on `pgdo-lib` to match the
   new version from the previous step.
3. Run `cargo update --workspace` to ensure that `Cargo.lock` is up to date.
4. Paste updated `-h` output into pgdo-cli's `README.md`. On macOS the command
   `cargo run -- -h | pbcopy` is helpful. **Note** that `--help` output is not
   the same as `-h` output: it's more verbose and too much for an overview.
5. Build, test, docs: `cargo build && cargo test && cargo doc --no-deps`.
6. Commit everything with the message "Bump version to `$VERSION`."
7. Tag with "v`$VERSION`", e.g. `git tag v1.0.10`.
8. Push: `git push && git push --tags`.
9. Publish library: `cargo publish --package pgdo-lib`.
10. Publish binary: `cargo publish --package pgdo-cli`.

## License

This package is licensed under the [Apache 2.0 License][].

[Apache 2.0 License]: https://www.apache.org/licenses/LICENSE-2.0
