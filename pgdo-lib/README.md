# pgdo-lib

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

There is a [pgdo][] command-line application that uses this library. That may be
the easiest way to see how pgdo might help you.

[pgdo]: https://crates.io/crates/pgdo

## Use as a library

The essential functionality in this crate is in the `Cluster` struct and its
implementation. This covers the logic you need to create, run, and destroy
PostgreSQL clusters of any officially supported version (and a few older
versions that are not supported upstream).

```rust
use pgdo::prelude::*;
for runtime in runtime::strategy::default().runtimes() {
  let data_dir = tempdir::TempDir::new("data")?;
  let cluster = Cluster::new(&data_dir, runtime)?;
  cluster.start()?;
  assert_eq!(cluster.databases()?, vec!["postgres", "template0", "template1"]);
  let mut conn = cluster.connect("template1")?;
  let rows = conn.query("SELECT 1234 -- …", &[])?;
  let collations: Vec<i32> = rows.iter().map(|row| row.get(0)).collect();
  assert_eq!(collations, vec![1234]);
  cluster.stop()?;
}
# Ok::<(), ClusterError>(())
```

**However**, you may want to use this with the functions in the `coordinate`
module like [`run_and_stop`][`coordinate::run_and_stop`] and
[`run_and_destroy`][`coordinate::run_and_destroy`]. These add locking to the
setup and teardown steps of using a cluster so that multiple processes can
safely share a single on-demand cluster.

## Contributing

If you feel the urge to hack on this code, here's how to get started:

- [Install cargo][install-cargo],
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

- [`runtime::strategy::default()`] may be able to automatically find those
  installed runtimes – this is the function used by tests.

- If pgdo's platform-specific knowledge doesn't cover your platform, have a go
  at adding to it. [`runtime::strategy::RuntimesOnPlatform`] is a good place to
  start.

#### Debian & Ubuntu

From <https://wiki.postgresql.org/wiki/Apt>:

```shellsession
$ sudo apt-get install -y postgresql-common
$ sudo sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
$ sudo apt-get install -y postgresql-{9.{4,5,6},10,11,12,13}  # Adjust as necessary.
```

#### macOS

Using [Homebrew](https://brew.sh/):

```shellsession
$ brew install postgresql  # Latest version.
$ brew install postgresql@{9.{4,5,6},10,11,12,13}  # Adjust as necessary.
```

### Making a release

1. Bump version in `Cargo.toml`.
2. Build **and** test: `cargo build && cargo test`. The latter on its own does
   do a build, but a test build can hide warnings about dead code, so do both.
3. Commit with message "Bump version to `$VERSION`."
4. Tag with "v`$VERSION`", e.g. `git tag v1.0.10`.
5. Push: `git push && git push --tags`.
6. Publish: `cargo publish`.

## License

This package is licensed under the [Apache 2.0 License][].

[Apache 2.0 License]: https://www.apache.org/licenses/LICENSE-2.0
