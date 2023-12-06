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

# pgdo-cli

[![pgdo CI](https://github.com/allenap/pgdo/actions/workflows/build.yml/badge.svg)](https://github.com/allenap/pgdo/actions/workflows/build.yml)

A [Rust](https://www.rust-lang.org/) command-line tool for creating standalone
PostgreSQL clusters and databases with a focus on convenience and rapid
prototyping – such as one sees using SQLite. Scaling down the developer
experience to meet individuals working to build something new, build something
rapidly, is a key goal of this project.

This is the front-end to [pgdo-lib][]; in that package there's more information
about the project as a whole.

[pgdo-lib]: ../pgdo-lib

## Getting started

After [installing Cargo][install-cargo], `cargo install pgdo-cli` will install a
`pgdo` binary in `~/.cargo/bin`, which the Cargo installation process will
probably have added to your `PATH`.

**Note** that this tool does _not_ (yet) come with any PostgreSQL runtimes. You
must install these yourself. The `pgdo` command has some platform-specific
smarts and might be able to find those installed runtimes without further
configuration. To check, use the `runtimes` subcommand. If the runtime you want
to use doesn't show up, add its `bin` directory to `PATH`.

```shellsession
$ pgdo -h
The convenience of SQLite – but with PostgreSQL

Usage: pgdo [OPTIONS] [COMMAND]

Commands:
  shell     Start a psql shell, creating and starting the cluster as necessary (DEFAULT)
  exec      Execute an arbitrary command, creating and starting the cluster as necessary
  clone     Perform a one-off clone/backup of an existing cluster
  backup    Point-in-time backup for an existing cluster
  restore   Point-in-time restore/recovery from a backup made previously with the `backup` command
  runtimes  List discovered PostgreSQL runtimes
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help (see more with '--help')
  -V, --version  Print version

Options for shell:
  -D, --datadir <PGDATA>              The directory in which the cluster lives [env: PGDATA=] [default: cluster]
  -d, --database <PGDATABASE>         The database to connect to [env: PGDATABASE=] [default: postgres]
      --mode <MODE>                   Run the cluster in a "safer" or "faster" mode [possible values: slower-but-safer, faster-but-less-safe]
      --runtime-default <CONSTRAINT>  Select the default runtime, used when creating new clusters
      --destroy                       Destroy the cluster after use. WARNING: This will DELETE THE DATA DIRECTORY. The default is to NOT destroy the cluster

$ pgdo runtimes
   10.22      /opt/homebrew/Cellar/postgresql@10/10.22_6/bin
   11.21      /opt/homebrew/Cellar/postgresql@11/11.21/bin
   12.16      /opt/homebrew/Cellar/postgresql@12/12.16/bin
   13.12      /opt/homebrew/Cellar/postgresql@13/13.12/bin
   14.9       /opt/homebrew/Cellar/postgresql@14/14.9/bin
   15.4       /opt/homebrew/Cellar/postgresql@15/15.4/bin
=> 16.0       /opt/homebrew/bin

$ pgdo shell
postgres=# select …

$ pgdo exec pg_dump
--
-- PostgreSQL database dump
--
…
```

## Contributing

If you feel the urge to hack on this code, here's how to get started:

- [Install Cargo][install-cargo],
- Clone this repository,
- Build it: `cargo build`.

[install-cargo]: https://crates.io/install

### Running the tests

Right now, the pgdo package doesn't have many/any automated tests. That will
surely change, but for now, please test your changes manually with as many
PostgreSQL runtimes as you can. See [pgdo-lib][] for platform-specific notes on
installing runtimes.

### Making a release

See [pgdo-lib][] for notes on how to make a release.

## License

This package is licensed under the [Apache 2.0 License][].

[Apache 2.0 License]: https://www.apache.org/licenses/LICENSE-2.0
