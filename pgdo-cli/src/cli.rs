use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use pgdo::runtime::constraint::Constraint;

/// Work with ephemeral PostgreSQL clusters.
#[derive(Parser)]
#[clap(author, version, about = "The convenience of SQLite – but with PostgreSQL", long_about = None)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Option<Command>,

    // Default command, `shell`. Note that `ShellArgs` appears here AND in the
    // `Shell` subcommand. This pattern (along with `next_help_heading`) is a
    // way to have a default subcommand with clap.
    // https://github.com/clap-rs/clap/issues/975#issuecomment-1426424232
    #[clap(flatten)]
    pub shell: ShellArgs,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start a psql shell, creating and starting the cluster as necessary
    /// (DEFAULT).
    #[clap(display_order = 1)]
    Shell(ShellArgs),

    /// Execute an arbitrary command, creating and starting the cluster as
    /// necessary.
    #[clap(display_order = 2)]
    Exec(ExecArgs),

    /// List discovered PostgreSQL runtimes.
    ///
    /// The runtime shown on the line beginning with `=>` is the default, i.e.
    /// the runtime that will be used when creating a new cluster.
    #[clap(display_order = 3)]
    Runtimes(RuntimeArgs),
}

#[derive(Args)]
#[clap(next_help_heading = Some("Options for shell"))]
pub struct ShellArgs {
    #[clap(flatten)]
    pub cluster: ClusterArgs,

    #[clap(flatten)]
    pub database: DatabaseArgs,

    #[clap(flatten)]
    pub lifecycle: LifecycleArgs,

    #[clap(flatten)]
    pub runtime: RuntimeArgs,
}

#[derive(Args)]
#[clap(next_help_heading = Some("Options for exec"))]
pub struct ExecArgs {
    #[clap(flatten)]
    pub cluster: ClusterArgs,

    #[clap(flatten)]
    pub database: DatabaseArgs,

    #[clap(flatten)]
    pub lifecycle: LifecycleArgs,

    #[clap(flatten)]
    pub runtime: RuntimeArgs,

    /// The executable to invoke. By default it will start a shell.
    #[clap(env = "SHELL", value_name = "COMMAND", display_order = 999)]
    pub command: OsString,

    /// Arguments to pass to the executable.
    #[clap(value_name = "ARGUMENTS", display_order = 1000)]
    pub args: Vec<OsString>,
}

#[derive(Args)]
pub struct ClusterArgs {
    /// The directory in which to place, or find, the cluster.
    #[clap(
        short = 'D',
        long = "datadir",
        env = "PGDATA",
        value_name = "PGDATA",
        default_value = "cluster",
        display_order = 1
    )]
    pub dir: PathBuf,

    /// Run the cluster in a "safer" or "faster" mode.
    ///
    /// DANGER! Choosing "faster-but-less-safe" makes the cluster faster but it
    /// can lead to unrecoverable data corruption in the event of a power
    /// failure or system crash.
    ///
    /// The mode is STICKY. Running with a mode reconfigures the cluster, and it
    /// will continue to run in that mode. To find out which mode the cluster is
    /// configured for, open a `psql` shell (e.g. `pgdo shell`) and run `SHOW
    /// fsync; SHOW full_page_writes; SHOW synchronous_commit;`.
    #[clap(long = "mode", display_order = 4)]
    pub mode: Option<Mode>,
}

#[derive(Args)]
pub struct DatabaseArgs {
    /// The database to connect to.
    #[clap(
        short = 'd',
        long = "database",
        env = "PGDATABASE",
        value_name = "PGDATABASE",
        default_value = "postgres",
        display_order = 2
    )]
    pub name: String,
}

#[derive(Args)]
pub struct RuntimeArgs {
    /// Select the default runtime, used when creating new clusters.
    #[clap(
        long = "runtime-default",
        value_name = "CONSTRAINT",
        display_order = 80
    )]
    pub fallback: Option<Constraint>,
}

#[derive(Args)]
pub struct LifecycleArgs {
    /// Destroy the cluster after use. WARNING: This will DELETE THE DATA
    /// DIRECTORY. The default is to NOT destroy the cluster.
    #[clap(long = "destroy", display_order = 100)]
    pub destroy: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub enum Mode {
    /// Resets fsync, full_page_writes, and synchronous_commit to defaults.
    #[value(name = "slower-but-safer", alias = "safe")]
    Slow,

    /// Disable fsync, full_page_writes, and synchronous_commit. DANGER!
    #[value(name = "faster-but-less-safe", alias = "fast")]
    Fast,
}
