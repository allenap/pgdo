use clap::{Parser, Subcommand};

use crate::command;

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
    pub shell: command::shell::Args,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start a psql shell, creating and starting the cluster as necessary
    /// (DEFAULT).
    #[clap(display_order = 1)]
    Shell(command::shell::Args),

    /// Execute an arbitrary command, creating and starting the cluster as
    /// necessary.
    #[clap(display_order = 2)]
    Exec(command::exec::Args),

    /// Clone a cluster even while it's running.
    #[clap(display_order = 3)]
    Clone(command::clone::Args),

    /// List discovered PostgreSQL runtimes.
    ///
    /// The runtime shown on the line beginning with `=>` is the default, i.e.
    /// the runtime that will be used when creating a new cluster.
    #[clap(display_order = 4)]
    Runtimes(command::runtimes::Args),
}
