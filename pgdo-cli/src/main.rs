#![doc = include_str!("../README.md")]

mod args;
mod command;
mod runner;

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;

fn main() -> Result<ExitCode> {
    color_eyre::install()?;

    let cli = Cli::parse();
    // `Shell` is the default command when none is specified.
    let command = cli.command.unwrap_or(Command::Shell(cli.shell));
    match command {
        Command::Shell(shell) => shell.invoke(),
        Command::Exec(exec) => exec.invoke(),
        Command::Clone(clone) => clone.invoke(),
        Command::Runtimes(runtimes) => runtimes.invoke(),
    }
}

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
    pub shell: command::shell::Shell,
}

#[derive(Subcommand)]
pub enum Command {
    #[clap(display_order = 1)]
    Shell(command::shell::Shell),

    #[clap(display_order = 2)]
    Exec(command::exec::Exec),

    #[clap(display_order = 3)]
    Clone(command::clone::Clone),

    #[clap(display_order = 4)]
    Runtimes(command::runtimes::Runtimes),
}
