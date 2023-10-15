#![doc = include_str!("../README.md")]

mod args;
mod command;
mod runner;

use clap::Parser;

pub(crate) type Result = color_eyre::eyre::Result<std::process::ExitCode>;

fn main() -> Result {
    color_eyre::install()?;
    // Parse command-line arguments.
    let Options { command, default } = Options::parse();
    // Use the default command when none has been specified.
    command.unwrap_or_else(|| default.into()).invoke()
}

/// Work with ephemeral PostgreSQL clusters.
#[derive(Parser)]
#[clap(author, version, about = "The convenience of SQLite – but with PostgreSQL", long_about = None)]
struct Options {
    #[clap(subcommand)]
    command: Option<command::Command>,

    // Default command. Note that this command's arguments appear here AND in
    // the the `Command` enum (used for subcommand selection). This pattern
    // (along with `next_help_heading`) is a way to have a default subcommand
    // with clap.
    // https://github.com/clap-rs/clap/issues/975#issuecomment-1426424232
    #[clap(flatten)]
    default: command::Default,
}
