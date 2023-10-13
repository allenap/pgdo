#![doc = include_str!("../README.md")]

mod args;
mod cli;
mod command;
mod runner;

use std::process::exit;

use clap::Parser;
use color_eyre::eyre::Result;

fn main() -> Result<()> {
    color_eyre::install()?;

    let cli = cli::Cli::parse();
    // `Shell` is the default command when none is specified.
    let command = cli.command.unwrap_or(cli::Command::Shell(cli.shell));
    let result = match command {
        cli::Command::Shell(args) => crate::command::shell::invoke(args),
        cli::Command::Exec(args) => crate::command::exec::invoke(args),
        cli::Command::Runtimes(runtime) => crate::command::runtimes::invoke(runtime),
    };

    match result {
        Ok(code) => exit(code),
        Err(report) => Err(report),
    }
}
