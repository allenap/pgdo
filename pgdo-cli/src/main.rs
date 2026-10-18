#![doc = include_str!("../README.md")]

mod args;
mod command;
mod runner;

use std::io::{stdout, IsTerminal};
use std::process::ExitCode;

use clap::Parser;

pub(crate) type ExitResult = anyhow::Result<ExitCode>;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            report(&err);
            ExitCode::FAILURE
        }
    }
}

fn run() -> ExitResult {
    // Configure logging. Not using local timestamps because `simple_logger`
    // panics when emitting log messages from within a Tokio runtime context:
    // https://github.com/borntyping/rust-simple_logger/issues/84
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Warn)
        .with_colors(stdout().is_terminal())
        .env()
        .init()?;
    // Parse command-line arguments.
    let Options { command, default } = Options::parse();
    // Use the default command when none has been specified.
    command.unwrap_or_else(|| default.into()).invoke()
}

/// Print an error, what caused it, and – if we know – what to do about it.
fn report(err: &anyhow::Error) {
    eprintln!("Error: {err}");
    let mut causes = err.chain().skip(1).peekable();
    if causes.peek().is_some() {
        eprintln!("Caused by:");
        for cause in causes {
            eprintln!("  - {cause}");
        }
    }
    if let Some(help) = help(err) {
        eprintln!("Help: {help}");
    }
}

/// Help for an error: from the first error in its chain that offers any.
fn help(err: &anyhow::Error) -> Option<String> {
    use pgdo::cluster::ClusterError;
    // The library's other errors with help, e.g. `ClientError`, reach us only
    // inside a `ClusterError`, whose `help` includes theirs.
    err.chain().find_map(|cause| {
        cause
            .downcast_ref::<ClusterError>()
            .and_then(ClusterError::help)
            .or_else(|| {
                cause
                    .downcast_ref::<runner::StrategyError>()
                    .map(|err| err.help().to_owned())
            })
    })
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

#[cfg(test)]
mod tests {
    use pgdo::cluster::ClusterError;

    use super::{help, runner::StrategyError};

    #[test]
    fn help_finds_help_anywhere_in_chain() {
        let err = anyhow::Error::new(ClusterError::StalePidFile { pid: 1 })
            .context("could not start")
            .context("could not run");
        assert_eq!(help(&err), ClusterError::StalePidFile { pid: 1 }.help());

        let constraint = "15".parse().expect("valid constraint");
        let err = anyhow::Error::new(StrategyError::ConstraintNotSatisfied(constraint))
            .context("could not choose a runtime");
        assert!(help(&err).is_some());
    }

    #[test]
    fn help_is_none_when_no_error_in_chain_has_help() {
        let err = anyhow::Error::new(ClusterError::Interrupted).context("could not start");
        assert_eq!(help(&err), None);
        assert_eq!(help(&anyhow::anyhow!("something else")), None);
    }
}
