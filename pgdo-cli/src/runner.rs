use std::fs;
use std::io;
use std::process::ExitCode;
use std::process::ExitStatus;

use miette::{bail, IntoDiagnostic, Result, WrapErr};

use crate::{args, ExitResult};

use pgdo::{
    cluster,
    runtime::{
        self,
        constraint::Constraint,
        strategy::{Strategy, StrategyLike},
    },
    version::PartialVersion,
};

/// Check the exit status of a process and return an appropriate exit code.
pub(crate) fn check_exit(status: ExitStatus) -> ExitResult {
    match status.code() {
        None => bail!("Command terminated: {status}"),
        Some(code) => Ok(u8::try_from(code)
            .map(ExitCode::from)
            .unwrap_or(ExitCode::FAILURE)),
    }
}

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub(crate) enum StrategyError {
    #[error("No runtime matches constraint {0:?}")]
    #[diagnostic(help("Use `runtimes` to see available runtimes"))]
    ConstraintNotSatisfied(runtime::constraint::Constraint),
    #[error(
        "PostgreSQL {0} is not supported; pgdo requires PostgreSQL {min} or later",
        min = runtime::MINIMUM_VERSION
    )]
    #[diagnostic(help("Use `runtimes` to see available runtimes"))]
    UnsupportedVersion(PartialVersion),
}

/// Determine the strategy to use for a cluster, given an optional constraint.
pub(crate) fn determine_strategy(fallback: Option<Constraint>) -> Result<Strategy, StrategyError> {
    // Unsupported runtimes are never discovered, so a constraint that asks for
    // one can never be satisfied. Say so specifically.
    if let Some(Constraint::Version(version)) = fallback {
        if !runtime::is_supported(version) {
            return Err(StrategyError::UnsupportedVersion(version));
        }
    }
    let strategy = runtime::strategy::Strategy::default();
    let fallback: Option<_> = match fallback {
        Some(constraint) => match strategy.select(&constraint) {
            Some(runtime) => Some(runtime),
            None => return Err(StrategyError::ConstraintNotSatisfied(constraint)),
        },
        None => None,
    };
    let strategy = match fallback {
        Some(fallback) => strategy.push_front(fallback),
        None => strategy,
    };
    Ok(strategy)
}

/// Ensure that a given named database exists in a cluster.
///
/// The cluster should be running.
pub(crate) fn ensure_database(cluster: &cluster::Cluster, database_name: &str) -> Result<()> {
    cluster
        .createdb(database_name)
        .wrap_err_with(|| "Could not create database")
        .wrap_err_with(|| format!("Database: {database_name}"))?;
    Ok(())
}

/// Run an action on a cluster.
///
/// This is the main entry point for most `pgdo` commands (though not all). It
/// takes care of creating, starting, stopping, and destroying the cluster – via
/// a [`cluster::Session`] – and running the given action.
pub(crate) fn run<ACTION>(
    args::ClusterArgs { dir: datadir }: args::ClusterArgs,
    args::ClusterModeArgs { mode: cluster_mode }: args::ClusterModeArgs,
    args::RuntimeArgs { fallback }: args::RuntimeArgs,
    lifecycle: args::LifecycleArgs,
    action: ACTION,
) -> ExitResult
where
    ACTION: FnOnce(&cluster::Cluster) -> ExitResult,
{
    // Attempt to create the cluster directory. Unlike `Cluster::session`, do
    // not create parent directories; a mistyped path should not create a tree.
    match fs::create_dir(&datadir) {
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
        err @ Err(_) => err
            .into_diagnostic()
            .wrap_err_with(|| "Could not create cluster directory")
            .wrap_err_with(|| format!("Cluster directory: {}", datadir.display()))?,
        _ => (),
    }

    let strategy = determine_strategy(fallback)?;
    let session = cluster::Cluster::new(datadir, strategy)?
        .session(&[])?
        .finish(lifecycle.finish());

    let act = || {
        if let Some(cluster_mode) = cluster_mode {
            set_cluster_mode(cluster_mode, &session)?;
        }

        // Ignore SIGINT, TERM, and HUP (with ctrlc feature "termination"). The
        // child process will receive the signal, presumably terminate, then
        // we'll tidy up.
        ctrlc::set_handler(|| ())
            .into_diagnostic()
            .context("Could not set signal handler")?;

        // Finally, run the given action.
        action(&session)
    };

    // If the action panics, dropping `session` still ends it (and logs any
    // error in doing so).
    let result = act();
    match (result, session.end()) {
        (Ok(code), Ok(_)) => Ok(code),
        (Ok(_), Err(err)) => Err(err).wrap_err("Could not end session with cluster"),
        (Err(err), Ok(_)) => Err(err),
        (Err(err), Err(end_err)) => {
            log::error!("Could not end session with cluster: {end_err}");
            Err(err)
        }
    }
}
/// Set the cluster's "mode", i.e. configure appropriate PostgreSQL settings,
/// e.g. `fsync`, `full_page_writes`, etc. that need to be set early.
fn set_cluster_mode(
    mode: args::ClusterMode,
    cluster: &cluster::Cluster,
) -> Result<(), cluster::ClusterError> {
    use pgdo::cluster::config::{self, Parameter};

    const FSYNC: Parameter = Parameter("fsync");
    const FULL_PAGE_WRITES: Parameter = Parameter("full_page_writes");
    const SYNCHRONOUS_COMMIT: Parameter = Parameter("synchronous_commit");

    match mode {
        args::ClusterMode::Fast => {
            FSYNC.set(cluster, false)?;
            FULL_PAGE_WRITES.set(cluster, false)?;
            SYNCHRONOUS_COMMIT.set(cluster, false)?;
        }
        args::ClusterMode::Slow => {
            FSYNC.reset(cluster)?;
            FULL_PAGE_WRITES.reset(cluster)?;
            SYNCHRONOUS_COMMIT.reset(cluster)?;
        }
    }
    // TODO: Check `pg_file_settings` for errors before reloading.
    config::reload(cluster)?;
    Ok(())
}
