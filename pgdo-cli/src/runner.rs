use std::fs;
use std::io;
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::process::ExitStatus;

use miette::{bail, IntoDiagnostic, Result, WrapErr};

use crate::{args, ExitResult};

use pgdo::{
    cluster, coordinate, lock,
    runtime::{
        self,
        constraint::Constraint,
        strategy::{Strategy, StrategyLike},
    },
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
}

/// Determine the strategy to use for a cluster, given an optional constraint.
pub(crate) fn determine_strategy(fallback: Option<Constraint>) -> Result<Strategy, StrategyError> {
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

const UUID_NS: uuid::Uuid = uuid::Uuid::from_u128(93875103436633470414348750305797058811);

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub(crate) enum LockForError {
    #[error("Could not canonicalize cluster directory ({1})")]
    ClusterDirectoryError(#[source] std::io::Error, PathBuf),
    #[error("Could not create UUID-based lock file (uuid = {1})")]
    UuidLockError(#[source] std::io::Error, uuid::Uuid),
}

/// Provide an unlocked lock for the given directory.
pub(crate) fn lock_for<P: AsRef<Path>>(
    path: P,
) -> Result<(PathBuf, lock::UnlockedFile), LockForError> {
    let path = path.as_ref();
    let path = path
        .canonicalize()
        .map_err(|err| LockForError::ClusterDirectoryError(err, path.into()))?;
    let name = path.as_os_str().as_bytes();
    let lock_uuid = uuid::Uuid::new_v5(&UUID_NS, name);
    let lock = lock::UnlockedFile::try_from(&lock_uuid)
        .map_err(|err| LockForError::UuidLockError(err, lock_uuid))?;
    Ok((path, lock))
}

#[allow(clippy::enum_variant_names)]
pub(crate) enum Runner {
    RunAndStop,
    RunAndStopIfExists,
    RunAndDestroy,
}

/// Run an action on a cluster.
///
/// This is the main entry point for most `pgdo` commands (though not all). It
/// takes care of creating, locking, starting, stopping, and destroying the
/// cluster, and running the given action.
pub(crate) fn run<ACTION>(
    runner: Runner,
    args::ClusterArgs { dir: cluster_dir }: args::ClusterArgs,
    args::ClusterModeArgs { mode: cluster_mode }: args::ClusterModeArgs,
    args::RuntimeArgs { fallback }: args::RuntimeArgs,
    action: ACTION,
) -> ExitResult
where
    ACTION: FnOnce(&cluster::Cluster) -> ExitResult + std::panic::UnwindSafe,
{
    match runner {
        Runner::RunAndStop | Runner::RunAndDestroy => {
            // Attempt to create the cluster directory.
            match fs::create_dir(&cluster_dir) {
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
                err @ Err(_) => err
                    .into_diagnostic()
                    .wrap_err_with(|| "Could not create cluster directory")
                    .wrap_err_with(|| format!("Cluster directory: {}", cluster_dir.display()))?,
                _ => (),
            }
        }
        Runner::RunAndStopIfExists => {
            // Do not create cluster directory. If the cluster directory does
            // not exist, we expect to crash later.
        }
    };

    let (datadir, lock) = lock_for(&cluster_dir)?;
    let strategy = determine_strategy(fallback)?;
    let cluster = cluster::Cluster::new(datadir, strategy)?;

    let runner = match runner {
        Runner::RunAndStop => coordinate::run_and_stop,
        Runner::RunAndStopIfExists => coordinate::run_and_stop_if_exists,
        Runner::RunAndDestroy => coordinate::run_and_destroy,
    };

    runner(&cluster, lock, || {
        if let Some(cluster_mode) = cluster_mode {
            let rt = tokio::runtime::Runtime::new().into_diagnostic()?;
            rt.block_on(set_cluster_mode(cluster_mode, &cluster))?;
        }

        // Ignore SIGINT, TERM, and HUP (with ctrlc feature "termination"). The
        // child process will receive the signal, presumably terminate, then
        // we'll tidy up.
        ctrlc::set_handler(|| ())
            .into_diagnostic()
            .context("Could not set signal handler")?;

        // Finally, run the given action.
        action(&cluster)
    })?
}

/// Set the cluster's "mode", i.e. configure appropriate PostgreSQL settings,
/// e.g. `fsync`, `full_page_writes`, etc. that need to be set early.
async fn set_cluster_mode(
    mode: args::ClusterMode,
    cluster: &cluster::Cluster,
) -> Result<(), cluster::ClusterError> {
    use pgdo::cluster::config::{self, Parameter};

    static FSYNC: Parameter = Parameter("fsync");
    static FULL_PAGE_WRITES: Parameter = Parameter("full_page_writes");
    static SYNCHRONOUS_COMMIT: Parameter = Parameter("synchronous_commit");

    match mode {
        args::ClusterMode::Fast => {
            let pool = cluster.pool(None)?;
            FSYNC.set(&pool, false).await?;
            FULL_PAGE_WRITES.set(&pool, false).await?;
            SYNCHRONOUS_COMMIT.set(&pool, false).await?;
            // TODO: Check `pg_file_settings` for errors before reloading.
            config::reload(&pool).await?;
            Ok(())
        }
        args::ClusterMode::Slow => {
            let pool = cluster.pool(None)?;
            FSYNC.reset(&pool).await?;
            FULL_PAGE_WRITES.reset(&pool).await?;
            SYNCHRONOUS_COMMIT.reset(&pool).await?;
            // TODO: Check `pg_file_settings` for errors before reloading.
            config::reload(&pool).await?;
            Ok(())
        }
    }
}
