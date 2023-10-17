use std::fs;
use std::io;
use std::process::ExitCode;
use std::process::ExitStatus;

use color_eyre::eyre::{bail, eyre, Result, WrapErr};
use color_eyre::{Help, SectionExt};

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

/// Determine the strategy to use for a cluster, given an optional constraint.
pub(crate) fn determine_strategy(fallback: Option<Constraint>) -> Result<Strategy> {
    let strategy = runtime::strategy::Strategy::default();
    let fallback: Option<_> = match fallback {
        Some(constraint) => match strategy.select(&constraint) {
            Some(runtime) => Some(runtime),
            None => Err(eyre!("no runtime matches constraint {constraint:?}"))
                .with_context(|| "cannot select fallback runtime")
                .with_suggestion(|| "use `runtimes` to see available runtimes")?,
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
        .wrap_err("Could not create database")
        .with_section(|| database_name.to_owned().header("Database:"))?;
    Ok(())
}

const UUID_NS: uuid::Uuid = uuid::Uuid::from_u128(93875103436633470414348750305797058811);

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
                    .wrap_err("Could not create cluster directory")
                    .with_section(|| {
                        format!("{}", cluster_dir.display()).header("Cluster directory:")
                    })?,
                _ => (),
            }
        }
        Runner::RunAndStopIfExists => {
            // Do not create cluster directory. If the cluster directory does
            // not exist, we expect to crash later.
        }
    };

    // Obtain a canonical path to the cluster directory.
    let cluster_dir = cluster_dir
        .canonicalize()
        .wrap_err("Could not canonicalize cluster directory")
        .with_section(|| format!("{}", cluster_dir.display()).header("Cluster directory:"))?;

    // Use the canonical path to construct the UUID with which we'll lock this
    // cluster. Use the `Debug` form of `database_dir` for the lock file UUID.
    let lock_uuid = uuid::Uuid::new_v5(&UUID_NS, format!("{:?}", &cluster_dir).as_bytes());
    let lock = lock::UnlockedFile::try_from(&lock_uuid)
        .wrap_err("Could not create UUID-based lock file")
        .with_section(|| lock_uuid.to_string().header("UUID for lock file:"))?;

    let strategy = determine_strategy(fallback)?;
    let cluster = cluster::Cluster::new(&cluster_dir, strategy)?;

    let runner = match runner {
        Runner::RunAndStop => coordinate::run_and_stop,
        Runner::RunAndStopIfExists => coordinate::run_and_stop_if_exists,
        Runner::RunAndDestroy => coordinate::run_and_destroy,
    };

    runner(&cluster, lock, |cluster: &cluster::Cluster| {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(initialise(cluster_mode, cluster))?;
        drop(rt);

        // Ignore SIGINT, TERM, and HUP (with ctrlc feature "termination"). The
        // child process will receive the signal, presumably terminate, then
        // we'll tidy up.
        ctrlc::set_handler(|| ()).wrap_err("Could not set signal handler")?;

        // Finally, run the given action.
        action(cluster)
    })?
}

/// Create an initialisation function that will set appropriate PostgreSQL
/// settings, e.g. `fsync`, `full_page_writes`, etc. that need to be set early.
async fn initialise(
    mode: Option<args::ClusterMode>,
    cluster: &cluster::Cluster,
) -> Result<(), cluster::ClusterError> {
    use pgdo::cluster::sqlx;
    match mode {
        Some(args::ClusterMode::Fast) => {
            let pool = cluster.pool(None);
            sqlx::query("ALTER SYSTEM SET fsync = 'off'")
                .execute(&pool)
                .await?;
            sqlx::query("ALTER SYSTEM SET full_page_writes = 'off'")
                .execute(&pool)
                .await?;
            sqlx::query("ALTER SYSTEM SET synchronous_commit = 'off'")
                .execute(&pool)
                .await?;
            // TODO: Check `pg_file_settings` for errors before reloading.
            sqlx::query("SELECT pg_reload_conf()")
                .execute(&pool)
                .await?;
            Ok(())
        }
        Some(args::ClusterMode::Slow) => {
            let pool = cluster.pool(None);
            sqlx::query("ALTER SYSTEM RESET fsync")
                .execute(&pool)
                .await?;
            sqlx::query("ALTER SYSTEM RESET full_page_writes")
                .execute(&pool)
                .await?;
            sqlx::query("ALTER SYSTEM RESET synchronous_commit")
                .execute(&pool)
                .await?;
            // TODO: Check `pg_file_settings` for errors before reloading.
            sqlx::query("SELECT pg_reload_conf()")
                .execute(&pool)
                .await?;
            Ok(())
        }
        None => Ok(()),
    }
}
