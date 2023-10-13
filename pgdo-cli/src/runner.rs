use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::process::ExitStatus;

use color_eyre::eyre::{bail, eyre, Result, WrapErr};
use color_eyre::{Help, SectionExt};

use crate::args;

use pgdo::{
    cluster, coordinate, lock,
    runtime::{
        self,
        constraint::Constraint,
        strategy::{Strategy, StrategyLike},
    },
};

pub(crate) fn check_exit(status: ExitStatus) -> Result<ExitCode> {
    match status.code() {
        None => bail!("Command terminated: {status}"),
        Some(code) => Ok(u8::try_from(code)
            .map(ExitCode::from)
            .unwrap_or(ExitCode::FAILURE)),
    }
}

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

const UUID_NS: uuid::Uuid = uuid::Uuid::from_u128(93875103436633470414348750305797058811);

pub(crate) fn run<INIT, ACTION>(
    database_dir: PathBuf,
    database_name: &str,
    strategy: Strategy,
    destroy: bool,
    initialise: INIT,
    action: ACTION,
) -> Result<ExitCode>
where
    INIT: std::panic::UnwindSafe + FnOnce(&cluster::Cluster) -> Result<(), cluster::ClusterError>,
    ACTION: FnOnce(&cluster::Cluster) -> Result<ExitCode> + std::panic::UnwindSafe,
{
    // Create the cluster directory first.
    match fs::create_dir(&database_dir) {
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
        err @ Err(_) => err
            .wrap_err("Could not create database directory")
            .with_section(|| format!("{}", database_dir.display()).header("Database directory:"))?,
        _ => (),
    };

    // Obtain a canonical path to the cluster directory.
    let database_dir = database_dir
        .canonicalize()
        .wrap_err("Could not canonicalize database directory")
        .with_section(|| format!("{}", database_dir.display()).header("Database directory:"))?;

    // Use the canonical path to construct the UUID with which we'll lock this
    // cluster. Use the `Debug` form of `database_dir` for the lock file UUID.
    let lock_uuid = uuid::Uuid::new_v5(&UUID_NS, format!("{:?}", &database_dir).as_bytes());
    let lock = lock::UnlockedFile::try_from(&lock_uuid)
        .wrap_err("Could not create UUID-based lock file")
        .with_section(|| lock_uuid.to_string().header("UUID for lock file:"))?;

    let cluster = cluster::Cluster::new(&database_dir, strategy)?;

    let runner = if destroy {
        coordinate::run_and_destroy
    } else {
        coordinate::run_and_stop
    };

    runner(&cluster, lock, |cluster: &cluster::Cluster| {
        initialise(cluster)?;

        if !cluster
            .databases()
            .wrap_err("Could not list databases")?
            .contains(&database_name.to_string())
        {
            cluster
                .createdb(database_name)
                .wrap_err("Could not create database")
                .with_section(|| database_name.to_owned().header("Database:"))?;
        }

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
pub(crate) fn initialise(
    mode: Option<args::ClusterMode>,
) -> impl std::panic::UnwindSafe + FnOnce(&cluster::Cluster) -> Result<(), cluster::ClusterError> {
    match mode {
        Some(args::ClusterMode::Fast) => {
            |cluster: &cluster::Cluster| {
                let mut conn = cluster.connect("template1")?;
                conn.execute("ALTER SYSTEM SET fsync = 'off'", &[])?;
                conn.execute("ALTER SYSTEM SET full_page_writes = 'off'", &[])?;
                conn.execute("ALTER SYSTEM SET synchronous_commit = 'off'", &[])?;
                // TODO: Check `pg_file_settings` for errors before reloading.
                conn.execute("SELECT pg_reload_conf()", &[])?;
                Ok(())
            }
        }
        Some(args::ClusterMode::Slow) => {
            |cluster: &cluster::Cluster| {
                let mut conn = cluster.connect("template1")?;
                conn.execute("ALTER SYSTEM RESET fsync", &[])?;
                conn.execute("ALTER SYSTEM RESET full_page_writes", &[])?;
                conn.execute("ALTER SYSTEM RESET synchronous_commit", &[])?;
                // TODO: Check `pg_file_settings` for errors before reloading.
                conn.execute("SELECT pg_reload_conf()", &[])?;
                Ok(())
            }
        }
        None => |_: &cluster::Cluster| Ok(()),
    }
}
