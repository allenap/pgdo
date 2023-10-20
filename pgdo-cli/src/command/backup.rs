use std::{ffi::OsStr, path::PathBuf};

use color_eyre::eyre::{eyre, WrapErr};
use color_eyre::{Help, SectionExt};
use either::{Left, Right};
use pgdo::coordinate::resource::{ResourceExclusive, ResourceShared};

use super::ExitResult;
use crate::{args, runner};

use pgdo::{
    cluster::{self, config},
    coordinate::{resource::ResourceFree, State},
};

/// Clone an existing cluster and arrange to continuously archive WAL
/// (Write-Ahead Log) files into that new cluster.
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for backup"))]
pub struct Backup {
    #[clap(flatten)]
    pub cluster: args::ClusterArgs,

    /// The directory into which to clone the cluster.
    #[clap(long = "destination", display_order = 100)]
    pub destination: PathBuf,
}

impl Backup {
    pub fn invoke(self) -> ExitResult {
        let Self { cluster, destination } = self;

        let (datadir, lock) = runner::lock_for(cluster.dir)?;
        let strategy = runner::determine_strategy(None)?;
        let cluster = cluster::Cluster::new(datadir, strategy)?;
        let resource = ResourceFree::new(lock, cluster);

        match backup(resource, destination) {
            Ok(exit_code) => Ok(exit_code),
            Err(error) => {
                log::error!("backup failed; cluster may still be running");
                Err(error)
            }
        }
    }
}

impl From<Backup> for super::Command {
    fn from(backup: Backup) -> Self {
        Self::Backup(backup)
    }
}

// ----------------------------------------------------------------------------

fn pause() {
    use std::{thread::sleep, time::Duration};
    static PAUSE: Duration = Duration::from_millis(2000);
    sleep(PAUSE);
}

use either::Either;

fn retry<F, L, R, E>(init: L, mut func: F) -> Result<R, E>
where
    F: FnMut(L) -> Result<Either<L, R>, E>,
{
    let mut either = func(init)?;
    loop {
        match either {
            Right(right) => break Ok(right),
            Left(left) => {
                pause();
                log::info!("retrying...");
                either = func(left)?;
            }
        }
    }
}

fn backup(resource: ResourceFree<cluster::Cluster>, destination: PathBuf) -> ExitResult {
    log::info!("obtaining exclusive lock");
    let resource = retry(resource, ResourceFree::try_exclusive)?;
    let facet = resource.facet();
    let started = facet.start()?;

    log::info!("downgrading to shared lock");
    let resource = retry(resource, ResourceExclusive::try_shared)?;
    let facet = resource.facet();

    let rt = tokio::runtime::Runtime::new()?;
    let restart = rt.block_on(async {
        let pool = facet.pool(None);
        let mut restart: bool = false;

        // Ensure that `wal_level` is set to `replica` or `logical`. If not,
        // set it to `replica`.
        match WAL_LEVEL.get(&pool).await? {
            Some(config::Value::String(level)) if level == "replica" || level == "logical" => {}
            Some(_) => {
                log::info!("Setting wal_level to 'replica'");
                WAL_LEVEL.set(&pool, "replica").await?;
                restart = true
            }
            None => return Err(eyre!("WAL is not supported; cannot proceed")),
        }

        // Ensure that `archive_mode` is set to `on` or `always`. If not,
        // set it to `on`.
        match ARCHIVE_MODE.get(&pool).await? {
            Some(config::Value::String(level)) if level == "on" || level == "always" => {}
            Some(_) => {
                log::info!("Setting archive_mode to 'on'");
                ARCHIVE_MODE.set(&pool, "on").await?;
                restart = true
            }
            None => return Err(eyre!("Archiving is not supported; cannot proceed")),
        }

        // We can't set `archive_command` if `archive_library` is already set.
        match ARCHIVE_LIBRARY.get(&pool).await? {
            Some(config::Value::String(library)) if library.is_empty() => {}
            Some(archive_library) => {
                return Err(eyre!("Archive library is already set; cannot proceed")
                    .with_section(|| archive_library.header("archive_command:")));
            }
            None => return Err(eyre!("Archiving is not supported; cannot proceed")),
        }

        let archive_command = "echo pgdo-archive p=%p f=%f && false";
        match ARCHIVE_COMMAND.get(&pool).await? {
            // Re. "(disabled)", see `show_archive_command` in xlog.c.
            Some(config::Value::String(command))
                if command == "(disabled)" || command == archive_command =>
            {
                log::info!("Parameter archive_command already set to {archive_command:?}");
            }
            Some(config::Value::String(command)) if command.is_empty() => {
                log::info!("Setting archive_command to {archive_command:?}");
                ARCHIVE_COMMAND.set(&pool, archive_command).await?;
            }
            Some(archive_command) => {
                return Err(eyre!("Archive command is already set; cannot proceed")
                    .with_section(|| archive_command.header("archive_command:")))
            }
            None => return Err(eyre!("Archiving is not supported; cannot proceed")),
        }

        Ok(restart)
    })?;

    let resource = if restart {
        log::info!("restarting cluster; first obtaining exclusive lock");
        let resource = retry(resource, ResourceShared::try_exclusive)?;
        let facet = resource.facet();
        log::info!("stopping...");
        facet.stop()?;
        log::info!("starting...");
        facet.start()?;
        log::info!("downgrading to shared lock");
        resource.shared()?
    } else {
        resource
    };
    let facet = resource.facet();

    log::info!("performing base backup...");
    let args: &[&OsStr] = &[
        "--pgdata".as_ref(),
        destination.as_ref(),
        "--format".as_ref(),
        "plain".as_ref(),
        "--progress".as_ref(),
    ];
    runner::check_exit(
        facet
            .exec(None, "pg_basebackup".as_ref(), args)
            .wrap_err("Executing command in cluster failed")?,
    )?;

    if started == State::Modified {
        // We started the cluster, so we try to shut it down.
        log::info!("shutting down cluster");
        match resource.try_exclusive()? {
            Left(resource) => resource.release(),
            Right(resource) => {
                resource.facet().stop()?;
                resource.release()
            }
        }?;
    } else {
        resource.release()?;
    }

    ExitResult::Ok(std::process::ExitCode::SUCCESS)
}

static ARCHIVE_MODE: config::Parameter = config::Parameter("archive_mode");
static ARCHIVE_COMMAND: config::Parameter = config::Parameter("archive_command");
static ARCHIVE_LIBRARY: config::Parameter = config::Parameter("archive_library");
static WAL_LEVEL: config::Parameter = config::Parameter("wal_level");
