use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::ExitCode,
};

use color_eyre::eyre::{eyre, WrapErr};
use color_eyre::{Help, SectionExt};
use either::{Either, Left, Right};

use super::ExitResult;
use crate::{args, runner};

use pgdo::{
    cluster::{self, config},
    coordinate::{
        cleanup::with_cleanup,
        finally::with_finally,
        resource::{ResourceFree, ResourceShared},
        State,
    },
    lock,
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

/// Internal tools for assisting with Continuous Archiving and Point-in-Time
/// Recovery (PITR) backups.
///
/// <https://www.postgresql.org/docs/current/continuous-archiving.html>
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for backup:tools"))]
pub struct BackupTools {
    #[clap(subcommand)]
    command: BackupTool,
}

impl BackupTools {
    pub fn invoke(self) -> ExitResult {
        match self.command {
            BackupTool::WalArchive { source, target } => {
                use std::{
                    fs::{read, write},
                    io::ErrorKind::NotFound,
                };
                match (read(&source), read(&target)) {
                    (Ok(wal_in), Err(err)) if err.kind() == NotFound => {
                        log::info!("WAL archiving from {source:?} to {target:?}");
                        match write(&target, wal_in) {
                            Ok(()) => Ok(ExitCode::SUCCESS),
                            Err(err) => {
                                log::error!("WAL archive failure; error writing {target:?}: {err}");
                                Ok(ExitCode::FAILURE)
                            }
                        }
                    }
                    (Ok(wal_in), Ok(wal_out)) if wal_in == wal_out => {
                        log::info!("WAL file {source:?} already archived");
                        Ok(ExitCode::SUCCESS)
                    }
                    (Ok(_), Ok(_)) => {
                        log::error!("WAL file {source:?} already archived to {target:?} BUT CONTENTS DIFFER");
                        Ok(ExitCode::FAILURE)
                    }
                    (Err(err), _) => {
                        log::error!("WAL archive failure; error accessing {source:?}: {err}");
                        Ok(ExitCode::FAILURE)
                    }
                    (_, Err(err)) => {
                        log::error!("WAL archive failure; error accessing {target:?}: {err}");
                        Ok(ExitCode::FAILURE)
                    }
                }
            }
        }
    }
}

impl From<BackupTools> for super::Command {
    fn from(tools: BackupTools) -> Self {
        Self::BackupTools(tools)
    }
}

#[derive(clap::Subcommand)]
pub(crate) enum BackupTool {
    /// Copy a WAL file to an archive; used in `archive_command`.
    #[clap(name = "wal:archive", display_order = 1)]
    WalArchive {
        /// Source WAL file path (corresponds to %p in `archive_command`).
        source: PathBuf,
        /// Destination WAL file path (corresponds to some/where/%f in `archive_command`).
        target: PathBuf,
    },
}

// ----------------------------------------------------------------------------

fn backup(resource: ResourceFree<cluster::Cluster>, destination: PathBuf) -> ExitResult {
    std::fs::create_dir_all(&destination)?;
    let destination = destination.canonicalize()?;
    // Where we're going to copy WAL files to.
    let destination_wal = destination.join("wal");
    std::fs::create_dir_all(&destination_wal)?;
    // Temporary location into which we'll later make the base backup.
    let destination_data_tmp =
        tempfile::TempDir::with_prefix_in(format!(".tmp.{DESTINATION_DATA_PREFIX}"), &destination)?;

    // The command we use to copy WAL files to `destination_wal`.
    // <https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-ARCHIVING-WAL>.
    let archive_command = {
        // Paths, shell escaped as necessary.
        let pgdo_exe_shell = std::env::current_exe().map(quote_sh)??;
        let destination_wal_shell = quote_sh(&destination_wal)?;
        format!("{pgdo_exe_shell} backup:tools wal:archive %p {destination_wal_shell}/%f",)
    };

    log::info!("Starting cluster (if not already started)…");
    let (started, resource) = cluster::resource::startup_if_exists(resource)?;

    // From here on we want an exclusive lock; if we have to set `archive_mode`
    // or `wal_level` then we need to restart the server.
    let resource = match resource {
        Right(exclusive) => exclusive,
        Left(shared) => {
            log::info!("Obtaining exclusive lock…");
            retry(shared, ResourceShared::try_exclusive)?
        }
    };

    let facet = resource.facet();
    let do_cleanup = || {
        if started == State::Modified {
            // We started the cluster, so we try to shut it down.
            log::info!("Shutting down cluster…");
            facet.stop()
        } else {
            Ok(State::Unmodified)
        }
    };

    let needs_restart = with_cleanup(do_cleanup, || {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
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

            match ARCHIVE_COMMAND.get(&pool).await? {
                Some(config::Value::String(command)) if command == archive_command => {
                    log::info!("Parameter archive_command already set to {archive_command:?}");
                }
                // Re. "(disabled)", see `show_archive_command` in xlog.c.
                Some(config::Value::String(command))
                    if command.is_empty() || command == "(disabled)" =>
                {
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
        })
    })?;

    if needs_restart {
        // Need to restart the cluster.
        with_cleanup(do_cleanup, || {
            log::info!("Stopping cluster…");
            facet.stop().and_then(|_| {
                log::info!("Starting cluster…");
                facet.start()
            })
        })?;
    };

    let backup = with_finally(do_cleanup, || {
        log::info!("Performing base backup…");
        let args: &[&OsStr] = &[
            "--pgdata".as_ref(),
            destination_data_tmp.path().as_ref(),
            "--format".as_ref(),
            "plain".as_ref(),
            "--progress".as_ref(),
        ];
        facet
            .exec(None, "pg_basebackup".as_ref(), args)
            .wrap_err("Executing command in cluster failed")
    })?;

    resource.release()?;

    if backup.success() {
        // Before calculating the target directory name or doing the actual
        // rename, take out a coordinating lock in `destination`.
        let destination_lock =
            lock::UnlockedFile::try_from(&destination.join(DESTINATION_LOCK_NAME))?
                .lock_exclusive()?;

        // Where we're going to move the new backup to. This is always a
        // directory named `{DESTINATION_DATA_PREFIX}.NNNNNNNNNN` where NNNNNNNNNN
        // is a zero-padded integer, the next available in `destination`.
        let destination_data = destination.join(format!(
            "{DESTINATION_DATA_PREFIX}{:010}",
            std::fs::read_dir(&destination)?
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| match entry.file_name().to_str() {
                    Some(name) if name.starts_with(DESTINATION_DATA_PREFIX) =>
                        name[DESTINATION_DATA_PREFIX.len()..].parse::<u32>().ok(),
                    Some(_) | None => None,
                })
                .max()
                .unwrap_or_default()
                + 1
        ));

        // Do the rename.
        std::fs::rename(&destination_data_tmp, destination_data)?;
        drop(destination_lock);

        // We don't need the temporary file to do clean-up now.
        let _ = destination_data_tmp.into_path();
    }

    runner::check_exit(backup)
}

static ARCHIVE_MODE: config::Parameter = config::Parameter("archive_mode");
static ARCHIVE_COMMAND: config::Parameter = config::Parameter("archive_command");
static ARCHIVE_LIBRARY: config::Parameter = config::Parameter("archive_library");
static WAL_LEVEL: config::Parameter = config::Parameter("wal_level");

// Successful backups have this directory name prefix.
static DESTINATION_DATA_PREFIX: &str = "data.";

// Coordinating lock for working in the backup destination directory.
static DESTINATION_LOCK_NAME: &str = ".lock";

// ----------------------------------------------------------------------------

fn pause() {
    use std::{thread::sleep, time::Duration};
    static PAUSE: Duration = Duration::from_millis(2000);
    sleep(PAUSE);
}

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
                log::info!("… Retrying…");
                either = func(left)?;
            }
        }
    }
}

fn quote_sh<P: AsRef<Path>>(path: P) -> color_eyre::Result<String> {
    let path = path.as_ref();
    shell_quote::sh::quote(path)
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| {
            eyre!("Cannot shell escape given path")
                .with_section(|| format!("{path:?}").header("Path:"))
        })
}
