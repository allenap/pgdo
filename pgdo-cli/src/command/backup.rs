use std::{
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{PoisonError, RwLock},
};

use color_eyre::eyre::eyre;
use color_eyre::{Help, SectionExt};
use either::{Left, Right};

use super::ExitResult;
use crate::{args, runner};

use pgdo::{
    cluster::{self, backup, resource, ClusterError},
    coordinate::{cleanup::with_cleanup, finally::with_finally, State},
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
        let resource = resource::ResourceFree::new(lock, cluster);
        backup(resource, destination)?;

        Ok(ExitCode::SUCCESS)
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
            BackupTool::WalArchive { source, target } => Ok(copy_wal_archive(source, target)),
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

/// Perform a backup of the given `resource` to `destination`.
///
/// This is a twofold process:
/// - Configure archiving into the destination.
/// - Perform a base backup into the destination.
///
/// TODO: Clean up old WAL files?
///
/// TODO: Handle table-spaces?
///
fn backup<D: AsRef<Path>>(
    resource: resource::ResourceFree,
    destination: D,
) -> color_eyre::Result<()> {
    // `Backup::prepare` creates the destination directory and the WAL archive
    // directory if these do not exist, and allocates a temporary location for
    // the base backup.
    let backup = {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async { backup::Backup::prepare(&destination).await })?
    };

    log::info!("Starting cluster (if not already started)…");
    let (started, resource) = resource::startup_if_exists(resource)?;
    // Wrap `resource` in an `RwLock` so that we can pass it around AND so that
    // `do_cleanup` can reference it in its closure.
    let resource = RwLock::new(resource);

    // Shuts down the cluster if we started it.
    let do_cleanup = || -> Result<State, ClusterError> {
        match (started, resource.read().as_deref()) {
            (State::Modified, Ok(Right(resource))) => {
                // We started the cluster AND we have an exclusive resource, so
                // we try to shut it down.
                log::info!("Shutting down cluster…");
                resource.facet().stop()
            }
            (State::Modified, Ok(Left(_)) | Err(_)) => {
                // Somehow we started the cluster BUT we have only a shared
                // resource – or a poisoned resource lock. Neither of those
                // should happen, but it's possible.
                log::warn!(concat!(
                    "Cluster was started for backup, but it cannot now be shut down; ",
                    "please shut it down manually."
                ));
                Ok(State::Unmodified)
            }
            (State::Unmodified, Ok(_)) => {
                // We didn't start the cluster, so do nothing.
                Ok(State::Unmodified)
            }
            (State::Unmodified, Err(_)) => {
                // Ignore lock poisoning errors.
                Ok(State::Unmodified)
            }
        }
    };

    // The command we use to copy WAL files to `destination_wal`.
    // <https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-ARCHIVING-WAL>.
    let archive_command = {
        let pgdo_exe_shell = std::env::current_exe().map(quote_sh)??;
        let destination_wal_shell = quote_sh(&backup.destination_wal)?;
        format!("{pgdo_exe_shell} backup:tools wal:archive %p {destination_wal_shell}/%f")
    };

    // Configure the cluster to continuously archive WAL files. This may require
    // a restart of the cluster on the first time through.
    let needs_restart = with_cleanup(do_cleanup, || {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            match resource.read().as_deref() {
                Ok(resource) => {
                    backup
                        .do_configure_archiving(resource, &archive_command)
                        .await
                }
                Err(err) => panic!("Could not acquire resource: {err}"),
            }
        })
    })?;

    if needs_restart {
        log::info!("The cluster must be restarted so that configuration changes come into effect.");
        match resource.read().as_deref() {
            Ok(Left(_)) => {
                // Need to restart the cluster BUT we do NOT have an exclusive lock.
                Err(backup::BackupError::GeneralError(
                    concat!(
                        "The cluster is in use, and so cannot be restarted automatically. ",
                        "Please restart the cluster manually then try this backup again.",
                    )
                    .into(),
                ))?;
            }
            Ok(Right(resource)) => {
                // Need to restart the cluster AND we have an exclusive lock.
                let facet = resource.facet();
                with_cleanup(do_cleanup, || {
                    log::info!("Restarting cluster; stopping…");
                    facet.stop().and_then(|_| {
                        log::info!("Restarting cluster; starting up again…");
                        facet.start()
                    })
                })?;
            }
            Err(err) => panic!("Could not acquire resource: {err}"),
        };
    }

    log::info!("Performing base backup…");
    let destination_data = match resource.read().as_deref() {
        Ok(resource) => with_finally(do_cleanup, || {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async { backup.do_base_backup(resource).await })
        }),
        Err(err) => panic!("Could not acquire resource: {err}"),
    }?;
    log::info!("Base backup complete; find it at {destination_data:?}");

    // Explicitly release resources, but allow the `ResourceFree` that we get
    // back to immediately be dropped. This allows errors to be visible.
    //
    // NOTE: The `unwrap_or_else` is to deal with lock poisoning. `PoisonError`
    // captures the panic that poisoned the lock, which can reference variables
    // in the function – which in turn can upset the compiler if we return the
    // `PoisonError` from this function, i.e. it sees lifetime violations. These
    // are confusing to diagnose. Anyway, while we don't expect poisoning, it is
    // in the types and so we must deal with it.
    resource
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner)
        .either(
            resource::ResourceShared::release,
            resource::ResourceExclusive::release,
        )?;

    Ok(())
}

/// Copy a WAL archive file. Used in `archive_command`.
fn copy_wal_archive(source: PathBuf, target: PathBuf) -> ExitCode {
    use std::{
        fs::File,
        io::{self, BufRead, ErrorKind::AlreadyExists, Write},
    };
    // Avoid loading entire WAL files into memory. Context: I've read that WAL
    // files can grow to be pretty large (`wal_segment_size`, with a default of
    // 16MiB, multiplied by the number of segments – which can vary, and grow
    // large esp. when there is sustained write activity).
    match File::open(&source) {
        Ok(file_source) => {
            // Try to open the target archive file.
            match File::options().write(true).create_new(true).open(&target) {
                // Target archive file is ready to write.
                Ok(file_target) => {
                    log::info!("WAL archiving from {source:?} to {target:?}");
                    let mut reader = io::BufReader::new(&file_source);
                    let mut writer = io::BufWriter::new(&file_target);
                    match io::copy(&mut reader, &mut writer)
                        .and_then(|_| writer.flush())
                        .and_then(|_| file_target.sync_all())
                    {
                        Ok(()) => ExitCode::SUCCESS,
                        Err(err) => {
                            log::error!("WAL archive failure; error while copying: {err}");
                            ExitCode::FAILURE
                        }
                    }
                }
                // Target archive file already exists.
                Err(err) if err.kind() == AlreadyExists => {
                    // Try to open target archive file to compare contents with
                    // source archive file.
                    match File::open(&target) {
                        // Target archive file is ready to read.
                        Ok(file_target) => {
                            let mut reader_source = io::BufReader::new(&file_source);
                            let mut reader_target = io::BufReader::new(&file_target);
                            loop {
                                let (bytes_source, bytes_target) = {
                                    let buf_source = match reader_source.fill_buf() {
                                        Ok(buf) => buf,
                                        Err(err) => {
                                            log::error!("WAL archive failure; error reading {source:?}: {err}");
                                            break ExitCode::FAILURE;
                                        }
                                    };
                                    let buf_target = match reader_target.fill_buf() {
                                        Ok(buf) => buf,
                                        Err(err) => {
                                            log::error!("WAL archive failure; error reading {target:?}: {err}");
                                            break ExitCode::FAILURE;
                                        }
                                    };
                                    if buf_source.is_empty() && buf_target.is_empty() {
                                        log::info!("WAL file {source:?} already archived okay");
                                        break ExitCode::SUCCESS;
                                    } else if buf_source != buf_target {
                                        log::error!("WAL file {source:?} already archived to {target:?} **BUT CONTENTS DIFFER**");
                                        break ExitCode::FAILURE;
                                    };
                                    (buf_source.len(), buf_target.len())
                                };
                                reader_source.consume(bytes_source);
                                reader_target.consume(bytes_target);
                            }
                        }
                        // Target archive file cannot be read.
                        Err(err) => {
                            log::error!("WAL archive failure; error accessing {target:?}: {err}");
                            ExitCode::FAILURE
                        }
                    }
                }
                // Target archive file cannot be opened for writing.
                Err(err) => {
                    log::error!("WAL archive failure; error accessing {target:?}: {err}");
                    ExitCode::FAILURE
                }
            }
        }
        // Source archive file cannot be read.
        Err(err) => {
            log::error!("WAL archive failure; error accessing {source:?}: {err}");
            ExitCode::FAILURE
        }
    }
}

// ----------------------------------------------------------------------------

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
