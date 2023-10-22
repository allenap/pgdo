use std::{ffi::OsStr, path::Path, sync::PoisonError};

use either::{Left, Right};

use super::{config, resource::ResourceFree};
use crate::lock;
use crate::{
    coordinate::{cleanup::with_cleanup, finally::with_finally, State},
    prelude::CoordinateError,
};

pub use error::BackupError;

#[allow(clippy::too_many_lines, unused)]
fn backup<D: AsRef<Path>>(resource: ResourceFree, destination: D) -> Result<(), BackupError> {
    // TODO: Clean up old WAL files?
    // TODO: Handle table-spaces?

    std::fs::create_dir_all(&destination)?;
    let destination = destination.as_ref().canonicalize()?;
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
    let (started, resource) = super::resource::startup_if_exists(resource)?;
    let resource = std::sync::RwLock::new(resource);

    let do_cleanup = || -> Result<State, super::ClusterError> {
        match (started, resource.read().as_deref()) {
            (State::Modified, Ok(Right(resource))) => {
                // We started the cluster AND we have an exclusive lock, so we
                // try to shut it down.
                log::info!("Shutting down cluster…");
                resource.facet().stop()
            }
            _ => Ok(State::Unmodified),
        }
    };

    let needs_restart = with_cleanup(do_cleanup, || {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let pool = match resource.read().as_deref() {
                Ok(Left(resource)) => resource.facet().pool(None),
                Ok(Right(resource)) => resource.facet().pool(None),
                Err(err) => panic!("Could not acquire resource: {err}"),
            };
            let mut restart: bool = false;

            // Ensure that `wal_level` is set to `replica` or `logical`. If not,
            // set it to `replica`.
            match WAL_LEVEL.get(&pool).await? {
                Some(config::Value::String(level)) if level == "replica" || level == "logical" => {
                    log::debug!("{WAL_LEVEL} already set to {level:?}");
                }
                Some(_) => {
                    log::info!("Setting {WAL_LEVEL} to 'replica'");
                    WAL_LEVEL.set(&pool, "replica").await?;
                    restart = true;
                }
                None => {
                    return Err(BackupError::ConfigError(
                        "WAL is not supported; cannot proceed".into(),
                    ))
                }
            }

            // Ensure that `archive_mode` is set to `on` or `always`. If not,
            // set it to `on`.
            match ARCHIVE_MODE.get(&pool).await? {
                Some(config::Value::String(level)) if level == "on" || level == "always" => {
                    log::debug!("{ARCHIVE_MODE} already set to {level:?}");
                }
                Some(_) => {
                    log::info!("Setting {ARCHIVE_MODE} to 'on'");
                    ARCHIVE_MODE.set(&pool, "on").await?;
                    restart = true;
                }
                None => {
                    return Err(BackupError::ConfigError(
                        "Archiving is not supported; cannot proceed".into(),
                    ))
                }
            }

            // We can't set `archive_command` if `archive_library` is already set.
            match ARCHIVE_LIBRARY.get(&pool).await? {
                Some(config::Value::String(library)) if library.is_empty() => {
                    log::debug!("{ARCHIVE_LIBRARY} not set (good for us)");
                }
                Some(archive_library) => {
                    return Err(BackupError::ConfigError(format!(
                        "{ARCHIVE_LIBRARY} is already set to {archive_library:?}; cannot proceed"
                    )))
                }
                None => {
                    return Err(BackupError::ConfigError(
                        "Archiving is not supported; cannot proceed".into(),
                    ))
                }
            }

            match ARCHIVE_COMMAND.get(&pool).await? {
                Some(config::Value::String(command)) if command == archive_command => {
                    log::debug!("{ARCHIVE_COMMAND} already set to {archive_command:?}");
                }
                // Re. "(disabled)", see `show_archive_command` in xlog.c.
                Some(config::Value::String(command))
                    if command.is_empty() || command == "(disabled)" =>
                {
                    log::info!("Setting {ARCHIVE_COMMAND} to {archive_command:?}");
                    ARCHIVE_COMMAND.set(&pool, archive_command).await?;
                }
                Some(archive_command) => {
                    return Err(BackupError::ConfigError(format!(
                        "{ARCHIVE_COMMAND} is already set to {archive_command:?}; cannot proceed"
                    )))
                }
                None => {
                    return Err(BackupError::ConfigError(
                        "Archiving is not supported; cannot proceed".into(),
                    ))
                }
            }

            Ok(restart)
        })
    })?;

    match (needs_restart, resource.read().as_deref()) {
        (true, Ok(Left(_))) => {
            // Need to restart the cluster BUT we do NOT have an exclusive lock.
            return Err(BackupError::GeneralError(concat!(
                "The configuration changes that were made only go into effect after the cluster is restarted. ",
                "The cluster is in use, and so cannot be restarted automatically. ",
                "Please restart the cluster manually then try this backup again.",
            ).into()));
        }
        (true, Ok(Right(resource))) => {
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
        (_, Err(err)) => panic!("Could not acquire resource: {err}"),
        (_, _) => {}
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
        match resource.read().as_deref() {
            Ok(Left(resource)) => resource.facet().exec(None, "pg_basebackup".as_ref(), args),
            Ok(Right(resource)) => resource.facet().exec(None, "pg_basebackup".as_ref(), args),
            Err(err) => panic!("Could not acquire resource: {err}"),
        }
    })?;

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
            super::resource::ResourceShared::release,
            super::resource::ResourceExclusive::release,
        )?;

    match backup.code() {
        None => Err(BackupError::GeneralError(format!(
            "Backup command terminated: {backup}"
        )))?,
        Some(0) => {
            // Before calculating the target directory name or doing the actual
            // rename, take out a coordinating lock in `destination`.
            let destination_lock =
                lock::UnlockedFile::try_from(&destination.join(DESTINATION_LOCK_NAME))?
                    .lock_exclusive()
                    .map_err(CoordinateError::UnixError)?;

            // Where we're going to move the new backup to. This is always a
            // directory named `{DESTINATION_DATA_PREFIX}.NNNNNNNNNN` where NNNNNNNNNN
            // is a zero-padded integer, the next available in `destination`.
            let destination_data = destination.join(format!(
                "{DESTINATION_DATA_PREFIX}{:010}",
                std::fs::read_dir(&destination)?
                    .filter_map(Result::ok)
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
        Some(code) => Err(backup)?,
    }

    Ok(())
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

fn quote_sh<P: AsRef<Path>>(path: P) -> Result<String, BackupError> {
    let path = path.as_ref();
    shell_quote::sh::quote(path)
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| BackupError::GeneralError(format!("could not quote {path:?} for shell")))
}

// ----------------------------------------------------------------------------

mod error {
    use std::process::ExitStatus;
    use std::{error, fmt, io};

    use crate::{cluster, coordinate};

    #[derive(Debug)]
    pub enum BackupError {
        IoError(io::Error),
        GeneralError(String),
        ConfigError(String),
        CoordinateError(coordinate::CoordinateError<cluster::ClusterError>),
        ClusterError(cluster::ClusterError),
        CommandError(ExitStatus),
        SqlxError(cluster::sqlx::Error),
    }

    impl fmt::Display for BackupError {
        fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            use BackupError::*;
            match *self {
                IoError(ref e) => write!(fmt, "input/output error: {e}"),
                GeneralError(ref e) => write!(fmt, "shell error: {e}"),
                ConfigError(ref e) => write!(fmt, "configuration error: {e}"),
                CoordinateError(ref e) => e.fmt(fmt),
                ClusterError(ref e) => e.fmt(fmt),
                CommandError(ref e) => write!(fmt, "external command failed: {e:?}"),
                SqlxError(ref e) => write!(fmt, "database error: {e}"),
            }
        }
    }

    impl error::Error for BackupError {
        fn source(&self) -> Option<&(dyn error::Error + 'static)> {
            match *self {
                Self::IoError(ref error) => Some(error),
                Self::GeneralError(_) => None,
                Self::ConfigError(_) => None,
                Self::CoordinateError(ref error) => Some(error),
                Self::ClusterError(ref error) => Some(error),
                Self::CommandError(_) => None,
                Self::SqlxError(ref error) => Some(error),
            }
        }
    }

    impl From<io::Error> for BackupError {
        fn from(error: io::Error) -> BackupError {
            Self::IoError(error)
        }
    }

    impl From<coordinate::CoordinateError<cluster::ClusterError>> for BackupError {
        fn from(error: coordinate::CoordinateError<cluster::ClusterError>) -> BackupError {
            Self::CoordinateError(error)
        }
    }

    impl From<cluster::ClusterError> for BackupError {
        fn from(error: cluster::ClusterError) -> BackupError {
            Self::ClusterError(error)
        }
    }

    impl From<ExitStatus> for BackupError {
        fn from(error: ExitStatus) -> BackupError {
            Self::CommandError(error)
        }
    }

    impl From<cluster::sqlx::Error> for BackupError {
        fn from(error: cluster::sqlx::Error) -> BackupError {
            Self::SqlxError(error)
        }
    }
}
