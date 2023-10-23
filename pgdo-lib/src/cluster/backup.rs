use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::{PoisonError, RwLock},
};

use either::{Either, Left, Right};
use tempfile::TempDir;

use super::{
    config,
    resource::{ResourceExclusive, ResourceFree, ResourceShared},
};
use crate::lock;
use crate::{
    coordinate::{cleanup::with_cleanup, finally::with_finally, State},
    prelude::CoordinateError,
};

pub use error::BackupError;

// ----------------------------------------------------------------------------

type BackupResource<'a> = Either<ResourceShared<'a>, ResourceExclusive<'a>>;

// ----------------------------------------------------------------------------

#[derive(Debug)]
pub struct Backup {
    destination: PathBuf,
    destination_wal: PathBuf,
    destination_tmp: TempDir,
}

impl Backup {
    pub fn prepare<D: AsRef<Path>>(destination: D) -> Result<Self, BackupError> {
        let destination = destination.as_ref().canonicalize()?;
        let destination_wal = destination.join("wal");
        std::fs::create_dir_all(&destination_wal)?;
        std::fs::create_dir_all(&destination)?;
        // Temporary location into which we'll make the base backup.
        let destination_tmp_prefix = format!(".tmp.{DESTINATION_DATA_PREFIX}");
        let destination_tmp = TempDir::with_prefix_in(destination_tmp_prefix, &destination)?;
        // All good; we're done.
        Ok(Self { destination, destination_wal, destination_tmp })
    }

    pub async fn do_configure_archiving<'a>(
        &self,
        resource: &'a BackupResource<'a>,
        archive_command: String,
    ) -> Result<bool, BackupError> {
        let pool = match resource {
            Left(resource) => resource.facet().pool(None),
            Right(resource) => resource.facet().pool(None),
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
                ARCHIVE_COMMAND.set(&pool, &archive_command).await?;
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
    }

    pub fn do_base_backup<'a>(
        &self,
        resource: &'a BackupResource<'a>,
    ) -> Result<PathBuf, BackupError> {
        log::info!("Performing base backup…");
        let args: &[&OsStr] = &[
            "--pgdata".as_ref(),
            self.destination_tmp.path().as_ref(),
            "--format".as_ref(),
            "plain".as_ref(),
            "--progress".as_ref(),
        ];
        let status = match resource {
            Left(resource) => resource.facet().exec(None, "pg_basebackup".as_ref(), args),
            Right(resource) => resource.facet().exec(None, "pg_basebackup".as_ref(), args),
        }?;
        if !status.success() {
            Err(status)?;
        }
        // Before calculating the target directory name or doing the actual
        // rename, take out a coordinating lock in `destination`.
        let destination_lock =
            lock::UnlockedFile::try_from(&self.destination.join(DESTINATION_LOCK_NAME))?
                .lock_exclusive()
                .map_err(CoordinateError::UnixError)?;

        // Where we're going to move the new backup to. This is always a
        // directory named `{DESTINATION_DATA_PREFIX}.NNNNNNNNNN` where
        // NNNNNNNNNN is a zero-padded integer, the next available in
        // `destination`.
        let destination_data = self.destination.join(format!(
            "{DESTINATION_DATA_PREFIX}{:010}",
            std::fs::read_dir(&self.destination)?
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
        std::fs::rename(&self.destination_tmp, &destination_data)?;
        drop(destination_lock);

        Ok(destination_data)
    }
}

#[allow(unused)]
fn backup<D: AsRef<Path>>(resource: ResourceFree, destination: D) -> Result<(), BackupError> {
    // TODO: Clean up old WAL files?
    // TODO: Handle table-spaces?

    let backup = Backup::prepare(&destination)?;

    log::info!("Starting cluster (if not already started)…");
    let (started, resource) = super::resource::startup_if_exists(resource)?;
    let resource = RwLock::new(resource);

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

    // The command we use to copy WAL files to `destination_wal`.
    // <https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-ARCHIVING-WAL>.
    let archive_command = {
        let pgdo_exe_shell = std::env::current_exe().map(quote_sh)??;
        let destination_wal_shell = quote_sh(&backup.destination_wal)?;
        format!("{pgdo_exe_shell} backup:tools wal:archive %p {destination_wal_shell}/%f")
    };

    let needs_restart = with_cleanup(do_cleanup, || {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            match resource.read().as_deref() {
                Ok(resource) => {
                    backup
                        .do_configure_archiving(resource, archive_command)
                        .await
                }
                Err(err) => panic!("Could not acquire resource: {err}"),
            }
        })
    })?;

    if needs_restart {
        match resource.read().as_deref() {
            Ok(Left(_)) => {
                // Need to restart the cluster BUT we do NOT have an exclusive lock.
                return Err(BackupError::GeneralError(concat!(
                    "The configuration changes that were made only go into effect after the cluster is restarted. ",
                    "The cluster is in use, and so cannot be restarted automatically. ",
                    "Please restart the cluster manually then try this backup again.",
                ).into()));
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
        Ok(resource) => with_finally(do_cleanup, || backup.do_base_backup(resource)),
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
            super::resource::ResourceShared::release,
            super::resource::ResourceExclusive::release,
        )?;

    Ok(())
}

// ----------------------------------------------------------------------------

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
