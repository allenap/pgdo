use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

use either::{Left, Right};
use tempfile::TempDir;
use tokio::{fs, task::block_in_place};
use tokio_stream::{wrappers::ReadDirStream, StreamExt};

use super::{config, resource::StartupResource};
use crate::lock;
use crate::prelude::CoordinateError;

pub use error::BackupError;

// ----------------------------------------------------------------------------

#[derive(Debug)]
pub struct Backup {
    pub destination: PathBuf,
    pub destination_wal: PathBuf,
}

impl Backup {
    /// Creates the destination directory and the WAL archive directory if these
    /// do not exist, and allocates a temporary location for the base backup.
    pub async fn prepare<D: AsRef<Path>>(destination: D) -> Result<Self, BackupError> {
        fs::create_dir_all(&destination).await?;
        let destination = destination.as_ref().canonicalize()?;
        let destination_wal = destination.join("wal");
        fs::create_dir_all(&destination_wal).await?;
        Ok(Self { destination, destination_wal })
    }

    /// Configures the cluster for continuous archiving.
    ///
    /// Returns a flag indicating if the cluster must be restarted for changes
    /// to take effect. If the cluster is already configured appropriately, this
    /// does nothing (and returns `false`).
    pub async fn do_configure_archiving<'a>(
        &self,
        resource: &'a StartupResource<'a>,
        archive_command: &str,
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
                log::debug!("{ARCHIVE_LIBRARY} is not supported (good for us)");
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
    }

    /// Performs a "base backup" of the cluster.
    ///
    /// Returns the directory into which the backup has been created. This is
    /// always a subdirectory of [`self.destination`].
    ///
    /// This must be performed _after_ configuring continuous archiving (see
    /// [`Backup::do_configure_archiving`]).
    pub async fn do_base_backup<'a>(
        &self,
        resource: &'a StartupResource<'a>,
    ) -> Result<PathBuf, BackupError> {
        // Temporary location into which we'll make the base backup.
        let destination_tmp = block_in_place(|| {
            TempDir::with_prefix_in(DESTINATION_DATA_PREFIX_TMP, &self.destination)
        })?;

        let args: &[&OsStr] = &[
            "--pgdata".as_ref(),
            destination_tmp.path().as_ref(),
            "--format".as_ref(),
            "plain".as_ref(),
            "--progress".as_ref(),
        ];
        let status = block_in_place(|| match resource {
            Left(resource) => resource.facet().exec(None, "pg_basebackup".as_ref(), args),
            Right(resource) => resource.facet().exec(None, "pg_basebackup".as_ref(), args),
        })?;
        if !status.success() {
            Err(status)?;
        }
        // Before calculating the target directory name or doing the actual
        // rename, take out a coordinating lock in `destination`.
        let destination_lock = block_in_place(|| {
            lock::UnlockedFile::try_from(&self.destination.join(DESTINATION_LOCK_NAME))?
                .lock_exclusive()
                .map_err(CoordinateError::UnixError)
        })?;

        // Where we're going to move the new backup to. This is always a
        // directory named `{DESTINATION_DATA_PREFIX}.NNNNNNNNNN` where
        // NNNNNNNNNN is a zero-padded integer, the next available in
        // `destination`.
        let destination_data = self.destination.join(format!(
            "{DESTINATION_DATA_PREFIX}{:010}",
            ReadDirStream::new(fs::read_dir(&self.destination).await?)
                .filter_map(Result::ok)
                .filter_map(|entry| match entry.file_name().to_str() {
                    Some(name) if name.starts_with(DESTINATION_DATA_PREFIX) =>
                        name[DESTINATION_DATA_PREFIX.len()..].parse::<u32>().ok(),
                    Some(_) | None => None,
                })
                .fold(0, Ord::max)
                .await
                + 1
        ));

        // Do the rename.
        fs::rename(&destination_tmp, &destination_data).await?;
        drop(destination_lock);

        Ok(destination_data)
    }
}

// ----------------------------------------------------------------------------

static ARCHIVE_MODE: config::Parameter = config::Parameter("archive_mode");
static ARCHIVE_COMMAND: config::Parameter = config::Parameter("archive_command");
static ARCHIVE_LIBRARY: config::Parameter = config::Parameter("archive_library");
static WAL_LEVEL: config::Parameter = config::Parameter("wal_level");

// Successful backups have this directory name prefix.
static DESTINATION_DATA_PREFIX: &str = "data.";

// In-progress backups have this directory name prefix.
static DESTINATION_DATA_PREFIX_TMP: &str = ".tmp.data.";

// Coordinating lock for working in the backup destination directory.
static DESTINATION_LOCK_NAME: &str = ".lock";

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
