use std::{
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
    process::ExitStatus,
};

use either::{Left, Right};
use tempfile::TempDir;
use tokio::{fs, task::block_in_place};
use tokio_stream::{wrappers::ReadDirStream, StreamExt};

use super::{config, resource::StartupResource};
use crate::lock;
use crate::prelude::CoordinateError;
use crate::{cluster, coordinate};

// ----------------------------------------------------------------------------

#[derive(Debug)]
pub struct Backup {
    pub backup_dir: PathBuf,
    pub backup_wal_dir: PathBuf,
}

impl Backup {
    /// Creates the destination directory and the WAL archive directory if these
    /// do not exist, and allocates a temporary location for the base backup.
    pub async fn prepare<D: AsRef<Path>>(backup_dir: D) -> Result<Self, BackupError> {
        fs::create_dir_all(&backup_dir).await?;
        let backup_dir = backup_dir.as_ref().canonicalize()?;
        let backup_wal_dir = backup_dir.join("wal");
        fs::create_dir_all(&backup_wal_dir).await?;
        Ok(Self { backup_dir, backup_wal_dir })
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
        }?;
        let mut restart: bool = false;

        // Ensure that `wal_level` is set to `replica` or `logical`. If not,
        // set it to `replica`.
        match WAL_LEVEL.get(&pool).await? {
            Some(config::Value::String(level)) if level == "replica" || level == "logical" => {
                log::debug!("{WAL_LEVEL:?} already set to {level:?}");
            }
            Some(_) => {
                log::info!("Setting {WAL_LEVEL:?} to 'replica'");
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
                log::debug!("{ARCHIVE_MODE:?} already set to {level:?}");
            }
            Some(_) => {
                log::info!("Setting {ARCHIVE_MODE:?} to 'on'");
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
                log::debug!("{ARCHIVE_LIBRARY:?} not set (good for us)");
            }
            Some(archive_library) => {
                return Err(BackupError::ConfigError(format!(
                    "{ARCHIVE_LIBRARY:?} is already set to {archive_library:?}; cannot proceed"
                )))
            }
            None => {
                log::debug!("{ARCHIVE_LIBRARY:?} is not supported (good for us)");
            }
        }

        match ARCHIVE_COMMAND.get(&pool).await? {
            Some(config::Value::String(command)) if command == archive_command => {
                log::debug!("{ARCHIVE_COMMAND:?} already set to {archive_command:?}");
            }
            // Re. "(disabled)", see `show_archive_command` in xlog.c.
            Some(config::Value::String(command))
                if command.is_empty() || command == "(disabled)" =>
            {
                log::info!("Setting {ARCHIVE_COMMAND:?} to {archive_command:?}");
                ARCHIVE_COMMAND.set(&pool, archive_command).await?;
            }
            Some(archive_command) => {
                return Err(BackupError::ConfigError(format!(
                    "{ARCHIVE_COMMAND:?} is already set to {archive_command:?}; cannot proceed"
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
    /// always a subdirectory of [`self.backup_dir`].
    ///
    /// This must be performed _after_ configuring continuous archiving (see
    /// [`Backup::do_configure_archiving`]).
    pub async fn do_base_backup<'a>(
        &self,
        resource: &'a StartupResource<'a>,
    ) -> Result<PathBuf, BackupError> {
        // Temporary location into which we'll make the base backup.
        let backup_tmp_dir =
            block_in_place(|| TempDir::with_prefix_in(BACKUP_DATA_PREFIX_TMP, &self.backup_dir))?;

        let args: &[&OsStr] = &[
            "--pgdata".as_ref(),
            backup_tmp_dir.path().as_ref(),
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
        // rename, take out a coordinating lock in `backup_dir`.
        let backup_lock = block_in_place(|| {
            lock::UnlockedFile::try_from(&self.backup_dir.join(BACKUP_LOCK_NAME))?
                .lock_exclusive()
                .map_err(CoordinateError::UnixError)
        })?;

        // Where we're going to move the new backup to. This is always a
        // directory named `{BACKUP_DATA_PREFIX}.NNNNNNNNNN` where NNNNNNNNNN is
        // a zero-padded integer, the next available in `destination`.
        let backup_data_dir = self.backup_dir.join(format!(
            "{BACKUP_DATA_PREFIX}{:010}",
            ReadDirStream::new(fs::read_dir(&self.backup_dir).await?)
                .filter_map(Result::ok)
                .filter_map(|entry| match entry.file_name().to_str() {
                    Some(name) if name.starts_with(BACKUP_DATA_PREFIX) =>
                        name[BACKUP_DATA_PREFIX.len()..].parse::<u32>().ok(),
                    Some(_) | None => None,
                })
                .fold(0, Ord::max)
                .await
                + 1
        ));

        // Do the rename.
        fs::rename(&backup_tmp_dir, &backup_data_dir).await?;
        drop(backup_lock);

        Ok(backup_data_dir)
    }
}

// ----------------------------------------------------------------------------

static ARCHIVE_MODE: config::Parameter = config::Parameter("archive_mode");
static ARCHIVE_COMMAND: config::Parameter = config::Parameter("archive_command");
static ARCHIVE_LIBRARY: config::Parameter = config::Parameter("archive_library");
static WAL_LEVEL: config::Parameter = config::Parameter("wal_level");

// Successful backups have this directory name prefix.
pub static BACKUP_DATA_PREFIX: &str = "data.";

// In-progress backups have this directory name prefix.
static BACKUP_DATA_PREFIX_TMP: &str = ".tmp.data.";

// Coordinating lock for working in the backup directory.
static BACKUP_LOCK_NAME: &str = ".lock";

// ----------------------------------------------------------------------------

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub enum BackupError {
    #[error("input/output error: {0}")]
    IoError(#[from] io::Error),
    #[error("shell error: {0}")]
    GeneralError(String),
    #[error("configuration error: {0}")]
    ConfigError(String),
    #[error(transparent)]
    CoordinateError(#[from] coordinate::CoordinateError<cluster::ClusterError>),
    #[error(transparent)]
    ClusterError(#[from] cluster::ClusterError),
    #[error("external command failed: {0:?}")]
    CommandError(ExitStatus),
    #[error("database error: {0}")]
    SqlxError(#[from] cluster::sqlx::Error),
}

impl From<ExitStatus> for BackupError {
    fn from(error: ExitStatus) -> BackupError {
        Self::CommandError(error)
    }
}
