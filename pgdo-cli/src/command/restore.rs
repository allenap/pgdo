use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

use color_eyre::eyre::eyre;

use crate::runner;

use super::ExitResult;

use pgdo::{
    cluster::{self, backup},
    coordinate::State,
};

/// Point-in-time restore/recovery from a backup made previously with the
/// `backup` command.
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for restore"))]
pub struct Restore {
    /// The directory from which to read backups.
    #[clap(long = "from", value_name = "DIR", display_order = 100)]
    pub backup_dir: PathBuf,

    /// The directory into which to restore.
    #[clap(long = "to", value_name = "DIR", display_order = 200)]
    pub restore_dir: PathBuf,
}

impl Restore {
    pub fn invoke(self) -> ExitResult {
        let Self { backup_dir, restore_dir } = self;
        restore(backup_dir, restore_dir)?;
        Ok(ExitCode::SUCCESS)
    }
}

impl From<Restore> for super::Command {
    fn from(restore: Restore) -> Self {
        Self::Restore(restore)
    }
}

// ----------------------------------------------------------------------------

#[derive(thiserror::Error, Debug)]
enum RestoreError {
    #[error("input/output error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("file copy error: {0}")]
    FileCopyError(#[from] fs_extra::error::Error),
    #[error("cluster error: {0}")]
    ClusterError(#[from] pgdo::cluster::ClusterError),
    #[error(transparent)]
    Other(#[from] color_eyre::Report),
}

impl From<&'static str> for RestoreError {
    fn from(s: &'static str) -> Self {
        Self::Other(eyre!(s))
    }
}

impl From<String> for RestoreError {
    fn from(s: String) -> Self {
        Self::Other(eyre!(s))
    }
}

/// Restore the latest backup into the given `resource` from `backup_dir`.
fn restore<D: AsRef<Path>>(backup_dir: D, restore_dir: D) -> Result<(), RestoreError> {
    let backup_dir = backup_dir.as_ref().canonicalize()?;
    let backup_wal_dir = backup_dir.join("wal");

    // Find latest backup.
    let backup_data_dir = backup_dir
        .read_dir()?
        .filter_map(|entry| entry.ok()) // Ignore errors.
        .filter_map(|entry| match entry.file_name().to_str() {
            Some(name) if name.starts_with(backup::BACKUP_DATA_PREFIX) => name
                [backup::BACKUP_DATA_PREFIX.len()..]
                .parse::<u32>()
                .ok()
                .map(|n| (n, entry)),
            Some(_) | None => None,
        })
        .max_by_key(|(n, _)| *n)
        .map(|(_, entry)| entry.path());
    let backup_data_dir = match backup_data_dir {
        Some(backup_data_dir) => backup_data_dir,
        None => return Err(format!("no base backup found in {backup_dir:?}"))?,
    };

    // Check on the restore directory.
    std::fs::create_dir_all(&restore_dir)?;
    let restore_dir = restore_dir.as_ref().canonicalize()?;
    if restore_dir.read_dir()?.next().is_some() {
        Err("Restore directory is not empty")?;
    }
    let mut perms = restore_dir.metadata()?.permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o700);
    std::fs::set_permissions(&restore_dir, perms)?;

    // Copy base backup into place.
    //
    // BUGBUG: `copy_with_progress` converts the file name to a string and
    // crashes if it doesn't convert, determining that it's an invalid file
    // name. This is a misunderstanding. The file name is valid – the OS gave it
    // to us! – but it's just not UTF-8. This is not likely to be a problem
    // though; just noting it because it's one of my pet peeves.
    fs_extra::dir::copy_with_progress(
        backup_data_dir,
        &restore_dir,
        &fs_extra::dir::CopyOptions::new().content_only(true),
        |progress| match progress.state {
            fs_extra::dir::TransitState::Exists => fs_extra::dir::TransitProcessResult::Abort,
            fs_extra::dir::TransitState::NoAccess => fs_extra::dir::TransitProcessResult::Abort,
            fs_extra::dir::TransitState::Normal => {
                print!(
                    "\r{count}/{total} bytes copied; {pct}% complete",
                    pct = percent(progress.copied_bytes, progress.total_bytes).unwrap_or_default(),
                    count = progress.copied_bytes,
                    total = progress.total_bytes,
                );
                fs_extra::dir::TransitProcessResult::ContinueOrAbort
            }
        },
    )?;
    // Clear the line. Hacky.
    print!("\r                                                               \r");

    // Remove WAL from restored backup.
    let restore_wal_dir = restore_dir.join("pg_wal");
    restore_wal_dir.read_dir()?.try_for_each(|entry| {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
        Ok::<_, std::io::Error>(())
    })?;

    // Create the `recovery.signal` file in the restore.
    std::fs::write(restore_dir.join("recovery.signal"), "")?;

    // Start up the cluster with `restore_command = some/command` and
    // `recovery_target_action = "shutdown"` (or "pause" if we want to
    // interactively inspect the cluster).
    let backup_wal_dir_shell = quote_sh(backup_wal_dir)?;
    let restore_command = format!("cp {backup_wal_dir_shell}/%f %p");

    let (datadir, _lock) = runner::lock_for(&restore_dir)?;
    let strategy = runner::determine_strategy(None)?;
    let cluster = cluster::Cluster::new(datadir, strategy)?;

    // TODO: Startup via resource.
    // let resource = cluster::resource::ResourceFree::new(lock, cluster);

    if cluster.start_with_options(&[
        (RESTORE_COMMAND, restore_command.into()),
        (RECOVERY_TARGET, "immediate".into()),
        (RECOVERY_TARGET_ACTION, "shutdown".into()),
    ])? == State::Unmodified
    {
        Err(format!(
            "Restored cluster is already running in {restore_dir:?}!"
        ))?;
    }

    let start = std::time::Instant::now();
    while cluster.running()? {
        std::thread::sleep(std::time::Duration::from_millis(2000));
        print!(
            "\rWaiting for restore to complete… ({} seconds elapsed)",
            start.elapsed().as_secs()
        );
    }
    // Clear the line. Hacky.
    print!("\r                                                               \r");

    // Remove the `recovery.signal` file in the restore.
    std::fs::remove_file(restore_dir.join("recovery.signal"))?;

    let restore_dir_sh = quote_sh(&restore_dir)?;

    // Determine superusers in the restored cluster. This can help us give the
    // user more specific advice about how to start the cluster.
    let superusers = cluster::determine_superuser_role_names(&cluster)?;
    match pgdo::util::current_user() {
        Ok(user) if superusers.contains(&user) => {
            println!("Restore complete!");
            println!("Use `pgdo -D {restore_dir_sh}` to start the cluster.");
        }
        Ok(_) | Err(_) => match superusers.iter().min() {
            Some(user) => {
                let user_sh = quote_sh(user)?;
                println!("Restore complete!");
                println!("WARNING: Current user does not match any superuser role in the restored cluster.");
                println!("Try `PGUSER={user_sh} pgdo -D {restore_dir_sh}` to start the cluster.");
            }
            None => {
                println!("Restore complete! Use `pgdo -D {restore_dir_sh}` to start the cluster.");
                println!("WARNING: No superuser role was found in the restored cluster!");
            }
        },
    }

    Ok(())
}

static RESTORE_COMMAND: cluster::config::Parameter = cluster::config::Parameter("restore_command");
static RECOVERY_TARGET: cluster::config::Parameter = cluster::config::Parameter("recovery_target");
static RECOVERY_TARGET_ACTION: cluster::config::Parameter =
    cluster::config::Parameter("recovery_target_action");

// ----------------------------------------------------------------------------

fn quote_sh<S: AsRef<std::ffi::OsStr>>(string: S) -> Result<String, String> {
    let string = string.as_ref();
    shell_quote::sh::quote(string)
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("Cannot shell escape given string: {string:?}"))
}

/// Calculate `numerator` divided by `denominator` as a percentage.
///
/// When `numerator` is very large we cannot multiply it by 100 without risking
/// wrapping, so this is careful to use checked arithmetic to avoid wrapping or
/// overflow. It scales down `numerator` and `denominator` by powers of two
/// until a percentage can be calculated. If `denominator` is zero, returns
/// `None`.
fn percent(numerator: u64, denominator: u64) -> Option<u64> {
    (0..=100u8.ilog2()).find_map(|n| {
        (numerator >> n)
            .checked_mul(100)
            .and_then(|numerator| numerator.checked_div(denominator >> n))
    })
}
