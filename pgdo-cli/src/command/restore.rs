use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

use color_eyre::eyre::eyre;
// use color_eyre::{Help, SectionExt};

use super::ExitResult;

use pgdo::cluster::backup;

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

/// Restore the latest backup into the given `resource` from `backup_dir`.
fn restore<D: AsRef<Path>>(backup_dir: D, restore_dir: D) -> color_eyre::Result<()> {
    let backup_dir = backup_dir.as_ref().canonicalize()?;

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
        None => {
            return Err(eyre!("No base backup found in {backup_dir:?}"));
        }
    };

    // Check on the restore directory.
    std::fs::create_dir_all(&restore_dir)?;
    let restore_dir = restore_dir.as_ref().canonicalize()?;
    // if restore_dir.read_dir()?.next().is_some() {
    //     return Err(eyre!("Restore directory is not empty"));
    // }

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
                    "\r{}/{} bytes copied; {}% complete",
                    progress.copied_bytes,
                    progress.total_bytes,
                    progress.copied_bytes * 100 / progress.total_bytes,
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

    // Start up the cluster with `restore_command = some/command` and
    // `recovery_target_action = "shutdown"` (or "pause" if we want to
    // interactively inspect the cluster).

    // let (datadir, lock) = runner::lock_for(cluster.dir)?;
    // let strategy = runner::determine_strategy(None)?;
    // let cluster = cluster::Cluster::new(datadir, strategy)?;
    // let resource = resource::ResourceFree::new(lock, cluster);

    Ok(())
}

// ----------------------------------------------------------------------------

// fn quote_sh<P: AsRef<Path>>(path: P) -> color_eyre::Result<String> {
//     let path = path.as_ref();
//     shell_quote::sh::quote(path)
//         .to_str()
//         .map(str::to_owned)
//         .ok_or_else(|| {
//             eyre!("Cannot shell escape given path")
//                 .with_section(|| format!("{path:?}").header("Path:"))
//         })
// }
