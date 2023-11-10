use std::{
    borrow::Cow,
    io::Write,
    path::{Path, PathBuf},
    process::ExitCode,
};

use crate::runner;

use super::ExitResult;

use pgdo::{
    cluster::{self, backup},
    coordinate::State,
};

/// Point-in-time restore/recovery from a backup made previously with the
/// `backup` command.
///
/// At present, this command only supports restoring the latest backup. In the
/// future it will be able to restoring to a specific point in time. Indeed,
/// since the `backup` command already records all the information necessary to
/// do this, it is possible to follow PostgreSQL's Point-in-Time Recovery
/// instructions to restore/recover your cluster and its data manually from a
/// backup created with the `backup` command.
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for restore"))]
pub struct Restore {
    /// The directory from which to read backups, previously created by the
    /// `backup` command.
    #[clap(long = "from", value_name = "BACKUP_DIR", display_order = 100)]
    pub backup_dir: PathBuf,

    /// The directory into which to restore. Should not exist or be empty. After
    /// the restore is complete this will be a usable cluster like any other.
    #[clap(long = "to", value_name = "RESTORE_DIR", display_order = 200)]
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

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
enum RestoreError {
    #[error("Input/output error")]
    IoError(#[from] std::io::Error),
    #[error("File copy error")]
    FileCopyError(#[from] fs_extra::error::Error),
    #[error("Cluster error")]
    ClusterError(#[from] pgdo::cluster::ClusterError),
    #[error(transparent)]
    StrategyError(#[from] runner::StrategyError),
    #[error(transparent)]
    LockForError(#[from] runner::LockForError),
    #[error("{0}")]
    Other(Cow<'static, str>),
}

impl From<&'static str> for RestoreError {
    fn from(s: &'static str) -> Self {
        Self::Other(s.into())
    }
}

impl From<String> for RestoreError {
    fn from(s: String) -> Self {
        Self::Other(s.into())
    }
}

/// Restore the latest backup into the given `resource` from `backup_dir`.
fn restore<D: AsRef<Path>>(backup_dir: D, restore_dir: D) -> Result<(), RestoreError> {
    let term = console::Term::stdout();

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
        .map(|(_, entry)| entry.path())
        .ok_or_else(|| format!("No base backup found in {backup_dir:?}"))?;

    // Check on the restore directory.
    std::fs::create_dir_all(&restore_dir)?;
    let restore_dir = restore_dir.as_ref().canonicalize()?;
    if restore_dir.read_dir()?.next().is_some() {
        Err("Restore directory is not empty")?;
    } else {
        let mut perms = restore_dir.metadata()?.permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o700);
        std::fs::set_permissions(&restore_dir, perms)?;
    }

    // Copy base backup into place.
    //
    // BUGBUG: `copy_with_progress` converts the file name to a string and
    // crashes if it doesn't convert, determining that it's an invalid file
    // name. This is a misunderstanding. The file name is valid – the OS gave it
    // to us! – but it's just not UTF-8. This is not likely to be a problem
    // though; just noting it because it's one of my pet peeves.
    {
        let progress_bar = indicatif::ProgressBar::hidden();
        progress_bar.set_draw_target(indicatif::ProgressDrawTarget::term(term.clone(), 20));
        progress_bar.set_style(
            indicatif::ProgressStyle::with_template(
                "{wide_bar} {percent}% complete; {msg}; {eta} remaining",
            )
            .expect("invalid progress bar template"),
        );
        fs_extra::dir::copy_with_progress(
            backup_data_dir,
            &restore_dir,
            &fs_extra::dir::CopyOptions::new().content_only(true),
            |progress| match progress.state {
                fs_extra::dir::TransitState::Exists => fs_extra::dir::TransitProcessResult::Abort,
                fs_extra::dir::TransitState::NoAccess => fs_extra::dir::TransitProcessResult::Abort,
                fs_extra::dir::TransitState::Normal => {
                    progress_bar.set_length(progress.total_bytes);
                    progress_bar.set_position(progress.copied_bytes);
                    progress_bar.set_message(format!(
                        "{count} of {total} copied",
                        count = indicatif::HumanBytes(progress.copied_bytes),
                        total = indicatif::HumanBytes(progress.total_bytes),
                    ));
                    fs_extra::dir::TransitProcessResult::ContinueOrAbort
                }
            },
        )?;
        progress_bar.finish_and_clear();
    }

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
    let backup_wal_dir_sh = quote_sh(backup_wal_dir)?;
    let restore_command = format!("cp {backup_wal_dir_sh}/%f %p");

    let (datadir, _lock) = runner::lock_for(&restore_dir)?;
    let strategy = runner::determine_strategy(None)?;
    let cluster = cluster::Cluster::new(datadir, strategy)?;

    // TODO: Startup via resource.
    // let resource = cluster::resource::ResourceFree::new(lock, cluster);

    if cluster.start_with_options(&[
        (ARCHIVE_MODE, "off".into()),
        (RESTORE_COMMAND, restore_command.into()),
        (RECOVERY_TARGET, "immediate".into()),
        (RECOVERY_TARGET_ACTION, "shutdown".into()),
    ])? == State::Unmodified
    {
        Err(format!(
            "Restored cluster is already running in {restore_dir:?}!"
        ))?;
    }

    // Wait for recovery to complete.
    {
        let start = std::time::Instant::now();
        let interval = std::time::Duration::from_secs(1);
        let message = "Waiting for database recovery…";
        term.write_line(message)?;
        while cluster.running()? {
            std::thread::sleep(interval);
            term.clear_last_lines(1)?;
            writeln!(
                &term,
                "{message} ({} elapsed)",
                indicatif::HumanDuration(start.elapsed())
            )?;
        }
        term.clear_last_lines(1)?;
    }

    // Remove the `recovery.signal` file in the restore so that subsequent
    // starts do not initiate database recovery.
    std::fs::remove_file(restore_dir.join("recovery.signal"))?;

    // Disable archiving.
    {
        writeln!(&term, "Disabling archiving…")?;
        cluster.start_with_options(&[(ARCHIVE_MODE, "off".into())])?;
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let pool = cluster.pool(None)?;

            write!(&term, "Resetting {ARCHIVE_MODE}…")?;
            ARCHIVE_MODE.reset(&pool).await?;
            writeln!(&term, " done.")?;

            write!(&term, "Resetting {ARCHIVE_COMMAND}…")?;
            ARCHIVE_COMMAND.reset(&pool).await?;
            writeln!(&term, " done.")?;

            write!(&term, "Resetting {ARCHIVE_LIBRARY}…")?;
            match ARCHIVE_LIBRARY.reset(&pool).await {
                Ok(_) => writeln!(&term, " done.")?,
                Err(err) => {
                    match err.as_database_error() {
                        // 42704 means UNDEFINED_OBJECT, i.e. this parameter is
                        // not supported in this version of PostgreSQL.
                        Some(err) if err.code() == Some("42704".into()) => {
                            writeln!(&term, " not supported.")?;
                            Ok(())
                        }
                        _ => Err(err),
                    }?;
                }
            };

            Ok::<_, cluster::ClusterError>(())
        })?;
        cluster.stop()?;
        writeln!(&term, "Archiving disabled in restored cluster.")?;
    }

    // Determine superusers in the restored cluster. This can help us give the
    // user more specific advice about how to start the cluster.
    let superusers = cluster::determine_superuser_role_names(&cluster)?;

    // Restore/recovery is done; give the user a hint about what next.
    let restore_dir_sh = quote_sh(&restore_dir)?;
    let title = console::style("Restore/recovery complete!")
        .bold()
        .bright()
        .white();
    let warning = console::style("WARNING").bold().yellow();
    let code = console::Style::new().bold().cyan();
    match pgdo::util::current_user() {
        Ok(user) if superusers.contains(&user) => {
            writeln!(
                &term,
                "{title} Use {} to start the cluster.",
                code.apply_to(format!("pgdo -D {restore_dir_sh}")),
            )?;
        }
        Ok(_) | Err(_) => match superusers.iter().min() {
            Some(user) => {
                let user_sh = quote_sh(user)?;
                writeln!(&term, "{title}")?;
                writeln!(&term, "{warning}: Current user does not match any superuser role in the restored cluster.")?;
                writeln!(
                    &term,
                    "Try {} to start the cluster.",
                    code.apply_to(format!("PGUSER={user_sh} pgdo -D {restore_dir_sh}")),
                )?;
            }
            None => {
                writeln!(
                    &term,
                    "{title} Use {} to start the cluster.",
                    code.apply_to(format!("pgdo -D {restore_dir_sh}")),
                )?;
                writeln!(
                    &term,
                    "WARNING: No superuser role was found in the restored cluster!"
                )?;
            }
        },
    }

    Ok(())
}

static ARCHIVE_MODE: cluster::config::Parameter = cluster::config::Parameter("archive_mode");
static ARCHIVE_COMMAND: cluster::config::Parameter = cluster::config::Parameter("archive_command");
static ARCHIVE_LIBRARY: cluster::config::Parameter = cluster::config::Parameter("archive_library");
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
