use std::{ffi::OsStr, path::PathBuf};

use color_eyre::eyre::{eyre, WrapErr};
use color_eyre::{Help, SectionExt};

use super::ExitResult;
use crate::{args, runner};

use pgdo::cluster::{self, config};

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
        runner::run(
            runner::Runner::RunAndStopIfExists,
            cluster,
            args::ClusterModeArgs::default(),
            args::RuntimeArgs::default(),
            backup(destination),
        )
    }
}

impl From<Backup> for super::Command {
    fn from(backup: Backup) -> Self {
        Self::Backup(backup)
    }
}

// ----------------------------------------------------------------------------

fn backup(destination: PathBuf) -> impl FnOnce(&cluster::Cluster) -> ExitResult {
    move |cluster| {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let pool = cluster.pool(None);

            // Ensure that `wal_level` is set to `replica` or `logical`. If not,
            // set it to `replica`.
            match WAL_LEVEL.get(&pool).await? {
                Some(config::Value::String(level)) if level == "replica" || level == "logical" => {}
                Some(_) => WAL_LEVEL.set(&pool, "replica").await?, // TODO: Restart server.
                None => return Err(eyre!("WAL is not supported; cannot proceed")),
            }

            // Ensure that `archive_mode` is set to `on` or `always`. If not,
            // set it to `on`.
            match ARCHIVE_MODE.get(&pool).await? {
                Some(config::Value::String(level)) if level == "on" || level == "always" => {}
                Some(_) => ARCHIVE_MODE.set(&pool, "on").await?, // TODO: Restart server.
                None => return Err(eyre!("Archiving is not supported; cannot proceed")),
            }

            match ARCHIVE_COMMAND.get(&pool).await? {
                // Re. "(disabled)", see `show_archive_command` in xlog.c.
                Some(config::Value::String(command)) if command == "(disabled)" => {}
                Some(archive_command) => {
                    return Err(eyre!("Archive command is already set; cannot proceed")
                        .with_section(|| archive_command.header("archive_command:")))
                }
                None => {}
            }
            match ARCHIVE_LIBRARY.get(&pool).await? {
                Some(config::Value::String(library)) if library.is_empty() => {}
                Some(archive_library) => {
                    return Err(eyre!("Archive library is already set; cannot proceed")
                        .with_section(|| archive_library.header("archive_command:")));
                }
                None => {}
            }

            ARCHIVE_COMMAND
                .set(&pool, "echo pgdo-archive p=%p f=%f && false")
                .await?;

            Ok(())
        })?;

        if true {
            return ExitResult::Ok(std::process::ExitCode::SUCCESS);
        }

        let args: &[&OsStr] = &[
            "--pgdata".as_ref(),
            destination.as_ref(),
            "--format".as_ref(),
            "plain".as_ref(),
            "--progress".as_ref(),
        ];
        runner::check_exit(
            cluster
                .exec(None, "pg_basebackup".as_ref(), args)
                .wrap_err("Executing command in cluster failed")?,
        )?;
        ExitResult::Ok(std::process::ExitCode::SUCCESS)
    }
}

static ARCHIVE_MODE: config::Parameter = config::Parameter("archive_mode");
static ARCHIVE_COMMAND: config::Parameter = config::Parameter("archive_command");
static ARCHIVE_LIBRARY: config::Parameter = config::Parameter("archive_library");
static WAL_LEVEL: config::Parameter = config::Parameter("wal_level");
