use std::{ffi::OsStr, path::PathBuf};

use color_eyre::eyre::{eyre, Result, WrapErr};
use color_eyre::{Help, SectionExt};

use super::ExitResult;
use crate::{args, runner};

use pgdo::cluster;

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
        let mut conn = cluster.connect(None)?;
        let archive_settings = ArchiveSettings::query(&mut conn)?;

        if let Some(archive_command) = archive_settings.archive_command {
            return Err(eyre!("Archive command is already set; cannot proceed")
                .with_section(|| archive_command.header("archive_command:")));
        }
        if let Some(archive_library) = archive_settings.archive_library {
            return Err(eyre!("Archive library is already set; cannot proceed")
                .with_section(|| archive_library.header("archive_command:")));
        }

        dbg!(archive_settings);

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

#[derive(Debug, Default)]
struct ArchiveSettings {
    archive_mode: Option<String>,
    archive_command: Option<String>,
    archive_library: Option<String>,
    wal_level: Option<String>,
}

impl ArchiveSettings {
    fn query(conn: &mut cluster::postgres::Client) -> Result<Self, cluster::ClusterError> {
        let results = conn.query(
            r"
                select
                    name,
                    nullif(
                        -- See `show_archive_command` in xlog.c.
                        nullif(setting, '(disabled)')
                        , ''
                    ) setting
                from
                    pg_settings
                where
                    name in (
                        'archive_mode',
                        'archive_command',
                        'archive_library',
                        'wal_level'
                    )
            ",
            &[],
        )?;
        let mut settings = Self::default();
        for row in results {
            let name: String = row.try_get("name")?;
            let value: Option<String> = row.try_get("setting")?;
            match name.as_ref() {
                "archive_mode" => settings.archive_mode = value,
                "archive_command" => settings.archive_command = value,
                "archive_library" => settings.archive_library = value,
                "wal_level" => settings.wal_level = value,
                _ => unreachable!(),
            }
        }
        Ok(settings)
    }
}
