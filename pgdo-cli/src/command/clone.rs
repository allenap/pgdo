use std::{ffi::OsStr, path::PathBuf};

use color_eyre::eyre::WrapErr;

use super::ExitResult;
use crate::{args, runner};

/// Perform a one-off clone/backup of an existing cluster.
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for clone"))]
pub struct Clone {
    #[clap(flatten)]
    pub cluster: args::ClusterArgs,

    /// The directory into which to clone the cluster.
    #[clap(long = "destination", display_order = 100)]
    pub destination: PathBuf,
}

impl Clone {
    pub fn invoke(self) -> ExitResult {
        let Self { cluster, destination } = self;
        let args: &[&OsStr] = &[
            "--pgdata".as_ref(),
            destination.as_ref(),
            "--format".as_ref(),
            "plain".as_ref(),
            "--progress".as_ref(),
        ];
        runner::run(
            runner::Runner::RunAndStopIfExists,
            cluster,
            args::ClusterModeArgs::default(),
            args::RuntimeArgs::default(),
            |cluster| {
                runner::check_exit(
                    cluster
                        .exec(None, "pg_basebackup".as_ref(), args)
                        .wrap_err("Executing command in cluster failed")?,
                )
            },
        )
    }
}

impl From<Clone> for super::Command {
    fn from(clone: Clone) -> Self {
        Self::Clone(clone)
    }
}
