use std::{ffi::OsStr, path::PathBuf, process::ExitCode};

use color_eyre::eyre::{Result, WrapErr};

use crate::{args, runner};

#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for clone"))]
pub struct Args {
    #[clap(flatten)]
    pub cluster: args::ClusterArgs,

    /// The directory into which to clone the cluster.
    #[clap(long = "destination", display_order = 100)]
    pub destination: PathBuf,
}

pub fn invoke(args: Args) -> Result<ExitCode> {
    let Args { cluster, destination } = args;

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
