use std::{ffi::OsStr, path::PathBuf, process::ExitCode};

use color_eyre::eyre::{Result, WrapErr};

use crate::{args, runner};

#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for clone"))]
pub struct Args {
    #[clap(flatten)]
    pub cluster: args::ClusterArgs,

    #[clap(flatten)]
    pub runtime: args::RuntimeArgs,

    /// The directory into which to clone the cluster.
    #[clap(short = 'd', long = "destination")]
    pub destination: PathBuf,
}

pub fn invoke(args: Args) -> Result<ExitCode> {
    let Args { cluster, runtime, destination } = args;

    let args: &[&OsStr] = &[
        "--pgdata".as_ref(),
        destination.as_ref(),
        "--format".as_ref(),
        "plain".as_ref(),
        "--progress".as_ref(),
    ];

    runner::run(
        cluster.dir,
        "template1", // TODO: Make `run` allow for "no database".
        runner::determine_strategy(runtime.fallback)?,
        runner::Runner::RunAndStopIfExists,
        runner::initialise(None),
        |cluster| {
            runner::check_exit(
                cluster
                    .exec("template1", "pg_basebackup".as_ref(), args)
                    .wrap_err("Executing command in cluster failed")?,
            )
        },
    )
}