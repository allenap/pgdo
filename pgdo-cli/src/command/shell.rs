use std::process::ExitCode;

use color_eyre::eyre::{Result, WrapErr};

use crate::{
    args,
    runner::{self, Runner},
};

#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for shell"))]
pub struct Args {
    #[clap(flatten)]
    pub cluster: args::ClusterArgs,

    #[clap(flatten)]
    pub database: args::DatabaseArgs,

    #[clap(flatten)]
    pub lifecycle: args::LifecycleArgs,

    #[clap(flatten)]
    pub runtime: args::RuntimeArgs,
}

pub fn invoke(args: Args) -> Result<ExitCode> {
    let Args { cluster, database, lifecycle, runtime } = args;
    runner::run(
        cluster.dir,
        Some(&database.name),
        runner::determine_strategy(runtime.fallback)?,
        if lifecycle.destroy {
            Runner::RunAndDestroy
        } else {
            Runner::RunAndStop
        },
        runner::initialise(cluster.mode),
        |cluster| {
            runner::check_exit(
                cluster
                    .shell(Some(&database.name))
                    .wrap_err("Starting PostgreSQL shell in cluster failed")?,
            )
        },
    )
}
