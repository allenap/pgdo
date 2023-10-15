use std::process::ExitCode;

use color_eyre::eyre::{Result, WrapErr};

use crate::{
    args,
    runner::{self, Runner},
};

/// Start a psql shell, creating and starting the cluster as necessary
/// (DEFAULT).
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for shell"))]
pub struct Args {
    #[clap(flatten)]
    pub cluster: args::ClusterArgs,

    #[clap(flatten)]
    pub cluster_mode: args::ClusterModeArgs,

    #[clap(flatten)]
    pub database: args::DatabaseArgs,

    #[clap(flatten)]
    pub lifecycle: args::LifecycleArgs,

    #[clap(flatten)]
    pub runtime: args::RuntimeArgs,
}

pub fn invoke(args: Args) -> Result<ExitCode> {
    let Args { cluster, cluster_mode, database, lifecycle, runtime } = args;

    runner::run(
        if lifecycle.destroy {
            Runner::RunAndDestroy
        } else {
            Runner::RunAndStop
        },
        cluster,
        cluster_mode,
        runtime,
        |cluster| {
            runner::ensure_database(cluster, &database.name)?;
            runner::check_exit(
                cluster
                    .shell(Some(&database.name))
                    .wrap_err("Starting PostgreSQL shell in cluster failed")?,
            )
        },
    )
}
