use std::{ffi::OsString, process::ExitCode};

use color_eyre::eyre::{Result, WrapErr};

use crate::{
    args,
    runner::{self, Runner},
};

#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for exec"))]
pub struct Args {
    #[clap(flatten)]
    pub cluster: args::ClusterArgs,

    #[clap(flatten)]
    pub database: args::DatabaseArgs,

    #[clap(flatten)]
    pub lifecycle: args::LifecycleArgs,

    #[clap(flatten)]
    pub runtime: args::RuntimeArgs,

    /// The executable to invoke. By default it will start a shell.
    #[clap(env = "SHELL", value_name = "COMMAND", display_order = 999)]
    pub command: OsString,

    /// Arguments to pass to the executable.
    #[clap(value_name = "ARGUMENTS", display_order = 1000)]
    pub args: Vec<OsString>,
}

pub fn invoke(args: Args) -> Result<ExitCode> {
    let Args { cluster, database, command, args, lifecycle, runtime } = args;

    runner::run(
        cluster.dir,
        &database.name,
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
                    .exec(&database.name, command, &args)
                    .wrap_err("Executing command in cluster failed")?,
            )
        },
    )
}