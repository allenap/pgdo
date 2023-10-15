use std::ffi::OsString;

use color_eyre::eyre::WrapErr;

use super::ExitResult;
use crate::{
    args,
    runner::{self, Runner},
};

/// Execute an arbitrary command, creating and starting the cluster as
/// necessary.
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for exec"))]
pub struct Exec {
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

    /// The executable to invoke. By default it will start a shell.
    #[clap(env = "SHELL", value_name = "COMMAND", display_order = 999)]
    pub command: OsString,

    /// Arguments to pass to the executable.
    #[clap(value_name = "ARGUMENTS", display_order = 1000)]
    pub args: Vec<OsString>,
}

impl Exec {
    pub fn invoke(self) -> ExitResult {
        let Self {
            cluster,
            cluster_mode,
            database,
            command,
            args,
            lifecycle,
            runtime,
        } = self;
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
                        .exec(Some(&database.name), command, &args)
                        .wrap_err("Executing command in cluster failed")?,
                )
            },
        )
    }
}

impl From<Exec> for super::Command {
    fn from(exec: Exec) -> Self {
        Self::Exec(exec)
    }
}
