use color_eyre::eyre::WrapErr;

use super::Result;
use crate::{
    args,
    runner::{self, Runner},
};

/// Start a psql shell, creating and starting the cluster as necessary
/// (DEFAULT).
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for shell"))]
pub struct Shell {
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

impl Shell {
    pub fn invoke(self) -> Result {
        let Self { cluster, cluster_mode, database, lifecycle, runtime } = self;
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
}

impl From<Shell> for super::Command {
    fn from(shell: Shell) -> Self {
        Self::Shell(shell)
    }
}
