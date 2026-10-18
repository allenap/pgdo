use std::path::PathBuf;

use clap::Args;

use pgdo::cluster::DATABASE_POSTGRES;
use pgdo::runtime::constraint::Constraint;

#[derive(Args, Debug, Default)]
pub struct ClusterArgs {
    /// The directory in which the cluster lives.
    #[clap(
        long_help,
        short = 'D',
        long = "datadir",
        env = "PGDATA",
        value_name = "PGDATA",
        default_value = "pgdo.cluster",
        display_order = 1
    )]
    pub dir: PathBuf,
}

#[derive(Args, Debug, Default)]
pub struct ClusterModeArgs {
    /// Run the cluster in a "safer" or "faster" mode.
    ///
    /// DANGER! Choosing "faster-but-less-safe" makes the cluster faster but it
    /// can lead to unrecoverable data corruption in the event of a power
    /// failure or system crash.
    ///
    /// The mode is STICKY. Running with a mode reconfigures the cluster, and it
    /// will continue to run in that mode. To find out which mode the cluster is
    /// configured for, open a `psql` shell (e.g. `pgdo shell`) and run `SHOW
    /// fsync; SHOW full_page_writes; SHOW synchronous_commit;`.
    #[clap(long = "mode", display_order = 4)]
    pub mode: Option<ClusterMode>,
}

#[derive(Args, Debug, Default)]
pub struct DatabaseArgs {
    /// The database to connect to.
    #[clap(
        long_help,
        short = 'd',
        long = "database",
        env = "PGDATABASE",
        value_name = "PGDATABASE",
        default_value = DATABASE_POSTGRES,
        display_order = 2
    )]
    pub name: String,
}

#[derive(Args, Debug, Default)]
pub struct RuntimeArgs {
    /// Select the default runtime, used when creating new clusters.
    #[clap(
        long_help,
        long = "runtime-default",
        value_name = "CONSTRAINT",
        display_order = 80
    )]
    pub fallback: Option<Constraint>,
}

#[derive(Args, Debug, Default)]
pub struct LifecycleArgs {
    /// Destroy the cluster after use. WARNING: This will DELETE THE DATA
    /// DIRECTORY. The default is to NOT destroy the cluster.
    #[clap(long_help, long = "destroy", display_order = 100)]
    pub destroy: bool,

    /// Take over a cluster last used on another host or VM, if it shows no
    /// signs of use there when watched for 30 seconds. DANGER: If it is in fact
    /// still running there, this WILL CORRUPT THE CLUSTER. The default is to
    /// refuse.
    #[clap(long_help, long = "force", display_order = 101)]
    pub force: bool,
}

impl LifecycleArgs {
    /// Whether to take over a cluster owned by another kernel.
    pub fn takeover(&self) -> pgdo::cluster::Takeover {
        if self.force {
            pgdo::cluster::Takeover::IfStale
        } else {
            pgdo::cluster::Takeover::Never
        }
    }

    /// What to do with the cluster when finished with it.
    pub fn finish(&self) -> pgdo::cluster::Finish {
        if self.destroy {
            pgdo::cluster::Finish::Destroy
        } else {
            pgdo::cluster::Finish::Stop
        }
    }
}

// ----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub enum ClusterMode {
    /// Resets fsync, full_page_writes, and synchronous_commit to defaults.
    #[value(name = "slower-but-safer", alias = "safe", verbatim_doc_comment)]
    Slow,

    /// Disable fsync, full_page_writes, and synchronous_commit. DANGER!
    #[value(name = "faster-but-less-safe", alias = "fast", verbatim_doc_comment)]
    Fast,
}

#[cfg(test)]
mod tests {
    use super::ClusterModeArgs;

    #[test]
    fn test_cluster_mode_args_default() {
        let default = ClusterModeArgs::default();
        assert_eq!(default.mode, None);
    }
}
