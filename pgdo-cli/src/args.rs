use std::path::PathBuf;

use clap::Args;

use pgdo::runtime::constraint::Constraint;

#[derive(Args, Debug, Default)]
pub struct ClusterArgs {
    /// The directory in which the cluster lives.
    #[clap(
        short = 'D',
        long = "datadir",
        env = "PGDATA",
        value_name = "PGDATA",
        default_value = "cluster",
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
        short = 'd',
        long = "database",
        env = "PGDATABASE",
        value_name = "PGDATABASE",
        default_value = "postgres",
        display_order = 2
    )]
    pub name: String,
}

#[derive(Args, Debug, Default)]
pub struct RuntimeArgs {
    /// Select the default runtime, used when creating new clusters.
    #[clap(
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
    #[clap(long = "destroy", display_order = 100)]
    pub destroy: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub enum ClusterMode {
    /// Resets fsync, full_page_writes, and synchronous_commit to defaults.
    #[value(name = "slower-but-safer", alias = "safe")]
    Slow,

    /// Disable fsync, full_page_writes, and synchronous_commit. DANGER!
    #[value(name = "faster-but-less-safe", alias = "fast")]
    Fast,
}

impl Default for ClusterMode {
    fn default() -> Self {
        ClusterMode::Slow
    }
}
