use std::{io, process::Output};

use crate::{cluster, runtime, util, version};

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub enum ClusterError {
    #[error("Input/output error")]
    IoError(#[from] io::Error),
    #[error(
        "Cluster is PostgreSQL {0}, which is not supported; pgdo requires PostgreSQL {min} or later",
        min = runtime::MINIMUM_VERSION
    )]
    #[diagnostic(help(
        "Upgrade the cluster with `pg_upgrade`, or use an older release of pgdo to work with it"
    ))]
    UnsupportedVersion(version::PartialVersion),
    #[error(
        "PostgreSQL runtime {0} is not supported; pgdo requires PostgreSQL {min} or later",
        min = runtime::MINIMUM_VERSION
    )]
    UnsupportedRuntime(version::Version),
    #[error("PostgreSQL version not known")]
    VersionError(#[from] version::VersionError),
    #[error("PostgreSQL runtime not found for version {0}")]
    RuntimeNotFound(version::PartialVersion),
    #[error("PostgreSQL runtime not found")]
    RuntimeDefaultNotFound,
    #[error("Runtime error")]
    RuntimeError(#[from] runtime::RuntimeError),
    #[error(transparent)]
    #[diagnostic(transparent)]
    ClientError(#[from] cluster::client::ClientError),
    #[error(transparent)]
    #[diagnostic(transparent)]
    ConfigError(#[from] cluster::config::ConfigError),
    #[error("Data directory {0:?} is not a PostgreSQL cluster, and is not empty: {entries}", entries = .1.join(", "))]
    #[diagnostic(help(
        "pgdo creates clusters only in empty directories (pgdo's own `pgdo.*` files excepted)"
    ))]
    DataDirNotEmpty(std::path::PathBuf, Vec<String>),
    #[error(
        "Cluster is in use on another host or VM ({hostname}); last heartbeat {}",
        super::owner::ago(*age)
    )]
    #[diagnostic(help(
        "pgdo cannot share a cluster between kernels, e.g. between macOS and a Linux VM. \
         Use the cluster from there, or stop using it there first."
    ))]
    InUseElsewhere { hostname: String, age: std::time::Duration },
    #[error(
        "Cluster was last used on another host or VM ({hostname}) {}, and may still be running there",
        super::owner::ago(*age)
    )]
    #[diagnostic(help(
        "If you are sure that nothing on {hostname} is using the cluster, take it over with \
         `--force` (or `Takeover::IfStale`)."
    ))]
    StaleElsewhere { hostname: String, age: std::time::Duration },
    #[error(
        "Cluster's postmaster.pid names PID {pid}, which is not a PostgreSQL server for this cluster"
    )]
    #[diagnostic(help(
        "Another process now has that PID. If you are sure that no PostgreSQL server is using \
         this data directory, remove postmaster.pid."
    ))]
    StalePidFile { pid: u32 },
    #[error("Could not tell whether the cluster is running in another namespace: could not connect to {path}")]
    #[diagnostic(help(
        "If the socket's path is too long for this platform (103 bytes on macOS, 107 on Linux), \
         use a data directory with a shorter path."
    ))]
    ServerStateUnknown {
        path: std::path::PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Interrupted while starting a session with the cluster")]
    Interrupted,
    #[error("Could not lock cluster")]
    LockError(#[from] nix::Error),
    #[error("External command failed: {0:?}")]
    CommandError(Output),
    #[error(transparent)]
    CurrentUserError(#[from] util::CurrentUserError),
    #[error("URL error")]
    UrlError(#[from] url::ParseError),
}
