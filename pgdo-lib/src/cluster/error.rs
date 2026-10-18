use std::{io, process::Output};

use crate::{cluster, runtime, util, version};

#[derive(thiserror::Error, Debug)]
pub enum ClusterError {
    #[error("Input/output error")]
    IoError(#[from] io::Error),
    #[error(
        "Cluster is PostgreSQL {0}, which is not supported; pgdo requires PostgreSQL {min} or later",
        min = runtime::MINIMUM_VERSION
    )]
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
    ClientError(#[from] cluster::client::ClientError),
    #[error(transparent)]
    ConfigError(#[from] cluster::config::ConfigError),
    #[error("Data directory {0:?} is not a PostgreSQL cluster, and is not empty: {entries}", entries = .1.join(", "))]
    DataDirNotEmpty(std::path::PathBuf, Vec<String>),
    #[error(
        "Cluster is in use on another host or VM ({hostname}); last heartbeat {}",
        super::owner::ago(*age)
    )]
    InUseElsewhere { hostname: String, age: std::time::Duration },
    #[error(
        "Cluster was last used on another host or VM ({hostname}) {}, and may still be running there",
        super::owner::ago(*age)
    )]
    StaleElsewhere { hostname: String, age: std::time::Duration },
    #[error(
        "Cluster's postmaster.pid names PID {pid}, which is not a PostgreSQL server for this cluster"
    )]
    StalePidFile { pid: u32 },
    #[error("Could not tell whether the cluster is running in another namespace: could not connect to {path}")]
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

impl ClusterError {
    /// A suggestion for how to resolve this error, if there is one. This looks
    /// into errors that this wraps too, e.g. a [`ClientError`] reported by the
    /// PostgreSQL server, which may include a hint.
    ///
    /// [`ClientError`]: cluster::client::ClientError
    pub fn help(&self) -> Option<String> {
        match self {
            Self::UnsupportedVersion(_) => Some(
                "Upgrade the cluster with `pg_upgrade`, or use an older release of pgdo to work \
                 with it."
                    .into(),
            ),
            Self::DataDirNotEmpty(..) => Some(
                "pgdo creates clusters only in empty directories (pgdo's own `pgdo.*` files \
                 excepted)."
                    .into(),
            ),
            Self::InUseElsewhere { .. } => Some(
                "pgdo cannot share a cluster between kernels, e.g. between macOS and a Linux VM. \
                 Use the cluster from there, or stop using it there first."
                    .into(),
            ),
            Self::StaleElsewhere { hostname, .. } => Some(format!(
                "If you are sure that nothing on {hostname} is using the cluster, take it over \
                 with `--force` (or `Takeover::IfStale`)."
            )),
            Self::StalePidFile { .. } => Some(
                "Another process now has that PID. If you are sure that no PostgreSQL server is \
                 using this data directory, remove postmaster.pid."
                    .into(),
            ),
            Self::ServerStateUnknown { .. } => Some(
                "If the socket's path is too long for this platform (103 bytes on macOS, 107 on \
                 Linux), use a data directory with a shorter path."
                    .into(),
            ),
            Self::ClientError(err) => err.help(),
            // Listed in full, so that a new variant must decide whether it
            // needs help.
            Self::IoError(_)
            | Self::UnsupportedRuntime(_)
            | Self::VersionError(_)
            | Self::RuntimeNotFound(_)
            | Self::RuntimeDefaultNotFound
            | Self::RuntimeError(_)
            | Self::ConfigError(_)
            | Self::Interrupted
            | Self::LockError(_)
            | Self::CommandError(_)
            | Self::CurrentUserError(_)
            | Self::UrlError(_) => None,
        }
    }
}
