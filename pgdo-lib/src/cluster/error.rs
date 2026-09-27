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
    #[error("Database error")]
    DatabaseError(#[from] cluster::postgres::Error),
    #[error(transparent)]
    #[diagnostic(transparent)]
    ConfigError(#[from] cluster::config::ConfigError),
    #[error("Could not lock cluster")]
    LockError(#[from] nix::Error),
    #[error("External command failed: {0:?}")]
    CommandError(Output),
    #[error(transparent)]
    CurrentUserError(#[from] util::CurrentUserError),
    #[error("URL error")]
    UrlError(#[from] url::ParseError),
}
