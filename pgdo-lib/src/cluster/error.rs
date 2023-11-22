use std::{io, process::Output};

use crate::{cluster, runtime, util, version};

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub enum ClusterError {
    #[error("Input/output error")]
    IoError(#[from] io::Error),
    #[error("PostgreSQL version not supported: {0}")]
    UnsupportedVersion(version::Version),
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
    #[error("Database error")]
    SqlxError(#[from] cluster::sqlx::Error),
    #[error("Cluster in use; cannot lock exclusively")]
    InUse,
    #[error("External command failed: {0:?}")]
    CommandError(Output),
    #[error(transparent)]
    CurrentUserError(#[from] util::CurrentUserError),
    #[error("URL error")]
    UrlError(#[from] url::ParseError),
}
