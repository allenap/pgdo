use std::{io, process::Output};

use thiserror::Error;

use crate::{cluster, runtime, util, version};

#[derive(Error, Debug)]
pub enum ClusterError {
    #[error("input/output error: {0}")]
    IoError(#[from] io::Error),
    #[error("PostgreSQL version not supported: {0}")]
    UnsupportedVersion(version::Version),
    #[error("PostgreSQL version not known: {0}")]
    VersionError(#[from] version::VersionError),
    #[error("PostgreSQL runtime not found for version {0}")]
    RuntimeNotFound(version::PartialVersion),
    #[error("PostgreSQL runtime not found")]
    RuntimeDefaultNotFound,
    #[error("runtime error: {0}")]
    RuntimeError(#[from] runtime::RuntimeError),
    #[error("database error: {0}")]
    DatabaseError(#[from] cluster::postgres::Error),
    #[error("database error: {0}")]
    SqlxError(#[from] cluster::sqlx::Error),
    #[error("cluster in use; cannot lock exclusively")]
    InUse,
    #[error("external command failed: {0:?}")]
    CommandError(Output),
    #[error("current user error: {0}")]
    CurrentUserError(#[from] util::CurrentUserError),
}
