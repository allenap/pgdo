use std::process::Output;
use std::{error, fmt, io};

use crate::{cluster, runtime, version};

#[derive(Debug)]
pub enum ClusterError {
    IoError(io::Error),
    UnixError(nix::Error),
    UnsupportedVersion(version::Version),
    VersionError(version::VersionError),
    RuntimeNotFound(version::PartialVersion),
    RuntimeDefaultNotFound,
    RuntimeError(runtime::RuntimeError),
    DatabaseError(cluster::postgres::Error),
    SqlxError(sqlx::Error),
    InUse, // Cluster is already in use; cannot lock exclusively.
    CommandError(Output),
}

impl fmt::Display for ClusterError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use ClusterError::*;
        match *self {
            IoError(ref e) => write!(fmt, "input/output error: {e}"),
            UnixError(ref e) => write!(fmt, "UNIX error: {e}"),
            UnsupportedVersion(ref e) => write!(fmt, "PostgreSQL version not supported: {e}"),
            VersionError(ref e) => write!(fmt, "PostgreSQL version not known: {e}"),
            RuntimeNotFound(ref v) => write!(fmt, "PostgreSQL runtime not found for version {v}"),
            RuntimeDefaultNotFound => write!(fmt, "PostgreSQL runtime not found"),
            RuntimeError(ref e) => write!(fmt, "runtime error: {e}"),
            DatabaseError(ref e) => write!(fmt, "database error: {e}"),
            SqlxError(ref e) => write!(fmt, "database error: {e}"),
            InUse => write!(fmt, "cluster in use; cannot lock exclusively"),
            CommandError(ref e) => write!(fmt, "external command failed: {e:?}"),
        }
    }
}

impl error::Error for ClusterError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::IoError(ref error) => Some(error),
            Self::UnixError(ref error) => Some(error),
            Self::UnsupportedVersion(_) => None,
            Self::VersionError(ref error) => Some(error),
            Self::RuntimeNotFound(_) => None,
            Self::RuntimeDefaultNotFound => None,
            Self::RuntimeError(ref error) => Some(error),
            Self::DatabaseError(ref error) => Some(error),
            Self::SqlxError(ref error) => Some(error),
            Self::InUse => None,
            Self::CommandError(_) => None,
        }
    }
}

impl From<io::Error> for ClusterError {
    fn from(error: io::Error) -> ClusterError {
        Self::IoError(error)
    }
}

impl From<nix::Error> for ClusterError {
    fn from(error: nix::Error) -> ClusterError {
        Self::UnixError(error)
    }
}

impl From<version::VersionError> for ClusterError {
    fn from(error: version::VersionError) -> ClusterError {
        Self::VersionError(error)
    }
}

impl From<postgres::error::Error> for ClusterError {
    fn from(error: postgres::error::Error) -> ClusterError {
        Self::DatabaseError(error)
    }
}

impl From<sqlx::Error> for ClusterError {
    fn from(error: sqlx::Error) -> ClusterError {
        Self::SqlxError(error)
    }
}

impl From<runtime::RuntimeError> for ClusterError {
    fn from(error: runtime::RuntimeError) -> ClusterError {
        // We could convert `RuntimeError::{IoError, VersionError}` into
        // `Self::{IoError, VersionError}`, but for now we leave the
        // chain of errors intact.
        Self::RuntimeError(error)
    }
}
