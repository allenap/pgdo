use std::{error, fmt, io};

use crate::cluster;

#[derive(Debug)]
pub enum CoordinateError {
    IoError(io::Error),
    UnixError(nix::Error),
    ClusterError(cluster::ClusterError),
    ClusterDoesNotExist,
}

impl fmt::Display for CoordinateError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::IoError(ref e) => write!(fmt, "input/output error: {e}"),
            Self::UnixError(ref e) => write!(fmt, "UNIX error: {e}"),
            Self::ClusterError(ref e) => e.fmt(fmt),
            Self::ClusterDoesNotExist => write!(fmt, "cluster does not exist"),
        }
    }
}

impl error::Error for CoordinateError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::IoError(ref error) => Some(error),
            Self::UnixError(ref error) => Some(error),
            Self::ClusterError(ref error) => Some(error),
            Self::ClusterDoesNotExist => None,
        }
    }
}

impl From<io::Error> for CoordinateError {
    fn from(error: io::Error) -> CoordinateError {
        Self::IoError(error)
    }
}

impl From<nix::Error> for CoordinateError {
    fn from(error: nix::Error) -> CoordinateError {
        Self::UnixError(error)
    }
}

impl From<cluster::ClusterError> for CoordinateError {
    fn from(error: cluster::ClusterError) -> CoordinateError {
        Self::ClusterError(error)
    }
}
