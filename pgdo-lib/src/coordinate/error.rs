use std::{error, fmt};

use crate::cluster;

#[derive(Debug)]
pub enum CoordinateError {
    ClusterError(cluster::ClusterError),
}

impl fmt::Display for CoordinateError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use CoordinateError::*;
        match *self {
            ClusterError(ref e) => e.fmt(fmt),
        }
    }
}

impl error::Error for CoordinateError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::ClusterError(ref error) => Some(error),
        }
    }
}

impl From<cluster::ClusterError> for CoordinateError {
    fn from(error: cluster::ClusterError) -> CoordinateError {
        Self::ClusterError(error)
    }
}
