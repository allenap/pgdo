use thiserror::Error;

use crate::version;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("input/output error: {0}")]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    VersionError(#[from] version::VersionError),
}
