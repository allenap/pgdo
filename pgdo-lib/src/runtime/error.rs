use crate::version;

#[derive(thiserror::Error, Debug)]
pub enum RuntimeError {
    #[error("Input/output error")]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    VersionError(#[from] version::VersionError),
}
