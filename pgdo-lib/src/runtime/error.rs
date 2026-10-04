use crate::version;

#[derive(thiserror::Error, Debug)]
pub enum RuntimeError {
    #[error("input/output error")]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    VersionError(#[from] version::VersionError),
}
