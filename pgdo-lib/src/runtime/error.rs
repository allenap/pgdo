use crate::version;

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub enum RuntimeError {
    #[error("input/output error: {0}")]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    VersionError(#[from] version::VersionError),
}
