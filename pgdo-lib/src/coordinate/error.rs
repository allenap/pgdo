#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub enum CoordinateError<C>
where
    C: std::error::Error,
{
    #[error("input/output error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("UNIX error: {0}")]
    UnixError(#[from] nix::Error),
    #[error(transparent)]
    ControlError(C),
    #[error("cluster does not exist")]
    DoesNotExist,
}
