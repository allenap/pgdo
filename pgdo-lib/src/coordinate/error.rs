#[derive(thiserror::Error, miette::Diagnostic, Debug)]
pub enum CoordinateError<C>
where
    C: std::error::Error,
{
    #[error("Input/output error")]
    IoError(#[from] std::io::Error),
    #[error("UNIX error")]
    UnixError(#[from] nix::Error),
    #[error(transparent)]
    ControlError(C),
    #[error("Subject does not exist")]
    DoesNotExist,
}
