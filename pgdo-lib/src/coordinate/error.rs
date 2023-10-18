use std::{error, fmt, io};

#[derive(Debug)]
pub enum CoordinateError<C> {
    IoError(io::Error),
    UnixError(nix::Error),
    ControlError(C),
    DoesNotExist,
}

impl<C> fmt::Display for CoordinateError<C>
where
    C: fmt::Display,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::IoError(ref e) => write!(fmt, "input/output error: {e}"),
            Self::UnixError(ref e) => write!(fmt, "UNIX error: {e}"),
            Self::ControlError(ref e) => e.fmt(fmt),
            Self::DoesNotExist => write!(fmt, "cluster does not exist"),
        }
    }
}

impl<C> error::Error for CoordinateError<C>
where
    C: error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::IoError(ref error) => Some(error),
            Self::UnixError(ref error) => Some(error),
            Self::ControlError(ref error) => Some(error),
            Self::DoesNotExist => None,
        }
    }
}

impl<C> From<io::Error> for CoordinateError<C> {
    fn from(error: io::Error) -> CoordinateError<C> {
        Self::IoError(error)
    }
}

impl<C> From<nix::Error> for CoordinateError<C> {
    fn from(error: nix::Error) -> CoordinateError<C> {
        Self::UnixError(error)
    }
}
