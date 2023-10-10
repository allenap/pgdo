use std::{error, fmt};

/// Error parsing a PostgreSQL version number.
#[derive(Debug, PartialEq)]
pub enum VersionError {
    BadlyFormed { text: Option<String> },
    NotFound { text: Option<String> },
}

impl VersionError {
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::BadlyFormed { text: Some(text) } => Some(text.as_str()),
            Self::NotFound { text: Some(text) } => Some(text.as_str()),
            _ => None,
        }
    }
}

impl fmt::Display for VersionError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VersionError::BadlyFormed { text: Some(text) } => {
                write!(fmt, "version string {text:?} is badly formed")
            }
            VersionError::BadlyFormed { text: None } => {
                write!(fmt, "version string is badly formed")
            }
            VersionError::NotFound { text: Some(text) } => {
                write!(fmt, "version not found in {text:?}")
            }
            VersionError::NotFound { text: None } => {
                write!(fmt, "version not found")
            }
        }
    }
}

impl error::Error for VersionError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}
