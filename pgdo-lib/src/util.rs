use std::env;
use std::ffi::OsString;
use std::path::Path;

use thiserror::Error;

type PrependedPath = Result<OsString, env::JoinPathsError>;

/// Prepend the given `dir` to the given `path`.
///
/// If `dir` is already in `path` it is moved to first place. Note that this does
/// *not* update `PATH` in the environment.
pub(crate) fn prepend_to_path(dir: &Path, path: Option<OsString>) -> PrependedPath {
    Ok(match path {
        None => env::join_paths([dir])?,
        Some(path) => {
            let mut paths = vec![dir.to_path_buf()];
            paths.extend(env::split_paths(&path).filter(|path| path != dir));
            env::join_paths(paths)?
        }
    })
}

#[derive(Error, Debug)]
pub enum CurrentUserError {
    #[error("user name in {0:?} environment variable cannot be decoded: {1:?}")]
    NotUnicode(&'static str, std::ffi::OsString),
    #[error("system error: {0}")]
    System(#[from] nix::Error),
    #[error("user unknown")]
    Unknown,
}

/// Determine the current user name to use.
///
/// Checks the `PGUSER` then `USER` environment variables first, which allows
/// the invoking user to override the current user name. If those are not set,
/// it obtains the user name from the OS.
pub fn current_user() -> Result<String, CurrentUserError> {
    use nix::unistd::{getuid, User};
    use std::env::{var, VarError::*};
    match var("PGUSER") {
        Ok(user) if !user.trim().is_empty() => Ok(user),
        Err(NotUnicode(value)) => Err(CurrentUserError::NotUnicode("PGUSER", value)),
        Ok(_) | Err(NotPresent) => match var("USER") {
            Ok(user) if !user.trim().is_empty() => Ok(user),
            Err(NotUnicode(value)) => Err(CurrentUserError::NotUnicode("USER", value)),
            Ok(_) | Err(NotPresent) => User::from_uid(getuid())?
                .map(|user| user.name)
                .ok_or(CurrentUserError::Unknown),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_prepend_to_path_prepends_given_dir_to_path() -> TestResult {
        let path = env::join_paths([tempfile::tempdir()?.path(), tempfile::tempdir()?.path()])?;
        let tempdir = tempfile::tempdir()?;
        let expected = {
            let mut tmp = vec![tempdir.path().to_path_buf()];
            tmp.extend(env::split_paths(&path));
            env::join_paths(tmp)?
        };
        let observed = { super::prepend_to_path(tempdir.path(), Some(path))? };
        assert_eq!(expected, observed);
        Ok(())
    }

    #[test]
    fn test_prepend_to_path_moves_dir_to_front_of_path() -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let path = env::join_paths([
            tempfile::tempdir()?.path(),
            tempfile::tempdir()?.path(),
            tempdir.path(),
        ])?;
        let expected = {
            let mut tmp = vec![tempdir.path().to_path_buf()];
            tmp.extend(env::split_paths(&path).take(2));
            env::join_paths(tmp)?
        };
        let observed = { super::prepend_to_path(tempdir.path(), Some(path))? };
        assert_eq!(expected, observed);
        Ok(())
    }

    #[test]
    fn test_prepend_to_path_returns_given_dir_if_path_is_empty() -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let expected = tempdir.path();
        let observed = super::prepend_to_path(tempdir.path(), None)?;
        assert_eq!(expected, observed);
        Ok(())
    }
}
