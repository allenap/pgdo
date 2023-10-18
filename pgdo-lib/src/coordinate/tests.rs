use std::sync::RwLock;

use super::{lock, run_and_destroy, run_and_stop, run_and_stop_if_exists, Subject};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn run_and_stop_still_stops_when_action_panics() -> TestResult {
    let subject = SubjectBasic::default();
    let (_setup, lock) = Setup::run()?;
    let panic = std::panic::catch_unwind(|| run_and_stop(&subject, lock, || panic!("test panic")));
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

#[test]
fn run_and_stop_still_panics_if_stop_fails() -> TestResult {
    // i.e. the error from `stop` is suppressed when the action has panicked.
    let subject = SubjectCannotStop::exists();
    let (_setup, lock) = Setup::run()?;
    let panic = std::panic::catch_unwind(|| run_and_stop(&subject, lock, || panic!("test panic")));
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

#[test]
fn run_and_stop_if_exists_still_stops_when_action_panics() -> TestResult {
    let subject = SubjectBasic::exists();
    let (_setup, lock) = Setup::run()?;
    let panic = std::panic::catch_unwind(|| {
        run_and_stop_if_exists(&subject, lock, || panic!("test panic"))
    });
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

#[test]
fn run_and_stop_if_exists_still_panics_if_stop_fails() -> TestResult {
    // i.e. the error from `stop` is suppressed when the action has panicked.
    let subject = SubjectCannotStop::exists();
    let (_setup, lock) = Setup::run()?;
    let panic = std::panic::catch_unwind(|| {
        run_and_stop_if_exists(&subject, lock, || panic!("test panic"))
    });
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

#[test]
fn run_and_destroy_still_removes_when_action_panics() -> TestResult {
    let subject = SubjectBasic::default();
    let (_setup, lock) = Setup::run()?;
    let panic =
        std::panic::catch_unwind(|| run_and_destroy(&subject, lock, || panic!("test panic")));
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

#[test]
fn run_and_destroy_still_panics_if_stop_fails() -> TestResult {
    // i.e. the error from `destroy` is suppressed when the action has panicked.
    let subject = SubjectCannotDestroy::default();
    let (_setup, lock) = Setup::run()?;
    let panic =
        std::panic::catch_unwind(|| run_and_destroy(&subject, lock, || panic!("test panic")));
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

#[allow(unused)]
struct Setup {
    tempdir: tempfile::TempDir,
}

impl Setup {
    fn run() -> TestResult<(Self, lock::UnlockedFile)> {
        let tempdir = tempfile::tempdir()?;
        let lockpath = tempdir.path().join("lock");
        Ok((Self { tempdir }, lock::UnlockedFile::try_from(&lockpath)?))
    }
}

#[derive(Debug)]
struct Error {
    error: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "{}", self.error)
    }
}

impl std::error::Error for Error {}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(error: std::sync::PoisonError<T>) -> Self {
        Self { error: format!("{error}") }
    }
}

#[derive(Debug, Default)]
struct SubjectBasic {
    exists: RwLock<bool>,
    running: RwLock<bool>,
}

impl SubjectBasic {
    fn exists() -> Self {
        Self { exists: RwLock::new(true), running: RwLock::new(false) }
    }
}

impl Subject for SubjectBasic {
    type Error = Error;

    fn start(&self) -> Result<(), Self::Error> {
        *self.exists.write()? = true;
        *self.running.write()? = true;
        Ok(())
    }

    fn stop(&self) -> Result<(), Self::Error> {
        *self.running.write()? = false;
        Ok(())
    }

    fn destroy(&self) -> Result<(), Self::Error> {
        *self.exists.write()? = false;
        *self.running.write()? = false;
        Ok(())
    }

    fn exists(&self) -> Result<bool, Self::Error> {
        Ok(*self.exists.read()?)
    }

    fn running(&self) -> Result<bool, Self::Error> {
        Ok(*self.running.read()?)
    }
}

#[derive(Debug, Default)]
struct SubjectCannotStop {
    exists: RwLock<bool>,
    running: RwLock<bool>,
}

impl SubjectCannotStop {
    fn exists() -> Self {
        Self { exists: RwLock::new(true), running: RwLock::new(false) }
    }
}

impl Subject for SubjectCannotStop {
    type Error = Error;

    fn start(&self) -> Result<(), Self::Error> {
        *self.exists.write()? = true;
        *self.running.write()? = true;
        Ok(())
    }

    fn stop(&self) -> Result<(), Self::Error> {
        Err(Error { error: "cannot stop".to_string() })
    }

    fn destroy(&self) -> Result<(), Self::Error> {
        *self.exists.write()? = false;
        *self.running.write()? = false;
        Ok(())
    }

    fn exists(&self) -> Result<bool, Self::Error> {
        Ok(*self.exists.read()?)
    }

    fn running(&self) -> Result<bool, Self::Error> {
        Ok(*self.running.read()?)
    }
}

#[derive(Debug, Default)]
struct SubjectCannotDestroy {
    exists: RwLock<bool>,
    running: RwLock<bool>,
}

impl Subject for SubjectCannotDestroy {
    type Error = Error;

    fn start(&self) -> Result<(), Self::Error> {
        *self.exists.write()? = true;
        *self.running.write()? = true;
        Ok(())
    }

    fn stop(&self) -> Result<(), Self::Error> {
        *self.running.write()? = false;
        Ok(())
    }

    fn destroy(&self) -> Result<(), Self::Error> {
        Err(Error { error: "cannot destroy".to_string() })
    }

    fn exists(&self) -> Result<bool, Self::Error> {
        Ok(*self.exists.read()?)
    }

    fn running(&self) -> Result<bool, Self::Error> {
        Ok(*self.running.read()?)
    }
}
