#![allow(clippy::single_match_else)]

use std::sync::Arc;
use std::sync::RwLock;

use super::{lock, run_and_destroy, run_and_stop, run_and_stop_if_exists, State, Subject};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

// ----------------------------------------------------------------------------

#[test]
fn run_and_stop_still_stops_when_action_panics() -> TestResult {
    let subject = SubjectExample::default();
    let status = subject.status.clone();
    let (_setup, lock) = Setup::run()?;
    let panic =
        std::panic::catch_unwind(|| run_and_stop(&subject, (), lock, || panic!("test panic")));
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    assert!(!status.read().unwrap().running);
    Ok(())
}

#[test]
fn run_and_stop_still_panics_if_stop_fails() -> TestResult {
    // i.e. the error from `stop` is suppressed when the action has panicked.
    let subject = SubjectExample::already_exists().but_cannot_stop();
    let (_setup, lock) = Setup::run()?;
    let panic =
        std::panic::catch_unwind(|| run_and_stop(&subject, (), lock, || panic!("test panic")));
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

#[test]
fn run_and_stop_if_exists_still_stops_when_action_panics() -> TestResult {
    let subject = SubjectExample::already_exists();
    let status = subject.status.clone();
    let (_setup, lock) = Setup::run()?;
    let panic = std::panic::catch_unwind(|| {
        run_and_stop_if_exists(&subject, (), lock, || panic!("test panic"))
    });
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    assert!(!status.read().unwrap().running);
    Ok(())
}

#[test]
fn run_and_stop_if_exists_still_panics_if_stop_fails() -> TestResult {
    // i.e. the error from `stop` is suppressed when the action has panicked.
    let subject = SubjectExample::already_exists().but_cannot_stop();
    let (_setup, lock) = Setup::run()?;
    let panic = std::panic::catch_unwind(|| {
        run_and_stop_if_exists(&subject, (), lock, || panic!("test panic"))
    });
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

#[test]
fn run_and_destroy_still_removes_when_action_panics() -> TestResult {
    let subject = SubjectExample::default();
    let status = subject.status.clone();
    let (_setup, lock) = Setup::run()?;
    let panic =
        std::panic::catch_unwind(|| run_and_destroy(&subject, (), lock, || panic!("test panic")));
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    assert!(!status.read().unwrap().exists);
    assert!(!status.read().unwrap().running);
    Ok(())
}

#[test]
fn run_and_destroy_still_panics_if_stop_fails() -> TestResult {
    // i.e. the error from `destroy` is suppressed when the action has panicked.
    let subject = SubjectExample::default().but_cannot_destroy();
    let (_setup, lock) = Setup::run()?;
    let panic =
        std::panic::catch_unwind(|| run_and_destroy(&subject, (), lock, || panic!("test panic")));
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    Ok(())
}

// ----------------------------------------------------------------------------

#[test]
fn guard_stops_subject() -> TestResult {
    let subject = SubjectExample::default();
    let status = subject.status.clone();
    let (_setup, lock) = Setup::run()?;
    let guard = super::guard::Guard::startup(lock, subject, ())?;
    assert!(status.read().unwrap().running);
    drop(guard);
    assert!(!status.read().unwrap().running);
    Ok(())
}

#[test]
fn guard_stops_subject_when_something_panics() -> TestResult {
    let subject = SubjectExample::default();
    let status1 = subject.status.clone();
    let status2 = subject.status.clone();
    let (_setup, lock) = Setup::run()?;
    let guard = super::guard::Guard::startup(lock, subject, ())?;
    let panic = std::panic::catch_unwind(move || {
        let _guard = guard;
        assert!(status1.read().unwrap().running);
        panic!("test panic")
    });
    assert!(panic.is_err());
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    assert!(!status2.read().unwrap().running);
    Ok(())
}

// ----------------------------------------------------------------------------

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
struct SubjectStatus {
    exists: bool,
    running: bool,
}

#[derive(Debug, Default)]
struct SubjectExample {
    status: Arc<RwLock<SubjectStatus>>,
    cannot_stop: bool,
    cannot_destroy: bool,
}

impl SubjectExample {
    fn already_exists() -> Self {
        Self {
            status: Arc::new(RwLock::new(SubjectStatus { exists: true, running: false })),
            cannot_stop: false,
            cannot_destroy: false,
        }
    }

    fn but_cannot_stop(mut self) -> Self {
        self.cannot_stop = true;
        self
    }

    fn but_cannot_destroy(mut self) -> Self {
        self.cannot_destroy = true;
        self
    }
}

impl Subject for SubjectExample {
    type Error = Error;
    type Options<'a> = ();

    fn start(&self, _options: Self::Options<'_>) -> Result<State, Self::Error> {
        let mut status = self.status.write()?;
        match *status {
            SubjectStatus { exists: true, running: true } => Ok(State::Unmodified),
            SubjectStatus { exists: _, running: _ } => {
                *status = SubjectStatus { exists: true, running: true };
                Ok(State::Modified)
            }
        }
    }

    fn stop(&self) -> Result<State, Self::Error> {
        if self.cannot_stop {
            Err(Error { error: "cannot stop".to_string() })
        } else {
            let mut status = self.status.write()?;
            match *status {
                SubjectStatus { exists: _, running: false } => Ok(State::Unmodified),
                SubjectStatus { exists, running: _ } => {
                    *status = SubjectStatus { exists, running: false };
                    Ok(State::Modified)
                }
            }
        }
    }

    fn destroy(&self) -> Result<State, Self::Error> {
        if self.cannot_destroy {
            Err(Error { error: "cannot stop".to_string() })
        } else {
            let mut status = self.status.write()?;
            match *status {
                SubjectStatus { exists: false, running: false } => Ok(State::Unmodified),
                SubjectStatus { exists: _, running: _ } => {
                    *status = SubjectStatus { exists: false, running: false };
                    Ok(State::Modified)
                }
            }
        }
    }

    fn exists(&self) -> Result<bool, Self::Error> {
        Ok(self.status.read()?.exists)
    }

    fn running(&self) -> Result<bool, Self::Error> {
        Ok(self.status.read()?.running)
    }
}
