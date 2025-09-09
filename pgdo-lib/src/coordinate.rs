//! Safely coordinate use of things that can be controlled.
//!
//! For example, if many concurrent processes want to make use of the same
//! cluster, e.g. as part of a test suite, you can use [`run_and_stop`] to
//! safely start and use the cluster, then stop it when it's no longer needed:
//!
//! ```rust
//! # use pgdo::{runtime, coordinate, cluster, lock};
//! let cluster_dir = tempfile::tempdir()?;
//! let data_dir = cluster_dir.path().join("data");
//! let strategy = runtime::strategy::Strategy::default();
//! let cluster = cluster::Cluster::new(&data_dir, strategy)?;
//! let lock_file = cluster_dir.path().join("lock");
//! let lock = lock::UnlockedFile::try_from(lock_file.as_path())?;
//! assert!(coordinate::run_and_stop(&cluster, &[], lock, || cluster::exists(&cluster))?);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod cleanup;
mod error;
pub mod finally;
pub mod guard;
pub mod resource;

#[cfg(test)]
mod tests;

use std::time::Duration;

use either::Either::{Left, Right};

use crate::lock;
pub use error::CoordinateError;

use self::finally::with_finally;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum State {
    /// The action we requested was performed from this process, e.g. we tried
    /// to create the subject, and we did indeed create the subject.
    Modified,
    /// The action we requested was performed by another process, or was not
    /// necessary, e.g. we tried to stop the subject but it was already stopped.
    Unmodified,
}

/// The trait that these coordinate functions work with.
pub trait Subject {
    type Error: std::error::Error + Send + Sync;
    type Options<'a>: Default;
    fn start(&self, options: Self::Options<'_>) -> Result<State, Self::Error>;
    fn stop(&self) -> Result<State, Self::Error>;
    fn destroy(&self) -> Result<State, Self::Error>;
    fn exists(&self) -> Result<bool, Self::Error>;
    fn running(&self) -> Result<bool, Self::Error>;
}

/// Perform `action` in `subject`.
///
/// Using the given lock for synchronisation, this creates the subject if it
/// does not exist, starts it if it's not running, performs the `action`, then
/// (maybe) stops the subject again, and finally returns the result of `action`.
/// If there are other users of the subject – i.e. if an exclusive lock cannot
/// be acquired during the shutdown phase – then the subject is left running.
pub fn run_and_stop<S, F, T>(
    subject: &S,
    options: S::Options<'_>,
    lock: lock::UnlockedFile,
    action: F,
) -> Result<T, CoordinateError<S::Error>>
where
    S: std::panic::RefUnwindSafe + Subject,
    F: std::panic::UnwindSafe + FnOnce() -> T,
{
    let retries: retries::BackoffIter<_> = backoff::ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(200))
        .with_max_elapsed_time(Some(Duration::from_secs(60)))
        .with_max_interval(Duration::from_millis(10000))
        .build()
        .into();
    let (lock, _) = startup(lock, subject, options, retries)?;
    with_finally(
        || shutdown::<S, _, _>(lock, || subject.stop()),
        || -> Result<T, CoordinateError<S::Error>> { Ok(action()) },
    )
}

/// Perform `action` in `subject` **if it exists**.
///
/// Using the given lock for synchronisation, this starts the subject it if it's
/// not running, performs the `action`, then (maybe) stops the subject again,
/// and finally returns the result of `action`. If there are other users of the
/// subject – i.e. if an exclusive lock cannot be acquired during the shutdown
/// phase – then the subject is left running.
pub fn run_and_stop_if_exists<S, F, T>(
    subject: &S,
    options: S::Options<'_>,
    lock: lock::UnlockedFile,
    action: F,
) -> Result<T, CoordinateError<S::Error>>
where
    S: std::panic::RefUnwindSafe + Subject,
    F: std::panic::UnwindSafe + FnOnce() -> T,
{
    let retries: retries::BackoffIter<_> = backoff::ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(200))
        .with_max_elapsed_time(Some(Duration::from_secs(60)))
        .with_max_interval(Duration::from_millis(10000))
        .build()
        .into();
    let (lock, _) = startup_if_exists(lock, subject, options, retries)?;
    with_finally(
        || shutdown::<S, _, _>(lock, || subject.stop()),
        || -> Result<T, CoordinateError<S::Error>> { Ok(action()) },
    )
}

/// Perform `action` in `subject`, destroying the subject before returning.
///
/// Similar to [`run_and_stop`] except this attempts to destroy the subject
/// – i.e. stop the subject and completely delete its data directory – before
/// returning. If there are other users of the subject – i.e. if an exclusive
/// lock cannot be acquired during the shutdown phase – then the subject is left
/// running and is **not** destroyed.
pub fn run_and_destroy<S, F, T>(
    subject: &S,
    options: S::Options<'_>,
    lock: lock::UnlockedFile,
    action: F,
) -> Result<T, CoordinateError<S::Error>>
where
    S: std::panic::RefUnwindSafe + Subject,
    F: std::panic::UnwindSafe + FnOnce() -> T,
{
    let retries: retries::BackoffIter<_> = backoff::ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(200))
        .with_max_elapsed_time(Some(Duration::from_secs(60)))
        .with_max_interval(Duration::from_millis(10000))
        .build()
        .into();
    let (lock, _) = startup(lock, subject, options, retries)?;
    with_finally(
        || shutdown::<S, _, _>(lock, || subject.destroy()),
        || -> Result<T, CoordinateError<S::Error>> { Ok(action()) },
    )
}

// ----------------------------------------------------------------------------

fn startup<S: Subject>(
    lock: lock::UnlockedFile,
    subject: &S,
    options: S::Options<'_>,
    retries: impl IntoIterator<Item = Duration>,
) -> Result<(lock::LockedFileShared, State), CoordinateError<S::Error>> {
    match retries::retry(
        retries,
        |lock| match lock
            .try_lock_exclusive()
            .map_err(CoordinateError::UnixError)
        {
            Ok(Left(lock)) => {
                // The subject is locked elsewhere, shared or exclusively. We
                // optimistically take a shared lock.
                match lock.try_lock_shared().map_err(CoordinateError::UnixError) {
                    Ok(Left(lock)) => {
                        // The subject is locked exclusively by another process.
                        retries::Outcome::Retry(lock)
                    }
                    Ok(Right(lock)) => {
                        // The subject is locked shared by another process.
                        match subject.running().map_err(CoordinateError::ControlError) {
                            Ok(true) => retries::Outcome::Ok(Left(lock)),
                            Ok(false) => match lock.unlock().map_err(CoordinateError::UnixError) {
                                Ok(lock) => retries::Outcome::Retry(lock),
                                Err(err) => retries::Outcome::Err(err),
                            },
                            Err(err) => retries::Outcome::Err(err),
                        }
                    }
                    Err(err) => retries::Outcome::Err(err),
                }
            }
            Ok(Right(lock)) => retries::Outcome::Ok(Right(lock)),
            Err(err) => retries::Outcome::Err(err),
        },
        lock,
    ) {
        Ok(Left(lock)) => {
            // We have a shared lock and the subject is running.
            Ok((lock, State::Unmodified))
        }
        Ok(Right(lock)) => {
            // We have an exclusive lock; start the subject.
            subject
                .start(options)
                .map_err(CoordinateError::ControlError)?;
            // Once started, downgrade to a shared lock.
            Ok((lock.lock_shared()?, State::Modified))
        }
        Err(retries::Error::Exhausted(_)) => Err(CoordinateError::Exhausted),
        Err(retries::Error::Other(err)) => Err(err)?,
    }
}

pub fn startup_if_exists<S: Subject>(
    lock: lock::UnlockedFile,
    subject: &S,
    options: S::Options<'_>,
    retries: impl IntoIterator<Item = Duration>,
) -> Result<(lock::LockedFileShared, State), CoordinateError<S::Error>> {
    match retries::retry(
        retries,
        |lock| match lock
            .try_lock_exclusive()
            .map_err(CoordinateError::UnixError)
        {
            Ok(Left(lock)) => {
                // The subject is locked elsewhere, shared or exclusively. We
                // optimistically take a shared lock.
                match lock.try_lock_shared().map_err(CoordinateError::UnixError) {
                    Ok(Left(lock)) => {
                        // The subject is locked exclusively by another process.
                        retries::Outcome::Retry(lock)
                    }
                    Ok(Right(lock)) => {
                        // The subject is locked shared by another process.
                        match subject.running().map_err(CoordinateError::ControlError) {
                            Ok(true) => retries::Outcome::Ok(Left(lock)),
                            Ok(false) => match lock.unlock().map_err(CoordinateError::UnixError) {
                                Ok(lock) => retries::Outcome::Retry(lock),
                                Err(err) => retries::Outcome::Err(err),
                            },
                            Err(err) => retries::Outcome::Err(err),
                        }
                    }
                    Err(err) => retries::Outcome::Err(err),
                }
            }
            Ok(Right(lock)) => retries::Outcome::Ok(Right(lock)),
            Err(err) => retries::Outcome::Err(err),
        },
        lock,
    ) {
        Ok(Left(lock)) => {
            // We have a shared lock and the subject is running.
            Ok((lock, State::Unmodified))
        }
        Ok(Right(lock)) => {
            // We have an exclusive lock; start the subject, IFF it exists.
            if subject.exists().map_err(CoordinateError::ControlError)? {
                subject
                    .start(options)
                    .map_err(CoordinateError::ControlError)?;
                // Once started, downgrade to a shared lock.
                Ok((lock.lock_shared()?, State::Modified))
            } else {
                Err(CoordinateError::DoesNotExist)
            }
        }
        Err(retries::Error::Exhausted(_)) => Err(CoordinateError::Exhausted),
        Err(retries::Error::Other(err)) => Err(err)?,
    }
}

fn shutdown<S, F, T>(
    lock: lock::LockedFileShared,
    action: F,
) -> Result<Option<T>, CoordinateError<S::Error>>
where
    S: Subject,
    F: FnOnce() -> Result<T, S::Error>,
{
    match lock.try_lock_exclusive() {
        Ok(Left(lock)) => {
            // The subject is in use elsewhere. There's nothing more we can do
            // here.
            lock.unlock()?;
            Ok(None)
        }
        Ok(Right(lock)) => {
            // We have an exclusive lock, so we can mutate the subject.
            match action() {
                Ok(result) => {
                    lock.unlock()?;
                    Ok(Some(result))
                }
                Err(err) => Err(CoordinateError::ControlError(err)),
            }
        }
        Err(err) => Err(err.into()),
    }
}

pub mod retries {
    use std::{thread::sleep, time::Duration};

    #[derive(Clone, Debug)]
    pub enum Outcome<T, State, E> {
        Ok(T),
        Retry(State),
        Err(E),
    }

    pub enum Error<State, E> {
        Exhausted(State),
        Other(E),
    }

    pub fn retry<Retries, Op, OpState, OpResult, T, E>(
        retries: Retries,
        mut operation: Op,
        mut state: OpState,
    ) -> Result<T, Error<OpState, E>>
    where
        Retries: IntoIterator<Item = Duration>,
        Op: FnMut(OpState) -> OpResult,
        OpResult: Into<Outcome<T, OpState, E>>,
    {
        let mut retries = retries.into_iter();

        loop {
            state = match operation(state).into() {
                Outcome::Err(error) => break Err(Error::Other(error)),
                Outcome::Ok(value) => break Ok(value),
                Outcome::Retry(state) => match retries.next() {
                    None => break Err(Error::Exhausted(state)),
                    Some(delay) => {
                        sleep(delay);
                        state
                    }
                },
            }
        }
    }

    use backoff::backoff::Backoff;

    pub struct BackoffIter<B: Backoff> {
        backoff: B,
        // TODO: Add budget.
    }

    impl<B: Backoff> Iterator for BackoffIter<B> {
        type Item = Duration;

        fn next(&mut self) -> Option<Self::Item> {
            self.backoff.next_backoff()
        }
    }

    impl<B: Backoff> From<B> for BackoffIter<B> {
        fn from(value: B) -> Self {
            Self { backoff: value }
        }
    }
}
