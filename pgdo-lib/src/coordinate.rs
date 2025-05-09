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
use rand::RngCore;

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
    let lock = startup(lock, subject, options)?;
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
    let lock = startup_if_exists(lock, subject, options)?;
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
    let lock = startup(lock, subject, options)?;
    with_finally(
        || shutdown::<S, _, _>(lock, || subject.destroy()),
        || -> Result<T, CoordinateError<S::Error>> { Ok(action()) },
    )
}

// ----------------------------------------------------------------------------

fn startup<S: Subject>(
    mut lock: lock::UnlockedFile,
    control: &S,
    options: S::Options<'_>,
) -> Result<lock::LockedFileShared, CoordinateError<S::Error>> {
    loop {
        lock = match lock.try_lock_exclusive() {
            Ok(Left(lock)) => {
                // The subject is locked elsewhere, shared or exclusively. We
                // optimistically take a shared lock. If the other lock is also
                // shared, this will not block. If the other lock is exclusive,
                // this will block until that lock is released (or changed to a
                // shared lock).
                let lock = lock.lock_shared()?;
                // If obtaining the lock blocked, i.e. the lock elsewhere was
                // exclusive, then the subject may have been started by the
                // process that held that exclusive lock. We should check.
                if control.running().map_err(CoordinateError::ControlError)? {
                    return Ok(lock);
                }
                // Release all locks then sleep for a random time between 200ms
                // and 1000ms in an attempt to make sure that when there are
                // many competing processes one of them rapidly acquires an
                // exclusive lock and is able to create and start the subject.
                let lock = lock.unlock()?;
                let delay = rand::rng().next_u32();
                let delay = 200 + (delay % 800);
                let delay = Duration::from_millis(u64::from(delay));
                std::thread::sleep(delay);
                lock
            }
            Ok(Right(lock)) => {
                // We have an exclusive lock, so try to start the subject.
                control
                    .start(options)
                    .map_err(CoordinateError::ControlError)?;
                // Once started, downgrade to a shared log.
                return Ok(lock.lock_shared()?);
            }
            Err(err) => return Err(err.into()),
        };
    }
}

fn startup_if_exists<S: Subject>(
    mut lock: lock::UnlockedFile,
    subject: &S,
    options: S::Options<'_>,
) -> Result<lock::LockedFileShared, CoordinateError<S::Error>> {
    loop {
        lock = match lock.try_lock_exclusive() {
            Ok(Left(lock)) => {
                // The subject is locked elsewhere, shared or exclusively. We
                // optimistically take a shared lock. If the other lock is also
                // shared, this will not block. If the other lock is exclusive,
                // this will block until that lock is released (or changed to a
                // shared lock).
                let lock = lock.lock_shared()?;
                // If obtaining the lock blocked, i.e. the lock elsewhere was
                // exclusive, then the subject may have been started by the
                // process that held that exclusive lock. We should check.
                if subject.running().map_err(CoordinateError::ControlError)? {
                    return Ok(lock);
                }
                // Release all locks then sleep for a random time between 200ms
                // and 1000ms in an attempt to make sure that when there are
                // many competing processes one of them rapidly acquires an
                // exclusive lock and is able to create and start the subject.
                let lock = lock.unlock()?;
                let delay = rand::rng().next_u32();
                let delay = 200 + (delay % 800);
                let delay = Duration::from_millis(u64::from(delay));
                std::thread::sleep(delay);
                lock
            }
            Ok(Right(lock)) => {
                // We have an exclusive lock, so try to start the subject.
                if subject.exists().map_err(CoordinateError::ControlError)? {
                    subject
                        .start(options)
                        .map_err(CoordinateError::ControlError)?;
                } else {
                    return Err(CoordinateError::DoesNotExist);
                }
                // Once started, downgrade to a shared log.
                return Ok(lock.lock_shared()?);
            }
            Err(err) => return Err(err.into()),
        };
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
