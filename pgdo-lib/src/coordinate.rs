//! Safely coordinate use of things that can be [`Control`]led.
//!
//! For example, if many concurrent processes want to make use of the same
//! cluster, e.g. as part of a test suite, you can use [`run_and_stop`] to
//! safely start and use the cluster, then stop it when it's no longer needed:
//!
//! ```rust
//! use pgdo::prelude::*;
//! let cluster_dir = tempfile::tempdir()?;
//! let data_dir = cluster_dir.path().join("data");
//! let strategy = runtime::strategy::Strategy::default();
//! let cluster = Cluster::new(&data_dir, strategy)?;
//! let lock_file = cluster_dir.path().join("lock");
//! let lock = lock::UnlockedFile::try_from(lock_file.as_path())?;
//! assert!(coordinate::run_and_stop(&cluster, lock, || cluster::exists(&cluster))?);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod cleanup;
mod error;
pub mod resource;

#[cfg(test)]
mod tests;

use std::time::Duration;

use either::Either::{Left, Right};
use rand::RngCore;

use crate::lock;
pub use error::CoordinateError;

#[derive(Debug, PartialEq, Eq)]
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
    fn start(&self) -> Result<State, Self::Error>;
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
    control: &S,
    lock: lock::UnlockedFile,
    action: F,
) -> Result<T, CoordinateError<S::Error>>
where
    S: Subject,
    F: std::panic::UnwindSafe + FnOnce() -> T,
{
    let lock = startup(control, lock)?;
    let action_res = std::panic::catch_unwind(action);
    let shutdown_res = shutdown::<S, _, _>(lock, || control.stop());
    match action_res {
        Ok(result) => shutdown_res.map(|_| result),
        Err(err) => std::panic::resume_unwind(err),
    }
}

/// Perform `action` in `subject` **if it exists**.
///
/// Using the given lock for synchronisation, this starts the subject it if it's
/// not running, performs the `action`, then (maybe) stops the subject again,
/// and finally returns the result of `action`. If there are other users of the
/// subject – i.e. if an exclusive lock cannot be acquired during the shutdown
/// phase – then the subject is left running.
pub fn run_and_stop_if_exists<S, F, T>(
    control: &S,
    lock: lock::UnlockedFile,
    action: F,
) -> Result<T, CoordinateError<S::Error>>
where
    S: Subject,
    F: std::panic::UnwindSafe + FnOnce() -> T,
{
    let lock = startup_if_exists(control, lock)?;
    let action_res = std::panic::catch_unwind(action);
    let shutdown_res = shutdown::<S, _, _>(lock, || control.stop());
    match action_res {
        Ok(result) => shutdown_res.map(|_| result),
        Err(err) => std::panic::resume_unwind(err),
    }
}

/// Perform `action` in `subject`, destroying the subject before returning.
///
/// Similar to [`run_and_stop`] except this attempts to destroy the subject
/// – i.e. stop the subject and completely delete its data directory – before
/// returning. If there are other users of the subject – i.e. if an exclusive
/// lock cannot be acquired during the shutdown phase – then the subject is left
/// running and is **not** destroyed.
pub fn run_and_destroy<S, F, T>(
    control: &S,
    lock: lock::UnlockedFile,
    action: F,
) -> Result<T, CoordinateError<S::Error>>
where
    S: Subject,
    F: std::panic::UnwindSafe + FnOnce() -> T,
{
    let lock = startup(control, lock)?;
    let action_res = std::panic::catch_unwind(action);
    let shutdown_res = shutdown::<S, _, _>(lock, || control.destroy());
    match action_res {
        Ok(result) => shutdown_res.map(|_| result),
        Err(err) => std::panic::resume_unwind(err),
    }
}

fn startup<S: Subject>(
    control: &S,
    mut lock: lock::UnlockedFile,
) -> Result<lock::LockedFileShared, CoordinateError<S::Error>> {
    loop {
        lock = match lock.try_lock_exclusive() {
            Ok(Left(lock)) => {
                // The subject is locked exclusively by someone/something else.
                // Switch to a shared lock optimistically. This blocks until we
                // get the shared lock.
                let lock = lock.lock_shared()?;
                // The subject may have been started while that exclusive lock
                // was held, so we must check if the subject is running now –
                // otherwise we loop back to the top again.
                if control.running().map_err(CoordinateError::ControlError)? {
                    return Ok(lock);
                }
                // Release all locks then sleep for a random time between 200ms
                // and 1000ms in an attempt to make sure that when there are
                // many competing processes one of them rapidly acquires an
                // exclusive lock and is able to create and start the subject.
                let lock = lock.unlock()?;
                let delay = rand::thread_rng().next_u32();
                let delay = 200 + (delay % 800);
                let delay = Duration::from_millis(u64::from(delay));
                std::thread::sleep(delay);
                lock
            }
            Ok(Right(lock)) => {
                // We have an exclusive lock, so try to start the subject.
                control.start().map_err(CoordinateError::ControlError)?;
                // Once started, downgrade to a shared log.
                return Ok(lock.lock_shared()?);
            }
            Err(err) => return Err(err.into()),
        };
    }
}

fn startup_if_exists<S: Subject>(
    control: &S,
    mut lock: lock::UnlockedFile,
) -> Result<lock::LockedFileShared, CoordinateError<S::Error>> {
    loop {
        lock = match lock.try_lock_exclusive() {
            Ok(Left(lock)) => {
                // The subject is locked exclusively by someone/something else.
                // Switch to a shared lock optimistically. This blocks until we
                // get the shared lock.
                let lock = lock.lock_shared()?;
                // The subject may have been started while that exclusive lock
                // was held, so we must check if the subject is running now –
                // otherwise we loop back to the top again.
                if control.running().map_err(CoordinateError::ControlError)? {
                    return Ok(lock);
                }
                // Release all locks then sleep for a random time between 200ms
                // and 1000ms in an attempt to make sure that when there are
                // many competing processes one of them rapidly acquires an
                // exclusive lock and is able to create and start the subject.
                let lock = lock.unlock()?;
                let delay = rand::thread_rng().next_u32();
                let delay = 200 + (delay % 800);
                let delay = Duration::from_millis(u64::from(delay));
                std::thread::sleep(delay);
                lock
            }
            Ok(Right(lock)) => {
                // We have an exclusive lock, so try to start the subject.
                if control.exists().map_err(CoordinateError::ControlError)? {
                    control.start().map_err(CoordinateError::ControlError)?;
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
            // The subject is in use by someone/something else. There's nothing
            // more we can do here.
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
