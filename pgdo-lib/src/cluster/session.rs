//! Use a cluster safely alongside other processes.
//!
//! A [`Session`] creates and starts a cluster as necessary, and stops it – or
//! destroys it – when the session ends, **unless** other sessions are still
//! using it. Many processes can hold sessions on the same cluster at once, e.g.
//! as part of a test suite; the last one out turns off the lights.
//!
//! ```rust
//! # use pgdo::{cluster::Cluster, runtime::strategy::Strategy};
//! let tempdir = tempfile::tempdir()?;
//! let cluster = Cluster::new(tempdir.path().join("data"), Strategy::default())?;
//! let session = cluster.session(&[])?;
//! assert!(session.running()?);
//! let databases = session.databases()?;
//! session.end()?; // Or drop it; see `Session::end` for the difference.
//! # Ok::<(), pgdo::cluster::ClusterError>(())
//! ```
//!
//! Coordination uses [`flock(2)`](https://linux.die.net/man/2/flock) locks on
//! the file `pgdo.lock` in the cluster's data directory: a shared lock while
//! the session is held; an exclusive lock to create and start the cluster, and
//! to stop or destroy it. Because the lock file lives in the data directory, it
//! works for any process that can see that directory, whatever path it uses to
//! get there, as long as `flock` works on the filesystem and all processes run
//! on the same kernel.
//!
//! When a session destroys a cluster it removes the lock file too. Another
//! process might be waiting on that lock file, or might create a new one
//! meanwhile, so after taking any lock, a session checks that the file it has
//! locked is still the file at `pgdo.lock`; if not, it tries again.
//!
//! Starting, ending, and dropping a session all block, sometimes for a while:
//! they may wait for other processes to release locks, and they run `pg_ctl`.
//! This is safe within an async context, but blocks the current thread; to
//! avoid that, start and end sessions within something like Tokio's
//! `spawn_blocking`. See also [Blocking][`Cluster#blocking`].

use std::os::unix::fs::MetadataExt;
use std::time::Duration;
use std::{fs, io, ops};

use either::Either::{Left, Right};
use rand::Rng;

use super::{Cluster, ClusterError, Options, State};
use crate::lock;

/// What to do with a cluster when a [`Session`] ends and no other sessions are
/// using the cluster.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Finish {
    /// Stop the cluster.
    #[default]
    Stop,
    /// Stop the cluster and **delete its data directory**.
    Destroy,
}

/// A cluster that this process is using. See the [module
/// documentation][`self`].
///
/// Dereferences to [`Cluster`].
#[derive(Debug)]
pub struct Session {
    cluster: Cluster,
    lock: Option<lock::LockedFileShared>,
    finish: Finish,
}

impl Cluster {
    /// Start a [`Session`] with this cluster, creating and starting it as
    /// necessary.
    ///
    /// The cluster's data directory is created if it does not exist, and the
    /// cluster's path is made absolute (canonicalized).
    pub fn session(mut self, options: Options<'_>) -> Result<Session, ClusterError> {
        // Refuse a directory that is neither a cluster nor empty before putting
        // a lock file in it. `Cluster::create` checks again, under the lock.
        // Skip this if a `Cluster::create` was interrupted while moving files
        // into place: it will finish the job.
        if self.datadir.is_dir() && !super::exists(&self) && !self.create_interrupted() {
            self.check_datadir_empty()?;
        }
        fs::create_dir_all(&self.datadir)?;
        self.datadir = self.datadir.canonicalize()?;
        let lock = startup(&self, options)?;
        Ok(Session { cluster: self, lock: Some(lock), finish: Finish::default() })
    }
}

impl Session {
    /// Choose what happens when this session ends; see [`Finish`].
    #[must_use]
    pub fn finish(mut self, finish: Finish) -> Self {
        self.finish = finish;
        self
    }

    /// End this session.
    ///
    /// If no other sessions are using the cluster, it is stopped or destroyed
    /// according to [`Finish`]. Returns [`State::Modified`] if this session did
    /// so, else [`State::Unmodified`].
    ///
    /// Dropping a session does the same, but can only log errors.
    pub fn end(mut self) -> Result<State, ClusterError> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> Result<State, ClusterError> {
        let Some(lock) = self.lock.take() else {
            return Ok(State::Unmodified);
        };
        match upgrade(lock)? {
            // The cluster is in use elsewhere. There's nothing more to do.
            None => Ok(State::Unmodified),
            // We may have unlocked and relocked; check that the lock file is
            // still current, i.e. that no one destroyed the cluster meanwhile.
            Some(lock) if !is_current(&self.cluster, &lock)? => Ok(State::Unmodified),
            // We have an exclusive lock, so we can stop or destroy the cluster.
            // The lock is released when `lock` is dropped, which must happen
            // only after everything else is done.
            Some(lock) => match self.finish {
                Finish::Stop => self.cluster.stop(),
                Finish::Destroy => {
                    let state = self.cluster.destroy()?;
                    remove_datadir(&self.cluster)?;
                    drop(lock);
                    Ok(state)
                }
            },
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Err(err) = self.shutdown() {
            log::error!(
                "Error ending session with cluster {}: {err}",
                self.cluster.datadir.display()
            );
        }
    }
}

impl ops::Deref for Session {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

// ----------------------------------------------------------------------------

/// Open the cluster's lock file, creating it – and the data directory – if
/// necessary.
fn open_lockfile(cluster: &Cluster) -> Result<lock::UnlockedFile, ClusterError> {
    let path = cluster.lockfile();
    match lock::UnlockedFile::try_from(path.as_path()) {
        // The data directory may have been removed by a session that destroyed
        // the cluster.
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(&cluster.datadir)?;
            Ok(lock::UnlockedFile::try_from(path.as_path())?)
        }
        result => Ok(result?),
    }
}

/// Is the locked file still the cluster's lock file? It may have been removed
/// – and maybe replaced – by a session that destroyed the cluster.
fn is_current<L: AsRef<fs::File>>(cluster: &Cluster, lock: &L) -> Result<bool, ClusterError> {
    let locked = lock.as_ref().metadata()?;
    match fs::metadata(cluster.lockfile()) {
        Ok(current) => Ok(locked.dev() == current.dev() && locked.ino() == current.ino()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err)?,
    }
}

/// Obtain a shared lock on the cluster, creating and starting it if necessary.
fn startup(
    cluster: &Cluster,
    options: Options<'_>,
) -> Result<lock::LockedFileShared, ClusterError> {
    loop {
        match open_lockfile(cluster)?.try_lock_exclusive()? {
            Left(lock) => {
                // The cluster is locked elsewhere, shared or exclusively. We
                // optimistically take a shared lock. If the other lock is also
                // shared, this will not block. If the other lock is exclusive,
                // this will block until that lock is released (or changed to a
                // shared lock).
                let lock = lock.lock_shared()?;
                if !is_current(cluster, &lock)? {
                    // The lock file was removed while we waited; try again.
                    continue;
                }
                // If obtaining the lock blocked, i.e. the lock elsewhere was
                // exclusive, then the cluster may have been started by the
                // process that held that exclusive lock. We should check.
                if cluster.running()? {
                    return Ok(lock);
                }
                // Release all locks then sleep for a random time between 200ms
                // and 1000ms in an attempt to make sure that when there are
                // many competing processes one of them rapidly acquires an
                // exclusive lock and is able to create and start the cluster.
                drop(lock);
                let delay = 200 + (rand::rng().next_u32() % 800);
                std::thread::sleep(Duration::from_millis(u64::from(delay)));
            }
            Right(lock) => {
                if !is_current(cluster, &lock)? {
                    // The lock file was removed before we locked it; try again.
                    continue;
                }
                // We have an exclusive lock, so try to start the cluster. If
                // this fails, the lock is released when `lock` is dropped.
                cluster.start(options)?;
                // Once started, downgrade to a shared lock. `flock` does not do
                // this atomically: it releases the exclusive lock then takes a
                // shared lock. In between, another session could stop – or even
                // destroy – the cluster, so check again.
                let lock = lock.lock_shared()?;
                if is_current(cluster, &lock)? && cluster.running()? {
                    return Ok(lock);
                }
            }
        }
    }
}

/// Try to upgrade a shared lock to an exclusive lock, without blocking. Returns
/// `None` if another process holds a lock, in which case ours is released.
///
/// `flock` does not convert locks atomically, and platforms differ when a
/// conversion fails: Linux has then released the shared lock, but macOS keeps
/// it. On macOS, two sessions ending at the same moment would each block the
/// other's upgrade, and neither would stop the cluster. So unlock, and try once
/// more: of two sessions doing this at once, the last to try will succeed.
fn upgrade(
    lock: lock::LockedFileShared,
) -> Result<Option<lock::LockedFileExclusive>, ClusterError> {
    match lock.try_lock_exclusive()? {
        Right(lock) => Ok(Some(lock)),
        Left(lock) => Ok(lock.unlock()?.try_lock_exclusive()?.right()),
    }
}

/// Remove the cluster's lock file and data directory. Call this only while
/// holding an exclusive lock, after destroying the cluster.
fn remove_datadir(cluster: &Cluster) -> Result<(), ClusterError> {
    match fs::remove_file(cluster.lockfile()) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err)?,
        _ => (),
    }
    // Use `remove_dir`, not `remove_dir_all`: if another process has created
    // something here in the meantime, e.g. a new lock file, leave it be.
    match fs::remove_dir(&cluster.datadir) {
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Ok(())
        }
        result => Ok(result?),
    }
}

#[cfg(test)]
mod tests {
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Two sessions ending at the same moment must not both fail to upgrade
    /// their locks, or neither would stop the cluster.
    #[test]
    fn upgrade_succeeds_for_one_of_two_sessions_ending_together() -> TestResult {
        use std::sync::{Arc, Barrier};
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("pgdo.lock");
        for _ in 0..200 {
            let barrier = Arc::new(Barrier::new(2));
            let ending = [0, 1].map(|_| {
                let lock = crate::lock::UnlockedFile::try_from(path.as_path())
                    .and_then(|lock| Ok(lock.lock_shared()?));
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let lock = lock.unwrap();
                    barrier.wait();
                    // Hold any exclusive lock until both have tried.
                    let upgraded = super::upgrade(lock).unwrap();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    upgraded.is_some()
                })
            });
            let upgraded = ending.map(|thread| thread.join().unwrap());
            assert!(
                upgraded.contains(&true),
                "neither session upgraded its lock"
            );
        }
        Ok(())
    }
}
