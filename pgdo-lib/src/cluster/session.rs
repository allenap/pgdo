//! Use a cluster safely alongside other processes.
//!
//! A [`Session`] creates and starts a cluster as necessary, and stops it – or
//! destroys it – when the session ends, **unless** other sessions are still
//! using it. Many processes can hold sessions on the same cluster at once, e.g.
//! as part of a test suite; the last one out turns off the lights.
//!
//! ```rust
//! # use pgdo::{cluster::Cluster, runtime::strategy::Strategy};
//! let cluster_dir = tempfile::tempdir()?;
//! let cluster = Cluster::new(cluster_dir.path().join("data"), Strategy::default())?;
//! let session = cluster.session(&[])?;
//! assert!(session.running()?);
//! let databases = session.databases()?;
//! session.end()?; // Or drop it; see `Session::end` for the difference.
//! # Ok::<(), pgdo::cluster::ClusterError>(())
//! ```
//!
//! Coordination uses [`flock(2)`](https://linux.die.net/man/2/flock) locks on
//! a file: a shared lock while the session is held; an exclusive lock to create
//! and start the cluster, and to stop or destroy it.
//!
//! Starting, ending, and dropping a session all block, sometimes for a while:
//! they may wait for other processes to release locks, and they run `pg_ctl`.
//! This is safe within an async context, but blocks the current thread; to
//! avoid that, start and end sessions within something like Tokio's
//! `spawn_blocking`. See also [Blocking][`Cluster#blocking`].

use std::os::unix::prelude::OsStrExt;
use std::time::Duration;
use std::{fs, ops};

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
        // The lock is named for the data directory's canonical path, so the
        // directory must exist first. This is duplicative – `Cluster::create`
        // also creates the data directory – but necessary.
        fs::create_dir_all(&self.datadir)?;
        self.datadir = self.datadir.canonicalize()?;
        let lock = startup(lock_for(&self)?, &self, options)?;
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
        match lock.try_lock_exclusive()? {
            // The cluster is in use elsewhere. There's nothing more to do.
            Left(lock) => {
                lock.unlock()?;
                Ok(State::Unmodified)
            }
            // We have an exclusive lock, so we can stop or destroy the cluster.
            // The lock is released when `lock` is dropped.
            Right(_lock) => match self.finish {
                Finish::Stop => self.cluster.stop(),
                Finish::Destroy => self.cluster.destroy(),
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

/// Namespace for `UUIDv5` lock names.
#[allow(clippy::unreadable_literal)]
const UUID_NS: uuid::Uuid = uuid::Uuid::from_u128(93875103436633470414348750305797058811);

/// The lock file for the given cluster, named for its data directory.
fn lock_for(cluster: &Cluster) -> Result<lock::UnlockedFile, ClusterError> {
    let lock_name = cluster.datadir.as_os_str().as_bytes();
    let lock_uuid = uuid::Uuid::new_v5(&UUID_NS, lock_name);
    Ok(lock::UnlockedFile::try_from(&lock_uuid)?)
}

/// Obtain a shared lock on the cluster, creating and starting it if necessary.
fn startup(
    mut lock: lock::UnlockedFile,
    cluster: &Cluster,
    options: Options<'_>,
) -> Result<lock::LockedFileShared, ClusterError> {
    loop {
        lock = match lock.try_lock_exclusive()? {
            Left(lock) => {
                // The cluster is locked elsewhere, shared or exclusively. We
                // optimistically take a shared lock. If the other lock is also
                // shared, this will not block. If the other lock is exclusive,
                // this will block until that lock is released (or changed to a
                // shared lock).
                let lock = lock.lock_shared()?;
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
                let lock = lock.unlock()?;
                let delay = 200 + (rand::rng().next_u32() % 800);
                std::thread::sleep(Duration::from_millis(u64::from(delay)));
                lock
            }
            Right(lock) => {
                // We have an exclusive lock, so try to start the cluster. If
                // this fails, the lock is released when `lock` is dropped.
                cluster.start(options)?;
                // Once started, downgrade to a shared lock.
                return Ok(lock.lock_shared()?);
            }
        };
    }
}
