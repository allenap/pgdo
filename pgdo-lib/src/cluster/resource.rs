//! A [resource][`crate::coordinate::resource`] for a [`Cluster`].

use std::time::Duration;
use std::{ffi::OsStr, process::ExitStatus};

use either::{Either, Left, Right};
use rand::RngCore;

use super::{
    coordinate::{resource, CoordinateError, State},
    exists, Cluster, ClusterError,
};

// ----------------------------------------------------------------------------

pub type ResourceFree<'a> = resource::ResourceFree<'a, Cluster>;
pub type ResourceShared<'a> = resource::ResourceShared<'a, Cluster>;
pub type ResourceExclusive<'a> = resource::ResourceExclusive<'a, Cluster>;

// ----------------------------------------------------------------------------

impl From<ClusterError> for CoordinateError<ClusterError> {
    fn from(err: ClusterError) -> Self {
        Self::ControlError(err)
    }
}

// ----------------------------------------------------------------------------

impl<'a> resource::Faceted<'a> for Cluster {
    type FacetFree = ClusterFree<'a>;
    type FacetShared = ClusterShared<'a>;
    type FacetExclusive = ClusterExclusive<'a>;

    fn facet_free(&'a self) -> Self::FacetFree {
        ClusterFree { cluster: self }
    }

    fn facet_shared(&'a self) -> Self::FacetShared {
        ClusterShared { cluster: self }
    }

    fn facet_exclusive(&'a self) -> Self::FacetExclusive {
        ClusterExclusive { cluster: self }
    }
}

// ----------------------------------------------------------------------------

pub struct ClusterFree<'a> {
    cluster: &'a Cluster,
}

/// When the cluster is not locked, all one can do is check for its existence
/// and if it is running. However, be careful of TOCTOU errors if you're using
/// this for more than informational purposes.
///
/// [TOCTOU]: https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use
impl<'a> ClusterFree<'a> {
    pub fn exists(&self) -> Result<bool, ClusterError> {
        Ok(exists(self.cluster))
    }

    pub fn running(&self) -> Result<bool, ClusterError> {
        self.cluster.running()
    }
}

// ----------------------------------------------------------------------------

pub struct ClusterShared<'a> {
    cluster: &'a Cluster,
}

/// When the cluster is shared, one can connect to the cluster, and execute
/// processes. It is possible to abuse this and shutdown the cluster, for
/// example, but that's on you; there's only so much that this library can do to
/// prevent misuse.
impl<'a> ClusterShared<'a> {
    pub fn exists(&self) -> Result<bool, ClusterError> {
        Ok(exists(self.cluster))
    }

    pub fn running(&self) -> Result<bool, ClusterError> {
        self.cluster.running()
    }

    /// Forwards to [`Cluster::pool`].
    pub fn pool(&self, database: Option<&str>) -> sqlx::PgPool {
        self.cluster.pool(database)
    }

    /// Forwards to [`Cluster::exec`].
    pub fn exec<T: AsRef<OsStr>>(
        &self,
        database: Option<&str>,
        command: T,
        args: &[T],
    ) -> Result<ExitStatus, ClusterError> {
        self.cluster.exec(database, command, args)
    }
}

// ----------------------------------------------------------------------------

pub struct ClusterExclusive<'a> {
    cluster: &'a Cluster,
}

/// When you have exclusive control of a cluster, you can start, stop, destroy,
/// reconfigure it – anything.
impl<'a> ClusterExclusive<'a> {
    pub fn start(&self) -> Result<State, ClusterError> {
        self.cluster.start()
    }

    pub fn stop(&self) -> Result<State, ClusterError> {
        self.cluster.stop()
    }

    pub fn destroy(&self) -> Result<State, ClusterError> {
        self.cluster.destroy()
    }

    pub fn exists(&self) -> Result<bool, ClusterError> {
        Ok(exists(self.cluster))
    }

    pub fn running(&self) -> Result<bool, ClusterError> {
        self.cluster.running()
    }

    /// Forwards to [`Cluster::pool`].
    pub fn pool(&self, database: Option<&str>) -> sqlx::PgPool {
        self.cluster.pool(database)
    }

    /// Forwards to [`Cluster::exec`].
    pub fn exec<T: AsRef<OsStr>>(
        &self,
        database: Option<&str>,
        command: T,
        args: &[T],
    ) -> Result<ExitStatus, ClusterError> {
        self.cluster.exec(database, command, args)
    }
}

// ----------------------------------------------------------------------------

pub type StartupResource<'a> = Either<ResourceShared<'a>, ResourceExclusive<'a>>;

/// Creates the cluster, if it doesn't already exist, and starts it in a
/// cooperative manner.
///
/// The return value has two parts: the state, [`State`], and the resource,
/// [`StartupResource`].
///
/// The state is [`State::Unmodified`] if the cluster was already running, else
/// [`State::Modified`] if the cluster was created or started by this function.
///
/// The resource is [`Left(ResourceShared)`] if the cluster is already in use,
/// or [`Right(ResourceExclusive)`] otherwise. Typically one would drop the
/// exclusive hold down to shared as soon as possible, but the option is there
/// to do maintenance, for example, that requires an exclusive hold.
pub fn startup(
    mut resource: ResourceFree,
) -> Result<(State, StartupResource), CoordinateError<ClusterError>> {
    loop {
        resource = match resource.try_exclusive() {
            Ok(Left(resource)) => {
                // The resource is locked exclusively by someone/something else.
                // Switch to a shared lock optimistically. This blocks until we
                // get the shared lock.
                let resource = resource.shared()?;
                // The resource may have been started while that exclusive lock
                // was held, so we must check if the resource is running now –
                // otherwise we loop back to the top again.
                if resource.facet().running()? {
                    return Ok((State::Unmodified, Left(resource)));
                }
                // Release all locks then sleep for a random time between 200ms
                // and 1000ms in an attempt to make sure that when there are
                // many competing processes one of them rapidly acquires an
                // exclusive lock and is able to create and start the resource.
                let resource = resource.release()?;
                let delay = rand::thread_rng().next_u32();
                let delay = 200 + (delay % 800);
                let delay = Duration::from_millis(u64::from(delay));
                std::thread::sleep(delay);
                resource
            }
            Ok(Right(resource)) => {
                // We have an exclusive lock, so try to start the resource.
                let state = resource.facet().start()?;
                return Ok((state, Right(resource)));
            }
            Err(err) => return Err(err),
        };
    }
}

/// Similar to [`startup`] but does not create the cluster, and thus only
/// succeeds if the cluster already exists.
pub fn startup_if_exists(
    mut resource: ResourceFree,
) -> Result<(State, StartupResource), CoordinateError<ClusterError>> {
    loop {
        resource = match resource.try_exclusive() {
            Ok(Left(resource)) => {
                // The resource is locked exclusively by someone/something else.
                // Switch to a shared lock optimistically. This blocks until we
                // get the shared lock.
                let resource = resource.shared()?;
                // The resource may have been started while that exclusive lock
                // was held, so we must check if the resource is running now –
                // otherwise we loop back to the top again.
                if resource.facet().running()? {
                    return Ok((State::Unmodified, Left(resource)));
                }
                // Release all locks then sleep for a random time between 200ms
                // and 1000ms in an attempt to make sure that when there are
                // many competing processes one of them rapidly acquires an
                // exclusive lock and is able to create and start the resource.
                let resource = resource.release()?;
                let delay = rand::thread_rng().next_u32();
                let delay = 200 + (delay % 800);
                let delay = Duration::from_millis(u64::from(delay));
                std::thread::sleep(delay);
                resource
            }
            Ok(Right(resource)) => {
                // We have an exclusive lock, so try to start the resource.
                let facet = resource.facet();
                let state = if facet.exists()? {
                    facet.start()?
                } else {
                    return Err(CoordinateError::DoesNotExist);
                };
                return Ok((state, Right(resource)));
            }
            Err(err) => return Err(err),
        };
    }
}

/// Shuts down the cluster if it is running and if there are no other concurrent
/// users.
///
/// The return value has two parts: the state, [`State`], and the resource.
///
/// The state is [`State::Unmodified`] if the cluster could not be shut down or
/// if it was already shut down, else [`State::Modified`].
///
/// The resource is [`Left(ResourceShared)`] if the cluster is already in use –
/// i.e. the resource passed in is returned – else [`Right(ResourceExclusive)`]
/// otherwise.
pub fn shutdown(
    resource: ResourceShared,
) -> Result<(State, Either<ResourceShared, ResourceExclusive>), CoordinateError<ClusterError>> {
    match resource.try_exclusive() {
        Ok(Left(resource)) => {
            // The resource is in use by someone/something else. There's nothing
            // more we can do here.
            Ok((State::Unmodified, Left(resource)))
        }
        Ok(Right(resource)) => {
            // We have an exclusive lock, so we can mutate the resource.
            match resource.facet().stop() {
                Ok(state) => Ok((state, Right(resource))),
                Err(err) => {
                    resource.release()?;
                    Err(err)?
                }
            }
        }
        Err(err) => Err(err),
    }
}

/// Similar to [`shutdown`] but also attempts to destroy the cluster, i.e.
/// remove it entirely from the filesystem.
pub fn destroy(
    resource: ResourceShared,
) -> Result<(State, Either<ResourceShared, ResourceExclusive>), CoordinateError<ClusterError>> {
    match resource.try_exclusive() {
        Ok(Left(resource)) => {
            // The resource is in use by someone/something else. There's nothing
            // more we can do here.
            Ok((State::Unmodified, Left(resource)))
        }
        Ok(Right(resource)) => {
            // We have an exclusive lock, so we can mutate the resource.
            match resource.facet().destroy() {
                Ok(state) => Ok((state, Right(resource))),
                Err(err) => {
                    resource.release()?;
                    Err(err)?
                }
            }
        }
        Err(err) => Err(err),
    }
}
