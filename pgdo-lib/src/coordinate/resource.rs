//! Manage a resource that can be started, stopped, and destroyed – i.e. a
//! [`Subject`] – and which has different facets depending on whether it is
//! locked exclusively, shared between multiple users, or unlocked/free.
//!
//! For example, a resource representing a PostgreSQL cluster would allow start,
//! stop, and destroy actions only when it is exclusively locked. The _type_ of
//! an unlocked cluster resource or a shared cluster resource would not even
//! have functions available to start, stop, or destroy the cluster.
//!
//! The intent is to codify safe behaviours into Rust's type system so that we
//! make it hard or impossible to mishandle a resource – and conversely, easier
//! to correctly handle a resource.

use super::{lock, CoordinateError, Subject};
use either::{Either, Left, Right};

// ----------------------------------------------------------------------------

pub trait FacetFree {
    type FacetFree<'a>
    where
        Self: 'a;

    fn facet_free(&self) -> Self::FacetFree<'_>;
}

pub trait FacetShared {
    type FacetShared<'a>
    where
        Self: 'a;

    fn facet_shared(&self) -> Self::FacetShared<'_>;
}

pub trait FacetExclusive {
    type FacetExclusive<'a>
    where
        Self: 'a;

    fn facet_exclusive(&self) -> Self::FacetExclusive<'_>;
}

// ----------------------------------------------------------------------------

/// An unlocked/free resource.
pub struct ResourceFree<S: Subject> {
    lock: lock::UnlockedFile,
    subject: S,
}

impl<S: Subject> ResourceFree<S> {
    pub fn new(lock: lock::UnlockedFile, inner: S) -> Self {
        Self { lock, subject: inner }
    }

    /// Attempt to obtain a shared lock on the resource.
    pub fn try_shared(self) -> Result<Either<Self, ResourceShared<S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_lock_shared()? {
            Left(lock) => Left(Self { subject: self.subject, lock }),
            Right(lock) => Right(ResourceShared { subject: self.subject, lock }),
        })
    }

    /// Obtain a shared lock on the resource. Can block indefinitely.
    pub fn shared(self) -> Result<ResourceShared<S>, CoordinateError<S::Error>> {
        let lock = self.lock.lock_shared()?;
        Ok(ResourceShared { subject: self.subject, lock })
    }

    /// Attempt to obtain an exclusive lock on the resource.
    pub fn try_exclusive(
        self,
    ) -> Result<Either<Self, ResourceExclusive<S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_lock_exclusive()? {
            Left(lock) => Left(Self { subject: self.subject, lock }),
            Right(lock) => Right(ResourceExclusive { subject: self.subject, lock }),
        })
    }

    /// Obtain an exclusive lock on the resource. Can block indefinitely.
    pub fn exclusive(self) -> Result<ResourceExclusive<S>, CoordinateError<S::Error>> {
        let lock = self.lock.lock_exclusive()?;
        Ok(ResourceExclusive { subject: self.subject, lock })
    }

    /// Disassembles this resource into the lock and the inner, managed, value.
    /// This can only be done from an unlocked/free resource.
    pub fn into_parts(self) -> (lock::UnlockedFile, S) {
        (self.lock, self.subject)
    }
}

impl<S: Subject + FacetFree> ResourceFree<S> {
    /// Return the [`FacetFree::FacetFree`] of the wrapped resource.
    pub fn facet(&self) -> S::FacetFree<'_> {
        self.subject.facet_free()
    }
}

// ----------------------------------------------------------------------------

/// A shared resource.
pub struct ResourceShared<S: Subject> {
    lock: lock::LockedFileShared,
    subject: S,
}

impl<S: Subject> ResourceShared<S> {
    pub fn new(lock: lock::LockedFileShared, inner: S) -> Self {
        Self { lock, subject: inner }
    }

    /// Attempt to obtain an exclusive lock on the resource.
    pub fn try_exclusive(
        self,
    ) -> Result<Either<Self, ResourceExclusive<S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_lock_exclusive()? {
            Left(lock) => Left(Self { subject: self.subject, lock }),
            Right(lock) => Right(ResourceExclusive { subject: self.subject, lock }),
        })
    }

    /// Obtain an exclusive lock on the resource. Can block indefinitely.
    pub fn exclusive(self) -> Result<ResourceExclusive<S>, CoordinateError<S::Error>> {
        let lock = self.lock.lock_exclusive()?;
        Ok(ResourceExclusive { subject: self.subject, lock })
    }

    /// Attempt to release this resource.
    pub fn try_release(self) -> Result<Either<Self, ResourceFree<S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_unlock()? {
            Left(lock) => Left(Self { subject: self.subject, lock }),
            Right(lock) => Right(ResourceFree { subject: self.subject, lock }),
        })
    }

    /// Release this resource. Can block indefinitely.
    pub fn release(self) -> Result<ResourceFree<S>, CoordinateError<S::Error>> {
        let lock = self.lock.unlock()?;
        Ok(ResourceFree { subject: self.subject, lock })
    }
}

impl<S: Subject + FacetShared> ResourceShared<S> {
    /// Return the [`FacetShared::FacetShared`] of the wrapped resource.
    pub fn facet(&self) -> S::FacetShared<'_> {
        self.subject.facet_shared()
    }
}

// ----------------------------------------------------------------------------

/// A resource held exclusively.
pub struct ResourceExclusive<S: Subject> {
    lock: lock::LockedFileExclusive,
    subject: S,
}

impl<S: Subject> ResourceExclusive<S> {
    pub fn new(lock: lock::LockedFileExclusive, inner: S) -> Self {
        Self { lock, subject: inner }
    }

    /// Attempt to obtain a shared lock on the resource.
    pub fn try_shared(self) -> Result<Either<Self, ResourceShared<S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_lock_shared()? {
            Left(lock) => Left(Self { subject: self.subject, lock }),
            Right(lock) => Right(ResourceShared { subject: self.subject, lock }),
        })
    }

    /// Obtain a shared lock on the resource. Can block indefinitely.
    pub fn shared(self) -> Result<ResourceShared<S>, CoordinateError<S::Error>> {
        let lock = self.lock.lock_shared()?;
        Ok(ResourceShared { subject: self.subject, lock })
    }

    /// Attempt to release this resource.
    pub fn try_release(self) -> Result<Either<Self, ResourceFree<S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_unlock()? {
            Left(lock) => Left(Self { subject: self.subject, lock }),
            Right(lock) => Right(ResourceFree { subject: self.subject, lock }),
        })
    }

    /// Release this resource. Can block indefinitely.
    pub fn release(self) -> Result<ResourceFree<S>, CoordinateError<S::Error>> {
        let lock = self.lock.unlock()?;
        Ok(ResourceFree { subject: self.subject, lock })
    }
}

impl<S: Subject + FacetExclusive> ResourceExclusive<S> {
    /// Return the [`FacetExclusive::FacetExclusive`] of the wrapped resource.
    pub fn facet(&self) -> S::FacetExclusive<'_> {
        self.subject.facet_exclusive()
    }
}
