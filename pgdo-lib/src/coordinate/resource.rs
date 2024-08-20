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
use std::marker::PhantomData;

// ----------------------------------------------------------------------------

pub trait FacetFree<'a> {
    type FacetFree;
    fn facet_free(&'a self) -> Self::FacetFree;
}

pub trait FacetShared<'a> {
    type FacetShared;
    fn facet_shared(&'a self) -> Self::FacetShared;
}

pub trait FacetExclusive<'a> {
    type FacetExclusive;
    fn facet_exclusive(&'a self) -> Self::FacetExclusive;
}

// ----------------------------------------------------------------------------

/// An unlocked/free resource.
pub struct ResourceFree<'a, R: Subject> {
    lock: lock::UnlockedFile,
    inner: R,
    phantom: PhantomData<&'a R>,
}

impl<'a, R: Subject> ResourceFree<'a, R> {
    pub fn new(lock: lock::UnlockedFile, inner: R) -> Self {
        Self { lock, inner, phantom: PhantomData }
    }

    /// Attempt to obtain a shared lock on the resource.
    pub fn try_shared(
        self,
    ) -> Result<Either<Self, ResourceShared<'a, R>>, CoordinateError<R::Error>> {
        Ok(match self.lock.try_lock_shared()? {
            Left(lock) => Left(Self { inner: self.inner, lock, phantom: PhantomData }),
            Right(lock) => Right(ResourceShared { inner: self.inner, lock, phantom: PhantomData }),
        })
    }

    /// Obtain a shared lock on the resource. Can block indefinitely.
    pub fn shared(self) -> Result<ResourceShared<'a, R>, CoordinateError<R::Error>> {
        let lock = self.lock.lock_shared()?;
        Ok(ResourceShared { inner: self.inner, lock, phantom: PhantomData })
    }

    /// Attempt to obtain an exclusive lock on the resource.
    pub fn try_exclusive(
        self,
    ) -> Result<Either<Self, ResourceExclusive<'a, R>>, CoordinateError<R::Error>> {
        Ok(match self.lock.try_lock_exclusive()? {
            Left(lock) => Left(Self { inner: self.inner, lock, phantom: PhantomData }),
            Right(lock) => {
                Right(ResourceExclusive { inner: self.inner, lock, phantom: PhantomData })
            }
        })
    }

    /// Obtain an exclusive lock on the resource. Can block indefinitely.
    pub fn exclusive(self) -> Result<ResourceExclusive<'a, R>, CoordinateError<R::Error>> {
        let lock = self.lock.lock_exclusive()?;
        Ok(ResourceExclusive { inner: self.inner, lock, phantom: PhantomData })
    }

    /// Disassembles this resource into the lock and the inner, managed, value.
    /// This can only be done from an unlocked/free resource.
    pub fn into_parts(self) -> (lock::UnlockedFile, R) {
        (self.lock, self.inner)
    }
}

impl<'a, R: Subject + FacetFree<'a>> ResourceFree<'a, R> {
    /// Return the [`FacetFree::FacetFree`] of the wrapped resource.
    pub fn facet(&'a self) -> R::FacetFree {
        self.inner.facet_free()
    }
}

// ----------------------------------------------------------------------------

/// A shared resource.
pub struct ResourceShared<'a, R: Subject> {
    lock: lock::LockedFileShared,
    inner: R,
    phantom: PhantomData<&'a R>,
}

impl<'a, R: Subject> ResourceShared<'a, R> {
    pub fn new(lock: lock::LockedFileShared, inner: R) -> Self {
        Self { lock, inner, phantom: PhantomData }
    }

    /// Attempt to obtain an exclusive lock on the resource.
    pub fn try_exclusive(
        self,
    ) -> Result<Either<Self, ResourceExclusive<'a, R>>, CoordinateError<R::Error>> {
        Ok(match self.lock.try_lock_exclusive()? {
            Left(lock) => Left(Self { inner: self.inner, lock, phantom: PhantomData }),
            Right(lock) => {
                Right(ResourceExclusive { inner: self.inner, lock, phantom: PhantomData })
            }
        })
    }

    /// Obtain an exclusive lock on the resource. Can block indefinitely.
    pub fn exclusive(self) -> Result<ResourceExclusive<'a, R>, CoordinateError<R::Error>> {
        let lock = self.lock.lock_exclusive()?;
        Ok(ResourceExclusive { inner: self.inner, lock, phantom: PhantomData })
    }

    /// Attempt to release this resource.
    pub fn try_release(
        self,
    ) -> Result<Either<Self, ResourceFree<'a, R>>, CoordinateError<R::Error>> {
        Ok(match self.lock.try_unlock()? {
            Left(lock) => Left(Self { inner: self.inner, lock, phantom: PhantomData }),
            Right(lock) => Right(ResourceFree { inner: self.inner, lock, phantom: PhantomData }),
        })
    }

    /// Release this resource. Can block indefinitely.
    pub fn release(self) -> Result<ResourceFree<'a, R>, CoordinateError<R::Error>> {
        let lock = self.lock.unlock()?;
        Ok(ResourceFree { inner: self.inner, lock, phantom: PhantomData })
    }
}

impl<'a, R: Subject + FacetShared<'a>> ResourceShared<'a, R> {
    /// Return the [`FacetShared::FacetShared`] of the wrapped resource.
    pub fn facet(&'a self) -> R::FacetShared {
        self.inner.facet_shared()
    }
}

// ----------------------------------------------------------------------------

/// A resource held exclusively.
pub struct ResourceExclusive<'a, R: Subject> {
    lock: lock::LockedFileExclusive,
    inner: R,
    phantom: PhantomData<&'a R>,
}

impl<'a, R: Subject> ResourceExclusive<'a, R> {
    pub fn new(lock: lock::LockedFileExclusive, inner: R) -> Self {
        Self { lock, inner, phantom: PhantomData }
    }

    /// Attempt to obtain a shared lock on the resource.
    pub fn try_shared(
        self,
    ) -> Result<Either<Self, ResourceShared<'a, R>>, CoordinateError<R::Error>> {
        Ok(match self.lock.try_lock_shared()? {
            Left(lock) => Left(Self { inner: self.inner, lock, phantom: PhantomData }),
            Right(lock) => Right(ResourceShared { inner: self.inner, lock, phantom: PhantomData }),
        })
    }

    /// Obtain a shared lock on the resource. Can block indefinitely.
    pub fn shared(self) -> Result<ResourceShared<'a, R>, CoordinateError<R::Error>> {
        let lock = self.lock.lock_shared()?;
        Ok(ResourceShared { inner: self.inner, lock, phantom: PhantomData })
    }

    /// Attempt to release this resource.
    pub fn try_release(
        self,
    ) -> Result<Either<Self, ResourceFree<'a, R>>, CoordinateError<R::Error>> {
        Ok(match self.lock.try_unlock()? {
            Left(lock) => Left(Self { inner: self.inner, lock, phantom: PhantomData }),
            Right(lock) => Right(ResourceFree { inner: self.inner, lock, phantom: PhantomData }),
        })
    }

    /// Release this resource. Can block indefinitely.
    pub fn release(self) -> Result<ResourceFree<'a, R>, CoordinateError<R::Error>> {
        let lock = self.lock.unlock()?;
        Ok(ResourceFree { inner: self.inner, lock, phantom: PhantomData })
    }
}

impl<'a, R: Subject + FacetExclusive<'a>> ResourceExclusive<'a, R> {
    /// Return the [`FacetExclusive::FacetExclusive`] of the wrapped resource.
    pub fn facet(&'a self) -> R::FacetExclusive {
        self.inner.facet_exclusive()
    }
}
