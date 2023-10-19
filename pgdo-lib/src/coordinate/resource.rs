use super::{lock, CoordinateError, Subject};
use either::{Either, Left, Right};
use std::marker::PhantomData;

pub trait Faceted<'a> {
    type FacetFree;
    type FacetShared;
    type FacetExclusive;

    fn facet_free(&'a self) -> Self::FacetFree;
    fn facet_shared(&'a self) -> Self::FacetShared;
    fn facet_exclusive(&'a self) -> Self::FacetExclusive;
}

pub struct ResourceFree<'a, S: Subject + Faceted<'a>> {
    lock: lock::UnlockedFile,
    subject: S,
    phantom: PhantomData<&'a S>,
}

impl<'a, S> ResourceFree<'a, S>
where
    S: Subject + Faceted<'a>,
{
    pub fn new(lock: lock::UnlockedFile, subject: S) -> Self {
        Self { lock, subject, phantom: PhantomData }
    }

    pub fn facet(&'a self) -> S::FacetFree {
        self.subject.facet_free()
    }

    pub fn try_shared(
        self,
    ) -> Result<Either<Self, ResourceShared<'a, S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_lock_shared()? {
            Left(lock) => Left(Self { subject: self.subject, lock, phantom: PhantomData }),
            Right(lock) => {
                Right(ResourceShared { subject: self.subject, lock, phantom: PhantomData })
            }
        })
    }

    pub fn shared(self) -> Result<ResourceShared<'a, S>, CoordinateError<S::Error>> {
        let lock = self.lock.lock_shared()?;
        Ok(ResourceShared { subject: self.subject, lock, phantom: PhantomData })
    }

    pub fn try_exclusive(
        self,
    ) -> Result<Either<Self, ResourceExclusive<'a, S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_lock_exclusive()? {
            Left(lock) => Left(Self { subject: self.subject, lock, phantom: PhantomData }),
            Right(lock) => {
                Right(ResourceExclusive { subject: self.subject, lock, phantom: PhantomData })
            }
        })
    }

    pub fn exclusive(self) -> Result<ResourceExclusive<'a, S>, CoordinateError<S::Error>> {
        let lock = self.lock.lock_exclusive()?;
        Ok(ResourceExclusive { subject: self.subject, lock, phantom: PhantomData })
    }
}

pub struct ResourceShared<'a, S: Subject + Faceted<'a>> {
    lock: lock::LockedFileShared,
    subject: S,
    phantom: PhantomData<&'a S>,
}

impl<'a, S> ResourceShared<'a, S>
where
    S: Subject + Faceted<'a>,
{
    pub fn new(lock: lock::LockedFileShared, subject: S) -> Self {
        Self { lock, subject, phantom: PhantomData }
    }

    pub fn facet(&'a self) -> S::FacetShared {
        self.subject.facet_shared()
    }

    pub fn try_exclusive(
        self,
    ) -> Result<Either<Self, ResourceExclusive<'a, S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_lock_exclusive()? {
            Left(lock) => Left(Self { subject: self.subject, lock, phantom: PhantomData }),
            Right(lock) => {
                Right(ResourceExclusive { subject: self.subject, lock, phantom: PhantomData })
            }
        })
    }

    pub fn exclusive(self) -> Result<ResourceExclusive<'a, S>, CoordinateError<S::Error>> {
        let lock = self.lock.lock_exclusive()?;
        Ok(ResourceExclusive { subject: self.subject, lock, phantom: PhantomData })
    }

    pub fn try_release(
        self,
    ) -> Result<Either<Self, ResourceFree<'a, S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_unlock()? {
            Left(lock) => Left(Self { subject: self.subject, lock, phantom: PhantomData }),
            Right(lock) => {
                Right(ResourceFree { subject: self.subject, lock, phantom: PhantomData })
            }
        })
    }

    pub fn release(self) -> Result<ResourceFree<'a, S>, CoordinateError<S::Error>> {
        let lock = self.lock.unlock()?;
        Ok(ResourceFree { subject: self.subject, lock, phantom: PhantomData })
    }
}

pub struct ResourceExclusive<'a, S: Subject + Faceted<'a>> {
    lock: lock::LockedFileExclusive,
    subject: S,
    phantom: PhantomData<&'a S>,
}

impl<'a, S> ResourceExclusive<'a, S>
where
    S: Subject + Faceted<'a>,
{
    pub fn new(lock: lock::LockedFileExclusive, subject: S) -> Self {
        Self { lock, subject, phantom: PhantomData }
    }

    pub fn facet(&'a self) -> S::FacetExclusive {
        self.subject.facet_exclusive()
    }

    pub fn try_shared(
        self,
    ) -> Result<Either<Self, ResourceShared<'a, S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_lock_shared()? {
            Left(lock) => Left(Self { subject: self.subject, lock, phantom: PhantomData }),
            Right(lock) => {
                Right(ResourceShared { subject: self.subject, lock, phantom: PhantomData })
            }
        })
    }

    pub fn shared(self) -> Result<ResourceShared<'a, S>, CoordinateError<S::Error>> {
        let lock = self.lock.lock_shared()?;
        Ok(ResourceShared { subject: self.subject, lock, phantom: PhantomData })
    }

    pub fn try_release(
        self,
    ) -> Result<Either<Self, ResourceFree<'a, S>>, CoordinateError<S::Error>> {
        Ok(match self.lock.try_unlock()? {
            Left(lock) => Left(Self { subject: self.subject, lock, phantom: PhantomData }),
            Right(lock) => {
                Right(ResourceFree { subject: self.subject, lock, phantom: PhantomData })
            }
        })
    }

    pub fn release(self) -> Result<ResourceFree<'a, S>, CoordinateError<S::Error>> {
        let lock = self.lock.unlock()?;
        Ok(ResourceFree { subject: self.subject, lock, phantom: PhantomData })
    }
}
