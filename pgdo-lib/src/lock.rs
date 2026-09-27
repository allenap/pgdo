//! File-based locking using [`flock(2)`](https://linux.die.net/man/2/flock).
//!
//! You must start with an [`UnlockedFile`].
//!
//! ```rust
//! let lockdir = tempfile::tempdir()?;
//! # use pgdo::lock::UnlockedFile;
//! let mut lock = UnlockedFile::try_from(lockdir.path().join("foo").as_path())?;
//! let lock = lock.lock_shared()?;
//! let lock = lock.lock_exclusive()?;
//! let lock = lock.unlock()?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Dropping a [`LockedFileShared`] or [`LockedFileExclusive`] will ordinarily
//! drop the underlying `flock`-based lock by virtue of dropping the [`File`]
//! they each wrap. However, if the file descriptor was duplicated prior to
//! creating the initial [`UnlockedFile`], the lock will persist as long as that
//! descriptor remains valid.

// Ignore deprecation warnings, for now, regarding `nix::fcntl::flock`, since
// the suggested replacement, `nix::fcntl::Flock`, does not provide the same
// functionality. This change was made in the `nix` crate on 2023-12-03; see
// https://github.com/nix-rust/nix/pull/2170. Some limitations of the new API
// reported 2024-04-07; see https://github.com/nix-rust/nix/issues/2356.
#![allow(deprecated)]

use std::fs::File;
use std::os::unix::io::AsRawFd;

use either::{Either, Left, Right};
use nix::errno::Errno;
use nix::fcntl::{flock, FlockArg};
use nix::Result;

#[derive(Debug)]
pub struct UnlockedFile(File);
#[derive(Debug)]
pub struct LockedFileShared(File);
#[derive(Debug)]
pub struct LockedFileExclusive(File);

// Allow access to the underlying file, e.g. to check its metadata. Do not use
// this to change or remove locks.
impl AsRef<File> for UnlockedFile {
    fn as_ref(&self) -> &File {
        &self.0
    }
}

impl AsRef<File> for LockedFileShared {
    fn as_ref(&self) -> &File {
        &self.0
    }
}

impl AsRef<File> for LockedFileExclusive {
    fn as_ref(&self) -> &File {
        &self.0
    }
}

impl From<File> for UnlockedFile {
    fn from(file: File) -> Self {
        Self(file)
    }
}

impl TryFrom<&std::path::Path> for UnlockedFile {
    type Error = std::io::Error;

    fn try_from(path: &std::path::Path) -> std::io::Result<Self> {
        std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map(UnlockedFile)
    }
}

impl TryFrom<&std::path::PathBuf> for UnlockedFile {
    type Error = std::io::Error;

    fn try_from(path: &std::path::PathBuf) -> std::io::Result<Self> {
        Self::try_from(path.as_path())
    }
}

#[allow(unused)]
impl UnlockedFile {
    pub fn try_lock_shared(self) -> Result<Either<Self, LockedFileShared>> {
        match flock(self.0.as_raw_fd(), FlockArg::LockSharedNonblock) {
            Ok(()) => Ok(Right(LockedFileShared(self.0))),
            Err(Errno::EAGAIN) => Ok(Left(self)),
            Err(err) => Err(err),
        }
    }

    pub fn lock_shared(self) -> Result<LockedFileShared> {
        flock(self.0.as_raw_fd(), FlockArg::LockShared)?;
        Ok(LockedFileShared(self.0))
    }

    pub fn try_lock_exclusive(self) -> Result<Either<Self, LockedFileExclusive>> {
        match flock(self.0.as_raw_fd(), FlockArg::LockExclusiveNonblock) {
            Ok(()) => Ok(Right(LockedFileExclusive(self.0))),
            Err(Errno::EAGAIN) => Ok(Left(self)),
            Err(err) => Err(err),
        }
    }

    pub fn lock_exclusive(self) -> Result<LockedFileExclusive> {
        flock(self.0.as_raw_fd(), FlockArg::LockExclusive)?;
        Ok(LockedFileExclusive(self.0))
    }
}

#[allow(unused)]
impl LockedFileShared {
    pub fn try_lock_exclusive(self) -> Result<Either<Self, LockedFileExclusive>> {
        match flock(self.0.as_raw_fd(), FlockArg::LockExclusiveNonblock) {
            Ok(()) => Ok(Right(LockedFileExclusive(self.0))),
            Err(Errno::EAGAIN) => Ok(Left(self)),
            Err(err) => Err(err),
        }
    }

    pub fn lock_exclusive(self) -> Result<LockedFileExclusive> {
        flock(self.0.as_raw_fd(), FlockArg::LockExclusive)?;
        Ok(LockedFileExclusive(self.0))
    }

    pub fn try_unlock(self) -> Result<Either<Self, UnlockedFile>> {
        match flock(self.0.as_raw_fd(), FlockArg::UnlockNonblock) {
            Ok(()) => Ok(Right(UnlockedFile(self.0))),
            Err(Errno::EAGAIN) => Ok(Left(self)),
            Err(err) => Err(err),
        }
    }

    pub fn unlock(self) -> Result<UnlockedFile> {
        flock(self.0.as_raw_fd(), FlockArg::Unlock)?;
        Ok(UnlockedFile(self.0))
    }
}

#[allow(unused)]
impl LockedFileExclusive {
    pub fn try_lock_shared(self) -> Result<Either<Self, LockedFileShared>> {
        match flock(self.0.as_raw_fd(), FlockArg::LockSharedNonblock) {
            Ok(()) => Ok(Right(LockedFileShared(self.0))),
            Err(Errno::EAGAIN) => Ok(Left(self)),
            Err(err) => Err(err),
        }
    }

    pub fn lock_shared(self) -> Result<LockedFileShared> {
        flock(self.0.as_raw_fd(), FlockArg::LockShared)?;
        Ok(LockedFileShared(self.0))
    }

    pub fn try_unlock(self) -> Result<Either<Self, UnlockedFile>> {
        match flock(self.0.as_raw_fd(), FlockArg::UnlockNonblock) {
            Ok(()) => Ok(Right(UnlockedFile(self.0))),
            Err(Errno::EAGAIN) => Ok(Left(self)),
            Err(err) => Err(err),
        }
    }

    pub fn unlock(self) -> Result<UnlockedFile> {
        flock(self.0.as_raw_fd(), FlockArg::Unlock)?;
        Ok(UnlockedFile(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::UnlockedFile;

    use std::fs::OpenOptions;
    use std::io;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;

    use either::Left;
    use nix::fcntl::{flock, FlockArg};

    fn can_lock<P: AsRef<Path>>(filename: P, exclusive: bool) -> nix::Result<()> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(filename)
            .unwrap();
        let mode = if exclusive {
            FlockArg::LockExclusiveNonblock
        } else {
            FlockArg::LockSharedNonblock
        };
        flock(file.as_raw_fd(), mode)
    }

    fn can_lock_exclusive<P: AsRef<Path>>(filename: P) -> nix::Result<()> {
        can_lock(filename, true)
    }

    fn can_lock_shared<P: AsRef<Path>>(filename: P) -> nix::Result<()> {
        can_lock(filename, false)
    }

    /// Retry `check` for up to a second until it succeeds.
    ///
    /// Other tests in this process spawn child processes. A child forked while
    /// a lock file here is open inherits a copy of its file descriptor – and so
    /// its `flock` – until it `exec`s, so a lock can briefly outlive the `File`
    /// that took it. Checks that a file is lockable must allow for that.
    fn eventually(check: impl Fn() -> nix::Result<()>) -> nix::Result<()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            match check() {
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                result => return result,
            }
        }
    }

    #[test]
    fn file_lock_exclusive_takes_exclusive_flock() -> io::Result<()> {
        let lockdir = tempfile::tempdir()?;
        let lockfile = lockdir.path().join("lock");
        let lock = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&lockfile)
            .map(UnlockedFile::from)?;

        assert_eq!(Ok(()), eventually(|| can_lock_exclusive(&lockfile)));
        assert_eq!(Ok(()), eventually(|| can_lock_shared(&lockfile)));

        let lock = lock.lock_exclusive()?;

        assert_ne!(Ok(()), can_lock_exclusive(&lockfile));
        assert_ne!(Ok(()), can_lock_shared(&lockfile));

        lock.unlock()?;

        assert_eq!(Ok(()), eventually(|| can_lock_exclusive(&lockfile)));
        assert_eq!(Ok(()), eventually(|| can_lock_shared(&lockfile)));

        Ok(())
    }

    #[test]
    fn file_try_lock_exclusive_does_not_block_on_existing_shared_lock() -> io::Result<()> {
        let lockdir = tempfile::tempdir()?;
        let lockfile = lockdir.path().join("lock");
        let open_lockfile = || {
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(&lockfile)
                .map(UnlockedFile::from)
        };

        let _lock_shared = open_lockfile()?.lock_shared()?;

        assert!(matches!(open_lockfile()?.try_lock_exclusive(), Ok(Left(_))));

        Ok(())
    }

    #[test]
    fn file_try_lock_exclusive_does_not_block_on_existing_exclusive_lock() -> io::Result<()> {
        let lockdir = tempfile::tempdir()?;
        let lockfile = lockdir.path().join("lock");
        let open_lockfile = || {
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(&lockfile)
                .map(UnlockedFile::from)
        };

        let _lock_exclusive = open_lockfile()?.lock_exclusive()?;

        assert!(matches!(open_lockfile()?.try_lock_exclusive(), Ok(Left(_)),));

        Ok(())
    }
}
