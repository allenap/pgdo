//! File-based locking using [`flock(2)`](https://linux.die.net/man/2/flock).
//!
//! You must start with an [`UnlockedFile`].
//!
//! ```rust
//! let lock_dir = tempfile::tempdir()?;
//! # use pgdo::lock::UnlockedFile;
//! let mut lock = UnlockedFile::try_from(lock_dir.path().join("foo").as_path())?;
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
use uuid::Uuid;

#[derive(Debug)]
pub struct UnlockedFile(File);
#[derive(Debug)]
pub struct LockedFileShared(File);
#[derive(Debug)]
pub struct LockedFileExclusive(File);

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

impl TryFrom<&Uuid> for UnlockedFile {
    type Error = std::io::Error;

    fn try_from(uuid: &Uuid) -> std::io::Result<Self> {
        let mut buffer = Uuid::encode_buffer();
        let uuid = uuid.simple().encode_lower(&mut buffer);
        let filename = ".pgdo.".to_owned() + uuid;
        let path = std::env::temp_dir().join(filename);
        UnlockedFile::try_from(&*path)
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

    fn can_lock<P: AsRef<Path>>(filename: P, exclusive: bool) -> bool {
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
        flock(file.as_raw_fd(), mode).is_ok()
    }

    fn can_lock_exclusive<P: AsRef<Path>>(filename: P) -> bool {
        can_lock(filename, true)
    }

    fn can_lock_shared<P: AsRef<Path>>(filename: P) -> bool {
        can_lock(filename, false)
    }

    #[test]
    fn file_lock_exclusive_takes_exclusive_flock() -> io::Result<()> {
        let lock_dir = tempfile::tempdir()?;
        let lock_filename = lock_dir.path().join("lock");
        let lock = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&lock_filename)
            .map(UnlockedFile::from)?;

        assert!(can_lock_exclusive(&lock_filename));
        assert!(can_lock_shared(&lock_filename));

        let lock = lock.lock_exclusive()?;

        assert!(!can_lock_exclusive(&lock_filename));
        assert!(!can_lock_shared(&lock_filename));

        lock.unlock()?;

        assert!(can_lock_exclusive(&lock_filename));
        assert!(can_lock_shared(&lock_filename));

        Ok(())
    }

    #[test]
    fn file_try_lock_exclusive_does_not_block_on_existing_shared_lock() -> io::Result<()> {
        let lock_dir = tempfile::tempdir()?;
        let lock_filename = lock_dir.path().join("lock");
        let open_lock_file = || {
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(&lock_filename)
                .map(UnlockedFile::from)
        };

        let _lock_shared = open_lock_file()?.lock_shared()?;

        assert!(matches!(
            open_lock_file()?.try_lock_exclusive(),
            Ok(Left(_))
        ));

        Ok(())
    }

    #[test]
    fn file_try_lock_exclusive_does_not_block_on_existing_exclusive_lock() -> io::Result<()> {
        let lock_dir = tempfile::tempdir()?;
        let lock_filename = lock_dir.path().join("lock");
        let open_lock_file = || {
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(&lock_filename)
                .map(UnlockedFile::from)
        };

        let _lock_exclusive = open_lock_file()?.lock_exclusive()?;

        assert!(matches!(
            open_lock_file()?.try_lock_exclusive(),
            Ok(Left(_)),
        ));

        Ok(())
    }
}
