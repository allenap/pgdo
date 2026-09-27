//! Discover and use PostgreSQL installations.
//!
//! You may have many versions of PostgreSQL installed on a system. For example,
//! on an Ubuntu system, they may be in `/usr/lib/postgresql/*`. On macOS using
//! Homebrew, you may find them in `/usr/local/Cellar/postgresql@*`. [`Runtime`]
//! represents one such runtime; and the [`strategy`] module has tools for
//! finding and selecting runtimes.

mod cache;
pub mod constraint;
mod error;
pub mod strategy;

use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::util;
use crate::version;
pub use error::RuntimeError;

/// The oldest version of PostgreSQL that pgdo supports.
///
/// The rule: this is the oldest major version still supported by the
/// PostgreSQL project, or 15, whichever is greater. Bump it in the first
/// release after a major version reaches end-of-life; see the [PostgreSQL
/// "Versioning Policy" page][versioning] for dates.
///
/// [versioning]: https://www.postgresql.org/support/versioning/
pub const MINIMUM_VERSION: version::Version = version::Version::Post10(15, 0);

/// Does pgdo support the given version of PostgreSQL?
///
/// This is pgdo's policy, separate from the [`version`] module, which can parse
/// and represent any version of PostgreSQL. See [`MINIMUM_VERSION`].
///
/// Note that [`version::Version`] orders all pre-10 versions before 10 and
/// later, so a plain comparison works across both versioning schemes.
pub fn is_supported<V: Into<version::Version>>(version: V) -> bool {
    version.into() >= MINIMUM_VERSION
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Runtime {
    /// Path to the directory containing the `pg_ctl` executable and other
    /// PostgreSQL binaries.
    pub bindir: PathBuf,

    /// Version of this runtime.
    pub version: version::Version,
}

impl Runtime {
    pub fn new<P: AsRef<Path>>(bindir: P) -> Result<Self, RuntimeError> {
        let version = cache::version(bindir.as_ref().join("pg_ctl"))?;
        Ok(Self { bindir: bindir.as_ref().to_owned(), version })
    }

    /// Does pgdo support this runtime? See [`is_supported`].
    pub fn is_supported(&self) -> bool {
        is_supported(self.version)
    }

    /// Return a [`Command`] prepped to run the given `program` in this
    /// PostgreSQL runtime.
    ///
    /// ```rust
    /// # use pgdo::runtime::{RuntimeError, strategy::{Strategy, StrategyLike}};
    /// # let runtime = Strategy::default().fallback().unwrap();
    /// let version = runtime.execute("pg_ctl").arg("--version").output()?;
    /// # Ok::<(), RuntimeError>(())
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if it's not possible to calculate `PATH`; see
    /// [`env::join_paths`].
    pub fn execute<T: AsRef<OsStr>>(&self, program: T) -> Command {
        let mut command = Command::new(self.bindir.join(program.as_ref()));
        command.env(
            "PATH",
            util::prepend_to_path(&self.bindir, env::var_os("PATH")).unwrap(),
        );
        command
    }

    /// Return a [`Command`] prepped to run the given `program` with this
    /// PostgreSQL runtime at the front of `PATH`. This is very similar to
    /// [`Self::execute`] except it does not qualify the given program name with
    /// [`Self::bindir`].
    ///
    /// ```rust
    /// # use pgdo::runtime::{RuntimeError, strategy::{Strategy, StrategyLike}};
    /// # let runtime = Strategy::default().fallback().unwrap();
    /// let hello = runtime.command("bash").arg("-c").arg("echo hello").output()?;
    /// # Ok::<(), RuntimeError>(())
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if it's not possible to calculate `PATH`; see
    /// [`env::join_paths`].
    pub fn command<T: AsRef<OsStr>>(&self, program: T) -> Command {
        let mut command = Command::new(program);
        command.env(
            "PATH",
            util::prepend_to_path(&self.bindir, env::var_os("PATH")).unwrap(),
        );
        command
    }
}

#[cfg(test)]
mod tests {
    use super::{is_supported, Runtime, RuntimeError, MINIMUM_VERSION};
    use crate::version::{PartialVersion, Version};

    use std::env;
    use std::path::PathBuf;

    type TestResult = Result<(), RuntimeError>;

    fn find_bindir() -> PathBuf {
        env::split_paths(&env::var_os("PATH").expect("PATH not set"))
            .find(|path| path.join("pg_ctl").exists())
            .expect("pg_ctl not on PATH")
    }

    #[test]
    fn runtime_new() -> TestResult {
        let bindir = find_bindir();
        let pg = Runtime::new(&bindir)?;
        assert_eq!(bindir, pg.bindir);
        Ok(())
    }

    #[test]
    fn supports_versions_from_minimum_version() {
        let Version::Post10(major, minor) = MINIMUM_VERSION else {
            panic!("expected minimum version to be 10 or later");
        };
        assert!(is_supported(MINIMUM_VERSION));
        assert!(is_supported(Version::Post10(major, minor + 1)));
        assert!(is_supported(Version::Post10(major + 1, 0)));
        assert!(is_supported(PartialVersion::Post10m(major)));
    }

    #[test]
    fn does_not_support_versions_before_minimum_version() {
        let Version::Post10(major, _) = MINIMUM_VERSION else {
            panic!("expected minimum version to be 10 or later");
        };
        assert!(!is_supported(Version::Post10(major - 1, 99)));
        assert!(!is_supported(PartialVersion::Post10m(major - 1)));
        assert!(!is_supported(Version::Pre10(9, 6, 24)));
        assert!(!is_supported(PartialVersion::Pre10m(9, 6)));
    }
}
