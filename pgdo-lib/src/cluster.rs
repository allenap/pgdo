//! Create, start, introspect, stop, and destroy PostgreSQL clusters.

pub mod config;
mod error;

use std::ffi::{OsStr, OsString};
use std::os::unix::prelude::OsStringExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::{env, fs, io};

use nix::errno::Errno;
use postgres;
use shell_quote::sh::escape_into;
pub use sqlx;

use crate::runtime::{
    strategy::{Strategy, StrategyLike},
    Runtime,
};
use crate::{
    coordinate::{
        self,
        State::{self, *},
    },
    version,
};
pub use error::ClusterError;

/// `template0` is always present in a PostgreSQL cluster.
///
/// This database is a template database, though it's used to a lesser extent
/// than `template1`.
///
/// `template0` should never be modified so it's rare to connect to this
/// database, even as a convenient default – see [`DATABASE_TEMPLATE1`] for an
/// explanation as to why.
pub static DATABASE_TEMPLATE0: &str = "template0";

/// `template1` is always present in a PostgreSQL cluster.
///
/// This database is used as the default template for creating new databases.
///
/// Connecting to a database prevents other sessions from creating new databases
/// using that database as a template; see PostgreSQL's [Template Databases][]
/// page to learn more about this limitation. Since `template1` is the default
/// template, connecting to this database prevents other sessions from using a
/// plain `CREATE DATABASE` command. In other words, it may be a good idea to
/// connect to this database _only_ when modifying it, not as a default.
///
/// [Template Databases]:
///     https://www.postgresql.org/docs/current/manage-ag-templatedbs.html
pub static DATABASE_TEMPLATE1: &str = "template0";

/// `postgres` is always created by `initdb` when building a PostgreSQL cluster.
///
/// From `initdb(1)`:
/// > The postgres database is a default database meant for use by users,
/// > utilities and third party applications.
///
/// Given that it can be problematic to connect to `template0` and `template1` –
/// see [`DATABASE_TEMPLATE1`] for an explanation – `postgres` is a convenient
/// default, hence this library uses `postgres` as the database from which to
/// perform administrative tasks, for example.
///
/// Unfortunately, `postgres` can be dropped, in which case some of the
/// functionality of this crate will be broken. Ideally we could connect to a
/// PostgreSQL cluster without specifying a database, but that is presently not
/// possible.
pub static DATABASE_POSTGRES: &str = "postgres";

/// Representation of a PostgreSQL cluster.
///
/// The cluster may not yet exist on disk. It may exist but be stopped, or it
/// may be running. The methods here can be used to create, start, introspect,
/// stop, and destroy the cluster. There's no protection against concurrent
/// changes to the cluster made by other processes, but the functions in the
/// [`coordinate`][`crate::coordinate`] module may help.
#[derive(Debug)]
pub struct Cluster {
    /// The data directory of the cluster.
    ///
    /// Corresponds to the `PGDATA` environment variable.
    pub datadir: PathBuf,
    /// How to select the PostgreSQL installation to use with this cluster.
    pub strategy: Strategy,
}

impl Cluster {
    /// Represent a cluster at the given path.
    pub fn new<P: AsRef<Path>, S: Into<Strategy>>(
        datadir: P,
        strategy: S,
    ) -> Result<Self, ClusterError> {
        Ok(Self {
            datadir: datadir.as_ref().to_owned(),
            strategy: strategy.into(),
        })
    }

    /// Determine the runtime to use with this cluster.
    fn runtime(&self) -> Result<Runtime, ClusterError> {
        match version(self)? {
            None => self
                .strategy
                .fallback()
                .ok_or_else(|| ClusterError::RuntimeDefaultNotFound),
            Some(version) => self
                .strategy
                .select(&version.into())
                .ok_or_else(|| ClusterError::RuntimeNotFound(version)),
        }
    }

    /// Return a [`Command`] that will invoke `pg_ctl` with the environment
    /// referring to this cluster.
    fn ctl(&self) -> Result<Command, ClusterError> {
        let mut command = self.runtime()?.execute("pg_ctl");
        command.env("PGDATA", &self.datadir);
        command.env("PGHOST", &self.datadir);
        Ok(command)
    }

    /// Check if this cluster is running.
    ///
    /// Tries to distinguish carefully between "definitely running", "definitely
    /// not running", and "don't know". The latter results in [`ClusterError`].
    pub fn running(&self) -> Result<bool, ClusterError> {
        let output = self.ctl()?.arg("status").output()?;
        let code = match output.status.code() {
            // Killed by signal; return early.
            None => return Err(ClusterError::CommandError(output)),
            // Success; return early (the server is running).
            Some(0) => return Ok(true),
            // More work required to decode what this means.
            Some(code) => code,
        };
        let runtime = self.runtime()?;
        // PostgreSQL has evolved to return different error codes in
        // later versions, so here we check for specific codes to avoid
        // masking errors from insufficient permissions or missing
        // executables, for example.
        let running = match runtime.version {
            // PostgreSQL 10.x and later.
            version::Version::Post10(_major, _minor) => {
                // PostgreSQL 10
                // https://www.postgresql.org/docs/10/static/app-pg-ctl.html
                match code {
                    // 3 means that the data directory is present and
                    // accessible but that the server is not running.
                    3 => Some(false),
                    // 4 means that the data directory is not present or is
                    // not accessible. If it's missing, then the server is
                    // not running. If it is present but not accessible
                    // then crash because we can't know if the server is
                    // running or not.
                    4 if !exists(self) => Some(false),
                    // For anything else we don't know.
                    _ => None,
                }
            }
            // PostgreSQL 9.x only.
            version::Version::Pre10(9, point, _minor) => {
                // PostgreSQL 9.4+
                // https://www.postgresql.org/docs/9.4/static/app-pg-ctl.html
                // https://www.postgresql.org/docs/9.5/static/app-pg-ctl.html
                // https://www.postgresql.org/docs/9.6/static/app-pg-ctl.html
                if point >= 4 {
                    match code {
                        // 3 means that the data directory is present and
                        // accessible but that the server is not running.
                        3 => Some(false),
                        // 4 means that the data directory is not present or is
                        // not accessible. If it's missing, then the server is
                        // not running. If it is present but not accessible
                        // then crash because we can't know if the server is
                        // running or not.
                        4 if !exists(self) => Some(false),
                        // For anything else we don't know.
                        _ => None,
                    }
                }
                // PostgreSQL 9.2+
                // https://www.postgresql.org/docs/9.2/static/app-pg-ctl.html
                // https://www.postgresql.org/docs/9.3/static/app-pg-ctl.html
                else if point >= 2 {
                    match code {
                        // 3 means that the data directory is present and
                        // accessible but that the server is not running OR
                        // that the data directory is not present.
                        3 => Some(false),
                        // For anything else we don't know.
                        _ => None,
                    }
                }
                // PostgreSQL 9.0+
                // https://www.postgresql.org/docs/9.0/static/app-pg-ctl.html
                // https://www.postgresql.org/docs/9.1/static/app-pg-ctl.html
                else {
                    match code {
                        // 1 means that the server is not running OR the data
                        // directory is not present OR that the data directory
                        // is not accessible.
                        1 => Some(false),
                        // For anything else we don't know.
                        _ => None,
                    }
                }
            }
            // All other versions.
            version::Version::Pre10(_major, _point, _minor) => None,
        };

        match running {
            Some(running) => Ok(running),
            // TODO: Perhaps include the exit code from `pg_ctl status` in the
            // error message, and whatever it printed out.
            None => Err(ClusterError::UnsupportedVersion(runtime.version)),
        }
    }

    /// Return the path to the PID file used in this cluster.
    ///
    /// The PID file does not necessarily exist.
    pub fn pidfile(&self) -> PathBuf {
        self.datadir.join("postmaster.pid")
    }

    /// Return the path to the log file used in this cluster.
    ///
    /// The log file does not necessarily exist.
    pub fn logfile(&self) -> PathBuf {
        self.datadir.join("postmaster.log")
    }

    /// Create the cluster if it does not already exist.
    pub fn create(&self) -> Result<State, ClusterError> {
        match self._create() {
            Err(ClusterError::UnixError(Errno::EAGAIN)) if exists(self) => Ok(Unmodified),
            Err(ClusterError::UnixError(Errno::EAGAIN)) => Err(ClusterError::InUse),
            other => other,
        }
    }

    fn _create(&self) -> Result<State, ClusterError> {
        if exists(self) {
            // Nothing more to do; the cluster is already in place.
            Ok(Unmodified)
        } else {
            // Create the cluster and report back that we did so.
            fs::create_dir_all(&self.datadir)?;
            #[allow(clippy::suspicious_command_arg_space)]
            self.ctl()?
                .arg("init")
                .arg("-s")
                .arg("-o")
                // Passing multiple flags in a single `arg(...)` is
                // intentional. These constitute the single value for the
                // `-o` flag above.
                .arg("-E utf8 --locale C -A trust")
                .env("TZ", "UTC")
                .output()?;
            Ok(Modified)
        }
    }

    /// Start the cluster if it's not already running.
    pub fn start(&self) -> Result<State, ClusterError> {
        match self._start() {
            Err(ClusterError::UnixError(Errno::EAGAIN)) if self.running()? => Ok(Unmodified),
            Err(ClusterError::UnixError(Errno::EAGAIN)) => Err(ClusterError::InUse),
            other => other,
        }
    }

    fn _start(&self) -> Result<State, ClusterError> {
        // Ensure that the cluster has been created.
        self._create()?;
        // Check if we're running already.
        if self.running()? {
            // We didn't start this cluster; say so.
            return Ok(Unmodified);
        }
        // Next, invoke `pg_ctl` to start the cluster.
        // pg_ctl options:
        //  -l <file> -- log file.
        //  -s -- no informational messages.
        //  -w -- wait until startup is complete.
        // postgres options:
        //  -h <arg> -- host name; empty arg means Unix socket only.
        //  -k -- socket directory.
        self.ctl()?
            .arg("start")
            .arg("-l")
            .arg(self.logfile())
            .arg("-s")
            .arg("-w")
            .arg("-o")
            .arg({
                let mut arg = b"-h '' -k "[..].into();
                escape_into(&self.datadir, &mut arg);
                OsString::from_vec(arg)
            })
            .output()?;
        // We did actually start the cluster; say so.
        Ok(Modified)
    }

    /// Connect to this cluster.
    ///
    /// When the database is not specified, connects to [`DATABASE_POSTGRES`].
    fn connect(&self, database: Option<&str>) -> Result<postgres::Client, ClusterError> {
        let user = &env::var("USER").unwrap_or_else(|_| "USER-not-set".to_string());
        let host = self.datadir.to_string_lossy(); // postgres crate API limitation.
        let client = postgres::Client::configure()
            .user(user)
            .dbname(database.unwrap_or(DATABASE_POSTGRES))
            .host(&host)
            .connect(postgres::NoTls)?;
        Ok(client)
    }

    /// Create a lazy SQLx pool for this cluster.
    ///
    /// Although it's possible to call this anywhere, at runtime it needs a
    /// Tokio context to work, e.g.:
    ///
    /// ```rust,no_run
    /// # let runtime = pgdo::runtime::strategy::Strategy::default();
    /// # let cluster = pgdo::cluster::Cluster::new("some/where", runtime)?;
    /// let tokio = tokio::runtime::Runtime::new()?;
    /// let rows = tokio.block_on(async {
    ///   let pool = cluster.pool(None);
    ///   sqlx::query("SELECT 1").fetch_all(&pool).await
    /// })?;
    /// # Ok::<(), pgdo::cluster::ClusterError>(())
    /// ```
    ///
    /// When the database is not specified, connects to [`DATABASE_POSTGRES`].
    pub fn pool(&self, database: Option<&str>) -> sqlx::PgPool {
        sqlx::PgPool::connect_lazy_with(
            sqlx::postgres::PgConnectOptions::new()
                .socket(&self.datadir)
                .database(database.unwrap_or(DATABASE_POSTGRES))
                .application_name("pgdo"),
        )
    }

    /// Run `psql` against this cluster, in the given database.
    ///
    /// When the database is not specified, connects to [`DATABASE_POSTGRES`].
    pub fn shell(&self, database: Option<&str>) -> Result<ExitStatus, ClusterError> {
        let mut command = self.runtime()?.execute("psql");
        command.arg("--quiet");
        command.env("PGDATA", &self.datadir);
        command.env("PGHOST", &self.datadir);
        command.env("PGDATABASE", database.unwrap_or(DATABASE_POSTGRES));
        Ok(command.spawn()?.wait()?)
    }

    /// Run the given command against this cluster.
    ///
    /// The command is run with the `PGDATA`, `PGHOST`, and `PGDATABASE`
    /// environment variables set appropriately.
    ///
    /// When the database is not specified, uses [`DATABASE_POSTGRES`].
    pub fn exec<T: AsRef<OsStr>>(
        &self,
        database: Option<&str>,
        command: T,
        args: &[T],
    ) -> Result<ExitStatus, ClusterError> {
        let mut command = self.runtime()?.command(command);
        command.args(args);
        command.env("PGDATA", &self.datadir);
        command.env("PGHOST", &self.datadir);
        command.env("PGDATABASE", database.unwrap_or(DATABASE_POSTGRES));
        Ok(command.spawn()?.wait()?)
    }

    /// The names of databases in this cluster.
    pub fn databases(&self) -> Result<Vec<String>, ClusterError> {
        let mut conn = self.connect(None)?;
        let rows = conn.query(
            "SELECT datname FROM pg_catalog.pg_database ORDER BY datname",
            &[],
        )?;
        let datnames: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        Ok(datnames)
    }

    /// Create the named database.
    ///
    /// Returns [`Unmodified`] if the database already exists, otherwise it
    /// returns [`Modified`].
    pub fn createdb(&self, database: &str) -> Result<State, ClusterError> {
        use postgres::error::SqlState;
        let statement = format!(
            "CREATE DATABASE {}",
            postgres_protocol::escape::escape_identifier(database)
        );
        match self.connect(None)?.execute(statement.as_str(), &[]) {
            Err(err) if err.code() == Some(&SqlState::DUPLICATE_DATABASE) => Ok(Unmodified),
            Err(err) => Err(err)?,
            Ok(_) => Ok(Modified),
        }
    }

    /// Drop the named database.
    ///
    /// Returns [`Unmodified`] if the database does not exist, otherwise it
    /// returns [`Modified`].
    pub fn dropdb(&self, database: &str) -> Result<State, ClusterError> {
        use postgres::error::SqlState;
        let statement = format!(
            "DROP DATABASE {}",
            postgres_protocol::escape::escape_identifier(database)
        );
        match self.connect(None)?.execute(statement.as_str(), &[]) {
            Err(err) if err.code() == Some(&SqlState::UNDEFINED_DATABASE) => Ok(Unmodified),
            Err(err) => Err(err)?,
            Ok(_) => Ok(Modified),
        }
    }

    /// Stop the cluster if it's running.
    pub fn stop(&self) -> Result<State, ClusterError> {
        match self._stop() {
            Err(ClusterError::UnixError(Errno::EAGAIN)) if !self.running()? => Ok(Unmodified),
            Err(ClusterError::UnixError(Errno::EAGAIN)) => Err(ClusterError::InUse),
            other => other,
        }
    }

    fn _stop(&self) -> Result<State, ClusterError> {
        // If the cluster's not already running, don't do anything.
        if !self.running()? {
            return Ok(Unmodified);
        }
        // pg_ctl options:
        //  -w -- wait for shutdown to complete.
        //  -m <mode> -- shutdown mode.
        self.ctl()?
            .arg("stop")
            .arg("-s")
            .arg("-w")
            .arg("-m")
            .arg("fast")
            .output()?;
        Ok(Modified)
    }

    /// Destroy the cluster if it exists, after stopping it.
    pub fn destroy(&self) -> Result<State, ClusterError> {
        match self._destroy() {
            Err(ClusterError::UnixError(Errno::EAGAIN)) => Err(ClusterError::InUse),
            other => other,
        }
    }

    fn _destroy(&self) -> Result<State, ClusterError> {
        if self._stop()? == Modified || self.datadir.is_dir() {
            fs::remove_dir_all(&self.datadir)?;
            Ok(Modified)
        } else {
            Ok(Unmodified)
        }
    }
}

impl AsRef<Path> for Cluster {
    fn as_ref(&self) -> &Path {
        &self.datadir
    }
}

/// A fairly simplistic but quick check: does the directory exist and does it
/// look like a PostgreSQL cluster data directory, i.e. does it contain a file
/// named `PG_VERSION`?
///
/// [`version()`] provides a more reliable measure, plus yields the version of
/// PostgreSQL required to use the cluster.
pub fn exists<P: AsRef<Path>>(datadir: P) -> bool {
    let datadir = datadir.as_ref();
    datadir.is_dir() && datadir.join("PG_VERSION").is_file()
}

/// Yields the version of PostgreSQL required to use a cluster.
///
/// This returns the version from the file named `PG_VERSION` in the data
/// directory if it exists, otherwise this returns `None`. For PostgreSQL
/// versions before 10 this is typically (maybe always) the major and point
/// version, e.g. 9.4 rather than 9.4.26. For version 10 and above it appears to
/// be just the major number, e.g. 14 rather than 14.2.
pub fn version<P: AsRef<Path>>(
    datadir: P,
) -> Result<Option<version::PartialVersion>, ClusterError> {
    let version_file = datadir.as_ref().join("PG_VERSION");
    match std::fs::read_to_string(version_file) {
        Ok(version) => Ok(Some(version.parse()?)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err)?,
    }
}

/// [`Cluster`] can be coordinated.
impl coordinate::Subject for Cluster {
    type Error = ClusterError;

    fn start(&self) -> Result<State, Self::Error> {
        self.start()
    }

    fn stop(&self) -> Result<State, Self::Error> {
        self.stop()
    }

    fn destroy(&self) -> Result<State, Self::Error> {
        self.destroy()
    }

    fn exists(&self) -> Result<bool, Self::Error> {
        Ok(exists(self))
    }

    fn running(&self) -> Result<bool, Self::Error> {
        self.running()
    }
}
