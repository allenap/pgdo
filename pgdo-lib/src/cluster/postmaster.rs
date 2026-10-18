//! Check the PID in a cluster's `postmaster.pid`.
//!
//! `pg_ctl status` and `pg_ctl stop` trust that PID: if a process with that PID
//! exists, the server is "running", and `pg_ctl stop` signals it. But the PID
//! may be stale – e.g. after a crash – and PIDs are reused, especially in
//! containers, where they are small and dense. A stale PID may belong to some
//! unrelated process, which `pg_ctl stop` would then interrupt.
//!
//! So pgdo checks that the process really is a PostgreSQL server for the data
//! directory: a PostgreSQL server – and each of its child processes – runs with
//! the data directory as its working directory.

use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// What the PID in `postmaster.pid` refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Postmaster {
    /// There is no `postmaster.pid`.
    Absent,
    /// A PostgreSQL server for this data directory.
    Running(u32),
    /// No process has this PID: `postmaster.pid` is stale. PostgreSQL deals
    /// with this itself when starting.
    Dead(u32),
    /// A process that is not a PostgreSQL server for this data directory, e.g.
    /// because the PID was reused. It must not be signalled.
    Other(u32),
    /// A process that we cannot inspect, e.g. because it belongs to another
    /// user, or because this platform is not supported.
    Unknown(u32),
}

/// The fields of a `postmaster.pid` file that pgdo uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PidFile {
    /// The server's PID. This is meaningful only in the server's PID namespace.
    pub(super) pid: u32,
    /// The port the server listens on, which determines its socket's name.
    /// `None` if the file does not say, e.g. because the server is starting.
    pub(super) port: Option<u16>,
}

/// Read the `postmaster.pid` file in `datadir`. `None` if there is none, i.e.
/// no server is running – in any namespace – for this data directory.
pub(super) fn read(datadir: &Path) -> io::Result<Option<PidFile>> {
    let path = datadir.join("postmaster.pid");
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let mut lines = text.lines().map(str::trim);
    // The first line is the PID. It is negated for a server in single-user
    // mode.
    let pid = lines
        .next()
        .and_then(|line| line.parse::<i64>().ok())
        .and_then(|pid| u32::try_from(pid.unsigned_abs()).ok())
        .filter(|&pid| pid > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("could not find a PID in {}", path.display()),
            )
        })?;
    // Then the data directory, the start time, and the port.
    let port = lines.nth(2).and_then(|line| line.parse().ok());
    Ok(Some(PidFile { pid, port }))
}

/// Check the PID in the `postmaster.pid` file in `datadir`.
pub(super) fn check(datadir: &Path) -> io::Result<Postmaster> {
    match read(datadir)? {
        None => Ok(Postmaster::Absent),
        Some(PidFile { pid, .. }) => {
            let datadir = fs::metadata(datadir)?;
            let datadir = (datadir.dev(), datadir.ino());
            Ok(match see(pid)? {
                Some(Seen::Process { cwd, name }) if cwd == datadir && is_postgres(&name) => {
                    Postmaster::Running(pid)
                }
                // pgdo's servers run as the current user, so we can inspect
                // them. One that we cannot inspect is not ours.
                Some(Seen::Process { .. } | Seen::Hidden | Seen::Gone) => Postmaster::Other(pid),
                None => Postmaster::Unknown(pid),
            }
            .or_dead_if(!exists(pid)))
        }
    }
}

impl Postmaster {
    fn or_dead_if(self, dead: bool) -> Self {
        match self {
            Postmaster::Other(pid) | Postmaster::Unknown(pid) if dead => Postmaster::Dead(pid),
            other => other,
        }
    }
}

/// Is this the name of a PostgreSQL server process? `postmaster` is an old name
/// for `postgres`, which some installations still provide.
fn is_postgres(name: &str) -> bool {
    matches!(name, "postgres" | "postmaster")
}

/// Does a process with this PID exist? A process that exists but belongs to
/// another user counts.
fn exists(pid: u32) -> bool {
    use nix::{errno::Errno, sys::signal::kill, unistd::Pid};
    if let Ok(pid) = i32::try_from(pid) {
        !matches!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH))
    } else {
        false
    }
}

/// What we can see of a process.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Seen {
    /// There is no such process, as far as we can see.
    Gone,
    /// The process exists, but we may not inspect it, e.g. because it belongs
    /// to another user.
    Hidden,
    /// The process's working directory, as device and inode numbers, and its
    /// name, i.e. that of the program it runs.
    ///
    /// Every process of a PostgreSQL server runs with the data directory as its
    /// working directory, but so might any other process, e.g. a shell, or an
    /// editor. The name tells them apart: `postmaster.pid` can name a process
    /// other than its server only when it is stale, and the PID reused.
    Process { cwd: (u64, u64), name: String },
}

/// Look at the process with this PID. `None` if this platform is not supported,
/// so we cannot tell.
#[cfg(target_os = "linux")]
fn see(pid: u32) -> io::Result<Option<Seen>> {
    let proc = |name: &str| format!("/proc/{pid}/{name}");
    let seen = |err: io::Error| match err.kind() {
        io::ErrorKind::NotFound => Ok(Seen::Gone),
        io::ErrorKind::PermissionDenied => Ok(Seen::Hidden),
        _ => Err(err),
    };
    let seen = match fs::metadata(proc("cwd")) {
        Ok(cwd) => match fs::read_to_string(proc("comm")) {
            Ok(name) => Seen::Process {
                cwd: (cwd.dev(), cwd.ino()),
                name: name.trim_end_matches('\n').to_owned(),
            },
            Err(err) => seen(err)?,
        },
        Err(err) => seen(err)?,
    };
    Ok(Some(seen))
}

/// Look at the process with this PID. `None` if this platform is not supported,
/// so we cannot tell.
#[cfg(target_os = "macos")]
fn see(pid: u32) -> io::Result<Option<Seen>> {
    use nix::libc;
    let seen = match i32::try_from(pid) {
        Err(_) => Seen::Gone,
        Ok(pid) => {
            let vnodes =
                proc_pidinfo::<libc::proc_vnodepathinfo>(pid, libc::PROC_PIDVNODEPATHINFO)?;
            let bsdinfo = proc_pidinfo::<libc::proc_bsdinfo>(pid, libc::PROC_PIDTBSDINFO)?;
            match (vnodes, bsdinfo) {
                (Ok(vnodes), Ok(bsdinfo)) => {
                    let cwd = vnodes.pvi_cdir.vip_vi.vi_stat;
                    let dev = u64::from(cwd.vst_dev);
                    let name = bsdinfo
                        .pbi_comm
                        .iter()
                        .take_while(|&&c| c != 0)
                        .map(|&c| c.cast_unsigned())
                        .collect::<Vec<u8>>();
                    Seen::Process {
                        cwd: (dev, cwd.vst_ino),
                        name: String::from_utf8_lossy(&name).into_owned(),
                    }
                }
                (Err(seen), _) | (_, Err(seen)) => seen,
            }
        }
    };
    Ok(Some(seen))
}

/// Call `proc_pidinfo` for the given flavour of information, `T`. Returns what
/// we can see instead if there is no such process, or we may not inspect it.
#[cfg(target_os = "macos")]
fn proc_pidinfo<T>(pid: i32, flavor: i32) -> io::Result<Result<T, Seen>> {
    use nix::{errno::Errno, libc};
    let mut info = std::mem::MaybeUninit::<T>::zeroed();
    let size = i32::try_from(std::mem::size_of::<T>()).expect("proc_pidinfo structures are small");
    // SAFETY: `info` is a writable buffer of `size` bytes, which is what
    // `proc_pidinfo` expects for the flavours we use.
    let written = unsafe { libc::proc_pidinfo(pid, flavor, 0, info.as_mut_ptr().cast(), size) };
    if written == size {
        // SAFETY: `proc_pidinfo` filled in the whole structure.
        Ok(Ok(unsafe { info.assume_init() }))
    } else {
        match Errno::last() {
            Errno::ESRCH => Ok(Err(Seen::Gone)),
            Errno::EPERM | Errno::EACCES => Ok(Err(Seen::Hidden)),
            errno => Err(errno.into()),
        }
    }
}

/// Look at the process with this PID. Not supported on this platform, so we
/// cannot tell.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn see(_pid: u32) -> io::Result<Option<Seen>> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::process::{Child, Command};

    use super::{check, read, see, PidFile, Postmaster, Seen};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const SUPPORTED: bool = cfg!(any(target_os = "linux", target_os = "macos"));

    /// A child process sleeping in the given directory, killed when dropped.
    struct Sleeper(Child);

    impl Sleeper {
        fn spawn(dir: &Path) -> std::io::Result<Self> {
            Self::spawn_program("sleep".as_ref(), dir)
        }

        /// Run a process called `postgres`, so that it looks like a PostgreSQL
        /// server: a copy of this test program, in `bindir`, that runs only
        /// [`sleep_as_postgres`]. (A copy of, or symlink to, `sleep` would not
        /// do: macOS kills copies of its system programs, and names a process
        /// after the target of a symlink.)
        fn spawn_as_postgres(bindir: &Path, dir: &Path) -> std::io::Result<Self> {
            let postgres = bindir.join("postgres");
            if !postgres.exists() {
                std::fs::copy(std::env::current_exe()?, &postgres)?;
            }
            Command::new(postgres)
                .args(["--exact", "--include-ignored", "--test-threads=1"])
                .arg("cluster::postmaster::tests::sleep_as_postgres")
                .env(SLEEP_AS_POSTGRES, "1")
                .current_dir(dir)
                .stdout(std::process::Stdio::null())
                .spawn()
                .map(Self)
        }

        fn spawn_program(program: &Path, dir: &Path) -> std::io::Result<Self> {
            Command::new(program)
                .arg("60")
                .current_dir(dir)
                .spawn()
                .map(Self)
        }

        fn pid(&self) -> u32 {
            self.0.id()
        }
    }

    impl Drop for Sleeper {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    const SLEEP_AS_POSTGRES: &str = "PGDO_TEST_SLEEP_AS_POSTGRES";

    /// Not a test: [`Sleeper::spawn_as_postgres`] runs this in a copy of this
    /// test program.
    #[test]
    #[ignore = "run by Sleeper::spawn_as_postgres"]
    fn sleep_as_postgres() {
        if std::env::var_os(SLEEP_AS_POSTGRES).is_some() {
            std::thread::sleep(std::time::Duration::from_secs(60));
        }
    }

    fn id(dir: &Path) -> std::io::Result<(u64, u64)> {
        let metadata = std::fs::metadata(dir)?;
        Ok((metadata.dev(), metadata.ino()))
    }

    #[test]
    fn see_reports_working_directory_and_name_of_process() -> TestResult {
        let here = tempfile::tempdir()?;
        let sleeper = Sleeper::spawn(here.path())?;
        if SUPPORTED {
            assert_eq!(
                see(sleeper.pid())?,
                Some(Seen::Process { cwd: id(here.path())?, name: "sleep".into() })
            );
        } else {
            assert_eq!(see(sleeper.pid())?, None);
        }
        Ok(())
    }

    #[test]
    fn see_reports_missing_process() -> TestResult {
        let pid = {
            let mut child = Command::new("true").spawn()?;
            child.wait()?;
            child.id()
        };
        if SUPPORTED {
            assert_eq!(see(pid)?, Some(Seen::Gone));
        }
        Ok(())
    }

    #[test]
    fn see_cannot_inspect_processes_of_other_users() -> TestResult {
        // PID 1 – init, systemd, launchd – belongs to root. Unless we are root,
        // we cannot inspect it.
        let result = see(1)?;
        if SUPPORTED {
            assert!(
                matches!(result, Some(Seen::Hidden | Seen::Process { .. })),
                "{result:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn read_returns_pid_and_port() -> TestResult {
        let datadir = tempfile::tempdir()?;
        let pidfile = datadir.path().join("postmaster.pid");
        assert_eq!(read(datadir.path())?, None);
        std::fs::write(&pidfile, "123\n/some/where\n1700000000\n5433\n/tmp\n")?;
        assert_eq!(
            read(datadir.path())?,
            Some(PidFile { pid: 123, port: Some(5433) })
        );
        // A server writes the file in stages as it starts.
        std::fs::write(&pidfile, "123\n")?;
        assert_eq!(
            read(datadir.path())?,
            Some(PidFile { pid: 123, port: None })
        );
        Ok(())
    }

    #[test]
    fn check_classifies_postmaster_pid() -> TestResult {
        let datadir = tempfile::tempdir()?;
        let bindir = tempfile::tempdir()?;
        let pidfile = datadir.path().join("postmaster.pid");
        assert_eq!(check(datadir.path())?, Postmaster::Absent);

        // A `postgres` process running in the data directory, like a server.
        let server = Sleeper::spawn_as_postgres(bindir.path(), datadir.path())?;
        std::fs::write(
            &pidfile,
            format!("{}\n{}\n", server.pid(), datadir.path().display()),
        )?;
        if SUPPORTED {
            assert_eq!(check(datadir.path())?, Postmaster::Running(server.pid()));
        }

        // Another program running in the data directory, e.g. a shell, with a
        // reused PID.
        let shell = Sleeper::spawn(datadir.path())?;
        std::fs::write(&pidfile, format!("{}\n", shell.pid()))?;
        if SUPPORTED {
            assert_eq!(check(datadir.path())?, Postmaster::Other(shell.pid()));
        }

        // A process elsewhere, e.g. a reused PID.
        let elsewhere = tempfile::tempdir()?;
        let other = Sleeper::spawn_as_postgres(bindir.path(), elsewhere.path())?;
        std::fs::write(&pidfile, format!("{}\n", other.pid()))?;
        if SUPPORTED {
            assert_eq!(check(datadir.path())?, Postmaster::Other(other.pid()));
        }

        // No such process.
        let pid = other.pid();
        drop(other);
        assert_eq!(check(datadir.path())?, Postmaster::Dead(pid));

        // Single-user mode: the PID is negated.
        std::fs::write(&pidfile, format!("-{}\n", server.pid()))?;
        if SUPPORTED {
            assert_eq!(check(datadir.path())?, Postmaster::Running(server.pid()));
        }

        // Garbage.
        std::fs::write(&pidfile, "garbage\n")?;
        assert!(check(datadir.path()).is_err());
        Ok(())
    }
}
