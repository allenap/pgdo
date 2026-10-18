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
    let Some(PidFile { pid, .. }) = read(datadir)? else {
        return Ok(Postmaster::Absent);
    };
    let datadir = fs::metadata(datadir)?;
    Ok(match cwd_is(pid, (datadir.dev(), datadir.ino()))? {
        Some(true) => Postmaster::Running(pid),
        Some(false) => Postmaster::Other(pid),
        None => Postmaster::Unknown(pid),
    }
    .or_dead_if(!exists(pid)))
}

impl Postmaster {
    fn or_dead_if(self, dead: bool) -> Self {
        match self {
            Postmaster::Other(pid) | Postmaster::Unknown(pid) if dead => Postmaster::Dead(pid),
            other => other,
        }
    }
}

/// Does a process with this PID exist? A process that exists but belongs to
/// another user counts.
fn exists(pid: u32) -> bool {
    use nix::{errno::Errno, sys::signal::kill, unistd::Pid};
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    !matches!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH))
}

/// Is the working directory of the process with this PID the directory with
/// the given device and inode numbers? `None` if we cannot tell, e.g. because
/// the process belongs to another user. `Some(false)` if there is no such
/// process.
#[cfg(target_os = "linux")]
pub(super) fn cwd_is(pid: u32, (dir_dev, dir_ino): (u64, u64)) -> io::Result<Option<bool>> {
    match fs::metadata(format!("/proc/{pid}/cwd")) {
        Ok(cwd) => Ok(Some(cwd.dev() == dir_dev && cwd.ino() == dir_ino)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Some(false)),
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => Ok(None),
        Err(err) => Err(err),
    }
}

/// Is the working directory of the process with this PID the directory with
/// the given device and inode numbers? `None` if we cannot tell, e.g. because
/// the process belongs to another user. `Some(false)` if there is no such
/// process.
#[cfg(target_os = "macos")]
pub(super) fn cwd_is(pid: u32, (dir_dev, dir_ino): (u64, u64)) -> io::Result<Option<bool>> {
    use nix::{errno::Errno, libc};
    let Ok(pid) = i32::try_from(pid) else {
        return Ok(Some(false));
    };
    let mut vnodes = std::mem::MaybeUninit::<libc::proc_vnodepathinfo>::zeroed();
    let size = i32::try_from(std::mem::size_of::<libc::proc_vnodepathinfo>())
        .expect("proc_vnodepathinfo is small");
    // SAFETY: `vnodes` is a writable buffer of `size` bytes, which is what
    // `proc_pidinfo` expects for `PROC_PIDVNODEPATHINFO`.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            vnodes.as_mut_ptr().cast(),
            size,
        )
    };
    if written == size {
        // SAFETY: `proc_pidinfo` filled in the whole structure.
        let vnodes = unsafe { vnodes.assume_init() };
        let cwd = vnodes.pvi_cdir.vip_vi.vi_stat;
        Ok(Some(
            u64::from(cwd.vst_dev) == dir_dev && cwd.vst_ino == dir_ino,
        ))
    } else {
        match Errno::last() {
            Errno::ESRCH => Ok(Some(false)),
            Errno::EPERM | Errno::EACCES => Ok(None),
            errno => Err(errno.into()),
        }
    }
}

/// Is the working directory of the process with this PID the given directory?
/// Not supported on this platform, so we cannot tell.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) fn cwd_is(_pid: u32, _dir: (u64, u64)) -> io::Result<Option<bool>> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;
    use std::process::{Child, Command};

    use super::{check, cwd_is, Postmaster};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// A child process sleeping in the given directory, killed when dropped.
    struct Sleeper(Child);

    impl Sleeper {
        fn spawn(dir: &std::path::Path) -> std::io::Result<Self> {
            Command::new("sleep")
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

    fn id(dir: &std::path::Path) -> std::io::Result<(u64, u64)> {
        let metadata = std::fs::metadata(dir)?;
        Ok((metadata.dev(), metadata.ino()))
    }

    #[test]
    fn cwd_is_recognises_working_directory_of_process() -> TestResult {
        let here = tempfile::tempdir()?;
        let elsewhere = tempfile::tempdir()?;
        let sleeper = Sleeper::spawn(here.path())?;
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert_eq!(cwd_is(sleeper.pid(), id(here.path())?)?, Some(true));
            assert_eq!(cwd_is(sleeper.pid(), id(elsewhere.path())?)?, Some(false));
        } else {
            assert_eq!(cwd_is(sleeper.pid(), id(here.path())?)?, None);
        }
        Ok(())
    }

    #[test]
    fn cwd_is_reports_missing_process() -> TestResult {
        let here = tempfile::tempdir()?;
        let pid = {
            let mut child = Command::new("true").spawn()?;
            child.wait()?;
            child.id()
        };
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert_eq!(cwd_is(pid, id(here.path())?)?, Some(false));
        }
        Ok(())
    }

    #[test]
    fn cwd_is_cannot_tell_for_processes_it_cannot_inspect() -> TestResult {
        // PID 1 – init, systemd, launchd – belongs to root. Unless we are root,
        // we cannot inspect it; if we are root, it is not in this directory.
        let here = tempfile::tempdir()?;
        let result = cwd_is(1, id(here.path())?)?;
        assert!(matches!(result, None | Some(false)), "{result:?}");
        Ok(())
    }

    #[test]
    fn check_classifies_postmaster_pid() -> TestResult {
        let datadir = tempfile::tempdir()?;
        let pidfile = datadir.path().join("postmaster.pid");
        assert_eq!(check(datadir.path())?, Postmaster::Absent);

        // A process running in the data directory, like a real server.
        let server = Sleeper::spawn(datadir.path())?;
        std::fs::write(
            &pidfile,
            format!("{}\n{}\n", server.pid(), datadir.path().display()),
        )?;
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert_eq!(check(datadir.path())?, Postmaster::Running(server.pid()));
        }

        // A process elsewhere, e.g. a reused PID.
        let elsewhere = tempfile::tempdir()?;
        let other = Sleeper::spawn(elsewhere.path())?;
        std::fs::write(&pidfile, format!("{}\n", other.pid()))?;
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert_eq!(check(datadir.path())?, Postmaster::Other(other.pid()));
        }

        // No such process.
        let pid = other.pid();
        drop(other);
        assert_eq!(check(datadir.path())?, Postmaster::Dead(pid));

        // Single-user mode: the PID is negated.
        std::fs::write(&pidfile, format!("-{}\n", server.pid()))?;
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert_eq!(check(datadir.path())?, Postmaster::Running(server.pid()));
        }

        // Garbage.
        std::fs::write(&pidfile, "garbage\n")?;
        assert!(check(datadir.path()).is_err());
        Ok(())
    }
}
