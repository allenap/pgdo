//! Record which kernel and namespace owns a running cluster.
//!
//! [`Session`][super::Session]s coordinate with `flock`, but that only works
//! between processes on the same kernel, and PostgreSQL's own check for a
//! running server – the PID in `postmaster.pid` – only works between processes
//! in the same PID namespace. A data directory shared between a host and a VM,
//! or between containers, can escape both, and two servers running on one data
//! directory will corrupt it.
//!
//! So a session that starts a cluster records its owner in `pgdo.owner`:
//!
//! - the kernel's boot ID, to recognise processes on the same kernel;
//! - on Linux, the PID and IPC namespaces, to recognise processes in the same
//!   container;
//! - the data directory's device and inode numbers, to recognise a copy of the
//!   data directory – e.g. from a backup – with a copied owner file;
//! - the hostname and PID, for diagnostics.
//!
//! The file is created with [`link(2)`], which fails if the file exists, so
//! only one process can claim a cluster, even across kernels. While sessions
//! use the cluster they update the file's modification time – a heartbeat – so
//! that other kernels can see that it is still in use.
//!
//! [`link(2)`]: https://man7.org/linux/man-pages/man2/link.2.html

use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write as _};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use rand::Rng;

/// How often sessions update the owner file's modification time.
pub(super) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// How long without a heartbeat before an owner is considered stale.
///
/// Staleness is advisory: it decides only how pgdo describes a cluster owned
/// by another kernel, and whether it will consider taking it over when asked.
/// Before taking over, pgdo also [observes][observe] the owner file, which does
/// not depend on clocks agreeing. A generous threshold allows for slow startup
/// and shutdown – e.g. crash recovery, or a long checkpoint – and for clocks
/// that disagree, e.g. a VM's after its host sleeps.
const STALE_AFTER: Duration = Duration::from_mins(5);

/// How long to [observe] an owner file before taking over: long enough to see
/// a few heartbeats, if there are any.
pub(super) const OBSERVE_BEFORE_TAKEOVER: Duration = Duration::from_secs(30);

/// The version of the owner file format.
const VERSION: &str = "1";

/// Identifies a kernel and, where possible, a namespace within it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Fingerprint {
    /// Changes every time the kernel boots. `None` if unknown.
    boot_id: Option<String>,
    /// Linux only; `None` elsewhere.
    pid_ns: Option<u64>,
    /// Linux only; `None` elsewhere.
    ipc_ns: Option<u64>,
}

impl Fingerprint {
    /// The fingerprint of the current process.
    fn current() -> Self {
        Self {
            boot_id: boot_id(),
            pid_ns: namespace("pid"),
            ipc_ns: namespace("ipc"),
        }
    }
}

#[cfg(target_os = "linux")]
fn boot_id() -> Option<String> {
    fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty())
}

#[cfg(target_os = "macos")]
fn boot_id() -> Option<String> {
    use nix::libc;
    let name = c"kern.bootsessionuuid";
    let mut buf = [0u8; 64];
    let mut len = buf.len();
    // SAFETY: `name` is NUL-terminated; `buf` and `len` describe a writable
    // buffer, which `sysctlbyname` will not overrun; no new value is set.
    let result = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if result == 0 {
        let id = String::from_utf8_lossy(&buf[..len]);
        let id = id.trim_end_matches('\0').trim();
        (!id.is_empty()).then(|| id.to_owned())
    } else {
        None
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn boot_id() -> Option<String> {
    None
}

/// The inode number of the given namespace of the current process. Linux only.
fn namespace(kind: &str) -> Option<u64> {
    if cfg!(target_os = "linux") {
        fs::metadata(format!("/proc/self/ns/{kind}"))
            .ok()
            .map(|m| m.ino())
    } else {
        None
    }
}

/// How the owner of a cluster relates to the current process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Relation {
    /// Same kernel, same namespace: PostgreSQL's own tools work.
    SameNamespace,
    /// Same kernel, different namespace: `flock` and the Unix-domain socket
    /// work, but `pg_ctl` cannot see or signal the server.
    SameKernel,
    /// A different kernel, or one we cannot identify: nothing is shared but the
    /// filesystem.
    OtherKernel,
    /// The owner file was copied from another data directory, e.g. restored
    /// from a backup, and is meaningless here.
    Copy,
}

/// The contents of an owner file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Owner {
    fingerprint: Fingerprint,
    /// `false` if this records a server whose namespace is not known, e.g. one
    /// that a session joined via its Unix-domain socket. Such a server is in
    /// another namespace from every session's point of view, so no session will
    /// try to stop it with `pg_ctl`.
    namespace_known: bool,
    pub(super) hostname: String,
    pid: u32,
    /// Device and inode numbers of the data directory.
    datadir: (u64, u64),
}

impl Owner {
    /// An owner record for the current process and the given data directory.
    pub(super) fn current(datadir: &Path) -> io::Result<Self> {
        let metadata = fs::metadata(datadir)?;
        Ok(Self {
            fingerprint: Fingerprint::current(),
            namespace_known: true,
            hostname: nix::unistd::gethostname()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            pid: std::process::id(),
            datadir: (metadata.dev(), metadata.ino()),
        })
    }

    /// This owner record, but for a server in an unknown namespace on this
    /// kernel; see [`Owner::namespace_known`].
    pub(super) fn with_unknown_namespace(mut self) -> Self {
        self.namespace_known = false;
        self.fingerprint.pid_ns = None;
        self.fingerprint.ipc_ns = None;
        self
    }

    /// How does this owner relate to `current`, the owner record for the
    /// current process?
    pub(super) fn relation(&self, current: &Owner) -> Relation {
        let (ours, theirs) = (&current.fingerprint, &self.fingerprint);
        match (&ours.boot_id, &theirs.boot_id) {
            (Some(ours_id), Some(theirs_id)) if ours_id == theirs_id => {
                // Device and inode numbers are only comparable on one kernel.
                // Across kernels, e.g. via a VM's shared filesystem, the same
                // directory can have different numbers.
                if self.datadir != current.datadir {
                    Relation::Copy
                } else if self.namespace_known
                    && ours.pid_ns == theirs.pid_ns
                    && ours.ipc_ns == theirs.ipc_ns
                {
                    Relation::SameNamespace
                } else {
                    Relation::SameKernel
                }
            }
            _ => Relation::OtherKernel,
        }
    }

    fn serialize(&self) -> String {
        let mut out = String::new();
        let mut line = |key: &str, value: &dyn std::fmt::Display| {
            writeln!(out, "{key}={value}").expect("writing to a String cannot fail");
        };
        line("version", &VERSION);
        line("hostname", &self.hostname.replace('\n', " "));
        line("pid", &self.pid);
        if let Some(boot_id) = &self.fingerprint.boot_id {
            line("boot_id", boot_id);
        }
        if let Some(pid_ns) = self.fingerprint.pid_ns {
            line("pid_ns", &pid_ns);
        }
        if let Some(ipc_ns) = self.fingerprint.ipc_ns {
            line("ipc_ns", &ipc_ns);
        }
        if !self.namespace_known {
            line("namespace", &"unknown");
        }
        line("datadir_dev", &self.datadir.0);
        line("datadir_ino", &self.datadir.1);
        out
    }

    /// Parse an owner file. Unknown keys are ignored.
    fn parse(text: &str) -> Option<Self> {
        let mut version = None;
        let (mut hostname, mut pid, mut boot_id, mut pid_ns, mut ipc_ns) =
            (None, None, None, None, None);
        let (mut dev, mut ino) = (None, None);
        let mut namespace_known = true;
        for line in text.lines() {
            let (key, value) = line.split_once('=')?;
            match key {
                "version" => version = Some(value),
                "hostname" => hostname = Some(value.to_owned()),
                "pid" => pid = Some(value.parse().ok()?),
                "boot_id" => boot_id = Some(value.to_owned()),
                "pid_ns" => pid_ns = Some(value.parse().ok()?),
                "ipc_ns" => ipc_ns = Some(value.parse().ok()?),
                "namespace" if value == "unknown" => namespace_known = false,
                "namespace" => return None,
                "datadir_dev" => dev = Some(value.parse().ok()?),
                "datadir_ino" => ino = Some(value.parse().ok()?),
                _ => (),
            }
        }
        (version? == VERSION).then_some(())?;
        Some(Self {
            fingerprint: Fingerprint { boot_id, pid_ns, ipc_ns },
            namespace_known,
            hostname: hostname?,
            pid: pid?,
            datadir: (dev?, ino?),
        })
    }
}

/// An owner file as found on disk.
#[derive(Debug)]
pub(super) struct Found {
    pub(super) owner: Owner,
    /// Time since the last heartbeat. Zero if the file's modification time is
    /// in the future, e.g. because of clock skew between host and VM.
    pub(super) age: Duration,
}

impl Found {
    /// Has the owner sent a heartbeat recently? If not, the owner is stale.
    pub(super) fn is_fresh(&self) -> bool {
        self.age <= STALE_AFTER
    }
}

/// Read the owner file at `path`, if it exists.
///
/// Owner files are written in full before they are linked into place, so a
/// partial read should be impossible, but try a few times before giving up.
/// If the file cannot be understood, returns an error: pgdo will not guess.
pub(super) fn read(path: &Path) -> io::Result<Option<Found>> {
    const ATTEMPTS: u32 = 3;
    for attempt in 1..=ATTEMPTS {
        // Take the contents and the modification time from one open file. A
        // process on another kernel may remove or replace the owner file at
        // any moment; looking it up twice by path could fail, or mix up two
        // different files.
        let mut file = match fs::File::open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        let modified = file.metadata()?.modified()?;
        let mut text = String::new();
        io::Read::read_to_string(&mut file, &mut text)?;
        if let Some(owner) = Owner::parse(&text) {
            let age = SystemTime::now()
                .duration_since(modified)
                .unwrap_or_default();
            return Ok(Some(Found { owner, age }));
        }
        if attempt < ATTEMPTS {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("could not understand owner file {}", path.display()),
    ))
}

/// Describe how long ago something happened, e.g. "just now", "45s ago",
/// "5m 2s ago", or "3h 7m ago".
pub(super) fn ago(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0 => "just now".to_owned(),
        1..=59 => format!("{secs}s ago"),
        60..=3599 => format!("{}m {}s ago", secs / 60, secs % 60),
        3600..=86399 => format!("{}h {}m ago", secs / 3600, secs % 3600 / 60),
        _ => format!("{}d {}h ago", secs / 86400, secs % 86400 / 3600),
    }
}

/// What became of an owner file while it was [observed][observe].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Observed {
    /// The file did not change: no heartbeats, so its owner is probably gone.
    Unchanged,
    /// The file's modification time changed, or the file was replaced: its
    /// owner, or a new one, is alive.
    Changed,
    /// The file was removed: its owner stopped the cluster, or gave it up.
    Gone,
    /// Watching was interrupted before it finished.
    Interrupted,
}

/// Watch the owner file at `path` for the given time, to see if its owner is
/// alive. Unlike comparing the file's modification time with the current time,
/// this does not depend on the clocks of different kernels agreeing.
///
/// `interrupted` is checked frequently; if it returns `true`, watching stops.
pub(super) fn observe(
    path: &Path,
    window: Duration,
    interrupted: &dyn Fn() -> bool,
) -> io::Result<Observed> {
    // Open the file each time, rather than looking up its metadata by path:
    // on network and shared filesystems, opening a file revalidates cached
    // attributes, which might otherwise hide heartbeats for a while.
    let identity = || -> io::Result<Option<_>> {
        match fs::File::open(path) {
            Ok(file) => {
                let metadata = file.metadata()?;
                Ok(Some((metadata.dev(), metadata.ino(), metadata.modified()?)))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    };
    let Some(initial) = identity()? else {
        return Ok(Observed::Gone);
    };
    let poll = (window / 10).min(Duration::from_secs(1));
    let deadline = Instant::now() + window;
    loop {
        std::thread::sleep(poll.min(deadline.saturating_duration_since(Instant::now())));
        if interrupted() {
            return Ok(Observed::Interrupted);
        }
        match identity()? {
            None => return Ok(Observed::Gone),
            Some(current) if current != initial => return Ok(Observed::Changed),
            Some(_) if Instant::now() >= deadline => return Ok(Observed::Unchanged),
            Some(_) => continue,
        }
    }
}

/// Claim ownership by creating the owner file at `path`. Returns `false` if the
/// file already exists, i.e. another process has claimed ownership.
///
/// The record is written to a temporary file – `pgdo.owner.` followed by 16
/// random hex digits, because processes on other kernels may be doing the same
/// – which is then linked into place. `link` fails if the destination exists,
/// making this an atomic claim, and readers never see a partially written file.
pub(super) fn claim(path: &Path, owner: &Owner) -> io::Result<bool> {
    claim_via(path, owner, std::iter::repeat_with(|| temp_path(path)))
}

/// Like [`claim`], but trying the given temporary file names in turn.
fn claim_via(
    path: &Path,
    owner: &Owner,
    temps: impl IntoIterator<Item = PathBuf>,
) -> io::Result<bool> {
    let (temp, mut file) = create_temp(temps)?;
    // From here on, `temp` is ours, so we must remove it, come what may.
    let result = (|| {
        file.write_all(owner.serialize().as_bytes())?;
        file.sync_all()?;
        match fs::hard_link(&temp, path) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(err) => Err(err),
        }
    })();
    drop(file);
    match fs::remove_file(&temp) {
        Err(err) if err.kind() != io::ErrorKind::NotFound && result.is_ok() => Err(err),
        _ => result,
    }
}

/// Create a new file at the first of the given paths that does not already
/// exist. Gives up after a few attempts: repeated collisions between random
/// names mean that something else is wrong.
fn create_temp(temps: impl IntoIterator<Item = PathBuf>) -> io::Result<(PathBuf, fs::File)> {
    const ATTEMPTS: usize = 8;
    for temp in temps.into_iter().take(ATTEMPTS) {
        match fs::File::options().write(true).create_new(true).open(&temp) {
            Ok(file) => return Ok((temp, file)),
            // Someone else's temporary file; leave it be and try another name.
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not create a temporary file for the owner record",
    ))
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(format!(".{:016x}", rand::rng().next_u64()));
    path.with_file_name(name)
}

/// Remove the owner file at `path`, if it exists.
pub(super) fn release(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
        _ => Ok(()),
    }
}

/// Update the modification time of the owner file at `path`, if it exists.
///
/// This sets the time to "now", rather than to an explicit time: the former
/// requires only permission to write to the file, the latter requires owning it.
fn touch(path: &Path) -> io::Result<()> {
    use nix::sys::stat::{utimensat, UtimensatFlags};
    use nix::sys::time::TimeSpec;
    match utimensat(
        nix::fcntl::AT_FDCWD,
        path,
        &TimeSpec::UTIME_NOW,
        &TimeSpec::UTIME_NOW,
        UtimensatFlags::FollowSymlink,
    ) {
        Ok(()) | Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(errno) => Err(errno.into()),
    }
}

/// Touches an owner file periodically, on a background thread, until dropped.
///
/// A channel serves as both the timer and the stop signal: the thread waits on
/// it for up to the heartbeat interval, and stops when the sender is dropped.
#[derive(Debug)]
pub(super) struct Heartbeat {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Heartbeat {
    pub(super) fn start(path: PathBuf, interval: Duration) -> Self {
        let (stop, stopped) = mpsc::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("pgdo-heartbeat".into())
            .spawn(move || loop {
                if let Err(err) = touch(&path) {
                    log::warn!("Could not update {}: {err}", path.display());
                }
                // Wait for the interval to pass. If instead the sender was
                // dropped, or sent a message, stop.
                if !matches!(
                    stopped.recv_timeout(interval),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    break;
                }
            })
            .ok();
        if thread.is_none() {
            log::warn!(
                "Could not start heartbeat thread; other hosts may think the cluster is idle."
            );
        }
        Self { stop: Some(stop), thread }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        // Dropping the sender wakes the thread, which then stops.
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(boot_id: &str, pid_ns: u64, datadir: (u64, u64)) -> Owner {
        Owner {
            fingerprint: Fingerprint {
                boot_id: Some(boot_id.into()),
                pid_ns: Some(pid_ns),
                ipc_ns: Some(1),
            },
            namespace_known: true,
            hostname: "example".into(),
            pid: 1234,
            datadir,
        }
    }

    #[test]
    fn fingerprint_of_current_process_identifies_kernel() {
        let fingerprint = Fingerprint::current();
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(fingerprint.boot_id.is_some());
        }
        assert_eq!(fingerprint.pid_ns.is_some(), cfg!(target_os = "linux"));
    }

    #[test]
    fn serialize_and_parse_round_trip() {
        let owner = owner("boot", 42, (1, 2));
        assert_eq!(Owner::parse(&owner.serialize()), Some(owner));
        let mut owner = Owner::current(Path::new("/")).unwrap();
        owner.fingerprint.pid_ns = None;
        owner.fingerprint.boot_id = None;
        assert_eq!(Owner::parse(&owner.serialize()), Some(owner));
    }

    #[test]
    fn serialize_and_parse_round_trip_with_unknown_namespace() {
        let joined = owner("boot", 42, (1, 2)).with_unknown_namespace();
        assert!(joined.serialize().contains("namespace=unknown\n"));
        assert_eq!(Owner::parse(&joined.serialize()), Some(joined));
        let text = owner("boot", 42, (1, 2)).serialize() + "namespace=bogus\n";
        assert_eq!(Owner::parse(&text), None);
    }

    #[test]
    fn relation_with_unknown_namespace_is_never_same_namespace() {
        let current = owner("boot", 42, (1, 2));
        let joined = current.clone().with_unknown_namespace();
        assert_eq!(joined.relation(&current), Relation::SameKernel);
        assert_eq!(joined.relation(&joined), Relation::SameKernel);
        // A copy is still a copy; another kernel is still another kernel.
        let mut copied = joined.clone();
        copied.datadir = (1, 3);
        assert_eq!(copied.relation(&current), Relation::Copy);
        let mut other = joined.clone();
        other.fingerprint.boot_id = Some("other".into());
        assert_eq!(other.relation(&current), Relation::OtherKernel);
    }

    #[test]
    fn ago_describes_durations() {
        let ago = |secs| super::ago(Duration::from_secs(secs));
        assert_eq!(super::ago(Duration::from_millis(999)), "just now");
        assert_eq!(ago(45), "45s ago");
        assert_eq!(ago(73), "1m 13s ago");
        assert_eq!(ago(302), "5m 2s ago");
        assert_eq!(ago(3 * 3600 + 7 * 60 + 5), "3h 7m ago");
        assert_eq!(ago(2 * 86400 + 4 * 3600), "2d 4h ago");
    }

    #[test]
    fn parse_rejects_malformed_or_unknown_versions() {
        assert_eq!(Owner::parse(""), None);
        assert_eq!(Owner::parse("version=1\nhostname=x\n"), None);
        assert_eq!(Owner::parse("garbage"), None);
        let text = owner("boot", 42, (1, 2))
            .serialize()
            .replace("version=1", "version=2");
        assert_eq!(Owner::parse(&text), None);
    }

    #[test]
    fn relation() {
        let current = owner("boot", 42, (1, 2));
        assert_eq!(
            owner("boot", 42, (1, 2)).relation(&current),
            Relation::SameNamespace
        );
        assert_eq!(
            owner("boot", 43, (1, 2)).relation(&current),
            Relation::SameKernel
        );
        assert_eq!(owner("boot", 42, (1, 3)).relation(&current), Relation::Copy);
        assert_eq!(
            owner("other", 42, (1, 2)).relation(&current),
            Relation::OtherKernel
        );
        // Across kernels, device and inode numbers are not comparable.
        assert_eq!(
            owner("other", 42, (9, 9)).relation(&current),
            Relation::OtherKernel
        );
        // An unknown kernel is treated as another kernel.
        let mut unknown = owner("boot", 42, (1, 2));
        unknown.fingerprint.boot_id = None;
        assert_eq!(unknown.relation(&current), Relation::OtherKernel);
    }

    #[test]
    fn claim_is_exclusive_and_read_reports_age() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        let first = owner("boot", 1, (1, 2));
        assert!(claim(&path, &first)?);
        assert!(!claim(&path, &owner("boot", 2, (1, 2)))?);
        // Temporary files are cleaned up.
        assert_eq!(fs::read_dir(dir.path())?.count(), 1);
        let found = read(&path)?.expect("owner file missing");
        assert_eq!(found.owner, first);
        assert!(found.is_fresh());
        // Backdate the file: stale.
        let file = fs::File::options().write(true).open(&path)?;
        file.set_modified(SystemTime::now() - STALE_AFTER * 2)?;
        assert!(!read(&path)?.expect("owner file missing").is_fresh());
        // Touching it makes it fresh again.
        touch(&path)?;
        assert!(read(&path)?.expect("owner file missing").is_fresh());
        release(&path)?;
        assert!(read(&path)?.is_none());
        release(&path)?; // Idempotent.
        Ok(())
    }

    #[test]
    fn claim_skips_existing_temporary_files_and_leaves_them_alone() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        // Another process's temporary file, with the name we will try first.
        let theirs = dir.path().join("pgdo.owner.theirs");
        fs::write(&theirs, "theirs")?;
        let ours = dir.path().join("pgdo.owner.ours");
        let owner = owner("boot", 1, (1, 2));
        assert!(claim_via(&path, &owner, [theirs.clone(), ours.clone()])?);
        assert_eq!(read(&path)?.expect("owner file missing").owner, owner);
        // Their temporary file is untouched; ours is gone.
        assert_eq!(fs::read_to_string(&theirs)?, "theirs");
        assert!(!ours.exists());
        Ok(())
    }

    #[test]
    fn claim_gives_up_after_repeated_collisions() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        let taken = dir.path().join("pgdo.owner.taken");
        fs::write(&taken, "taken")?;
        let owner = owner("boot", 1, (1, 2));
        let err = claim_via(&path, &owner, std::iter::repeat(taken.clone())).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert!(!path.exists());
        assert_eq!(fs::read_to_string(&taken)?, "taken");
        Ok(())
    }

    #[test]
    fn observe_sees_unchanged_file() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        fs::write(&path, "")?;
        assert_eq!(
            observe(&path, Duration::from_millis(100), &|| false)?,
            Observed::Unchanged
        );
        Ok(())
    }

    #[test]
    fn observe_sees_heartbeats_even_with_stale_times() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        fs::write(&path, "")?;
        // A heartbeat from a kernel whose clock is an hour behind: every time
        // looks stale, but the file keeps changing.
        let base = SystemTime::now() - Duration::from_secs(3600);
        fs::File::options()
            .write(true)
            .open(&path)?
            .set_modified(base)?;
        let beating = {
            let path = path.clone();
            std::thread::spawn(move || {
                for i in 1..=10 {
                    std::thread::sleep(Duration::from_millis(20));
                    let file = fs::File::options().write(true).open(&path).unwrap();
                    file.set_modified(base + Duration::from_secs(i)).unwrap();
                }
            })
        };
        assert_eq!(
            observe(&path, Duration::from_millis(500), &|| false)?,
            Observed::Changed
        );
        beating.join().unwrap();
        Ok(())
    }

    #[test]
    fn observe_stops_promptly_when_interrupted() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        fs::write(&path, "")?;
        let started = Instant::now();
        let observed = observe(&path, Duration::from_secs(30), &|| {
            started.elapsed() > Duration::from_millis(100)
        })?;
        assert_eq!(observed, Observed::Interrupted);
        assert!(started.elapsed() < Duration::from_secs(3));
        Ok(())
    }

    #[test]
    fn observe_sees_replaced_and_removed_files() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        assert_eq!(
            observe(&path, Duration::from_millis(100), &|| false)?,
            Observed::Gone
        );
        fs::write(&path, "")?;
        let replacing = {
            let path = path.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                let temp = path.with_extension("new");
                fs::write(&temp, "").unwrap();
                fs::rename(&temp, &path).unwrap();
            })
        };
        assert_eq!(
            observe(&path, Duration::from_millis(500), &|| false)?,
            Observed::Changed
        );
        replacing.join().unwrap();
        let removing = {
            let path = path.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                fs::remove_file(&path).unwrap();
            })
        };
        assert_eq!(
            observe(&path, Duration::from_millis(500), &|| false)?,
            Observed::Gone
        );
        removing.join().unwrap();
        Ok(())
    }

    #[test]
    fn read_rejects_unintelligible_file() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        fs::write(&path, "garbage")?;
        assert_eq!(read(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
        Ok(())
    }

    #[test]
    fn heartbeat_touches_file_until_dropped() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pgdo.owner");
        fs::write(&path, "")?;
        let old = SystemTime::now() - Duration::from_secs(3600);
        fs::File::options()
            .write(true)
            .open(&path)?
            .set_modified(old)?;
        let heartbeat = Heartbeat::start(path.clone(), Duration::from_millis(20));
        std::thread::sleep(Duration::from_millis(100));
        assert!(fs::metadata(&path)?.modified()? > old);
        drop(heartbeat);
        // After dropping, no more heartbeats.
        fs::File::options()
            .write(true)
            .open(&path)?
            .set_modified(old)?;
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(fs::metadata(&path)?.modified()?, old);
        Ok(())
    }
}
