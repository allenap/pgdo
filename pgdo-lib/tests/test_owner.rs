//! Tests for cluster ownership, i.e. `pgdo.owner`.
//!
//! Other kernels and namespaces are simulated by editing a real owner file:
//! changing `boot_id` makes it look like another kernel, and changing `pid_ns`
//! makes it look like another namespace on this kernel. None of this depends on
//! the version of PostgreSQL, so these tests use only the default runtime.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use pgdo::cluster::{Cluster, ClusterError, ClusterStatus, State};
use pgdo::runtime::strategy::Strategy;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct Setup {
    _tempdir: tempfile::TempDir,
    datadir: PathBuf,
}

impl Setup {
    fn new() -> TestResult<Self> {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        Ok(Self { _tempdir: tempdir, datadir })
    }

    fn cluster(&self) -> TestResult<Cluster> {
        Ok(Cluster::new(&self.datadir, Strategy::default())?)
    }

    fn ownerfile(&self) -> PathBuf {
        self.datadir.join("pgdo.owner")
    }

    /// Create the cluster, and return the owner file that a session in this
    /// process writes. The cluster is left stopped, with no owner file.
    fn our_owner(&self) -> TestResult<String> {
        let session = self.cluster()?.session(&[])?;
        let owner = std::fs::read_to_string(self.ownerfile())?;
        assert_eq!(session.end()?, State::Modified);
        assert!(!self.ownerfile().exists());
        Ok(owner)
    }

    fn write_owner(&self, owner: &str) -> TestResult {
        std::fs::write(self.ownerfile(), owner)?;
        Ok(())
    }

    fn backdate_owner(&self, by: Duration) -> TestResult {
        let file = std::fs::File::options()
            .write(true)
            .open(self.ownerfile())?;
        file.set_modified(SystemTime::now() - by)?;
        Ok(())
    }
}

/// Replace – or add – the given key in an owner file.
fn with(owner: &str, key: &str, value: &str) -> String {
    let prefix = format!("{key}=");
    let mut lines: Vec<String> = owner
        .lines()
        .filter(|line| !line.starts_with(&prefix))
        .map(str::to_owned)
        .collect();
    lines.push(format!("{prefix}{value}"));
    lines.join("\n") + "\n"
}

fn get<'a>(owner: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}=");
    owner.lines().find_map(|line| line.strip_prefix(&prefix))
}

fn other_kernel(owner: &str) -> String {
    with(
        &with(owner, "boot_id", "some-other-kernel"),
        "hostname",
        "elsewhere",
    )
}

fn other_namespace(owner: &str) -> String {
    // On macOS there are no namespaces, so this adds one; that also differs.
    with(owner, "pid_ns", "4026531111")
}

fn status(datadir: &Path) -> TestResult<ClusterStatus> {
    Ok(Cluster::new(datadir, Strategy::default())?.status()?)
}

#[test]
fn session_records_owner_while_running() -> TestResult {
    let setup = Setup::new()?;
    let session = setup.cluster()?.session(&[])?;
    let owner = std::fs::read_to_string(setup.ownerfile())?;
    assert_eq!(get(&owner, "version"), Some("1"));
    assert_eq!(
        get(&owner, "pid"),
        Some(std::process::id().to_string().as_str())
    );
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        assert!(get(&owner, "boot_id").is_some());
    }
    // The owner file is removed when the session stops the cluster.
    assert_eq!(session.end()?, State::Modified);
    assert!(!setup.ownerfile().exists());
    Ok(())
}

#[test]
fn session_refuses_cluster_in_use_on_other_kernel() -> TestResult {
    let setup = Setup::new()?;
    let owner = other_kernel(&setup.our_owner()?);
    setup.write_owner(&owner)?;
    // Taking over, which first watches the owner file, is tested in the unit
    // tests of `pgdo::cluster::session`, where the watch is much shorter.
    assert!(matches!(
        setup.cluster()?.session(&[]),
        Err(ClusterError::InUseElsewhere { hostname, .. }) if hostname == "elsewhere"
    ));
    // Nothing was started, and the owner file is untouched.
    assert_eq!(status(&setup.datadir)?, ClusterStatus::Stopped);
    assert_eq!(std::fs::read_to_string(setup.ownerfile())?, owner);
    Ok(())
}

/// Taking over is tested in the unit tests of `pgdo::cluster::session`, where
/// pgdo observes the owner file for less time before taking over.
#[test]
fn session_refuses_stale_cluster_on_other_kernel_unless_taking_over() -> TestResult {
    let setup = Setup::new()?;
    setup.write_owner(&other_kernel(&setup.our_owner()?))?;
    setup.backdate_owner(Duration::from_secs(3600))?;
    assert!(matches!(
        setup.cluster()?.session(&[]),
        Err(ClusterError::StaleElsewhere { hostname, .. }) if hostname == "elsewhere"
    ));
    assert_eq!(status(&setup.datadir)?, ClusterStatus::Stopped);
    Ok(())
}

#[test]
fn session_joins_cluster_running_in_other_namespace_and_leaves_it_running() -> TestResult {
    let setup = Setup::new()?;
    let owner = other_namespace(&setup.our_owner()?);
    // Play the other namespace: start the server, and record its owner.
    let cluster = setup.cluster()?;
    cluster.start(&[])?;
    setup.write_owner(&owner)?;

    let session = setup.cluster()?.session(&[])?;
    assert!(!session.databases()?.is_empty());
    // Only the owner's namespace can stop the server, so it's left running.
    assert_eq!(session.end()?, State::Unmodified);
    assert_eq!(cluster.status()?, ClusterStatus::Running);
    assert_eq!(std::fs::read_to_string(setup.ownerfile())?, owner);

    cluster.stop()?;
    Ok(())
}

#[test]
fn session_takes_over_from_other_namespace_whose_server_has_gone() -> TestResult {
    let setup = Setup::new()?;
    let ours = setup.our_owner()?;
    // The other namespace's server has gone, but its owner file remains, e.g.
    // after a crash. It could even have a fresh heartbeat.
    setup.write_owner(&other_namespace(&ours))?;

    let session = setup.cluster()?.session(&[])?;
    assert_eq!(session.status()?, ClusterStatus::Running);
    let owner = std::fs::read_to_string(setup.ownerfile())?;
    assert_eq!(get(&owner, "pid"), get(&ours, "pid"));
    assert_eq!(get(&owner, "pid_ns"), get(&ours, "pid_ns"));
    assert_eq!(session.end()?, State::Modified);
    Ok(())
}

#[test]
fn session_ignores_owner_file_copied_from_another_data_directory() -> TestResult {
    let setup = Setup::new()?;
    let ours = setup.our_owner()?;
    // Same kernel and namespace, but a different data directory, e.g. this one
    // was restored from a backup.
    setup.write_owner(&with(&ours, "datadir_ino", "1"))?;

    let session = setup.cluster()?.session(&[])?;
    assert_eq!(session.status()?, ClusterStatus::Running);
    let owner = std::fs::read_to_string(setup.ownerfile())?;
    assert_eq!(get(&owner, "datadir_ino"), get(&ours, "datadir_ino"));
    assert_eq!(session.end()?, State::Modified);
    Ok(())
}

#[test]
fn session_uses_and_then_stops_running_cluster_without_owner() -> TestResult {
    let setup = Setup::new()?;
    setup.our_owner()?;
    // A server started without an owner file, e.g. by an older pgdo.
    let cluster = setup.cluster()?;
    cluster.start(&[])?;

    let session = setup.cluster()?.session(&[])?;
    assert!(!session.databases()?.is_empty());
    assert_eq!(session.end()?, State::Modified);
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    Ok(())
}

#[test]
fn session_refuses_unintelligible_owner_file() -> TestResult {
    let setup = Setup::new()?;
    setup.our_owner()?;
    setup.write_owner("garbage")?;
    assert!(setup.cluster()?.session(&[]).is_err());
    assert_eq!(status(&setup.datadir)?, ClusterStatus::Stopped);
    assert_eq!(std::fs::read_to_string(setup.ownerfile())?, "garbage");
    Ok(())
}

/// A child process sleeping in a directory unrelated to any cluster, killed
/// when dropped. Plays the part of a process that reused a server's PID.
struct Bystander(std::process::Child);

impl Bystander {
    fn spawn() -> TestResult<Self> {
        let child = std::process::Command::new("sleep")
            .arg("60")
            .current_dir("/")
            .spawn()?;
        Ok(Self(child))
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }

    fn alive(&mut self) -> TestResult<bool> {
        Ok(self.0.try_wait()?.is_none())
    }
}

impl Drop for Bystander {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn session_claims_running_cluster_without_owner() -> TestResult {
    let setup = Setup::new()?;
    setup.our_owner()?;
    // A server started without an owner file, e.g. by an older pgdo.
    let cluster = setup.cluster()?;
    cluster.start(&[])?;

    let session = setup.cluster()?.session(&[])?;
    // Claimed, so that other kernels can see that it is in use.
    let owner = std::fs::read_to_string(setup.ownerfile())?;
    assert_eq!(
        get(&owner, "pid"),
        Some(std::process::id().to_string().as_str())
    );
    assert_eq!(get(&owner, "namespace"), None);
    assert_eq!(session.end()?, State::Modified);
    assert!(!setup.ownerfile().exists());
    Ok(())
}

#[test]
fn session_claims_cluster_visible_only_via_socket_with_unknown_namespace() -> TestResult {
    let setup = Setup::new()?;
    setup.our_owner()?;
    let cluster = setup.cluster()?;
    cluster.start(&[])?;
    let result = in_other_namespace(&setup.datadir, || {
        let session = setup.cluster()?.session(&[])?;
        let owner = std::fs::read_to_string(setup.ownerfile())?;
        assert_eq!(get(&owner, "namespace"), Some("unknown"));
        // We cannot stop a server in another namespace, so it's left running.
        assert_eq!(session.end()?, State::Unmodified);
        Ok(())
    });
    assert_eq!(cluster.status()?, ClusterStatus::Running);
    cluster.stop()?;
    result
}

/// A server in another namespace that listens on a port other than pgdo's, e.g.
/// because `PGPORT` was set when it started, must still be seen as running.
/// Otherwise a session here would release its owner's claim and start a second
/// server on the same data.
#[test]
fn session_joins_server_in_other_namespace_on_other_port() -> TestResult {
    let setup = Setup::new()?;
    let ours = setup.our_owner()?;
    let cluster = setup.cluster()?;
    cluster.start(&[("port".into(), 5433.into())])?;
    setup.write_owner(&other_namespace(&ours))?;
    let result = in_other_namespace(&setup.datadir, || {
        let session = setup.cluster()?.session(&[])?;
        // The owner's claim stands, and the server is left running.
        assert_eq!(
            get(&std::fs::read_to_string(setup.ownerfile())?, "pid_ns"),
            get(&other_namespace(&ours), "pid_ns")
        );
        assert_eq!(session.end()?, State::Unmodified);
        Ok(())
    });
    assert!(setup.datadir.join(".s.PGSQL.5433").exists());
    cluster.stop()?;
    result
}

/// If we cannot tell whether a server in another namespace is running – here,
/// because the path of its socket is too long to connect to – refuse, rather
/// than assume it is not and start a second server.
#[test]
fn session_refuses_when_it_cannot_tell_if_server_is_running() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("d".repeat(120));
    let cluster = Cluster::new(&datadir, Strategy::default())?;
    cluster.create()?;
    std::fs::write(
        datadir.join("postmaster.pid"),
        format!("{}\n{}\n0\n5432\n", dead_pid()?, datadir.display()),
    )?;
    assert!(matches!(
        cluster.session(&[]),
        Err(ClusterError::ServerStateUnknown { .. })
    ));
    assert!(!datadir.join("pgdo.owner").exists());
    Ok(())
}

/// A PID that no process has. It could be reused, but not soon.
fn dead_pid() -> TestResult<u32> {
    let mut child = std::process::Command::new("true").spawn()?;
    child.wait()?;
    Ok(child.id())
}

/// Run `f` while the server running in `datadir` looks as if it belongs to
/// another PID namespace: its `postmaster.pid` is still there, but its PID
/// means nothing here. PostgreSQL rechecks `postmaster.pid` only every minute,
/// so changing it briefly is safe.
fn in_other_namespace(datadir: &Path, f: impl FnOnce() -> TestResult) -> TestResult {
    let pidfile = datadir.join("postmaster.pid");
    let original = std::fs::read_to_string(&pidfile)?;
    let (_, rest) = original
        .split_once('\n')
        .ok_or("postmaster.pid has no lines")?;
    std::fs::write(&pidfile, format!("{}\n{rest}", dead_pid()?))?;
    let result = f();
    std::fs::write(&pidfile, original)?;
    result
}

#[test]
fn session_does_not_signal_unrelated_process_named_in_postmaster_pid() -> TestResult {
    let setup = Setup::new()?;
    let ours = setup.our_owner()?;
    let pidfile = setup.datadir.join("postmaster.pid");
    let mut bystander = Bystander::spawn()?;
    std::fs::write(
        &pidfile,
        format!("{}\n{}\n", bystander.pid(), setup.datadir.display()),
    )?;

    // With no owner file.
    assert!(matches!(
        setup.cluster()?.session(&[]),
        Err(ClusterError::StalePidFile { pid }) if pid == bystander.pid()
    ));
    // With an owner file from a namespace whose server has gone, e.g. a
    // container that was killed, whose server's PID has been reused.
    setup.write_owner(&other_namespace(&ours))?;
    assert!(matches!(
        setup.cluster()?.session(&[]),
        Err(ClusterError::StalePidFile { pid }) if pid == bystander.pid()
    ));
    assert!(bystander.alive()?);
    assert_eq!(
        std::fs::read_to_string(&pidfile)?.lines().next(),
        Some(bystander.pid().to_string().as_str())
    );
    Ok(())
}

#[test]
fn session_releases_claim_when_server_does_not_start() -> TestResult {
    let setup = Setup::new()?;
    setup.our_owner()?;
    // An invalid setting stops the server from starting at all.
    let options = [("max_connections".into(), "not-a-number".into())];
    assert!(setup.cluster()?.session(&options).is_err());
    assert!(!setup.ownerfile().exists());
    assert_eq!(status(&setup.datadir)?, ClusterStatus::Stopped);
    Ok(())
}
