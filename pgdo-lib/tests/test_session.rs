use std::path::PathBuf;

use pgdo::cluster::{Cluster, ClusterStatus, Finish, State};
use pgdo::{lock, runtime};
use pgdo_test::for_all_runtimes;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[for_all_runtimes]
#[test]
fn session_creates_and_starts_cluster_then_stops_it() -> TestResult {
    let setup = Setup::new()?;
    let session = setup.cluster(runtime)?.session(&[])?;
    assert_eq!(session.status()?, ClusterStatus::Running);
    assert!(!session.databases()?.is_empty());
    assert_eq!(session.end()?, State::Modified);
    let cluster = setup.cluster(Setup::strategy())?;
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_with_finish_destroy_removes_cluster() -> TestResult {
    let setup = Setup::new()?;
    let session = setup
        .cluster(runtime)?
        .session(&[])?
        .finish(Finish::Destroy);
    assert_eq!(session.status()?, ClusterStatus::Running);
    assert_eq!(session.end()?, State::Modified);
    // The whole data directory is removed, including the lock file.
    assert!(!setup.datadir.exists());
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_leaves_cluster_running_while_other_sessions_use_it() -> TestResult {
    let setup = Setup::new()?;
    let session1 = setup.cluster(runtime.clone())?.session(&[])?;
    let session2 = setup.cluster(runtime)?.session(&[])?;
    // The first session to end leaves the cluster running for the second.
    assert_eq!(session1.end()?, State::Unmodified);
    assert_eq!(session2.status()?, ClusterStatus::Running);
    // The last session to end stops the cluster.
    assert_eq!(session2.end()?, State::Modified);
    let cluster = setup.cluster(Setup::strategy())?;
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_stops_cluster_when_dropped() -> TestResult {
    let setup = Setup::new()?;
    let session = setup.cluster(runtime)?.session(&[])?;
    assert_eq!(session.status()?, ClusterStatus::Running);
    drop(session);
    let cluster = setup.cluster(Setup::strategy())?;
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_stops_cluster_when_something_panics() -> TestResult {
    let setup = Setup::new()?;
    let session = setup.cluster(runtime)?.session(&[])?;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _session = session;
        panic!("test panic")
    }));
    let payload = *panic.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(payload, "test panic");
    let cluster = setup.cluster(Setup::strategy())?;
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_makes_data_directory_absolute() -> TestResult {
    let setup = Setup::new()?;
    let cwd = std::env::current_dir()?;
    let relative = pathdiff(&setup.datadir, &cwd);
    let session = Cluster::new(&relative, runtime)?.session(&[])?;
    assert!(session.datadir.is_absolute());
    assert_eq!(session.datadir, setup.datadir.canonicalize()?);
    session.end()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_keeps_lock_file_and_socket_in_data_directory() -> TestResult {
    let setup = Setup::new()?;
    let session = setup.cluster(runtime)?.session(&[])?;
    assert!(session.datadir.join("pgdo.lock").is_file());
    assert!(session.datadir.join(".s.PGSQL.5432").exists());
    assert!(session.datadir.join("PG_VERSION").is_file());
    assert!(!session.datadir.join("pgdo.init").exists());
    session.end()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_after_destroy_recreates_cluster() -> TestResult {
    let setup = Setup::new()?;
    let session = setup.cluster(runtime.clone())?.session(&[])?;
    session.finish(Finish::Destroy).end()?;
    assert!(!setup.datadir.exists());
    let session = setup.cluster(runtime)?.session(&[])?;
    assert_eq!(session.status()?, ClusterStatus::Running);
    assert!(session.datadir.join("pgdo.lock").is_file());
    session.end()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_does_not_destroy_cluster_while_other_sessions_use_it() -> TestResult {
    let setup = Setup::new()?;
    let session1 = setup.cluster(runtime.clone())?.session(&[])?;
    let session2 = setup.cluster(runtime)?.session(&[])?;
    assert_eq!(session1.finish(Finish::Destroy).end()?, State::Unmodified);
    assert_eq!(session2.status()?, ClusterStatus::Running);
    assert_eq!(session2.end()?, State::Modified);
    assert!(setup.datadir.join("PG_VERSION").exists());
    Ok(())
}

/// Simulate a session destroying the cluster – removing its lock file – while
/// another session waits for the lock, and a third session starting the cluster
/// afresh, with a new lock file, before the waiting session wakes. The waiting
/// session must notice that the file it locked has gone, and lock the current
/// file instead. Otherwise it would hold a lock no one else can see, and the
/// third session – believing itself alone – could stop the cluster under it.
#[for_all_runtimes]
#[test]
fn session_locks_current_lock_file_when_waiting_through_destroy() -> TestResult {
    use std::os::unix::fs::MetadataExt;

    let setup = Setup::new()?;
    std::fs::create_dir(&setup.datadir)?;
    let lockfile = setup.datadir.join("pgdo.lock");

    // Play the destroying session: hold an exclusive lock on the lock file.
    let old_lock = lock::UnlockedFile::try_from(lockfile.as_path())?.lock_exclusive()?;
    let old_ino = std::fs::metadata(&lockfile)?.ino();

    let waiting_cluster = setup.cluster(runtime.clone())?;
    let starting_cluster = setup.cluster(runtime)?;
    let (session, new_lock) = std::thread::scope(|scope| {
        let waiting = scope.spawn(|| waiting_cluster.session(&[]));
        // Give the waiting session time to block on the exclusive lock.
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert!(!waiting.is_finished());
        // Finish "destroying": remove the lock file.
        std::fs::remove_file(&lockfile)?;
        // Play the third session: create a new lock file, lock it, and start
        // the cluster, then downgrade to a shared lock.
        let new_lock = lock::UnlockedFile::try_from(lockfile.as_path())?.lock_exclusive()?;
        starting_cluster.start(&[])?;
        let new_lock = new_lock.lock_shared()?;
        // Now release the old lock, waking the waiting session.
        drop(old_lock);
        let session = waiting.join().unwrap()?;
        Ok::<_, Box<dyn std::error::Error>>((session, new_lock))
    })?;
    assert_ne!(std::fs::metadata(&lockfile)?.ino(), old_ino);

    // The third session ends. The waiting session must hold a lock on the
    // current lock file, so an exclusive lock – which the third session would
    // need to stop the cluster – is not available.
    drop(new_lock);
    let other = lock::UnlockedFile::try_from(lockfile.as_path())?;
    assert!(other.try_lock_exclusive()?.is_left());
    assert_eq!(session.status()?, ClusterStatus::Running);

    // Ending the session stops the cluster.
    assert_eq!(session.end()?, State::Modified);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn session_refuses_non_empty_directory_without_leaving_lockfile() -> TestResult {
    let setup = Setup::new()?;
    std::fs::create_dir(&setup.datadir)?;
    std::fs::write(setup.datadir.join("notes.txt"), "")?;
    assert!(matches!(
        setup.cluster(runtime)?.session(&[]),
        Err(pgdo::cluster::ClusterError::DataDirNotEmpty(..))
    ));
    assert!(!setup.datadir.join("pgdo.lock").exists());
    Ok(())
}

/// Express `path` relative to `base` using `..` components. Both must be
/// absolute.
fn pathdiff(path: &std::path::Path, base: &std::path::Path) -> PathBuf {
    let path: Vec<_> = path.components().collect();
    let base: Vec<_> = base.components().collect();
    let common = path.iter().zip(&base).take_while(|(a, b)| a == b).count();
    let mut relative = PathBuf::new();
    for _ in common..base.len() {
        relative.push("..");
    }
    for component in &path[common..] {
        relative.push(component);
    }
    relative
}

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

    fn cluster<S: Into<runtime::strategy::Strategy>>(&self, strategy: S) -> TestResult<Cluster> {
        Ok(Cluster::new(&self.datadir, strategy)?)
    }

    /// A strategy for inspecting a cluster after its session has ended. The
    /// cluster's own `PG_VERSION` selects the runtime.
    fn strategy() -> runtime::strategy::Strategy {
        runtime::strategy::Strategy::default()
    }
}
