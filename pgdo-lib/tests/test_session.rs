use std::path::PathBuf;

use pgdo::cluster::{Cluster, ClusterStatus, Finish, State};
use pgdo::runtime;
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

/// A session must be able to finish a `Cluster::create` that was interrupted
/// while moving files into place, despite the data directory not being empty.
#[for_all_runtimes]
#[test]
fn session_finishes_interrupted_create() -> TestResult {
    let setup = Setup::new()?;
    // Make a cluster elsewhere to play the part of `initdb`'s output, then
    // arrange it as if a move was interrupted.
    let source = setup.datadir.with_file_name("source");
    Cluster::new(&source, runtime.clone())?.create()?;
    std::fs::create_dir(&setup.datadir)?;
    std::fs::rename(&source, setup.datadir.join("pgdo.init"))?;
    std::fs::rename(
        setup.datadir.join("pgdo.init/PG_VERSION"),
        setup.datadir.join("PG_VERSION.init"),
    )?;
    std::fs::rename(
        setup.datadir.join("pgdo.init/base"),
        setup.datadir.join("base"),
    )?;

    let session = setup.cluster(runtime)?.session(&[])?;
    assert_eq!(session.status()?, ClusterStatus::Running);
    assert!(!setup.datadir.join("pgdo.init").exists());
    assert!(!setup.datadir.join("PG_VERSION.init").exists());
    session.end()?;
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
