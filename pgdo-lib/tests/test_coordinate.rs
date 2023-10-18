use pgdo::coordinate::{run_and_destroy, run_and_stop, run_and_stop_if_exists};
use pgdo::prelude::*;
use pgdo_test::for_all_runtimes;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[for_all_runtimes]
#[test]
fn run_and_stop_leaves_the_cluster_in_place() -> TestResult {
    let setup = Setup::new(runtime)?;
    let databases = run_and_stop(&setup.cluster, setup.lock, Cluster::databases)??;
    assert!(!databases.is_empty());
    assert!(!setup.cluster.running()?);
    assert!(setup.datadir.exists());
    Ok(())
}
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_stop_if_exists_leaves_the_cluster_in_place() -> TestResult {
    let setup = Setup::new(runtime)?;
    setup.cluster.create()?;
    let databases = run_and_stop(&setup.cluster, setup.lock, Cluster::databases)??;
    assert!(!databases.is_empty());
    assert!(!setup.cluster.running()?);
    assert!(setup.datadir.exists());
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_stop_if_exists_returns_error_if_cluster_does_not_exist() -> TestResult {
    let setup = Setup::new(runtime)?;
    assert!(matches!(
        run_and_stop_if_exists(&setup.cluster, setup.lock, Cluster::databases),
        Err(CoordinateError::ClusterDoesNotExist)
    ));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_destroy_removes_the_cluster() -> TestResult {
    let setup = Setup::new(runtime)?;
    let databases = run_and_destroy(&setup.cluster, setup.lock, Cluster::databases)??;
    assert!(!databases.is_empty());
    assert!(!setup.cluster.running()?);
    assert!(!setup.datadir.exists());
    Ok(())
}
    Ok(())
}

#[allow(unused)]
struct Setup {
    tempdir: tempfile::TempDir,
    datadir: std::path::PathBuf,
    cluster: Cluster,
    lockpath: std::path::PathBuf,
    lock: lock::UnlockedFile,
}

impl Setup {
    fn new<S: Into<runtime::strategy::Strategy>>(strategy: S) -> TestResult<Self> {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        let cluster = Cluster::new(&datadir, strategy)?;
        let lockpath = tempdir.path().join("lock");
        let lock = lock::UnlockedFile::try_from(&lockpath)?;
        Ok(Self { tempdir, datadir, cluster, lockpath, lock })
    }
}
