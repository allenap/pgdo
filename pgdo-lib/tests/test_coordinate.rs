use pgdo::coordinate::{run_and_destroy, run_and_stop, run_and_stop_if_exists};
use pgdo::prelude::*;
use pgdo_test::for_all_runtimes;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[for_all_runtimes]
#[test]
fn run_and_stop_leaves_the_cluster_in_place() -> TestResult {
    let (setup, lock) = Setup::run(runtime)?;
    let databases = run_and_stop(&setup.cluster, &[], lock, || setup.cluster.databases())??;
    assert!(!databases.is_empty());
    assert!(!setup.cluster.running()?);
    assert!(setup.datadir.exists());
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_stop_if_exists_leaves_the_cluster_in_place() -> TestResult {
    let (setup, lock) = Setup::run(runtime)?;
    setup.cluster.create()?;
    let databases = run_and_stop(&setup.cluster, &[], lock, || setup.cluster.databases())??;
    assert!(!databases.is_empty());
    assert!(!setup.cluster.running()?);
    assert!(setup.datadir.exists());
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_stop_if_exists_returns_error_if_cluster_does_not_exist() -> TestResult {
    let (setup, lock) = Setup::run(runtime)?;
    assert!(matches!(
        run_and_stop_if_exists(&setup.cluster, &[], lock, || setup.cluster.databases()),
        Err(CoordinateError::DoesNotExist)
    ));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_destroy_removes_the_cluster() -> TestResult {
    let (setup, lock) = Setup::run(runtime)?;
    let databases = run_and_destroy(&setup.cluster, &[], lock, || setup.cluster.databases())??;
    assert!(!databases.is_empty());
    assert!(!setup.cluster.running()?);
    assert!(!setup.datadir.exists());
    Ok(())
}

#[allow(unused)]
struct Setup {
    tempdir: tempfile::TempDir,
    datadir: std::path::PathBuf,
    cluster: Cluster,
}

impl Setup {
    fn run<S: Into<runtime::strategy::Strategy>>(
        strategy: S,
    ) -> TestResult<(Self, lock::UnlockedFile)> {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        let cluster = Cluster::new(&datadir, strategy)?;
        let lockpath = tempdir.path().join("lock");
        Ok((
            Self { tempdir, datadir, cluster },
            lock::UnlockedFile::try_from(&lockpath)?,
        ))
    }
}
