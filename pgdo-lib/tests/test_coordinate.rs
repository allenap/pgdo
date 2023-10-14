use pgdo::coordinate::{run_and_destroy, run_and_stop, run_and_stop_if_exists};
use pgdo::prelude::*;
use pgdo_test::for_all_runtimes;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[for_all_runtimes]
#[test]
fn run_and_stop_leaves_the_cluster_in_place() -> TestResult {
    let tempdir = tempdir::TempDir::new("somewhere")?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(&datadir, runtime)?;
    let lockpath = tempdir.path().join("lock");
    let lock = lock::UnlockedFile::try_from(&lockpath)?;
    let databases = run_and_stop(&cluster, lock, Cluster::databases)??;
    assert!(!databases.is_empty());
    assert!(!cluster.running()?);
    assert!(datadir.exists());
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_stop_if_exists_leaves_the_cluster_in_place() -> TestResult {
    let tempdir = tempdir::TempDir::new("somewhere")?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(&datadir, runtime)?;
    cluster.create()?;
    let lockpath = tempdir.path().join("lock");
    let lock = lock::UnlockedFile::try_from(&lockpath)?;
    let databases = run_and_stop(&cluster, lock, Cluster::databases)??;
    assert!(!databases.is_empty());
    assert!(!cluster.running()?);
    assert!(datadir.exists());
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_stop_if_exists_returns_error_if_cluster_does_not_exist() -> TestResult {
    let tempdir = tempdir::TempDir::new("somewhere")?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    let lockpath = tempdir.path().join("lock");
    let lock = lock::UnlockedFile::try_from(&lockpath)?;
    assert!(matches!(
        run_and_stop_if_exists(&cluster, lock, Cluster::databases),
        Err(CoordinateError::ClusterDoesNotExist)
    ));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_and_destroy_removes_the_cluster() -> TestResult {
    let tempdir = tempdir::TempDir::new("somewhere")?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(&datadir, runtime)?;
    let lockpath = tempdir.path().join("lock");
    let lock = lock::UnlockedFile::try_from(&lockpath)?;
    let databases = run_and_destroy(&cluster, lock, Cluster::databases)??;
    assert!(!databases.is_empty());
    assert!(!cluster.running()?);
    assert!(!datadir.exists());
    Ok(())
}
