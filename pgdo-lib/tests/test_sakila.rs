use pgdo::cluster::{Cluster, ClusterError};
use pgdo::runtime::strategy::Strategy;
use pgdo_test::sakila::load_sakila;

type TestResult = Result<(), ClusterError>;

/// Loading Sakila is slow-ish, so this uses only the default runtime.
#[test]
fn load_sakila_loads_sample_data() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    let session = Cluster::new(data_dir.path().join("data"), Strategy::default())?.session(&[])?;
    session.createdb("sakila")?;
    let mut client = session.connect(Some("sakila"))?;
    load_sakila(&mut client)?;
    let films: i64 = client.query_one("SELECT count(*) FROM film", &[])?.get(0);
    assert_eq!(films, 1000);
    session.end()?;
    Ok(())
}
