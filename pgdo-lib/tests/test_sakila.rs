use pgdo::cluster::Cluster;
use pgdo::runtime::strategy::Strategy;
use pgdo_test::sakila::load_sakila;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Loading Sakila is slow-ish, so this uses only the default runtime.
#[test]
fn load_sakila_loads_sample_data() -> TestResult {
    let datadir = tempfile::tempdir()?;
    let session = Cluster::new(datadir.path().join("data"), Strategy::default())?.session(&[])?;
    session.createdb("sakila")?;
    let user = pgdo::util::current_user()?;
    let mut client = pgdo_test::connect(&session.datadir, &user, "sakila")?;
    load_sakila(&mut client)?;
    let films: i64 = client.query_one("SELECT count(*) FROM film", &[])?.get(0);
    assert_eq!(films, 1000);
    session.end()?;
    Ok(())
}
