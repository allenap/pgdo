use pgdo::cluster::{config, Cluster, ClusterError};
use pgdo_test::for_all_runtimes;

type TestResult = Result<(), ClusterError>;

#[for_all_runtimes(min = "9.4")]
#[test]
fn cluster_set_parameter() -> TestResult {
    let data_dir = tempdir::TempDir::new("data")?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start()?;

    let mut client = cluster.connect(None)?;

    // By default, `trace_notify` is disabled.
    let parameter = config::Parameter::from("trace_notify");
    let value = parameter.get(&mut client)?;
    assert_eq!(value, Some(config::Value::Boolean(false)));

    // We'll enable it.
    parameter.set(&mut client, true)?;

    // We need to reload the configuration.
    config::reload(&mut client)?;

    // BUGBUG: We also need a fresh connection, otherwise the test below is
    // flaky. It is non-deterministic whether or not the setting is picked up.
    let mut client = cluster.connect(None)?;

    // Now `trace_notify` is enabled.
    let value = parameter.get(&mut client)?;
    assert_eq!(value, Some(config::Value::Boolean(true)));

    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_parameter_fetch() -> TestResult {
    let data_dir = tempdir::TempDir::new("data")?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start()?;
    let mut client = cluster.connect(None)?;
    let value = config::Parameter::from("application_name").get(&mut client)?;
    assert_eq!(value, Some(config::Value::String("".to_owned())));
    cluster.stop()?;
    Ok(())
}
