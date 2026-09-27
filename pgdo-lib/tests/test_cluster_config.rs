use pgdo::cluster::{config, Cluster, ClusterError};
use pgdo_test::for_all_runtimes;

type TestResult = Result<(), ClusterError>;

#[for_all_runtimes]
#[test]
fn cluster_parameter_set() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start(&[])?;

    let mut client = cluster.connect(None)?;

    // By default, `trace_notify` is disabled.
    let parameter = config::Parameter::from("trace_notify");
    let value = parameter.get(&mut client)?;
    assert_eq!(value, Some(config::Value::Boolean(false)));

    // We'll enable it.
    parameter.set(&mut client, true)?;

    // We need to reload the configuration.
    config::reload(&mut client)?;

    // We also need a fresh connection, otherwise it is non-deterministic whether
    // the setting is picked up.
    let mut client = cluster.connect(None)?;

    // Now `trace_notify` is enabled.
    let value = parameter.get(&mut client)?;
    assert_eq!(value, Some(config::Value::Boolean(true)));

    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_parameter_get() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start(&[])?;

    let mut client = cluster.connect(None)?;
    let value = config::Parameter::from("application_name").get(&mut client)?;
    assert_eq!(value, Some(config::Value::String("pgdo".to_owned())));

    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_setting_list() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start(&[])?;

    let settings = config::Setting::list(&mut cluster.connect(None)?)?;
    let mapping: std::collections::HashMap<config::Parameter, config::Value> = settings
        .iter()
        .map(|setting| (setting.into(), setting.try_into().unwrap()))
        .collect();

    for (parameter, value) in mapping {
        println!("{parameter}: {value}");
    }

    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_setting_get() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start(&[])?;

    let parameter = config::Parameter::from("application_name");
    let application_name = config::Setting::get(parameter, &mut cluster.connect(None)?)?
        .expect("missing application_name setting");

    assert_eq!(application_name.setting, "pgdo");
    assert_eq!(application_name.vartype, "string");

    cluster.stop()?;
    Ok(())
}
