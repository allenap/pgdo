use pgdo::cluster::{config, Cluster, ClusterError};
use pgdo_test::for_all_runtimes;

type TestResult = Result<(), ClusterError>;

#[for_all_runtimes]
#[test]
fn cluster_parameter_set() -> TestResult {
    let datadir = tempfile::tempdir()?;
    let cluster = Cluster::new(&datadir, runtime)?;
    cluster.start(&[])?;

    // By default, `trace_notify` is disabled.
    let parameter = config::Parameter::from("trace_notify");
    let value = parameter.get(&cluster)?;
    assert_eq!(value, Some(config::Value::Boolean(false)));

    // We'll enable it.
    parameter.set(&cluster, true)?;

    // We need to reload the configuration.
    config::reload(&cluster)?;

    // Each call makes a fresh connection, which picks up the reloaded setting.
    // Now `trace_notify` is enabled.
    let value = parameter.get(&cluster)?;
    assert_eq!(value, Some(config::Value::Boolean(true)));

    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_parameter_get() -> TestResult {
    let datadir = tempfile::tempdir()?;
    let cluster = Cluster::new(&datadir, runtime)?;
    cluster.start(&[])?;

    let value = config::Parameter::from("application_name").get(&cluster)?;
    assert_eq!(value, Some(config::Value::String("pgdo".to_owned())));

    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_setting_list() -> TestResult {
    let datadir = tempfile::tempdir()?;
    let cluster = Cluster::new(&datadir, runtime)?;
    cluster.start(&[])?;

    let settings = config::Setting::list(&cluster)?;
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
    let datadir = tempfile::tempdir()?;
    let cluster = Cluster::new(&datadir, runtime)?;
    cluster.start(&[])?;

    let parameter = config::Parameter::from("application_name");
    let application_name =
        config::Setting::get(parameter, &cluster)?.expect("missing application_name setting");

    assert_eq!(application_name.setting, "pgdo");
    assert_eq!(application_name.vartype, "string");

    cluster.stop()?;
    Ok(())
}
