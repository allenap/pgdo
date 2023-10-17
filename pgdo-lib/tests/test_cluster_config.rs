use pgdo::cluster::{config, sqlx, Cluster, ClusterError};
use pgdo_test::for_all_runtimes;

type TestResult = Result<(), ClusterError>;

#[for_all_runtimes(min = "9.5")]
#[test]
fn cluster_set_parameter() -> TestResult {
    let data_dir = tempdir::TempDir::new("data")?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start()?;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let pool = cluster.pool(None);

        // By default, `trace_notify` is disabled.
        let parameter = config::Parameter::from("trace_notify");
        let value = parameter.get(&pool).await?;
        assert_eq!(value, Some(config::Value::Boolean(false)));

        // We'll enable it.
        parameter.set(&pool, true).await?;

        // We need to reload the configuration.
        config::reload(&pool).await?;

        // BUGBUG: We also need fresh connections, otherwise the test below is
        // flaky. It is non-deterministic whether the setting is picked up.
        // TODO: Maybe `RESET ALL` would work?
        let pool = cluster.pool(None);

        // Now `trace_notify` is enabled.
        let value = parameter.get(&pool).await?;
        assert_eq!(value, Some(config::Value::Boolean(true)));

        Ok::<(), sqlx::Error>(())
    })?;

    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes(min = "9.5")]
#[test]
fn cluster_parameter_fetch() -> TestResult {
    let data_dir = tempdir::TempDir::new("data")?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start()?;

    let rt = tokio::runtime::Runtime::new()?;
    let value = rt.block_on(async {
        config::Parameter::from("application_name")
            .get(&cluster.pool(None))
            .await
    })?;
    assert_eq!(value, Some(config::Value::String("pgdo".to_owned())));

    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes(min = "9.5")]
#[test]
fn cluster_settings_list() -> TestResult {
    let data_dir = tempdir::TempDir::new("data")?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    cluster.start()?;

    let rt = tokio::runtime::Runtime::new()?;
    let settings = rt.block_on(async { config::Setting::list(&cluster.pool(None)).await })?;
    let mapping: std::collections::HashMap<config::Parameter, config::Value> = settings
        .iter()
        .map(|setting| (setting.into(), setting.into()))
        .collect();

    for (parameter, value) in mapping {
        println!("{parameter}: {value}");
    }

    cluster.stop()?;
    Ok(())
}
