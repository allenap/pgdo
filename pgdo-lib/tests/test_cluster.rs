use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::File;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use shell_quote::{QuoteExt, Sh};

use pgdo::cluster::{
    self, exists,
    sqlx::{query, Row},
    version, Cluster, ClusterError, ClusterStatus,
};
use pgdo::coordinate::State::*;
use pgdo::version::{PartialVersion, Version};
use pgdo_test::for_all_runtimes;

type TestResult = Result<(), ClusterError>;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Runtime::new().unwrap().block_on(future)
}

#[for_all_runtimes]
#[test]
fn cluster_new() -> TestResult {
    let cluster = Cluster::new("some/path", runtime)?;
    assert_eq!(Path::new("some/path"), cluster.datadir);
    assert_eq!(cluster.status()?, ClusterStatus::Missing);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_does_not_exist() -> TestResult {
    let cluster = Cluster::new("some/path", runtime)?;
    assert!(!exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_does_exist() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(&data_dir, runtime.clone())?;
    cluster.create()?;
    assert!(exists(&cluster));
    let cluster = Cluster::new(&data_dir, runtime)?;
    assert!(exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_has_no_version_when_it_does_not_exist() -> TestResult {
    let cluster = Cluster::new("some/path", runtime)?;
    assert!(matches!(version(&cluster), Ok(None)));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_has_version_when_it_does_exist() -> TestResult {
    let data_dir = tempfile::tempdir()?; // NOT a subdirectory.
    let version_file = data_dir.path().join("PG_VERSION");
    File::create(&version_file)?;
    let pg_version: PartialVersion = runtime.version.into();
    let pg_version = pg_version.widened(); // e.g. 9.6.5 -> 9.6 or 14.3 -> 14.
    std::fs::write(&version_file, format!("{pg_version}\n"))?;
    let cluster = Cluster::new(&data_dir, runtime)?;
    assert!(matches!(version(&cluster), Ok(Some(_))));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_has_pid_file() -> TestResult {
    let data_dir = PathBuf::from("/some/where");
    let cluster = Cluster::new(data_dir, runtime)?;
    assert_eq!(
        PathBuf::from("/some/where/postmaster.pid"),
        cluster.pidfile()
    );
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_has_log_file() -> TestResult {
    let data_dir = PathBuf::from("/some/where");
    let cluster = Cluster::new(data_dir, runtime)?;
    assert_eq!(
        PathBuf::from("/some/where/postmaster.log"),
        cluster.logfile()
    );
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_create_creates_cluster() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    assert!(!exists(&cluster));
    assert!(cluster.create()? == Modified);
    assert!(exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_create_creates_cluster_with_neutral_locale_and_timezone() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime.clone())?;
    cluster.start(&[])?;
    let result = block_on(async {
        let pool = cluster.pool(None)?;
        Ok::<_, ClusterError>(query("SHOW ALL").fetch_all(&pool).await?)
    })?;
    let params: std::collections::HashMap<String, String> = result
        .into_iter()
        .map(|row| (row.get::<String, _>(0), row.get::<String, _>(1)))
        .collect();
    // PostgreSQL 9.4.22's release notes reveal:
    //
    //   Etc/UCT is now a backward-compatibility link to Etc/UTC,
    //   instead of being a separate zone that generates the
    //   abbreviation UCT, which nowadays is typically a typo.
    //   PostgreSQL will still accept UCT as an input zone abbreviation,
    //   but it won't output it.
    //     -- https://www.postgresql.org/docs/9.4/release-9-4-22.html
    //
    if runtime.version < Version::from_str("9.4.22")? {
        let dealias = |tz: &String| (if tz == "UCT" { "UTC" } else { tz }).to_owned();
        assert_eq!(params.get("TimeZone").map(dealias), Some("UTC".into()));
        assert_eq!(params.get("log_timezone").map(dealias), Some("UTC".into()));
    } else {
        assert_eq!(params.get("TimeZone"), Some(&"UTC".into()));
        assert_eq!(params.get("log_timezone"), Some(&"UTC".into()));
    }
    // PostgreSQL 16's release notes reveal:
    //
    //   Remove read-only server variables lc_collate and lc_ctype …
    //   Collations and locales can vary between databases so having
    //   them as read-only server variables was unhelpful.
    //     -- https://www.postgresql.org/docs/16/release-16.html
    //
    if runtime.version >= Version::from_str("16.0")? {
        assert_eq!(params.get("lc_collate"), None);
        assert_eq!(params.get("lc_ctype"), None);
        // 🚨 Also in PostgreSQL 16, lc_messages is _sometimes_ the empty string
        // when specified as "C" via any mechanism:
        //
        // - Explicitly given to `initdb`, e.g. `initdb --locale=C`, `initdb
        //   --lc-messages=C`.
        //
        // - Inherited from the environment (LC_ALL, LC_MESSAGES) at any point
        //   (`initdb`, `pg_ctl start`, or from the client).
        //
        // When a different locale is used with `initdb --locale` or `initdb
        // --lc-messages`, e.g. POSIX, es_ES, the locale IS used; lc_messages
        // reflects the choice.
        //
        // It's not yet clear if this is a bug or intentional. There has been no
        // response to the bug report (link below), but the behaviour here has
        // changed by 16.2 (possibly earlier; I did not check).
        //
        // Bug report:
        // https://www.postgresql.org/message-id/18136-4914128da6cfc502%40postgresql.org
        if runtime.version >= Version::from_str("16.2")? {
            assert_eq!(params.get("lc_messages"), Some(&"C".into()));
        } else {
            assert_eq!(params.get("lc_messages"), Some(&String::new()));
        }
    } else {
        assert_eq!(params.get("lc_collate"), Some(&"C".into()));
        assert_eq!(params.get("lc_ctype"), Some(&"C".into()));
        assert_eq!(params.get("lc_messages"), Some(&"C".into()));
    }
    assert_eq!(params.get("lc_monetary"), Some(&"C".into()));
    assert_eq!(params.get("lc_numeric"), Some(&"C".into()));
    assert_eq!(params.get("lc_time"), Some(&"C".into()));
    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_create_does_nothing_when_it_already_exists() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    assert!(!exists(&cluster));
    assert!(cluster.create()? == Modified);
    assert!(exists(&cluster));
    assert!(cluster.create()? == Unmodified);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_start_stop_starts_and_stops_cluster() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    assert_eq!(cluster.status()?, ClusterStatus::Missing);
    cluster.create()?;
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    cluster.start(&[])?;
    assert_eq!(cluster.status()?, ClusterStatus::Running);
    cluster.stop()?;
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    Ok(())
}

/// Versions before 9.2 don't appear to support custom settings, i.e. those with
/// a period in the middle, so it's hard to test this on older versions.
#[for_all_runtimes(min = "9.2")]
#[test]
fn cluster_start_with_options() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.start(&[("example.setting".into(), "Hello, World!".into())])?;
    let example_setting = block_on(async {
        let pool = cluster.pool(None)?;
        Ok::<_, ClusterError>(query("SHOW example.setting").fetch_one(&pool).await?)
    })
    .map(|row| row.get::<String, _>(0))?;
    assert_eq!(example_setting, "Hello, World!");
    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_exec_sets_environment() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.create()?;
    cluster.start(&[])?;
    let env_file = temp_dir.path().join("env");
    let mut env_command: Vec<u8> = "env -0 > ".into();
    env_command.push_quoted(Sh, &env_file);
    let env_args: [OsString; 2] = ["-c".into(), OsString::from_vec(env_command)];
    cluster.exec(None, "sh".into(), &env_args)?;
    let env = std::fs::read_to_string(env_file)?;
    let env = env
        .split('\u{0}')
        .filter_map(|line| line.split_once('='))
        .collect::<HashMap<_, _>>();
    assert_eq!(
        env.get("PGDATA").map(PathBuf::from).as_ref(),
        Some(&cluster.datadir)
    );
    assert_eq!(
        env.get("PGHOST").map(PathBuf::from).as_ref(),
        Some(&cluster.datadir)
    );
    assert_eq!(env.get("PGDATABASE"), Some("postgres").as_ref());
    assert!(matches!(env.get("DATABASE_URL"), Some(url) if url.starts_with("postgresql://")));
    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_destroy_stops_and_removes_cluster() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.create()?;
    cluster.start(&[])?;
    assert!(exists(&cluster));
    cluster.destroy()?;
    assert!(!exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_destroy_removes_cluster() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.create()?;
    assert!(exists(&cluster));
    cluster.destroy()?;
    assert!(!exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_destroy_does_nothing_if_cluster_does_not_exist() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    assert!(!exists(&cluster));
    cluster.destroy()?;
    assert!(!exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_databases_returns_vec_of_database_names() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.start(&[])?;

    let expected: HashSet<String> = ["postgres", "template0", "template1"]
        .iter()
        .map(ToString::to_string)
        .collect();
    let observed: HashSet<String> = cluster.databases()?.iter().cloned().collect();
    assert_eq!(expected, observed);

    cluster.destroy()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_databases_with_non_plain_names_can_be_created_and_dropped() -> TestResult {
    // PostgreSQL identifiers containing hyphens, for example, or where we
    // want to preserve capitalisation, are possible.
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.start(&[])?;
    cluster.createdb("foo-bar")?;
    cluster.createdb("Foo-BAR")?;

    let expected: HashSet<String> = ["foo-bar", "Foo-BAR", "postgres", "template0", "template1"]
        .iter()
        .map(ToString::to_string)
        .collect();
    let observed: HashSet<String> = cluster.databases()?.iter().cloned().collect();
    assert_eq!(expected, observed);

    cluster.dropdb("foo-bar")?;
    cluster.dropdb("Foo-BAR")?;
    cluster.destroy()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_databases_that_already_exist_can_be_created_without_error() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.start(&[])?;
    assert!(matches!(cluster.createdb("foo-bar")?, Modified));
    assert!(matches!(cluster.createdb("foo-bar")?, Unmodified));
    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_databases_that_do_not_exist_can_be_dropped_without_error() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.start(&[])?;
    cluster.createdb("foo-bar")?;
    assert!(matches!(cluster.dropdb("foo-bar")?, Modified));
    assert!(matches!(cluster.dropdb("foo-bar")?, Unmodified));
    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn determine_superuser_role_names() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = Cluster::new(data_dir, runtime)?;
    cluster.create()?;
    let superusers = cluster::determine_superuser_role_names(&cluster)?;
    assert!(!superusers.is_empty());
    Ok(())
}

#[for_all_runtimes]
#[test]
fn run_starts_cluster_and_returns_guard() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let cluster = cluster::run(data_dir, Default::default()).unwrap();
    assert_eq!(cluster.status()?, ClusterStatus::Running);
    Ok(())
}
