use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::File;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use shell_quote::{QuoteExt, Sh};

use pgdo::cluster::State::*;
use pgdo::cluster::{self, exists, version, Cluster, ClusterError, ClusterStatus};
use pgdo::runtime::strategy::Strategy;
use pgdo::version::{PartialVersion, Version};
use pgdo_test::for_all_runtimes;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn connect(cluster: &Cluster) -> Result<postgres::Client, Box<dyn std::error::Error>> {
    let user = pgdo::util::current_user()?;
    Ok(pgdo_test::connect(
        &cluster.datadir,
        &user,
        cluster::DATABASE_POSTGRES,
    )?)
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
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(&datadir, runtime.clone())?;
    cluster.create()?;
    assert!(exists(&cluster));
    let cluster = Cluster::new(&datadir, runtime)?;
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
    let datadir = tempfile::tempdir()?; // NOT a subdirectory.
    let versionfile = datadir.path().join("PG_VERSION");
    File::create(&versionfile)?;
    let pg_version: PartialVersion = runtime.version.into();
    let pg_version = pg_version.widened(); // e.g. 16.4 -> 16.
    std::fs::write(&versionfile, format!("{pg_version}\n"))?;
    let cluster = Cluster::new(&datadir, runtime)?;
    assert!(matches!(version(&cluster), Ok(Some(_))));
    Ok(())
}

#[test]
fn cluster_with_unsupported_version_is_an_error() -> TestResult {
    let datadir = tempfile::tempdir()?;
    let cluster = Cluster::new(&datadir, Strategy::default())?;
    for old_version in ["9.6", "14"] {
        std::fs::write(
            datadir.path().join("PG_VERSION"),
            format!("{old_version}\n"),
        )?;
        assert!(matches!(
            cluster.status(),
            Err(ClusterError::UnsupportedVersion(version))
                if version.to_string() == old_version
        ));
    }
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_has_pidfile() -> TestResult {
    let datadir = PathBuf::from("/some/where");
    let cluster = Cluster::new(datadir, runtime)?;
    assert_eq!(
        PathBuf::from("/some/where/postmaster.pid"),
        cluster.pidfile()
    );
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_has_logfile() -> TestResult {
    let datadir = PathBuf::from("/some/where");
    let cluster = Cluster::new(datadir, runtime)?;
    assert_eq!(
        PathBuf::from("/some/where/postmaster.log"),
        cluster.logfile()
    );
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_create_creates_cluster() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    assert!(!exists(&cluster));
    assert!(cluster.create()? == Modified);
    assert!(exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_create_creates_cluster_with_neutral_locale_and_timezone() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime.clone())?;
    cluster.start(&[])?;
    let result = connect(&cluster)?.query("SHOW ALL", &[])?;
    let params: std::collections::HashMap<String, String> = result
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(params.get("TimeZone"), Some(&"UTC".into()));
    assert_eq!(params.get("log_timezone"), Some(&"UTC".into()));
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
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    assert!(!exists(&cluster));
    assert!(cluster.create()? == Modified);
    assert!(exists(&cluster));
    assert!(cluster.create()? == Unmodified);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_start_stop_starts_and_stops_cluster() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    assert_eq!(cluster.status()?, ClusterStatus::Missing);
    cluster.create()?;
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    cluster.start(&[])?;
    assert_eq!(cluster.status()?, ClusterStatus::Running);
    cluster.stop()?;
    assert_eq!(cluster.status()?, ClusterStatus::Stopped);
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_start_with_options() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    cluster.start(&[("example.setting".into(), "Hello, World!".into())])?;
    let example_setting: String = connect(&cluster)?
        .query_one("SHOW example.setting", &[])?
        .get(0);
    assert_eq!(example_setting, "Hello, World!");
    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_exec_sets_environment() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    cluster.create()?;
    cluster.start(&[])?;
    let envfile = tempdir.path().join("env");
    let mut env_command: Vec<u8> = "env -0 > ".into();
    env_command.push_quoted(Sh, &envfile);
    let env_args: [OsString; 2] = ["-c".into(), OsString::from_vec(env_command)];
    cluster.exec(None, "sh".into(), &env_args)?;
    let env = std::fs::read_to_string(envfile)?;
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
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
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
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    cluster.create()?;
    assert!(exists(&cluster));
    cluster.destroy()?;
    assert!(!exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_destroy_does_nothing_if_cluster_does_not_exist() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    assert!(!exists(&cluster));
    cluster.destroy()?;
    assert!(!exists(&cluster));
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_databases_returns_vec_of_database_names() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
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
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
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
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    cluster.start(&[])?;
    assert!(matches!(cluster.createdb("foo-bar")?, Modified));
    assert!(matches!(cluster.createdb("foo-bar")?, Unmodified));
    cluster.stop()?;
    Ok(())
}

#[for_all_runtimes]
#[test]
fn cluster_databases_that_do_not_exist_can_be_dropped_without_error() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
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
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = Cluster::new(datadir, runtime)?;
    cluster.create()?;
    let superusers = cluster::determine_superuser_role_names(&cluster)?;
    assert!(!superusers.is_empty());
    Ok(())
}

// ----------------------------------------------------------------------------
// Creating and destroying clusters alongside pgdo's own files.

fn default_cluster(datadir: &Path) -> Result<Cluster, ClusterError> {
    Cluster::new(datadir, Strategy::default())
}

fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn cluster_create_in_directory_with_pgdo_files() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    std::fs::create_dir(&datadir)?;
    std::fs::write(datadir.join("pgdo.lock"), "")?;
    let cluster = default_cluster(&datadir)?;
    assert_eq!(cluster.create()?, Modified);
    assert!(exists(&cluster));
    assert!(datadir.join("pgdo.lock").is_file());
    assert!(!datadir.join("pgdo.init").exists());
    assert!(!datadir.join("PG_VERSION.init").exists());
    Ok(())
}

#[test]
fn cluster_create_refuses_directory_with_other_files() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    std::fs::create_dir(&datadir)?;
    std::fs::write(datadir.join("pgdo.lock"), "")?;
    std::fs::write(datadir.join("important.txt"), "keep me")?;
    let cluster = default_cluster(&datadir)?;
    assert!(matches!(
        cluster.create(),
        Err(ClusterError::DataDirNotEmpty(dir, entries))
            if dir == datadir && entries == vec!["important.txt"]
    ));
    // Nothing was changed.
    assert_eq!(entries(&datadir), vec!["important.txt", "pgdo.lock"]);
    Ok(())
}

#[test]
fn cluster_create_resumes_interrupted_move() -> TestResult {
    use std::os::unix::fs::PermissionsExt;

    // Make a cluster elsewhere to play the part of `initdb`'s output.
    let tempdir = tempfile::tempdir()?;
    let source = default_cluster(&tempdir.path().join("source"))?;
    source.create()?;

    // Arrange the data directory as if a move was interrupted: `PG_VERSION`
    // has been moved to `PG_VERSION.init`, and some entries have been moved.
    let datadir = tempdir.path().join("data");
    std::fs::create_dir(&datadir)?;
    std::fs::rename(&source.datadir, datadir.join("pgdo.init"))?;
    std::fs::rename(
        datadir.join("pgdo.init/PG_VERSION"),
        datadir.join("PG_VERSION.init"),
    )?;
    for name in ["base", "global"] {
        std::fs::rename(datadir.join("pgdo.init").join(name), datadir.join(name))?;
    }

    let cluster = default_cluster(&datadir)?;
    assert!(!exists(&cluster));
    assert_eq!(cluster.create()?, Modified);
    assert!(exists(&cluster));
    assert!(!datadir.join("pgdo.init").exists());
    assert!(!datadir.join("PG_VERSION.init").exists());
    let mode = std::fs::metadata(&datadir)?.permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    // It's a working cluster.
    cluster.start(&[])?;
    assert!(cluster.databases()?.contains(&"postgres".to_owned()));
    cluster.stop()?;
    Ok(())
}

#[test]
fn cluster_create_discards_interrupted_initdb() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    std::fs::create_dir_all(datadir.join("pgdo.init/base"))?;
    std::fs::write(datadir.join("pgdo.init/junk"), "")?;
    let cluster = default_cluster(&datadir)?;
    assert_eq!(cluster.create()?, Modified);
    assert!(exists(&cluster));
    assert!(!datadir.join("junk").exists());
    assert!(!datadir.join("pgdo.init").exists());
    Ok(())
}

#[test]
fn cluster_create_makes_data_directory_private() -> TestResult {
    use std::os::unix::fs::PermissionsExt;
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    std::fs::create_dir(&datadir)?;
    std::fs::set_permissions(&datadir, std::fs::Permissions::from_mode(0o755))?;
    let cluster = default_cluster(&datadir)?;
    cluster.create()?;
    let mode = std::fs::metadata(&datadir)?.permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    Ok(())
}

#[test]
fn cluster_destroy_keeps_lockfile() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    std::fs::create_dir(&datadir)?;
    std::fs::write(datadir.join("pgdo.lock"), "")?;
    let cluster = default_cluster(&datadir)?;
    cluster.start(&[])?;
    assert_eq!(cluster.destroy()?, Modified);
    assert_eq!(entries(&datadir), vec!["pgdo.lock"]);
    Ok(())
}

#[test]
fn cluster_destroy_removes_data_directory_without_pgdo_files() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let datadir = tempdir.path().join("data");
    let cluster = default_cluster(&datadir)?;
    cluster.start(&[])?;
    assert_eq!(cluster.destroy()?, Modified);
    assert!(!datadir.exists());
    assert_eq!(cluster.destroy()?, Unmodified);
    Ok(())
}
