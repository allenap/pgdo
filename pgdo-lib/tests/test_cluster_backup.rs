use std::collections::HashSet;
use std::ffi::OsString;

use pgdo::cluster::{backup, resource, Cluster, ClusterError};
use pgdo::coordinate;
use pgdo_test::for_all_runtimes;

type TestResult = Result<(), ClusterError>;

#[for_all_runtimes(min = "10")]
#[test]
fn cluster_backup() -> TestResult {
    let rt = tokio::runtime::Runtime::new()?;

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let backup_dir = tempfile::TempDir::new()?;

    let cluster = Cluster::new(data_dir, runtime)?;
    let backup = rt
        .block_on(backup::Backup::prepare(backup_dir.path()))
        .unwrap();
    let lock = pgdo::lock::UnlockedFile::try_from(&temp_dir.path().join(".lock"))?;
    let resource = coordinate::resource::ResourceFree::new(lock, cluster);

    // Start the cluster and obtain `resource`.
    let (state, resource) = resource::startup(resource, &[]).unwrap();
    assert_eq!(state, coordinate::State::Modified); // Cluster was started.
    assert!(matches!(&resource, either::Right(_))); // Exclusive lock.

    // Run backup 3 times.
    for num in 1..=3 {
        let archive_command = format!("cp %p {}/%f", &backup.backup_wal_dir.display());
        let restart_needed = rt
            .block_on(backup.do_configure_archiving(&resource, &archive_command))
            .unwrap();
        assert!((restart_needed && num == 1) || (!restart_needed && num > 1));

        // Restart cluster via the `resource`.
        if let either::Right(ref resource) = resource {
            resource.facet().stop()?;
            resource.facet().start(&[])?;
        }

        // Run backup.
        rt.block_on(backup.do_base_backup(&resource)).unwrap();

        // WAL files have been archived.
        let files_wal = backup
            .backup_wal_dir
            .read_dir()?
            .filter_map(Result::ok)
            .filter(is_file)
            .collect::<Vec<_>>();
        assert_ne!(files_wal.len(), 0);

        // A base backup is in place alongside the WAL file directory.
        let dirs_observed = backup
            .backup_dir
            .read_dir()?
            .filter_map(Result::ok)
            .filter(is_dir)
            .map(|entry| entry.file_name())
            .collect::<HashSet<OsString>>();
        let dirs_expected = ["wal"]
            .into_iter()
            .map(String::from)
            .chain((1u32..=num).map(|n| format!("data.{n:010}")))
            .map(Into::into)
            .collect::<HashSet<OsString>>();
        assert_eq!(dirs_observed, dirs_expected);
    }

    Ok(())
}

fn is_file(entry: &std::fs::DirEntry) -> bool {
    entry
        .file_type()
        .ok()
        .map(|file_type| file_type.is_file())
        .unwrap_or_default()
}

fn is_dir(entry: &std::fs::DirEntry) -> bool {
    entry
        .file_type()
        .ok()
        .map(|file_type| file_type.is_dir())
        .unwrap_or_default()
}
