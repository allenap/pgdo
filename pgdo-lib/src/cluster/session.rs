//! Use a cluster safely alongside other processes.
//!
//! A [`Session`] creates and starts a cluster as necessary, and stops it – or
//! destroys it – when the session ends, **unless** other sessions are still
//! using it. Many processes can hold sessions on the same cluster at once, e.g.
//! as part of a test suite; the last one out turns off the lights.
//!
//! ```rust
//! # use pgdo::{cluster::Cluster, runtime::strategy::Strategy};
//! let tempdir = tempfile::tempdir()?;
//! let cluster = Cluster::new(tempdir.path().join("data"), Strategy::default())?;
//! let session = cluster.session(&[])?;
//! assert!(session.running()?);
//! let databases = session.databases()?;
//! session.end()?; // Or drop it; see `Session::end` for the difference.
//! # Ok::<(), pgdo::cluster::ClusterError>(())
//! ```
//!
//! Coordination uses [`flock(2)`](https://linux.die.net/man/2/flock) locks on
//! the file `pgdo.lock` in the cluster's data directory: a shared lock while
//! the session is held; an exclusive lock to create and start the cluster, and
//! to stop or destroy it. Because the lock file lives in the data directory, it
//! works for any process that can see that directory, whatever path it uses to
//! get there, as long as `flock` works on the filesystem and all processes run
//! on the same kernel.
//!
//! `flock` does not work between kernels, e.g. between macOS and a Linux VM,
//! and PostgreSQL's own check for a running server does not work between PID
//! namespaces, e.g. between containers. So a session that starts a cluster
//! also records its owner in `pgdo.owner`, and sessions refuse to use a cluster
//! owned by another kernel; see [`Takeover`]. On the same kernel, a session in
//! another namespace can use a running cluster via its Unix-domain socket, but
//! only a session in the owner's namespace will stop it.
//!
//! When a session destroys a cluster it removes the lock file too. Another
//! process might be waiting on that lock file, or might create a new one
//! meanwhile, so after taking any lock, a session checks that the file it has
//! locked is still the file at `pgdo.lock`; if not, it tries again.
//!
//! Starting, ending, and dropping a session all block, sometimes for a while:
//! they may wait for other processes to release locks, and they run `pg_ctl`.
//! This is safe within an async context, but blocks the current thread; to
//! avoid that, start and end sessions within something like Tokio's
//! `spawn_blocking`. See also [Blocking][`Cluster#blocking`].

use std::marker::PhantomData;
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{fs, io, ops};

use either::Either::{Left, Right};
use rand::Rng;

use super::owner::{self, Relation};
use super::postmaster::{self, Postmaster};
use super::{client, Cluster, ClusterError, Options, State};
use crate::lock;

/// Set by [`interrupt`].
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Ask sessions that are starting up in this process to give up as soon as they
/// safely can, e.g. when the user presses Ctrl-C.
///
/// Sessions that are starting up stop with [`ClusterError::Interrupted`],
/// stopping any server they started, and releasing anything they claimed.
/// Sessions that have already started are not affected.
///
/// Signals are process-wide, so this is too: once called, every attempt to
/// start a session in this process fails. It is safe to call from a signal
/// handler.
pub fn interrupt() {
    INTERRUPTED.store(true, Ordering::SeqCst);
}

/// Has [`interrupt`] been called?
fn interrupted() -> bool {
    INTERRUPTED.load(Ordering::SeqCst)
}

/// What to do with a cluster when a [`Session`] ends and no other sessions are
/// using the cluster.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Finish {
    /// Stop the cluster.
    #[default]
    Stop,
    /// Stop the cluster and **delete its data directory**.
    Destroy,
}

/// Whether a [`Session`] may take over a cluster that was last used on another
/// host or VM, i.e. from another kernel.
///
/// pgdo cannot tell whether a server on another kernel is still running, only
/// whether sessions there show signs of life – their heartbeats. A cluster
/// without heartbeats may still be running, e.g. if pgdo was killed, so pgdo
/// will not take it over unless asked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Takeover {
    /// Refuse to use a cluster owned by another kernel.
    #[default]
    Never,
    /// Take over a cluster owned by another kernel if it shows no signs of
    /// life: its owner file is watched for a while – currently 30 seconds –
    /// and if it changes at all, the cluster is not taken over. This does not
    /// depend on the clocks of different kernels agreeing. **Danger**: if its
    /// server is in fact still running, the cluster will be corrupted.
    IfStale,
}

/// A cluster that this process is using. See the [module
/// documentation][`self`].
///
/// Dereferences to [`Cluster`].
#[derive(Debug)]
pub struct Session {
    cluster: Cluster,
    lock: Option<lock::LockedFileShared>,
    finish: Finish,
    heartbeat: Option<owner::Heartbeat>,
}

impl Cluster {
    /// Start a [`Session`] with this cluster, creating and starting it as
    /// necessary.
    ///
    /// The cluster's data directory is created if it does not exist, and the
    /// cluster's path is made absolute (canonicalized).
    pub fn session(self, options: Options<'_>) -> Result<Session, ClusterError> {
        self.session_with_takeover(options, Takeover::Never)
    }

    /// Start a [`Session`] with this cluster, as with
    /// [`session`][Self::session], but choosing whether to take over a cluster
    /// last used on another host or VM; see [`Takeover`].
    pub fn session_with_takeover(
        self,
        options: Options<'_>,
        takeover: Takeover,
    ) -> Result<Session, ClusterError> {
        self.start_session(options, takeover, &interrupted)
    }

    /// Start a [`Session`], checking `interrupted` – rather than [`interrupt`]
    /// – to see if startup should be abandoned.
    fn start_session(
        mut self,
        options: Options<'_>,
        takeover: Takeover,
        interrupted: Interrupted<'_>,
    ) -> Result<Session, ClusterError> {
        // Refuse a directory that is neither a cluster nor empty before putting
        // a lock file in it. `Cluster::create` checks again, under the lock.
        // Skip this if a `Cluster::create` was interrupted while moving files
        // into place: it will finish the job.
        if self.datadir.is_dir() && !super::exists(&self) && !self.create_interrupted() {
            self.check_datadir_empty()?;
        }
        fs::create_dir_all(&self.datadir)?;
        self.datadir = self.datadir.canonicalize()?;
        let (lock, heartbeat) = startup(&self, options, takeover, interrupted)?;
        Ok(Session {
            cluster: self,
            lock: Some(lock),
            finish: Finish::default(),
            heartbeat: Some(heartbeat),
        })
    }
}

impl Session {
    /// Choose what happens when this session ends; see [`Finish`].
    #[must_use]
    pub fn finish(mut self, finish: Finish) -> Self {
        self.finish = finish;
        self
    }

    /// End this session.
    ///
    /// If no other sessions are using the cluster, it is stopped or destroyed
    /// according to [`Finish`]. Returns [`State::Modified`] if this session did
    /// so, else [`State::Unmodified`].
    ///
    /// Dropping a session does the same, but can only log errors.
    pub fn end(mut self) -> Result<State, ClusterError> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> Result<State, ClusterError> {
        let Some(lock) = self.lock.take() else {
            return Ok(State::Unmodified);
        };
        // Keep the heartbeat going until this function returns, i.e. until
        // the cluster is stopped and the owner file is gone; stopping can take
        // a while. A heartbeat after the owner file is removed does nothing.
        let _heartbeat = self.heartbeat.take();
        match upgrade(lock)? {
            // The cluster is in use elsewhere. There's nothing more to do.
            None => Ok(State::Unmodified),
            // We have an exclusive lock, so we can stop or destroy the cluster.
            // The lock is released when `lock` is dropped, which must happen
            // only after everything else is done.
            Some(lock) => {
                let Some(verified) = verify(&self.cluster, &lock)? else {
                    // Our lock file was removed: while `upgrade` had released
                    // our lock, another session destroyed the cluster. There is
                    // nothing more to do.
                    return Ok(State::Unmodified);
                };
                match ownership(&self.cluster, &verified)? {
                    // Only a process in the owner's namespace can stop the
                    // server: `pg_ctl` signals the PID in `postmaster.pid`,
                    // which means nothing – or something else entirely – in
                    // other namespaces. Leave it running; a session in the
                    // owner's namespace will stop it when that session ends.
                    Ownership::SameKernel | Ownership::OtherKernel(_) => {
                        log::warn!(
                            "Leaving cluster {} running: it was started in another \
                             namespace, from where it must be stopped.",
                            self.cluster.datadir.display()
                        );
                        Ok(State::Unmodified)
                    }
                    Ownership::SameNamespace | Ownership::None | Ownership::Copy => {
                        match self.finish {
                            Finish::Stop => {
                                let state = stop(&self.cluster)?;
                                owner::release(&self.cluster.ownerfile())?;
                                Ok(state)
                            }
                            Finish::Destroy => {
                                stop(&self.cluster)?;
                                let state = self.cluster.remove()?;
                                remove_datadir(&self.cluster)?;
                                drop(lock);
                                Ok(state)
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Err(err) = self.shutdown() {
            log::error!(
                "Error ending session with cluster {}: {err}",
                self.cluster.datadir.display()
            );
        }
    }
}

impl ops::Deref for Session {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

// ----------------------------------------------------------------------------

/// Open the cluster's lock file, creating it – and the data directory – if
/// necessary.
fn open_lockfile(cluster: &Cluster) -> Result<lock::UnlockedFile, ClusterError> {
    let path = cluster.lockfile();
    match lock::UnlockedFile::try_from(path.as_path()) {
        // The data directory may have been removed by a session that destroyed
        // the cluster.
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(&cluster.datadir)?;
            Ok(lock::UnlockedFile::try_from(path.as_path())?)
        }
        result => Ok(result?),
    }
}

/// Proof that this process holds a lock on the cluster's current lock file, as
/// of when it was checked. Obtained only from [`verify`], and borrows the lock,
/// so it cannot outlive it. Functions whose results are meaningful only while
/// holding the lock, e.g. [`ownership`], require one.
struct Verified<'a>(PhantomData<&'a ()>);

/// Check that the locked file is still the cluster's lock file. It may have
/// been removed – and maybe replaced – by a session that destroyed the cluster.
fn verify<'a, L: lock::Held>(
    cluster: &Cluster,
    lock: &'a L,
) -> Result<Option<Verified<'a>>, ClusterError> {
    let locked = lock.as_ref().metadata()?;
    let current = match fs::metadata(cluster.lockfile()) {
        Ok(current) => current,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => Err(err)?,
    };
    let same = locked.dev() == current.dev() && locked.ino() == current.ino();
    Ok(same.then_some(Verified(PhantomData)))
}

/// Who owns the cluster, from the point of view of this process.
enum Ownership {
    /// No owner file: the cluster is not running, or was started without one,
    /// e.g. by an older version of pgdo, or by hand.
    None,
    /// The owner file was copied from another data directory; ignore it.
    Copy,
    /// Owned from this namespace.
    SameNamespace,
    /// Owned from another namespace – e.g. another container – on this kernel.
    SameKernel,
    /// Owned from another kernel, e.g. a VM.
    OtherKernel(owner::Found),
}

/// Who owns the cluster? Requires proof of a current lock on the cluster.
///
/// The answer may be out of date by the time the caller acts on it: another
/// process could claim the cluster just after we look. That's safe:
///
/// - On this kernel, a process claims the cluster only while holding an
///   exclusive lock, so no claim can happen while we hold any lock.
/// - On another kernel, our lock means nothing, so a claim can happen at any
///   time. But when there is no owner, the caller's next step is to claim the
///   cluster itself, and only one claim can succeed. If ours fails, the caller
///   looks again, and finds the new owner.
fn ownership(cluster: &Cluster, _: &Verified<'_>) -> Result<Ownership, ClusterError> {
    let Some(found) = owner::read(&cluster.ownerfile())? else {
        return Ok(Ownership::None);
    };
    let current = owner::Record::current(&cluster.datadir)?;
    Ok(match found.record.relation(&current) {
        Relation::Copy => Ownership::Copy,
        Relation::SameNamespace => Ownership::SameNamespace,
        Relation::SameKernel => Ownership::SameKernel,
        Relation::OtherKernel => Ownership::OtherKernel(found),
    })
}

/// Is the cluster running? How we can tell depends on who owns it. Requires
/// proof of a current lock on the cluster.
fn running(
    cluster: &Cluster,
    ownership: &Ownership,
    _: &Verified<'_>,
) -> Result<bool, ClusterError> {
    Ok(match ownership {
        // In the same namespace, we can check the server's PID.
        Ownership::SameNamespace => server_here(cluster, postmaster::check(&cluster.datadir)?)?,
        // From another namespace, only the socket can tell us.
        Ownership::SameKernel => server_elsewhere(cluster)?,
        // With no (meaningful) owner, a server could have been started from
        // any namespace, so check both ways.
        Ownership::None | Ownership::Copy => {
            server_here(cluster, postmaster::check(&cluster.datadir)?)?
                || server_elsewhere(cluster)?
        }
        // We cannot tell. Callers must deal with this case first.
        Ownership::OtherKernel(_) => unreachable!("cannot tell if cluster runs on another kernel"),
    })
}

/// Is a server for this cluster running in a namespace where we can signal it,
/// i.e. where its PID in `postmaster.pid` means something?
fn server_here(cluster: &Cluster, postmaster: Postmaster) -> Result<bool, ClusterError> {
    match postmaster {
        Postmaster::Running(_) => Ok(true),
        Postmaster::Absent | Postmaster::Dead(_) | Postmaster::Other(_) => Ok(false),
        // We cannot check the PID on this platform, so fall back to `pg_ctl
        // status`, which trusts it.
        Postmaster::Unknown(_) => cluster.running(),
    }
}

/// Is a server for this cluster running, perhaps in another namespace, where
/// its PID in `postmaster.pid` means nothing to us? Only its socket can tell.
///
/// The socket's name comes from the port in `postmaster.pid`, in case the
/// server was started on a port other than [`PORT`][super::PORT]. But not the
/// socket's directory: that is a path in the server's namespace, which may name
/// another place here, e.g. when the data directory is a bind mount. pgdo puts
/// the socket in the data directory, so look there.
fn server_elsewhere(cluster: &Cluster) -> Result<bool, ClusterError> {
    match postmaster::read(&cluster.datadir)? {
        // A running server always has a `postmaster.pid`.
        None => Ok(false),
        Some(pidfile) => {
            let port = pidfile.port.unwrap_or(super::PORT);
            client::accepts_connections(&cluster.datadir, port).map_err(|source| {
                ClusterError::ServerStateUnknown {
                    path: client::socket_path(&cluster.datadir, port),
                    source,
                }
            })
        }
    }
}

/// Stop the cluster's server, if there is one. Never signal a process that is
/// not a PostgreSQL server for this cluster, e.g. one that reused the PID of a
/// server that has gone.
fn stop(cluster: &Cluster) -> Result<State, ClusterError> {
    match postmaster::check(&cluster.datadir)? {
        Postmaster::Running(_) | Postmaster::Unknown(_) => cluster.stop(),
        Postmaster::Absent | Postmaster::Dead(_) => Ok(State::Unmodified),
        Postmaster::Other(pid) => {
            log::warn!(
                "Not stopping cluster {}: its postmaster.pid names PID {pid}, which is not \
                 a PostgreSQL server for this cluster.",
                cluster.datadir.display()
            );
            Ok(State::Unmodified)
        }
    }
}

/// Refuse to use a cluster owned by another kernel, unless taking over is
/// allowed. Whether to take over is then decided by observing the owner file.
fn refuse_other_kernel(found: &owner::Found, takeover: Takeover) -> Result<(), ClusterError> {
    let (hostname, age) = (found.record.hostname.clone(), found.age);
    match takeover {
        Takeover::IfStale => Ok(()),
        Takeover::Never if found.is_fresh() => Err(ClusterError::InUseElsewhere { hostname, age }),
        Takeover::Never => Err(ClusterError::StaleElsewhere { hostname, age }),
    }
}

/// Sleep for a random time between 200ms and 1000ms. When many processes
/// compete, this helps one of them rapidly acquire an exclusive lock and create
/// and start the cluster.
fn backoff() {
    let delay = 200 + (rand::rng().next_u32() % 800);
    std::thread::sleep(Duration::from_millis(u64::from(delay)));
}

/// How long to watch a stale owner file from another kernel before taking over.
/// Much shorter in unit tests.
const OBSERVE_BEFORE_TAKEOVER: Duration = if cfg!(test) {
    Duration::from_millis(300)
} else {
    owner::OBSERVE_BEFORE_TAKEOVER
};

/// Start sending heartbeats for the cluster. Do this as soon as a session owns
/// the cluster, or joins one that is running.
fn heartbeat(cluster: &Cluster) -> owner::Heartbeat {
    owner::Heartbeat::start(cluster.ownerfile(), owner::HEARTBEAT_INTERVAL)
}

/// Downgrade an exclusive lock to a shared lock, and start sending heartbeats.
/// `flock` does not do this atomically: it releases the exclusive lock then
/// takes a shared lock. In between, another session could stop – or even
/// destroy – the cluster, so check again: returns `None` if the cluster is not
/// running, or the lock file is not current.
fn downgrade(
    cluster: &Cluster,
    lock: lock::LockedFileExclusive,
    ownership: &Ownership,
    heartbeat: owner::Heartbeat,
) -> Result<Option<(lock::LockedFileShared, owner::Heartbeat)>, ClusterError> {
    let lock = lock.lock_shared()?;
    match verify(cluster, &lock)? {
        Some(verified) if running(cluster, ownership, &verified)? => Ok(Some((lock, heartbeat))),
        Some(_) | None => Ok(None),
    }
}

/// Take over a cluster owned by another kernel, after watching its owner file
/// for signs of life. Call this only with an exclusive lock, and only when
/// taking over is allowed; see [`refuse_other_kernel`].
fn take_over(
    cluster: &Cluster,
    found: &owner::Found,
    interrupted: Interrupted<'_>,
) -> Result<(), ClusterError> {
    // Its owner's clock may disagree with ours, or its owner may be slow, so
    // watch for heartbeats before taking over, whatever the owner file's age.
    let ownerfile = cluster.ownerfile();
    let hostname = &found.record.hostname;
    log::warn!(
        "Checking that cluster {} is no longer in use on {hostname}; this takes {}s.",
        cluster.datadir.display(),
        OBSERVE_BEFORE_TAKEOVER.as_secs(),
    );
    match owner::observe(&ownerfile, OBSERVE_BEFORE_TAKEOVER, interrupted)? {
        owner::Observed::Changed => {
            Err(ClusterError::InUseElsewhere { hostname: hostname.clone(), age: Duration::ZERO })
        }
        // The owner gave up the cluster.
        owner::Observed::Gone => Ok(()),
        // Someone had second thoughts about taking over.
        owner::Observed::Interrupted => Err(ClusterError::Interrupted),
        owner::Observed::Unchanged => {
            log::warn!(
                "Taking over cluster {} from {hostname}; last heartbeat {}.",
                cluster.datadir.display(),
                owner::ago(found.age),
            );
            Ok(owner::release(&ownerfile)?)
        }
    }
}

/// Claim ownership and start the cluster, or – if a server is running without
/// an owner, e.g. one started by an older pgdo, or by hand – claim it and join
/// it. Call this only with an exclusive lock, when there is no owner. Returns
/// `None` if the caller should try again.
fn start_or_join(
    cluster: &Cluster,
    lock: lock::LockedFileExclusive,
    options: Options<'_>,
    interrupted: Interrupted<'_>,
) -> Result<Option<(lock::LockedFileShared, owner::Heartbeat)>, ClusterError> {
    let ownerfile = cluster.ownerfile();
    let postmaster = postmaster::check(&cluster.datadir)?;
    if let Postmaster::Other(pid) = postmaster {
        // `pg_ctl` would think this unrelated process is our server, and
        // PostgreSQL would refuse to start.
        return Err(ClusterError::StalePidFile { pid });
    }
    // If a server is running, claim it so that other kernels see that it is in
    // use, then join it. If we can see it only via its socket, it is in another
    // namespace, which the claim must say.
    let current = owner::Record::current(&cluster.datadir)?;
    let joining = if server_here(cluster, postmaster)? {
        Some(current.clone())
    } else if server_elsewhere(cluster)? {
        Some(current.clone().with_unknown_namespace())
    } else {
        None
    };
    if interrupted() {
        Err(ClusterError::Interrupted)
    } else if let Some(record) = joining {
        if owner::claim(&ownerfile, &record)? {
            let ownership = match verify(cluster, &lock)? {
                Some(verified) => ownership(cluster, &verified)?,
                None => return Ok(None),
            };
            downgrade(cluster, lock, &ownership, heartbeat(cluster))
        } else {
            // Another process claimed it first – which can happen only from
            // another kernel, where our lock means nothing.
            Ok(None)
        }
    } else if owner::claim(&ownerfile, &current)? {
        // Start the heartbeat now: starting the cluster can take a while, e.g.
        // during crash recovery.
        let heartbeat = heartbeat(cluster);
        match cluster.start(options) {
            // Interrupted, e.g. by Ctrl-C, which also kills `pg_ctl`. Either
            // way, do not leave a server running that no one is using.
            Ok(_) | Err(_) if interrupted() => {
                abandon_start(cluster, heartbeat)?;
                Err(ClusterError::Interrupted)
            }
            Err(err) => {
                abandon_start(cluster, heartbeat)?;
                Err(err)
            }
            Ok(_) => downgrade(cluster, lock, &Ownership::SameNamespace, heartbeat),
        }
    } else {
        // As above: another kernel claimed it first.
        Ok(None)
    }
}

/// Abandon starting the cluster, e.g. because `pg_ctl start` failed, or the
/// user interrupted it. Call this only after claiming ownership and trying to
/// start the cluster, while holding an exclusive lock.
///
/// If our server is running – `pg_ctl start` may give up waiting, or be killed,
/// while the server carries on starting – stop it, so that no server is left
/// running with no one using it, then release our claim. If it cannot be
/// stopped, keep our claim: other kernels must not start another server.
fn abandon_start(cluster: &Cluster, heartbeat: owner::Heartbeat) -> Result<(), ClusterError> {
    match postmaster::check(&cluster.datadir)? {
        Postmaster::Absent | Postmaster::Dead(_) | Postmaster::Other(_) => (),
        // We claimed the cluster and started its server from this namespace,
        // so this is our server.
        Postmaster::Running(_) | Postmaster::Unknown(_) => {
            if let Err(err) = cluster.stop() {
                log::warn!(
                    "Could not stop cluster {}, which may still be starting; leaving the \
                     cluster claimed: {err}",
                    cluster.datadir.display()
                );
                return Ok(());
            }
        }
    }
    drop(heartbeat);
    Ok(owner::release(&cluster.ownerfile())?)
}

/// Checks whether startup has been interrupted. See [`interrupt`].
type Interrupted<'a> = &'a dyn Fn() -> bool;

/// Take a shared lock, waiting while another process holds an exclusive lock,
/// e.g. while it starts the cluster. Polls, rather than blocks, so that it can
/// be interrupted.
fn lock_shared(
    mut lock: lock::UnlockedFile,
    interrupted: Interrupted<'_>,
) -> Result<lock::LockedFileShared, ClusterError> {
    loop {
        match lock.try_lock_shared()? {
            Right(lock) => return Ok(lock),
            Left(_) if interrupted() => return Err(ClusterError::Interrupted),
            Left(unlocked) => {
                lock = unlocked;
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
        }
    }
}

/// Obtain a shared lock on the cluster, creating and starting it if necessary,
/// and start sending heartbeats.
fn startup(
    cluster: &Cluster,
    options: Options<'_>,
    takeover: Takeover,
    interrupted: Interrupted<'_>,
) -> Result<(lock::LockedFileShared, owner::Heartbeat), ClusterError> {
    loop {
        if interrupted() {
            return Err(ClusterError::Interrupted);
        }
        match open_lockfile(cluster)?.try_lock_exclusive()? {
            Left(lock) => {
                // The cluster is locked elsewhere, shared or exclusively. We
                // optimistically take a shared lock. If the other lock is also
                // shared, this will not wait. If the other lock is exclusive,
                // this will wait until that lock is released (or changed to a
                // shared lock).
                let lock = lock_shared(lock, interrupted)?;
                let Some(verified) = verify(cluster, &lock)? else {
                    // The lock file was removed while we waited; try again.
                    continue;
                };
                // If obtaining the lock blocked, i.e. the lock elsewhere was
                // exclusive, then the cluster may have been started by the
                // process that held that exclusive lock. We should check.
                let ownership = ownership(cluster, &verified)?;
                if let Ownership::OtherKernel(found) = &ownership {
                    // Taking over needs an exclusive lock. Release ours, back
                    // off, and try again for one.
                    refuse_other_kernel(found, takeover)?;
                    drop(lock);
                    backoff();
                    continue;
                } else if running(cluster, &ownership, &verified)? {
                    return Ok((lock, heartbeat(cluster)));
                } else {
                    // Release all locks, back off, and try again.
                    drop(lock);
                    backoff();
                    continue;
                }
            }
            Right(lock) => {
                let Some(verified) = verify(cluster, &lock)? else {
                    // The lock file was removed before we locked it; try again.
                    continue;
                };
                let ownerfile = cluster.ownerfile();
                match ownership(cluster, &verified)? {
                    Ownership::OtherKernel(found) => {
                        refuse_other_kernel(&found, takeover)?;
                        take_over(cluster, &found, interrupted)?;
                        continue;
                    }
                    Ownership::Copy => {
                        owner::release(&ownerfile)?;
                        continue;
                    }
                    ownership @ (Ownership::SameNamespace | Ownership::SameKernel) => {
                        if running(cluster, &ownership, &verified)? {
                            // Use the running server.
                            match downgrade(cluster, lock, &ownership, heartbeat(cluster))? {
                                Some(joined) => return Ok(joined),
                                None => continue,
                            }
                        } else {
                            // The owner's server has gone, e.g. it crashed. Our
                            // exclusive lock shows that no session on this
                            // kernel is using the cluster, so we can take over.
                            owner::release(&ownerfile)?;
                            continue;
                        }
                    }
                    Ownership::None => match start_or_join(cluster, lock, options, interrupted)? {
                        Some(started_or_joined) => return Ok(started_or_joined),
                        None => continue,
                    },
                }
            }
        }
    }
}

/// Try to upgrade a shared lock to an exclusive lock, without blocking. Returns
/// `None` if another process holds a lock, in which case ours is released.
///
/// `flock` does not convert locks atomically, and platforms differ when a
/// conversion fails: Linux has then released the shared lock, but macOS keeps
/// it. On macOS, two sessions ending at the same moment would each block the
/// other's upgrade, and neither would stop the cluster. So unlock, and try once
/// more: of two sessions doing this at once, the last to try will succeed.
fn upgrade(
    lock: lock::LockedFileShared,
) -> Result<Option<lock::LockedFileExclusive>, ClusterError> {
    match lock.try_lock_exclusive()? {
        Right(lock) => Ok(Some(lock)),
        Left(lock) => Ok(lock.unlock()?.try_lock_exclusive()?.right()),
    }
}

/// Remove the cluster's owner file, lock file, and data directory. Call this
/// only while holding an exclusive lock, after destroying the cluster.
fn remove_datadir(cluster: &Cluster) -> Result<(), ClusterError> {
    owner::release(&cluster.ownerfile())?;
    match fs::remove_file(cluster.lockfile()) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err)?,
        _ => (),
    }
    // Use `remove_dir`, not `remove_dir_all`: if another process has created
    // something here in the meantime, e.g. a new lock file, leave it be.
    match fs::remove_dir(&cluster.datadir) {
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Ok(())
        }
        result => Ok(result?),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::{ClusterError, Takeover};
    use crate::cluster::{Cluster, ClusterStatus, State};
    use crate::runtime::strategy::Strategy;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Create a stopped cluster with an owner file that appears to be from
    /// another kernel, last updated an hour ago.
    fn cluster_owned_by_other_kernel(datadir: &std::path::Path) -> TestResult {
        let session = Cluster::new(datadir, Strategy::default())?.session(&[])?;
        let ours = std::fs::read_to_string(datadir.join("pgdo.owner"))?;
        assert_eq!(session.end()?, State::Modified);
        let theirs = ours
            .lines()
            .filter(|line| !line.starts_with("boot_id=") && !line.starts_with("hostname="))
            .chain(["boot_id=some-other-kernel", "hostname=elsewhere"])
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let ownerfile = datadir.join("pgdo.owner");
        std::fs::write(&ownerfile, theirs)?;
        let file = std::fs::File::options().write(true).open(&ownerfile)?;
        file.set_modified(SystemTime::now() - Duration::from_secs(3600))?;
        Ok(())
    }

    #[test]
    fn session_takes_over_stale_cluster_from_other_kernel_without_heartbeats() -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        cluster_owned_by_other_kernel(&datadir)?;
        let session = Cluster::new(&datadir, Strategy::default())?
            .session_with_takeover(&[], Takeover::IfStale)?;
        assert_eq!(session.status()?, ClusterStatus::Running);
        let record = std::fs::read_to_string(datadir.join("pgdo.owner"))?;
        assert!(!record.contains("some-other-kernel"));
        assert_eq!(session.end()?, State::Modified);
        Ok(())
    }

    /// If the owner's clock is ahead of ours, its owner file looks fresh – even
    /// from the future – for a long time. Taking over relies on observing the
    /// owner file, not on its age.
    #[test]
    fn session_takes_over_from_other_kernel_with_clock_ahead() -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        cluster_owned_by_other_kernel(&datadir)?;
        let file = std::fs::File::options()
            .write(true)
            .open(datadir.join("pgdo.owner"))?;
        file.set_modified(SystemTime::now() + Duration::from_secs(3600))?;
        // Without taking over: refused as in use.
        assert!(matches!(
            Cluster::new(&datadir, Strategy::default())?.session(&[]),
            Err(ClusterError::InUseElsewhere { .. })
        ));
        // Taking over: no signs of life, so it is taken over.
        let session = Cluster::new(&datadir, Strategy::default())?
            .session_with_takeover(&[], Takeover::IfStale)?;
        assert_eq!(session.status()?, ClusterStatus::Running);
        assert_eq!(session.end()?, State::Modified);
        Ok(())
    }

    /// Someone runs with `--force`, then thinks better of it and presses
    /// Ctrl-C while pgdo watches the owner file. Nothing must be taken over.
    #[test]
    fn session_interrupted_while_watching_does_not_take_over() -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        cluster_owned_by_other_kernel(&datadir)?;
        let before = std::fs::read_to_string(datadir.join("pgdo.owner"))?;
        let started = std::time::Instant::now();
        let result = Cluster::new(&datadir, Strategy::default())?.start_session(
            &[],
            Takeover::IfStale,
            &|| started.elapsed() > Duration::from_millis(50),
        );
        assert!(matches!(result, Err(ClusterError::Interrupted)));
        assert_eq!(std::fs::read_to_string(datadir.join("pgdo.owner"))?, before);
        let status = Cluster::new(&datadir, Strategy::default())?.status()?;
        assert_eq!(status, ClusterStatus::Stopped);
        Ok(())
    }

    /// Interrupted just as the server has started, i.e. after `pg_ctl start`
    /// has returned: stop the server, and release the claim. (An interrupt
    /// while `pg_ctl start` waits – e.g. Ctrl-C, which kills `pg_ctl` – makes
    /// it fail, which the same path handles.)
    #[test]
    fn session_interrupted_after_starting_stops_server() -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        let pidfile = datadir.join("postmaster.pid");
        let result = Cluster::new(&datadir, Strategy::default())?.start_session(
            &[],
            Takeover::Never,
            &|| pidfile.exists(),
        );
        assert!(matches!(result, Err(ClusterError::Interrupted)));
        assert!(!datadir.join("pgdo.owner").exists());
        let status = Cluster::new(&datadir, Strategy::default())?.status()?;
        assert_eq!(status, ClusterStatus::Stopped);
        Ok(())
    }

    /// Interrupted while waiting for another process's exclusive lock, e.g.
    /// while it starts the cluster.
    #[test]
    fn session_interrupted_while_waiting_for_lock() -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        std::fs::create_dir(&datadir)?;
        let lockfile = datadir.join("pgdo.lock");
        let _exclusive =
            crate::lock::UnlockedFile::try_from(lockfile.as_path())?.lock_exclusive()?;
        let started = std::time::Instant::now();
        let result = Cluster::new(&datadir, Strategy::default())?.start_session(
            &[],
            Takeover::Never,
            &|| started.elapsed() > Duration::from_millis(200),
        );
        assert!(matches!(result, Err(ClusterError::Interrupted)));
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(!datadir.join("pgdo.owner").exists());
        Ok(())
    }

    /// Clocks on different kernels can disagree, e.g. a VM's after its host
    /// sleeps, so a live owner's heartbeats can look stale. Before taking over,
    /// a session watches for heartbeats, whatever their times.
    #[test]
    fn session_does_not_take_over_from_other_kernel_with_heartbeats() -> TestResult {
        let tempdir = tempfile::tempdir()?;
        let datadir = tempdir.path().join("data");
        cluster_owned_by_other_kernel(&datadir)?;
        let ownerfile = datadir.join("pgdo.owner");
        let before = std::fs::read_to_string(&ownerfile)?;

        // Heartbeats from a kernel whose clock is an hour behind.
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let beating = {
            let (ownerfile, stop) = (ownerfile.clone(), stop.clone());
            std::thread::spawn(move || {
                let base = SystemTime::now() - Duration::from_secs(3600);
                for i in 1.. {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    let file = std::fs::File::options()
                        .write(true)
                        .open(&ownerfile)
                        .unwrap();
                    file.set_modified(base + Duration::from_millis(i * 50))
                        .unwrap();
                    std::thread::sleep(Duration::from_millis(50));
                }
            })
        };
        let result = Cluster::new(&datadir, Strategy::default())?
            .session_with_takeover(&[], Takeover::IfStale);
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        beating.join().unwrap();

        assert!(matches!(
            result,
            Err(ClusterError::InUseElsewhere { hostname, .. }) if hostname == "elsewhere"
        ));
        assert_eq!(std::fs::read_to_string(&ownerfile)?, before);
        let status = Cluster::new(&datadir, Strategy::default())?.status()?;
        assert_eq!(status, ClusterStatus::Stopped);
        Ok(())
    }

    /// Two sessions ending at the same moment must not both fail to upgrade
    /// their locks, or neither would stop the cluster.
    #[test]
    fn upgrade_succeeds_for_one_of_two_sessions_ending_together() -> TestResult {
        use std::sync::{Arc, Barrier};
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("pgdo.lock");
        for _ in 0..200 {
            let barrier = Arc::new(Barrier::new(2));
            let ending = [0, 1].map(|_| {
                let lock = crate::lock::UnlockedFile::try_from(path.as_path())
                    .and_then(|lock| Ok(lock.lock_shared()?));
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let lock = lock.unwrap();
                    barrier.wait();
                    // Hold any exclusive lock until both have tried.
                    let upgraded = super::upgrade(lock).unwrap();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    upgraded.is_some()
                })
            });
            let upgraded = ending.map(|thread| thread.join().unwrap());
            assert!(
                upgraded.contains(&true),
                "neither session upgraded its lock"
            );
        }
        Ok(())
    }
}
