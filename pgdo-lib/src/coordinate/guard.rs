use super::{lock, shutdown, startup, CoordinateError, Subject};

enum GuardDropMode {
    Stop,
    Destroy,
}

/// Smart pointer around a [`Subject`] that ensures the subject is stopped or
/// destroyed when it goes out of scope.
///
/// Errors when stopping or destroying the subject are logged but otherwise
/// ignored.
pub struct Guard<SUBJECT>
where
    SUBJECT: Subject,
{
    mode: GuardDropMode,
    lock: Option<lock::LockedFileShared>,
    subject: SUBJECT,
}

impl<T> Guard<T>
where
    T: Subject,
{
    /// Starts the given subject and returns the guard.
    pub fn startup<L: Into<lock::UnlockedFile>>(
        lock: L,
        subject: T,
        options: T::Options<'_>,
    ) -> Result<Self, CoordinateError<T::Error>> {
        let lock = startup(lock.into(), &subject, options)?;
        Ok(Self { mode: GuardDropMode::Stop, lock: lock.into(), subject })
    }
}

impl<T> Guard<T>
where
    T: Subject,
{
    /// Configures the guard to *stop* the subject when it goes out of scope.
    #[must_use]
    pub fn and_stop(mut self) -> Self {
        self.mode = GuardDropMode::Stop;
        self
    }

    /// Configures the guard to *destroy* the subject when it goes out of scope.
    #[must_use]
    pub fn and_destroy(mut self) -> Self {
        self.mode = GuardDropMode::Destroy;
        self
    }
}

impl<T> std::ops::Deref for Guard<T>
where
    T: Subject,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.subject
    }
}

impl<T> Drop for Guard<T>
where
    T: Subject,
{
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            let result = match &self.mode {
                GuardDropMode::Stop => shutdown::<T, _, _>(lock, || self.subject.stop()),
                GuardDropMode::Destroy => shutdown::<T, _, _>(lock, || self.subject.destroy()),
            };
            match (&self.mode, result) {
                (GuardDropMode::Stop, Ok(_)) => (),
                (GuardDropMode::Stop, Err(err)) => {
                    log::error!("Error stopping subject: {err}");
                }
                (GuardDropMode::Destroy, Ok(_)) => (),
                (GuardDropMode::Destroy, Err(err)) => {
                    log::error!("Error destroying subject: {err}");
                }
            }
        }
    }
}
