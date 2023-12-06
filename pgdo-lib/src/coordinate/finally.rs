use std::panic::{catch_unwind, resume_unwind};

/// Perform `task`. On success, error, or panic, perform `finally` and return
/// the original success or error, or continue to panic.
///
/// This is resilient to errors and panics in `finally` too. If `task` finishes
/// successfully, errors and panics from `finally` are propagated. However,
/// errors and panics from `task` will be propagated in preference to those
/// coming out of `finally` – but they will be logged.
pub fn with_finally<TASK, T, E, FINALLY, FT, FE>(finally: FINALLY, task: TASK) -> Result<T, E>
where
    TASK: std::panic::UnwindSafe + FnOnce() -> Result<T, E>,
    FINALLY: std::panic::UnwindSafe + FnOnce() -> Result<FT, FE>,
    E: std::fmt::Display,
    FE: std::fmt::Display + Into<E>,
{
    match catch_unwind(task) {
        Ok(Ok(t)) => match catch_unwind(finally) {
            Ok(Ok(_)) => Ok(t),
            Ok(Err(ce)) => {
                log::error!("Task succeeded but cleaning-up failed");
                Err(ce.into())
            }
            Err(panic) => {
                log::error!("Task succeeded but cleaning-up panicked");
                resume_unwind(panic)
            }
        },
        Ok(Err(e)) => match catch_unwind(finally) {
            Ok(Ok(_)) => Err(e),
            Ok(Err(ce)) => {
                log::error!("Task failed & cleaning-up also failed: {ce}");
                Err(e)
            }
            Err(_) => {
                log::error!("Task failed & cleaning-up panicked (suppressed)");
                Err(e)
            }
        },
        Err(panic) => match catch_unwind(finally) {
            Ok(Ok(_)) => resume_unwind(panic),
            Ok(Err(ce)) => {
                log::error!("Task panicked & cleaning-up failed: {ce}");
                resume_unwind(panic)
            }
            Err(_) => {
                log::error!("Task panicked & cleaning-up also panicked (suppressed)");
                resume_unwind(panic)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::with_finally;

    #[test]
    fn test_with_cleanup() {
        let result: Result<&'static str, &'static str> = with_finally(
            || Ok::<&'static str, &'static str>("Ok/cleanup"),
            || Ok("Ok/task"),
        );
        assert!(matches!(result, Ok("Ok/task")));
    }

    #[test]
    fn test_with_cleanup_error_in_task() {
        let result: Result<(), &'static str> =
            with_finally(|| Ok::<(), &'static str>(()), || Err("Err/task")?);
        assert!(matches!(result, Err("Err/task")));
    }

    #[test]
    #[should_panic(expected = "Panic/task")]
    fn test_with_cleanup_panic_in_task() {
        let _result: Result<(), &'static str> =
            with_finally(|| Ok::<(), &'static str>(()), || panic!("Panic/task"));
    }

    #[test]
    fn test_with_cleanup_error_in_cleanup() {
        let result: Result<(), &'static str> =
            with_finally(|| Err::<(), &'static str>("Err/cleanup"), || Ok(()));
        assert!(matches!(result, Err("Err/cleanup")));
    }

    #[test]
    #[should_panic(expected = "Panic/cleanup")]
    fn test_with_cleanup_panic_in_cleanup() {
        let _result: Result<(), &'static str> = with_finally(
            || -> Result<(), &'static str> { panic!("Panic/cleanup") },
            || Ok(()),
        );
    }

    #[test]
    fn test_with_cleanup_error_in_task_and_cleanup() {
        let result: Result<(), &'static str> = with_finally(
            || Err::<(), &'static str>("Err/cleanup"),
            || Err("Err/task")?,
        );
        assert!(matches!(result, Err("Err/task")));
    }

    #[test]
    #[should_panic(expected = "Panic/task")]
    fn test_with_cleanup_panic_in_task_and_cleanup() {
        let _result: Result<(), &'static str> = with_finally(
            || -> Result<(), &'static str> { panic!("Panic/cleanup") },
            || panic!("Panic/task"),
        );
    }
}
