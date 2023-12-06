use std::panic::{catch_unwind, resume_unwind};

/// Perform `task`. On error or panic, perform `cleanup` and return the original
/// error, or continue to panic.
///
/// This is resilient to errors and panics in `cleanup` too: they will be
/// logged, but ultimately the errors and panics from `task` will be propagated.
pub fn with_cleanup<TASK, T, E, CLEANUP, CT, CE>(cleanup: CLEANUP, task: TASK) -> Result<T, E>
where
    TASK: std::panic::UnwindSafe + FnOnce() -> Result<T, E>,
    CLEANUP: std::panic::UnwindSafe + FnOnce() -> Result<CT, CE>,
    E: std::fmt::Display,
    CE: std::fmt::Display + Into<E>,
{
    match catch_unwind(task) {
        Ok(Ok(t)) => Ok(t),
        Ok(Err(e)) => match catch_unwind(cleanup) {
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
        Err(panic) => match catch_unwind(cleanup) {
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
    use super::with_cleanup;

    #[test]
    fn test_with_cleanup() {
        let result: Result<&'static str, &'static str> = with_cleanup(
            || Ok::<&'static str, &'static str>("Ok/cleanup"),
            || Ok("Ok/task"),
        );
        assert!(matches!(result, Ok("Ok/task")));
    }

    #[test]
    fn test_with_cleanup_error_in_task() {
        let result: Result<(), &'static str> =
            with_cleanup(|| Ok::<(), &'static str>(()), || Err("Err/task")?);
        assert!(matches!(result, Err("Err/task")));
    }

    #[test]
    #[should_panic(expected = "Panic/task")]
    fn test_with_cleanup_panic_in_task() {
        let _result: Result<(), &'static str> =
            with_cleanup(|| Ok::<(), &'static str>(()), || panic!("Panic/task"));
    }

    #[test]
    fn test_with_cleanup_error_in_cleanup() {
        let result: Result<(), &'static str> =
            with_cleanup(|| Err::<(), &'static str>("Err/cleanup"), || Ok(()));
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn test_with_cleanup_panic_in_cleanup() {
        let result: Result<(), &'static str> = with_cleanup(
            || -> Result<(), &'static str> { panic!("Panic/cleanup") },
            || Ok(()),
        );
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn test_with_cleanup_error_in_task_and_cleanup() {
        let result: Result<(), &'static str> = with_cleanup(
            || Err::<(), &'static str>("Err/cleanup"),
            || Err("Err/task")?,
        );
        assert!(matches!(result, Err("Err/task")));
    }

    #[test]
    #[should_panic(expected = "Panic/task")]
    fn test_with_cleanup_panic_in_task_and_cleanup() {
        let _result: Result<(), &'static str> = with_cleanup(
            || -> Result<(), &'static str> { panic!("Panic/cleanup") },
            || panic!("Panic/task"),
        );
    }
}
