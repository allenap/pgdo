pub mod sakila;
pub use pgdo_test_macros::for_all_runtimes;

#[ctor::ctor]
/// Initialise a logger for tests. Without this, logs are not emitted – and we
/// are left with less informative captured test output when tests fail.
unsafe fn init_logger() {
    use std::io::{stdout, IsTerminal};
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Warn)
        .with_colors(stdout().is_terminal())
        .env()
        .init()
        .expect("could not initialize logger");
}
