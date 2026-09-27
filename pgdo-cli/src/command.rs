mod exec;
mod runtimes;
mod shell;

use super::ExitResult;
pub(crate) use shell::Shell as Default;

#[derive(clap::Subcommand)]
pub(crate) enum Command {
    #[clap(display_order = 1)]
    Shell(shell::Shell),

    #[clap(display_order = 2)]
    Exec(exec::Exec),

    #[clap(display_order = 3)]
    Runtimes(runtimes::Runtimes),
}

impl Command {
    pub(crate) fn invoke(self) -> ExitResult {
        match self {
            Self::Shell(shell) => shell.invoke(),
            Self::Exec(exec) => exec.invoke(),
            Self::Runtimes(runtimes) => runtimes.invoke(),
        }
    }
}
