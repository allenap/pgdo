mod clone;
mod exec;
mod runtimes;
mod shell;

use super::Result;
pub(crate) use shell::Shell as Default;

#[derive(clap::Subcommand)]
pub(crate) enum Command {
    #[clap(display_order = 1)]
    Shell(shell::Shell),

    #[clap(display_order = 2)]
    Exec(exec::Exec),

    #[clap(display_order = 3)]
    Clone(clone::Clone),

    #[clap(display_order = 4)]
    Runtimes(runtimes::Runtimes),
}

impl Command {
    pub(crate) fn invoke(self) -> Result {
        match self {
            Self::Shell(shell) => shell.invoke(),
            Self::Exec(exec) => exec.invoke(),
            Self::Clone(clone) => clone.invoke(),
            Self::Runtimes(runtimes) => runtimes.invoke(),
        }
    }
}
