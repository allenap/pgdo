mod backup;
mod clone;
mod exec;
mod restore;
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
    Clone(clone::Clone),

    #[clap(display_order = 4)]
    Backup(backup::Backup),

    #[clap(name = "backup:tools", hide = true)]
    BackupTools(backup::BackupTools),

    #[clap(display_order = 5)]
    Restore(restore::Restore),

    #[clap(display_order = 6)]
    Runtimes(runtimes::Runtimes),
}

impl Command {
    pub(crate) fn invoke(self) -> ExitResult {
        match self {
            Self::Shell(shell) => shell.invoke(),
            Self::Exec(exec) => exec.invoke(),
            Self::Clone(clone) => clone.invoke(),
            Self::Backup(backup) => backup.invoke(),
            Self::BackupTools(tools) => tools.invoke(),
            Self::Restore(restore) => restore.invoke(),
            Self::Runtimes(runtimes) => runtimes.invoke(),
        }
    }
}
