use std::process::ExitCode;

use pgdo::runtime::strategy::StrategyLike;

use super::ExitResult;
use crate::{args, runner};

/// List discovered PostgreSQL runtimes.
///
/// The runtime shown on the line beginning with `=>` is the default, i.e. the
/// runtime that will be used when creating a new cluster.
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for runtimes"))]
pub struct Runtimes {
    #[clap(flatten)]
    pub runtime: args::RuntimeArgs,
}

impl Runtimes {
    pub fn invoke(self) -> ExitResult {
        let Self { runtime } = self;
        let strategy = runner::determine_strategy(runtime.fallback)?;
        let mut runtimes: Vec<_> = strategy.runtimes().collect();
        let fallback = strategy.fallback();

        // Sort by version. Higher versions will sort last.
        runtimes.sort_by(|ra, rb| ra.version.cmp(&rb.version));

        for runtime in runtimes {
            let default = match fallback {
                Some(ref default) if default == &runtime => "=>",
                _ => "",
            };
            println!(
                "{default:2} {version:10} {bindir}",
                bindir = runtime.bindir.display(),
                version = runtime.version,
            )
        }

        Ok(ExitCode::SUCCESS)
    }
}

impl From<Runtimes> for super::Command {
    fn from(runtimes: Runtimes) -> Self {
        Self::Runtimes(runtimes)
    }
}
