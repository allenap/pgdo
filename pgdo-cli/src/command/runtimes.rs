use std::process::ExitCode;

use color_eyre::eyre::Result;

use pgdo::runtime::strategy::StrategyLike;

use crate::{args, runner};

/// List discovered PostgreSQL runtimes.
///
/// The runtime shown on the line beginning with `=>` is the default, i.e. the
/// runtime that will be used when creating a new cluster.
#[derive(clap::Args)]
#[clap(next_help_heading = Some("Options for runtimes"))]
pub struct Args {
    #[clap(flatten)]
    pub runtime: args::RuntimeArgs,
}

pub fn invoke(args: Args) -> Result<ExitCode> {
    let strategy = runner::determine_strategy(args.runtime.fallback)?;
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
