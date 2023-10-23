use std::error::Error;

use pgdo::runtime::strategy::{Strategy, StrategyLike};

fn main() -> Result<(), Box<dyn Error>> {
    // The advice on this is not clear, but it seems that if we emit _any_
    // `rerun-if-changed` directives, the default rules are not applied. I think
    // this approximates the default.
    println!("cargo:rerun-if-changed=.");

    // Use `pgdo` itself to discover runtimes.
    let strategy = Strategy::default();

    // Ask Cargo to re-run if any of the runtime bindirs change. Sadly this will
    // not trigger when a new runtime is installed. I don't know how to do that
    // efficiently yet, especially on macOS w/ Homebrew because it would need to
    // watch the whole of the `Cellar` directory – which is a lot.
    for runtime in strategy.runtimes() {
        println!("cargo:rerun-if-changed={}", runtime.bindir.display());
    }

    Ok(())
}
