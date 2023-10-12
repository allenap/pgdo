use std::error::Error;
use std::path::PathBuf;

use quote::quote;

use pgdo::runtime::strategy::{Strategy, StrategyLike};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> Result<()> {
    let out_dir: PathBuf = std::env::var_os("OUT_DIR").expect("OUT_DIR not set").into();

    // The advice on this is not clear, but it seems that if we emit _any_
    // `rerun-if-changed` directives, the default rules are not applied. This
    // is, I think, the default.
    println!("cargo:rerun-if-changed=.");

    // Use `pgdo` itself to discover runtimes.
    let strategy = Strategy::default();

    // Since we cannot statically write out a `PathBuf` or OS string into Rust
    // source, in order to interpolate them into the token stream we need to
    // convert the `PathBuf`s we have into regular UTF-8 strings. This may fail.
    let bindirs = strategy
        .runtimes()
        .map(|runtime| runtime.bindir.to_str().expect("invalid UTF-8").to_owned())
        .collect::<Vec<_>>();

    // Ask Cargo to re-run if any of the runtime bindirs change. Sadly this will
    // not trigger when a new runtime is installed. I don't know how to do that
    // efficiently yet, especially on macOS w/ Homebrew because it would need to
    // watch the whole of the `Cellar` directory – which is a lot.
    for bindir in bindirs.iter() {
        println!("cargo:rerun-if-changed={bindir}");
    }

    let tokens = quote! { static RUNTIMES: &[&str] = &[#(#bindirs),*]; };
    let syntax_tree = syn::parse2(tokens)?;
    let formatted = prettyplease::unparse(&syntax_tree);

    let runtimes_rs = out_dir.join("runtimes.rs");
    std::fs::write(runtimes_rs, formatted)?;

    Ok(())
}
