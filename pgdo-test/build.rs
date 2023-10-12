use std::error::Error;
use std::path::PathBuf;

use quote::quote;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> Result<()> {
    let out_dir: PathBuf = std::env::var_os("OUT_DIR")
        .expect("OUT_DIR not set in build script")
        .into();

    let mut runtimes: Vec<PathBuf> = Vec::new();

    // TODO: Replace code below with `pgdo::runtimes::strategy`-based logic.

    // Linux; Debian & Ubuntu.
    //#[cfg(any(doc, target_os = "linux"))]
    {
        let glob: wax::Glob = "*/bin/pg_ctl".parse()?;
        let roots = ["/usr/lib/postgresql".parse::<PathBuf>()?];
        for root in roots.into_iter().filter(|path| path.is_dir()) {
            for entry in glob.walk(root) {
                if let Some(bindir) = entry?.into_path().parent() {
                    println!("cargo:rerun-if-changed={}", bindir.display());
                    runtimes.push(bindir.to_owned());
                }
            }
        }
    }

    // macOS; Homebrew.
    #[cfg(any(doc, target_os = "macos"))]
    {
        let glob: wax::Glob = "postgresql@*/*/bin/pg_ctl".parse()?;
        let roots = [
            "/opt/homebrew/Cellar".parse::<PathBuf>()?,
            "/usr/local/Cellar".parse()?,
        ];
        for root in roots.into_iter().filter(|path| path.is_dir()) {
            for entry in glob.walk(root) {
                if let Some(bindir) = entry?.into_path().parent() {
                    println!("cargo:rerun-if-changed={}", bindir.display());
                    runtimes.push(bindir.to_owned());
                }
            }
        }
    }

    // Write out `runtimes.rs`.
    {
        // Since we cannot statically write out a `PathBuf` or OS string into
        // Rust source, in order to interpolate them into the token stream we
        // need to convert the `PathBuf`s we have into regular UTF-8 strings.
        // This may fail.
        let runtimes = runtimes
            .into_iter()
            .map(|path| path.to_str().expect("invalid UTF-8").to_owned());
        let tokens = quote! {
            static RUNTIMES: &[&str] = &[#(#runtimes),*];
        };
        let syntax_tree = syn::parse2(tokens)?;
        let formatted = prettyplease::unparse(&syntax_tree);

        let runtimes_rs = out_dir.join("runtimes.rs");
        std::fs::write(runtimes_rs, formatted)?;
    }

    Ok(())
}
