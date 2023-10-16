use proc_macro::TokenStream;
use quote::quote;

use pgdo::{
    runtime::strategy::{Strategy, StrategyLike},
    version::PartialVersion,
};

/// Attribute macro to generate a test function for each discovered runtime.
///
/// The function will be named after the original function, with a suffix
/// containing the version number. The test body itself will have a "magic"
/// `runtime` variable available, which is a [`pgdo::runtime::Runtime`].
///
/// Optional inclusive minimum and maximum version constraints can be provided
/// as arguments to the attribute:
///
/// ```rust
/// # use pgdo_test::for_all_runtimes;
/// #[for_all_runtimes(min = "11", max = "13")]
/// #[test]
/// fn my_test() { /* … */ }
/// ```
///
/// **Note** that this macro will NOT work in unit tests of `pgdo-lib` because
/// it does not know that it's called `pgdo`. In any case, this macro is
/// intended for integration tests, so this limitation seems reasonable.
#[proc_macro_attribute]
pub fn for_all_runtimes(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut min: Option<PartialVersion> = None;
    let mut max: Option<PartialVersion> = None;

    if !attr.is_empty() {
        use syn::parse::Parser;
        use syn::{punctuated::Punctuated, Expr, ExprAssign, ExprLit, ExprPath, Lit, Token};

        let parser = Punctuated::<ExprAssign, Token![,]>::parse_separated_nonempty;
        let attrs = parser.parse(attr).expect("invalid attribute syntax");

        for attr in attrs {
            match (*attr.left, *attr.right) {
                (
                    Expr::Path(ExprPath { path, .. }),
                    Expr::Lit(ExprLit { lit: Lit::Str(version), .. }),
                ) if path.is_ident("min") => {
                    min = Some(version.value().parse().expect("invalid version number"))
                }
                (
                    Expr::Path(ExprPath { path, .. }),
                    Expr::Lit(ExprLit { lit: Lit::Str(version), .. }),
                ) if path.is_ident("max") => {
                    max = Some(version.value().parse().expect("invalid version number"))
                }
                _ => panic!("expected `min = \"…\"` and/or `max = \"…\"`"),
            }
        }
    }

    // Parse the input tokens into a syntax tree.
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    // Extract parts of the input function.
    let vis = &input.vis;
    let block = &input.block;

    Strategy::default()
        .runtimes()
        .filter(|runtime| {
            // Check if the runtime matches the given constraints.
            let runtime_version: PartialVersion = runtime.version.into();
            match (min, max) {
                (Some(min), _) if runtime_version < min => false,
                (_, Some(max)) if runtime_version > max => false,
                _ => true,
            }
        })
        .map(|runtime| {
            // Get the version of the runtime in an ident-friendlier format.
            let version = runtime.version.to_string().replace('.', "_");

            // Rename the function and change its signature.
            let ident_with_version = syn::Ident::new(
                format!("{ident}_v{version}", ident = input.sig.ident).as_str(),
                input.sig.ident.span(),
            );
            let signature_with_version =
                syn::Signature { ident: ident_with_version, ..input.sig.clone() };

            // Generate the new function.
            let bindir = runtime.bindir.to_str().unwrap();
            let expanded = quote! {
                #[::std::prelude::v1::test]
                #vis #signature_with_version {
                    let runtime = ::pgdo::runtime::Runtime::new(#bindir)?;
                    #block
                }
            };

            // Return the generated tokens.
            TokenStream::from(expanded)
        })
        .collect::<TokenStream>()
}
