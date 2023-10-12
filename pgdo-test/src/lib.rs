use proc_macro::TokenStream;
use quote::quote;

use pgdo::runtime::strategy::{Strategy, StrategyLike};

/// Attribute macro to generate a test function for each runtime discovered by
/// `pgdo`. The function will be named after the original function, with a
/// suffix containing the version number. The test body itself will have a
/// "magic" `runtime` variable available, which is a [`pgdo::runtime::Runtime`].
#[proc_macro_attribute]
pub fn for_all_runtimes(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    // Extract parts of the input function.
    let vis = &input.vis;
    let block = &input.block;

    Strategy::default()
        .runtimes()
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
                    let runtime = crate::runtime::Runtime::new(#bindir)?;
                    #block
                }
            };

            // Return the generated tokens.
            TokenStream::from(expanded)
        })
        .collect::<TokenStream>()
}
