use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Ident, ItemFn, Signature};

use pgdo::runtime::Runtime;

include!(concat!(env!("OUT_DIR"), "/runtimes.rs"));

#[proc_macro_attribute]
pub fn for_all_runtimes(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(item as ItemFn);
    // Extract parts of the input function
    let vis = &input.vis;
    let block = &input.block;

    let mut items = Vec::new();

    for bindir in RUNTIMES.iter() {
        let runtime = Runtime::new(bindir).unwrap();
        let version = runtime.version.to_string().replace('.', "_");

        // Rename the function and change its signature
        let new_fn_name = Ident::new(
            format!("{ident}_{version}", ident = input.sig.ident).as_str(),
            input.sig.ident.span(),
        );
        let new_signature = Signature {
            ident: new_fn_name,
            // inputs: parse_quote!(runtime: ::pgdo::runtime::Runtime),
            ..input.sig.clone()
        };

        // Generate the new function
        let expanded = quote! {
            #[::std::prelude::v1::test]
            #vis #new_signature {
                let runtime = crate::runtime::Runtime::new(#bindir)?;
                #block
            }
        };

        // Return the generated tokens
        let item = TokenStream::from(expanded);
        items.push(item);
    }

    TokenStream::from_iter(items)
}
