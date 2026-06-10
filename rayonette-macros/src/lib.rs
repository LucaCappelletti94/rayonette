//! The proc-macro shell for `#[rayonette::tasks]`.
//!
//! All logic lives in `rayonette-macros-core` (a normal, instrumented library);
//! this crate is the thin `proc-macro = true` boundary the compiler requires, so
//! it is the only piece excluded from coverage.

use proc_macro::TokenStream;

/// Scope a set of `net_map` call sites.
///
/// Rewrites each into its keyed terminal and emits the matching `register_task!`
/// registrations. See `rayonette-macros-core` for the rewriting logic and the
/// supported task forms.
#[proc_macro_attribute]
pub fn tasks(_attr: TokenStream, item: TokenStream) -> TokenStream {
    rayonette_macros_core::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
