//! The token-rewriting logic behind `#[rayonette::tasks]`, as pure functions.
//!
//! rayonette is SPMD: the coordinator and each agent compile the same source, so
//! both run this same expansion and derive the same task keys. The proc-macro
//! shell (`rayonette-macros`) only delegates here; keeping the logic in a normal
//! library means it is instrumented and unit-tested like any other code.
//!
//! [`expand`] rewrites every `net_map` / `net_map_with_fleet` call inside an
//! attributed function into a keyed `net_map_task` / `net_map_task_with_fleet`
//! call, and emits a sibling `register_task!` per call so the agent can register
//! the task by the same key. The task expression stays inline, so `net_map`'s
//! `Fn(Self::Item) -> O` bound still type-checks it at the call site: a wrong
//! input-type guess is a compile error there, never a runtime mis-decode.
//!
//! A closure registers at module scope, detached from its receiver, so its input
//! type must be named explicitly there. [`recover_input_type`] finds that type in
//! tiers: an explicit annotation (A), a typed `let` binding in scope (B), or a
//! literal or range receiver (C). When none applies it gives up (D) with a
//! compile error at the call site asking for an annotation. Because the guess is
//! re-checked by the call site's bound, the heuristic can be aggressive without
//! ever being unsound. Inferred-type generics still need an explicit
//! `register_task!`, but an explicit turbofish (`double::<u32>`) round-trips.

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned, ToTokens};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Comma;
use syn::visit_mut::{self, VisitMut};
use syn::{
    Expr, ExprClosure, ExprMacro, ExprMethodCall, ExprRange, GenericArgument, Ident, ItemFn, Lit,
    Local, Pat, PatType, PathArguments, Type,
};

/// The outcome of naming a closure's input type from source alone, in tier order.
pub enum Recovered {
    /// Tier A: the closure annotated its parameter, for example `|x: u32|`.
    Annotated(Box<Type>),
    /// Tier B: a typed `let` binding in scope gave the receiver's element type.
    Binding(Box<Type>),
    /// Tier C: a literal or range receiver gave the element type.
    Receiver(Box<Type>),
    /// Tier D: the input type cannot be named here, carrying the span to point
    /// the compile error at.
    GiveUp(Span),
}

impl std::fmt::Debug for Recovered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Render the recovered type (via its tokens, since `syn::Type` is not
        // `Debug` without an extra feature); the give-up span is not worth showing.
        match self {
            Self::Annotated(ty) => write!(f, "Annotated({})", ty.to_token_stream()),
            Self::Binding(ty) => write!(f, "Binding({})", ty.to_token_stream()),
            Self::Receiver(ty) => write!(f, "Receiver({})", ty.to_token_stream()),
            Self::GiveUp(_) => f.write_str("GiveUp"),
        }
    }
}

/// A deterministic, formatting-stable wire key for one task call site.
///
/// Named functions key by their path, closures by their ordinal within the
/// attributed scope. Both append a hash of the task tokens so two distinct tasks
/// (for example the same closure shape in two modules) never collide. Because the
/// key is a pure function of the tokens, the coordinator and the agent derive the
/// identical string from the identical source.
#[must_use]
pub fn task_key(scope: &Ident, arg: &Expr, ordinal: usize) -> String {
    let hash = fnv1a_hex(&arg.to_token_stream().to_string());
    match arg {
        Expr::Path(path) => {
            let name = path
                .path
                .segments
                .last()
                .map(|segment| segment.ident.to_string())
                .unwrap_or_default();
            format!("{scope}::{name}#{hash}")
        }
        _ => format!("{scope}::task#{ordinal}#{hash}"),
    }
}

/// Name a closure's single input type, in tier order: an explicit annotation (A),
/// a typed binding in scope (B), a literal or range receiver (C), or give up (D).
///
/// `bindings` are the typed `let` bindings in scope at the call site, which the
/// expansion tracks as it walks the body.
#[must_use]
pub fn recover_input_type(
    closure: &ExprClosure,
    receiver: &Expr,
    bindings: &[(Ident, Type)],
) -> Recovered {
    // Only a single-parameter closure can be a task (the item type is one type).
    let mut inputs = closure.inputs.iter();
    let (Some(only), None) = (inputs.next(), inputs.next()) else {
        return Recovered::GiveUp(closure.span());
    };
    if let Pat::Type(annotated) = only {
        return Recovered::Annotated(Box::new((*annotated.ty).clone()));
    }
    if let Some(ty) = binding_element_type(receiver, bindings) {
        return Recovered::Binding(Box::new(ty));
    }
    if let Some(ty) = receiver_element_type(receiver) {
        return Recovered::Receiver(Box::new(ty));
    }
    Recovered::GiveUp(closure.span())
}

/// Tier B: if the receiver is a bare binding whose declared type has an element,
/// that element type. `let v: Vec<u32> = ...; v.net_map(|x| ...)` recovers `u32`.
fn binding_element_type(receiver: &Expr, bindings: &[(Ident, Type)]) -> Option<Type> {
    let Expr::Path(path) = receiver else {
        return None;
    };
    let name = path.path.get_ident()?;
    let ty = bindings
        .iter()
        .rev()
        .find(|(bound, _)| bound == name)
        .map(|(_, ty)| ty)?;
    element_type(ty)
}

/// Tier C: if the receiver is a literal or range with an obvious element type,
/// that type. `vec![1u32, 2]` and `(0..3u32)` both recover `u32`.
fn receiver_element_type(receiver: &Expr) -> Option<Type> {
    match receiver {
        Expr::Paren(paren) => receiver_element_type(&paren.expr),
        Expr::Range(range) => range_element_type(range),
        Expr::Macro(call) => vec_macro_element_type(call),
        _ => None,
    }
}

/// The element type of a generic container type, for example `u32` from
/// `Vec<u32>` or `Range<u32>` (the first angle-bracketed type argument).
fn element_type(ty: &Type) -> Option<Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let PathArguments::AngleBracketed(generics) = &path.path.segments.last()?.arguments else {
        return None;
    };
    generics.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(inner) => Some(inner.clone()),
        _ => None,
    })
}

/// The element type of a range, from a suffixed integer bound (`0..3u32` -> `u32`).
fn range_element_type(range: &ExprRange) -> Option<Type> {
    [range.start.as_deref(), range.end.as_deref()]
        .into_iter()
        .flatten()
        .find_map(literal_suffix_type)
}

/// The element type of a `vec!` literal, from its first element's suffix.
fn vec_macro_element_type(call: &ExprMacro) -> Option<Type> {
    if !call.mac.path.is_ident("vec") {
        return None;
    }
    let elements = call
        .mac
        .parse_body_with(Punctuated::<Expr, Comma>::parse_terminated)
        .ok()?;
    elements.first().and_then(literal_suffix_type)
}

/// The type named by a suffixed integer literal, for example `u32` from `3u32`.
fn literal_suffix_type(expr: &Expr) -> Option<Type> {
    let Expr::Lit(literal) = expr else {
        return None;
    };
    let Lit::Int(int) = &literal.lit else {
        return None;
    };
    let suffix = int.suffix();
    if suffix.is_empty() {
        return None;
    }
    syn::parse_str::<Type>(suffix).ok()
}

/// Clone `closure` with `ty` written as its parameter's annotation, so a recovered
/// type becomes explicit in both the rewritten call and the registration.
fn annotate_closure(closure: &ExprClosure, ty: &Type) -> ExprClosure {
    let mut annotated = closure.clone();
    for input in &mut annotated.inputs {
        let pattern = input.clone();
        *input = Pat::Type(PatType {
            attrs: Vec::new(),
            pat: Box::new(pattern),
            colon_token: <syn::Token![:]>::default(),
            ty: Box::new(ty.clone()),
        });
    }
    annotated
}

/// Rewrite one `net_map` / `net_map_with_fleet` call into its keyed terminal, and
/// build the matching registration tokens.
///
/// Returns `(rewritten call, registration)`, both carrying the *same* key literal
/// and the *same* task expression (a closure gets its recovered type written in).
/// The registration is a `register_task!` for a named function or a recoverable
/// closure, or a `compile_error!` at the call site for an unrecoverable one.
#[must_use]
pub fn rewrite_call(
    scope: &Ident,
    call: &ExprMethodCall,
    ordinal: usize,
    bindings: &[(Ident, Type)],
) -> (TokenStream, TokenStream) {
    let receiver = &call.receiver;
    let task = &call.args[0];
    let key = task_key(scope, task, ordinal);
    let terminal = if call.method == "net_map_with_fleet" {
        Ident::new("net_map_task_with_fleet", call.method.span())
    } else {
        Ident::new("net_map_task", call.method.span())
    };
    // Any args after the task (the explicit `fleet`) pass through unchanged.
    let trailing = call.args.iter().skip(1);
    let (task_tokens, registration) = resolve_task(&key, task, receiver, bindings);
    let rewritten = quote! {
        #receiver.#terminal(#key, #task_tokens #(, #trailing)*)
    };
    (rewritten, registration)
}

/// Resolve a task argument into the tokens to emit at the call site and the
/// matching registration. A named function passes through verbatim (preserving an
/// explicit turbofish); a closure has its recovered type written in, or becomes a
/// Tier-D `compile_error!` when no type can be named.
fn resolve_task(
    key: &str,
    task: &Expr,
    receiver: &Expr,
    bindings: &[(Ident, Type)],
) -> (TokenStream, TokenStream) {
    let Expr::Closure(closure) = task else {
        return (
            quote!(#task),
            quote!(::rayonette::register_task! { #key, #task }),
        );
    };
    match recover_input_type(closure, receiver, bindings) {
        Recovered::Annotated(_) => (
            quote!(#closure),
            quote!(::rayonette::register_task! { #key, #closure }),
        ),
        Recovered::Binding(ty) | Recovered::Receiver(ty) => {
            let annotated = annotate_closure(closure, &ty);
            (
                quote!(#annotated),
                quote!(::rayonette::register_task! { #key, #annotated }),
            )
        }
        Recovered::GiveUp(span) => {
            let message = "rayonette: annotate the closure's input type \
                           (for example `|x: u32|`) or pass a named function. \
                           Its type cannot be recovered at this call site";
            (
                quote!(#task),
                quote_spanned! { span => ::core::compile_error!(#message); },
            )
        }
    }
}

/// Expand `#[rayonette::tasks]` over a function: rewrite every task call site and
/// append the sibling registrations.
///
/// # Errors
/// Returns an error if the annotated item is not a function (this phase scopes
/// tasks to a function body; module scope is a later refinement).
pub fn expand(input: TokenStream) -> Result<TokenStream, syn::Error> {
    let mut function: ItemFn = syn::parse2(input)?;
    let mut rewriter = Rewriter {
        scope: function.sig.ident.clone(),
        ordinal: 0,
        bindings: Vec::new(),
        registrations: Vec::new(),
    };
    rewriter.visit_item_fn_mut(&mut function);
    let registrations = rewriter.registrations;
    Ok(quote! {
        #function
        #(#registrations)*
    })
}

/// Walks a function body, rewriting task call sites in source order, tracking the
/// typed `let` bindings in scope, and collecting the registrations.
struct Rewriter {
    scope: Ident,
    ordinal: usize,
    bindings: Vec<(Ident, Type)>,
    registrations: Vec<TokenStream>,
}

impl VisitMut for Rewriter {
    fn visit_local_mut(&mut self, local: &mut Local) {
        // Visit the initializer first (it may contain a task call, and a binding
        // is not in scope within its own initializer), then record the binding.
        visit_mut::visit_local_mut(self, local);
        if let Some((name, ty)) = typed_binding(&local.pat) {
            self.bindings.push((name, ty));
        }
    }

    fn visit_expr_mut(&mut self, expr: &mut Expr) {
        // Recurse first so a task call nested in a receiver is handled before the
        // outer call, and the ordinal still follows source order deterministically.
        visit_mut::visit_expr_mut(self, expr);
        let rewrite = match &*expr {
            Expr::MethodCall(call) if is_task_call(call) => Some(rewrite_call(
                &self.scope,
                call,
                self.ordinal,
                &self.bindings,
            )),
            _ => None,
        };
        if let Some((rewritten, registration)) = rewrite {
            self.ordinal += 1;
            self.registrations.push(registration);
            *expr = Expr::Verbatim(rewritten);
        }
    }
}

/// The name and declared type of a `let name: Type = ...` binding, if it is one.
fn typed_binding(pat: &Pat) -> Option<(Ident, Type)> {
    let Pat::Type(typed) = pat else {
        return None;
    };
    let Pat::Ident(ident) = &*typed.pat else {
        return None;
    };
    Some((ident.ident.clone(), (*typed.ty).clone()))
}

/// Whether a method call is a `net_map` terminal carrying a task argument.
fn is_task_call(call: &ExprMethodCall) -> bool {
    (call.method == "net_map" || call.method == "net_map_with_fleet") && !call.args.is_empty()
}

/// A formatting-stable 64-bit FNV-1a hash, hex-encoded. Deterministic across
/// toolchains (unlike `std`'s default hasher), which matters because an agent may
/// build on a different toolchain than the coordinator yet must derive the same key.
fn fnv1a_hex(text: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::{expand, recover_input_type, rewrite_call, task_key, Recovered};
    use quote::quote;
    use syn::{parse_quote, Expr, ExprClosure, ExprMethodCall, Ident, Type};

    fn ident(name: &str) -> Ident {
        Ident::new(name, proc_macro2::Span::call_site())
    }

    // A receiver the heuristic cannot read anything from, for tests that exercise
    // a tier other than the receiver.
    fn opaque() -> Expr {
        parse_quote!(produce())
    }

    #[test]
    fn recover_explicit_annotation_wins() {
        let closure: ExprClosure = parse_quote!(|x: u32| x * 2);
        let recovered = recover_input_type(&closure, &opaque(), &[]);
        assert!(matches!(recovered, Recovered::Annotated(_)));
        // The Debug rendering carries the recovered type, so this also pins that
        // the parameter type was read as `u32`.
        assert_eq!(format!("{recovered:?}"), "Annotated(u32)");
    }

    #[test]
    fn recover_typed_binding_in_scope() {
        let closure: ExprClosure = parse_quote!(|x| x * 2);
        let receiver: Expr = parse_quote!(values);
        let bindings = vec![(ident("values"), parse_quote!(Vec<u32>))];
        let recovered = recover_input_type(&closure, &receiver, &bindings);
        assert_eq!(format!("{recovered:?}"), "Binding(u32)");
    }

    #[test]
    fn recover_literal_receiver() {
        let closure: ExprClosure = parse_quote!(|x| x * 2);
        let receiver: Expr = parse_quote!(vec![1u32, 2, 3]);
        let recovered = recover_input_type(&closure, &receiver, &[]);
        assert_eq!(format!("{recovered:?}"), "Receiver(u32)");
    }

    #[test]
    fn recover_range_receiver() {
        let closure: ExprClosure = parse_quote!(|x| x * 2);
        // Parenthesized, as it appears in real source: `(0..3u32).net_map(..)`.
        let receiver: Expr = parse_quote!((0..3u32));
        let recovered = recover_input_type(&closure, &receiver, &[]);
        assert_eq!(format!("{recovered:?}"), "Receiver(u32)");
    }

    #[test]
    fn recover_gives_up_on_opaque_receiver() {
        let closure: ExprClosure = parse_quote!(|x| x * 2);
        let recovered = recover_input_type(&closure, &opaque(), &[]);
        assert_eq!(format!("{recovered:?}"), "GiveUp");
    }

    #[test]
    fn recover_annotation_beats_inferable_receiver() {
        // The receiver would infer `u32`, but the explicit `i64` annotation wins:
        // the macro never overrides what the user wrote.
        let closure: ExprClosure = parse_quote!(|x: i64| x * 2);
        let receiver: Expr = parse_quote!(values);
        let bindings = vec![(ident("values"), parse_quote!(Vec<u32>))];
        let recovered = recover_input_type(&closure, &receiver, &bindings);
        assert_eq!(format!("{recovered:?}"), "Annotated(i64)");
    }

    #[test]
    fn recover_gives_up_on_a_multi_parameter_closure() {
        // A two-parameter closure is not a task (the item type is one type).
        let closure: ExprClosure = parse_quote!(|x, y| x + y);
        let recovered = recover_input_type(&closure, &opaque(), &[]);
        assert_eq!(format!("{recovered:?}"), "GiveUp");
    }

    #[test]
    fn recover_gives_up_when_the_binding_is_not_a_container() {
        // A binding whose type has no element (and an unknown binding) both fall
        // through to give up.
        let closure: ExprClosure = parse_quote!(|x| x * 2);
        let receiver: Expr = parse_quote!(scalar);
        let bindings = vec![(ident("scalar"), parse_quote!(u32))];
        assert_eq!(
            format!("{:?}", recover_input_type(&closure, &receiver, &bindings)),
            "GiveUp"
        );
        // An identifier with no binding at all also gives up.
        assert_eq!(
            format!("{:?}", recover_input_type(&closure, &receiver, &[])),
            "GiveUp"
        );
    }

    #[test]
    fn recover_gives_up_when_the_binding_type_is_not_a_path() {
        // A tuple-typed binding has no single element to recover.
        let closure: ExprClosure = parse_quote!(|x| x * 2);
        let receiver: Expr = parse_quote!(pair);
        let bindings: Vec<(Ident, Type)> = vec![(ident("pair"), parse_quote!((u32, u32)))];
        assert_eq!(
            format!("{:?}", recover_input_type(&closure, &receiver, &bindings)),
            "GiveUp"
        );
    }

    #[test]
    fn recover_gives_up_on_unreadable_receivers() {
        // A non-`vec!` macro, a range whose bounds are not literals, and a `vec!`
        // of non-integer literals are all unreadable, so each gives up.
        let closure: ExprClosure = parse_quote!(|x| x * 2);
        let receivers: [Expr; 3] = [
            parse_quote!(other_macro!(1u32)),
            parse_quote!((start..end)),
            parse_quote!(vec!["a", "b"]),
        ];
        for receiver in &receivers {
            assert_eq!(
                format!("{:?}", recover_input_type(&closure, receiver, &[])),
                "GiveUp"
            );
        }
    }

    #[test]
    fn recover_skips_a_lifetime_generic_argument() {
        // The first generic argument is a lifetime, not the element type, so the
        // recovery skips it and reads the following type argument.
        let closure: ExprClosure = parse_quote!(|x| x);
        let receiver: Expr = parse_quote!(text);
        let bindings: Vec<(Ident, Type)> = vec![(ident("text"), parse_quote!(Cow<'a, u32>))];
        assert_eq!(
            format!("{:?}", recover_input_type(&closure, &receiver, &bindings)),
            "Binding(u32)"
        );
    }

    #[test]
    fn key_is_identical_for_call_site_and_register() {
        let call: ExprMethodCall = parse_quote!((0..5u32).net_map(|x: u32| x * 2));
        let (rewritten, registration) = rewrite_call(&ident("scope"), &call, 0, &[]);
        let key = task_key(&ident("scope"), &call.args[0], 0);
        assert!(rewritten.to_string().contains(&key));
        assert!(registration.to_string().contains(&key));
    }

    #[test]
    fn key_is_stable_across_formatting() {
        let tight: Expr = parse_quote!(|x: u32| x * 2);
        let loose: Expr = parse_quote! {
            | x : u32 |    x  *  2
        };
        assert_eq!(
            task_key(&ident("scope"), &tight, 0),
            task_key(&ident("scope"), &loose, 0)
        );
    }

    #[test]
    fn recover_generic_turbofish_preserved() {
        // An explicit turbofish round-trips: it is passed through verbatim into
        // both the rewritten call and the registration (the old scanner dropped it).
        let call: ExprMethodCall = parse_quote!((0..5u32).net_map(double::<u32>));
        let (rewritten, registration) = rewrite_call(&ident("scope"), &call, 0, &[]);
        assert!(rewritten.to_string().contains("double :: < u32 >"));
        assert!(registration.to_string().contains("double :: < u32 >"));
    }

    #[test]
    fn expand_rewrites_single_annotated_call() {
        let input = quote! {
            fn run(fleet: &Fleet<L>) {
                let _ = (0..5u32).net_map_with_fleet(|x: u32| x * 2, fleet);
            }
        };
        let out = expand(input).unwrap().to_string();
        assert!(out.contains("net_map_task_with_fleet"));
        assert!(out.contains("register_task"));
        // The key is a string literal, so its contents carry no token spacing.
        assert!(out.contains("run::task#0"));
    }

    #[test]
    fn expand_recovers_a_typed_binding() {
        // An unannotated closure over a typed `let` binding gets the recovered
        // type written into both the rewritten call and the registration. The
        // earlier tuple-typed binding has no single name, so it is simply skipped.
        let input = quote! {
            fn run() {
                let (a, b): (u32, u32) = pair();
                let values: Vec<u32> = make();
                let _ = values.net_map(|x| x * 2);
            }
        };
        let out = expand(input).unwrap().to_string();
        assert!(out.contains("net_map_task"));
        assert!(out.contains("register_task"));
        assert!(out.contains("| x : u32 |"));
        assert!(!out.contains("compile_error"));
    }

    #[test]
    fn expand_registers_a_named_function() {
        let input = quote! {
            fn run() {
                let _ = (0..5u32).net_map(double);
            }
        };
        let out = expand(input).unwrap().to_string();
        assert!(out.contains("net_map_task"));
        assert!(out.contains("register_task"));
        // Named functions key by their path, not an ordinal.
        assert!(out.contains("run::double"));
        assert!(!out.contains("compile_error"));
    }

    #[test]
    fn expand_is_idempotent_on_no_netmap_input() {
        // A method call that is not a task terminal is left untouched and emits
        // no registration.
        let input = quote! {
            fn run(v: Vec<u32>) {
                let _ = v.len();
            }
        };
        let out = expand(input).unwrap().to_string();
        assert!(out.contains("v . len ()"));
        assert!(!out.contains("register_task"));
        assert!(!out.contains("net_map_task"));
    }

    #[test]
    fn expand_emits_compile_error_on_giveup() {
        // The receiver is an opaque function call, so the unannotated closure is
        // Tier D: a compile error at the call site, and no registration for it.
        let input = quote! {
            fn run() {
                let _ = produce().net_map(|x| x * 2);
            }
        };
        let out = expand(input).unwrap().to_string();
        assert!(out.contains("compile_error"));
        assert!(!out.contains("register_task"));
    }

    #[test]
    fn expand_rejects_a_non_function_scope() {
        let input = quote! {
            struct NotAFunction;
        };
        assert!(expand(input).is_err());
    }
}
