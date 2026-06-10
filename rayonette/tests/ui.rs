//! Compile-pass UI tests for `#[rayonette::tasks]`.
//!
//! These prove that every supported task shape compiles: a named function, an
//! annotated closure, a closure whose input type is recovered from a typed
//! binding or a literal or range receiver, and a turbofished generic instance.
//!
//! The failure guarantees (a wrong annotation rejected at the call site, a
//! capturing closure rejected by the const-assert, an unrecoverable closure type
//! rejected with a legible message) are `compile_fail` doctests on the public
//! surface instead of trybuild compile-fail tests, because matching exact
//! `.stderr` golden output is toolchain-specific (caret span widths and the
//! const-eval panic wording drift even between stable releases) while a
//! `compile_fail` doctest only asserts that the code does not compile, which
//! holds on every channel.

#[test]
fn ui_pass() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass/*.rs");
}
