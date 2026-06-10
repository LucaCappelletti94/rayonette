//! Compile-pass and compile-fail UI tests for `#[rayonette::tasks]`.
//!
//! The pass cases prove a named function and an annotated closure both become
//! tasks that compile. The fail cases are the soundness guarantees: a closure
//! annotated with the wrong input type is rejected at its own call site (the
//! macro only proposes a type, `net_map`'s `Fn(Self::Item) -> O` bound verifies
//! it), a capturing closure is rejected by the no-capture const-assert, and an
//! unannotated closure whose type cannot be recovered is a legible compile error
//! rather than a silent runtime miss.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass/*.rs");
    t.compile_fail("tests/ui/fail/*.rs");
}
