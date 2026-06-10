# Task registration (API review item 2): deviations from the approved plan

This log records every place the implementation departs from the letter of the approved plan (`~/.claude/plans/let-s-start-preparing-a-shimmering-sunset.md`), with the reason. Each entry is something a reviewer reading the plan would not otherwise expect from the diff. The plan's intent and the two standing constraints (never land an uncalled `pub fn`; every deletion atomic with its tests) are preserved throughout.

## Phase 1 (library plumbing)

1. Two keyed terminals instead of one. The plan names a single `net_map_task(key, map)`. The consumers use both forms: `examples/ssh-run` calls bare `net_map` (global fleet) and the other three call `net_map_with_fleet` (explicit fleet), so the `#[rayonette::tasks]` macro must rewrite both in later phases. I added both keyed terminals, `net_map_task` (global) and `net_map_task_with_fleet` (explicit), and made `net_map` / `net_map_with_fleet` thin wrappers that derive the key via `fn_key` and delegate to them. This keeps the `size_of::<F>() == 0` const-assert in one place and means every existing test exercises the keyed terminals through the wrappers.

2. The new keyed-terminal test drives the explicit-fleet form. The plan's `net_map_task_carries_the_explicit_key` shows `net_map_task("triple", double)`. Driving the global terminal through a specific in-process fleet would require installing a second process-global fleet, which races the one sanctioned global-fleet test (`net_map_uses_the_installed_global_fleet`, which asserts an empty-then-installed sequence). So the test drives `net_map_task_with_fleet("triple", double, &fleet)` against an `InProcess` fleet instead, deterministic and race-free, proving a non-`type_name` key is honored end to end. The global keyed terminal stays covered through `net_map`'s delegation inside the existing global-fleet test.

3. `NetMap.map` is now an intentionally-unread field. Moving the key derivation into the constructor means `run()` reads `self.key` and never calls `fn_key(&self.map)`, so the `map` field is no longer read. It must stay to carry `F`'s type for the terminals' `Fn(Self::Item) -> O` bound (the coordinator never calls the task), so it carries a documented `#[expect(dead_code, reason = ...)]`.

4. Two extra in-crate tests beyond the plan's four, to hold 100% function coverage. `inventory::submit!` const-evaluates `TaskEntry::new` inside a `static` initializer, so `new` never runs at runtime; and a public `TaskEntry` needs `Debug` (workspace lint) but nothing formatted it. `task_entry_new_builds_a_runnable_entry` exercises both at runtime (constructs via `new`, applies the registration closure, runs the handler, formats `Debug`). The `registry_add_*` and `from_inventory_*` tests were also written to run the registered handler so their inline task closures are covered, not just stored.

## Phase 2 (rayonette-macros-core + rayonette-macros shell, Tier A)

1. `recover_input_type` takes the closure, not the method call. The plan signs it `recover_input_type(&syn::ExprMethodCall, scope) -> Recovered`. In this phase only closures are recovered, so passing `&ExprMethodCall` would force a non-closure dispatch arm that sits uncovered, and `scope` is unused until the binding tier. It is `recover_input_type(&ExprClosure) -> Recovered` here; the method-call/receiver access and the `scope` argument are added in the heuristic phase when the receiver and binding tiers need them.

2. `Recovered` carries only `Annotated` and `GiveUp` in this phase, not the four variants the deliverable lists. This follows the plan's own rule, "Add each variant together with its test in Phase 3 so no match arm sits uncovered." `Annotated` boxes its `syn::Type` to satisfy clippy's `large_enum_variant`.

3. Scope is a function, not yet a function-or-module. `expand` parses an `ItemFn`; a non-function scope returns a parse error (covered by `expand_rejects_a_non_function_scope`). The `register_task!` siblings are emitted after the function at module scope. Module scope and the broader attribute-placement contract are an open detail the plan defers, so they wait for a later phase.

4. Give-up emits an inline `compile_error!` as the registration rather than aborting expansion with a returned `syn::Error`. So `expand` still rewrites the call (which compiles, because the receiver pins the input type) and the only error the user sees is our message at the call site. This is what `expand_emits_compile_error_on_giveup` asserts. The proc-macro shell's `unwrap_or_else(syn::Error::into_compile_error)` still handles genuine parse errors.

5. Three macros-core unit tests beyond the plan's six, to hold the new crate at 100/100 on its own: `recover_gives_up_on_an_unannotated_closure` (the give-up arm and its `Debug`), `expand_registers_a_named_function` (the named-function registration arm and the global `net_map_task` branch), and `expand_rejects_a_non_function_scope` (the parse-error path). Two llvm-cov closing-brace quirks were designed out rather than suppressed: `recover_input_type` is a `match` (not nested `if`s), and `Recovered`'s `Debug` renders the recovered type so the annotation test asserts it without an always-true `if let`.

6. The capturing-closure compile-fail case does not use `#[rayonette::tasks]`. It is bare `net_map`, exactly the migrated `fleet.rs` doctest. The no-capture const-assert fires regardless of the macro, and skipping the macro keeps the failure to the single no-capture message (a `#[tasks]`-wrapped capturing closure would also emit a confusing "cannot find captured value" error from the module-scope `register_task!`). The original `compile_fail` doctest is left in place for now; folding it fully into trybuild is a Phase 5 polish item.

7. trybuild `.stderr` goldens for the two compiler-generated failures (wrong-annotation, capturing) are pinned against the local toolchain (nightly 1.97) and are inherently toolchain-version-specific. The Tier-D golden pins our own stable message and is robust. Regenerate the two compiler-text goldens with `TRYBUILD=overwrite` if CI runs on a different channel.

## Phase 3 (heuristic Tiers B and C, generics and turbofish)

1. `recover_input_type` takes `(&ExprClosure, &Expr receiver, &[(Ident, Type)] bindings)`, not `(&ExprMethodCall, scope)`. Passing the closure, receiver, and bindings explicitly avoids a dead non-closure dispatch arm and keeps the function pure and testable without introducing a public `Scope` wrapper type (which would itself need a manual `Debug`). The plan's `scope` is realized as `bindings`, the typed `let` bindings in scope at the call site.

2. Tier B tracks `let` bindings only, not function parameters. A `fn run(v: Vec<u32>)` parameter does not feed Tier B, so `v.net_map(|x| ...)` over a bare parameter still gives up. This matches the plan's Tier B example (`let v: Vec<u32>`) and keeps the give-up tests honest; tracking parameters is a possible later ergonomic extension. It is why `expand_emits_compile_error_on_giveup` now uses an opaque function-call receiver.

3. The Phase 2 `unannotated_closure_tier_d.rs` trybuild fixture used a `Vec<u32>` binding, which Tier B now recovers, so it would compile and no longer fail. It is replaced (atomically, with a regenerated golden) by `tier_d_give_up_emits_compile_error.rs`, whose receiver is an opaque function call, the genuinely unrecoverable case.

4. The rewrite now writes the recovered type into both the rewritten call and the registration (via `annotate_closure`), so an unannotated-but-recovered closure registers as `|x: u32|` at module scope. This matches the design's illustrative expansion; Phase 2 had only ever passed closures through verbatim.

5. `element_type` supports generic-container path types (`Vec<T>`, `Range<T>`, `Cow<'a, T>`, and so on), not arrays, slices, or references. A binding of such a type gives up, which is safe. Array and slice support would each need its own arm and test and is deferred.

6. Five macros-core tests beyond the plan's six `recover_*` tests, to hold the crate at 100/100 by covering the defensive `None` branches in the extraction helpers: `recover_gives_up_on_a_multi_parameter_closure`, `recover_gives_up_when_the_binding_is_not_a_container`, `recover_gives_up_when_the_binding_type_is_not_a_path`, `recover_gives_up_on_unreadable_receivers` (a non-`vec!` macro, non-literal range bounds, non-integer literals), and `recover_skips_a_lifetime_generic_argument`. The Tier-B expand test also carries a tuple-typed `let` to cover the non-ident binding skip.

## Phase 4a (migrate the four consumers, scanner still present)

1. The `embed_microcrates!` registry half (`__rayonette_registry`) was removed in Phase 4a, not Phase 4b. The plan kept it alive through 4a (uncalled, under `#[allow(dead_code)]`) and dropped it in 4b. But llvm-cov attributes a `macro_rules!`-generated function to its definition site, `rayonette/src/lib.rs`, which the consumer ignore-regex does not exclude. Once every consumer calls `Registry::from_inventory()` instead of `__rayonette_registry()`, that generated function is uncovered and fails the 100-percent-functions gate. Dropping just the macro's registry half is low risk (no consumer references it, and the scanner plus its tests are untouched), so it moves to 4a to keep the phase gate-green. The genuinely risky 4b work (deleting the scanner functions and rewriting their coupled tests) still waits for 4b. `rayonette-build` still writes `rayonette_registry.rs` into `OUT_DIR`; it is simply no longer `include!`d.

2. Each `serve_if_agent` site builds its registry with the fully-qualified `rayonette::agent::Registry::from_inventory()`, since `Registry` is not in the prelude.

3. `#[rayonette::tasks]` is placed above `#[tokio::main]` on each `main`, so it rewrites the clean async body before tokio wraps it in a runtime.

4. The optional annotated-closure `net_map` was not added to any consumer. All four stay on named functions for the lowest-risk cutover (the plan's preference for `fixtures/consumer`). The Tier A and Tier C closure paths are already proven end to end by `macro_inprocess.rs` and the trybuild pass cases.

## Phase 4b (delete the scanner and registry emission)

1. `collect_rs_files` was deleted, not kept. The plan's keep-list named it, but its only caller was the scan loop in `extract_into`; removing the scan orphans it, and a kept-but-uncalled private function fails the 100-percent-functions gate. It had no dedicated test (it was covered only through the scan path), so the deletion is atomic. The rerun-if-changed list still comes from `bundle_source`'s returned bundled files, which is workspace-wide and strictly broader than a src-only enumeration would have been.

2. `extract_into_rejects_unparseable_source` was also deleted. The plan named only the `find_net_map_calls`-based `rejects_unparseable_source`, but once `extract_into` is bundling-only it no longer parses Rust, so it cannot error on unparseable source and that test's premise is gone.

3. `extract_into_scans_subdirs` was renamed to `extract_into_bundles_nested_subdirs` and its sample files changed to neutral content, since "scans" no longer describes it; it now asserts only that a nested file lands in the bundle.

4. Removed the now-unused `syn` dependency from `rayonette-build` (the scanner was its only user) and updated the crate's stale "extraction and codegen" description to "source bundling." A stale `serve_if_agent` doc example in `node.rs` that still showed `__rayonette_registry()` was updated to `Registry::from_inventory()`.

5. `extract_into` no longer writes `rayonette_registry.rs` at all (the plan had it lingering as a dead `OUT_DIR` file through 4b). Since the `embed_microcrates!` registry half was already dropped in 4a, there is no reader, so not writing it is cleaner than writing a dead file.

## Phase 5 (backstop, doctests, prelude, docs)

1. The coverage-ignore note went to `HARDENING.md`, not the README. The plan said to update the README and the coverage-command documentation, but the README is a two-line stub with no coverage or crate-layout content. The documented gate command lives in `HARDENING.md`, so the `rayonette-macros/src/` ignore and its rationale were recorded there.

2. The unknown-key backstop reads the registry's keys through a small private `Registry::keys()` accessor rather than the private `handlers` field directly, and the message is branchless: it always joins the sorted keys, so an empty registry simply renders as `[]`. That keeps the single existing `rejects_unknown_fn_key` test sufficient (no separate empty-registry case to cover a branch that does not exist).

3. The `tasks`, `register_task!`, and `from_inventory` doctests use an explicit `fn main()` so the macro's module-scope `register_task!` siblings land at module scope rather than being wrapped inside the doctest's implicit main. The forgotten-annotation `compile_fail` doctest (the plan's mirror of the `fleet.rs` capturing-closure doctest) lives on the `tasks` re-export, its natural home.

4. Coverage stayed above the 99 percent floor across 4b and 5, so no targeted top-up tests were needed. A stray semicolon in the Phase 2 `register_task!` doc was fixed in passing (the prose rule).

## Process note: stale coverage profdata

cargo-llvm-cov merges `.profraw` across runs within a session. After any edit that shifts line numbers, run `cargo llvm-cov clean --workspace` before measuring, or the report is garbage (a Phase 3 measurement read 79 percent purely from stale data). The four-command gate is reliable from a clean profdata state.

## Settled detail: the key-derivation scheme

The open Phase 2 detail (the exact key scheme) is settled as `{scope}::{name-or-task#ordinal}#{hash}`: a named function keys by its last path segment, a closure by its source-order ordinal within the scope, and both append a 16-hex FNV-1a hash of the canonical task tokens to avoid cross-module collisions. The hash is FNV-1a rather than `std`'s `DefaultHasher` because it must be identical on a coordinator and an agent built on different toolchains. It is formatting-stable because it hashes the parsed token rendering, not raw source. `key_is_identical_for_call_site_and_register` and `key_is_stable_across_formatting` pin these properties.
