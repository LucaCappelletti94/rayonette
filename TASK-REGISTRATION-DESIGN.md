# Task registration: design notes (API review item 2)

These are working notes for API-review item 2: today rayonette discovers task functions with a build-time syntactic scan (`rayonette-build::find_net_map_calls`, which matches literal `.net_map(IDENT)` / `.net_map_with_fleet(IDENT, ...)` call sites and ships a generated `Registry::new().with_fn(a).with_fn(b)`). Anything that is not a bare identifier at the call site is silently not registered, so the run fails at runtime with "unknown fn_key" instead of at compile time. This document records the design discussion and the (in-progress) research, so the choice between fixes can be made next session.

## The core requirement

A distributed task must be four things at once, because the coordinator and the agent are separately compiled binaries (the agent is built from the shipped source on a remote host, possibly with a different toolchain) that must agree on the task:

- Enumerable: the full set of tasks must be knowable at build/link time, since the agent has to contain a handler for each.
- Named: both sides key the task by a stable string. `fn_key` is `type_name::<F>()`; a named `fn` has a stable path (`consumer::double`), portable across builds and toolchains.
- Typed: registration needs a concrete `fn(Vec<u8>) -> Vec<u8>` wrapper that deserializes the input, runs the task, and serializes the output, which requires the concrete parameter and return types.
- Top-level: registration is a module-level / link-time act (the generated registry, or an inventory static), not something an inline expression can perform.

A plain named `fn` is the one construct that is all four for free. The two hard cases each break a different subset.

## Lambdas

Capturing is not the real blocker (the `size_of::<F>() == 0` const-assert already rejects capturing closures because their captured state cannot be serialized). The blockers for even a non-capturing closure are:

1. No types at metaprogramming time. The closure's parameter and return types are inference outputs (from the iterator item type and `net_map`'s signature). Both proc-macros and build.rs run before type inference, so they cannot write the typed wrapper's signature, and Rust forbids inferred types in a `fn` signature or a bare closure in a `const`/`static`.
2. Registration is top-level/link-time; a closure is inline. The agent never runs `main`, so a closure living in a `main` expression is never instantiated there. Lifting it to a registrable top-level item requires reproducing its lexical context.

Conclusion reached in discussion: a lift IS possible in principle if you had the concrete types and lifted in place via a call-site proc-macro that emits the lifted `fn` in the closure's own module (preserving its `use`s and local items), rewriting `net_map(|x| ...)` to `net_map(__task_n)`. But the antecedent ("if we had types") is exactly the expensive part, and two caveats remain even then: a closure that uses the enclosing function's generic parameters lifts to a generic task (back to the generics problem), and the lift must land in the closure's module (so a call-site proc-macro, not build.rs + `include!` at the crate root).

### Best-effort lambda lifting (revisited) -- this is more tractable than the above implies

The "if we had types" antecedent above is too strong. The lift does NOT need the full closure signature. It needs only the closure's INPUT type; the output type never has to be named, because the macro can keep the closure inline inside a wrapper and let local inference resolve the output:

```rust
// user wrote:  some_iter.net_map(|x: u32| x as u64 + 1)
// an attribute macro on the enclosing fn emits, hoisted to that module:
fn __task_0(input: Vec<u8>) -> Vec<u8> {
    let arg: u32 = deserialize(&input);          // input type: from the closure's own annotation
    let out = (|x: u32| x as u64 + 1)(arg);      // output type u64 is INFERRED here, never named
    serialize(&out)                               // only needs out: Serialize, resolved locally
}
// and rewrites the call site to: some_iter.net_map(__task_0)
```

This compiles with no type names beyond the parameter type, which the user already wrote. The mechanism is a `#[rayonette::tasks]` attribute macro on the function (or module) that contains the `net_map` calls: it sees the whole body as tokens, finds each `.net_map(closure)`, emits a wrapper as a sibling item in that module (so the closure's `use`s and module-level items stay in scope), assigns a deterministic key, and rewrites the call. Because rayonette recompiles the same source on both sides (SPMD), the coordinator and agent run the same macro and derive identical keys, so the wrappers line up for free. Entirely token-level: no rust-analyzer, no rustc, no inference engine.

Coverage of this best-effort lift:

- Non-capturing closure with an annotated parameter type: lifts and registers transparently. A large, realistic fraction of real call sites.
- Parameter type not annotated: the macro cannot name the input type, so it emits a compile error at the call site ("annotate the closure parameter type or pass a named fn"), NOT a silent runtime `unknown fn_key`. Optionally, recover the item type syntactically from an obvious receiver (e.g. `vec![1u32, 2, 3].net_map(|x| ...)`) and only error when genuinely unavailable.
- Captures an enclosing local: the lifted sibling cannot see the local, so it fails to compile (correct, captures are not serializable). Detectable for a clean message; the existing `size_of == 0` assert is the backstop.
- Uses the enclosing function's generic parameters: back to the generics problem; reject with a message.

Net effect: the genuinely-unreachable set shrinks to "unannotated params we cannot recover" plus "generic/capturing closures," and every one of those becomes a compile-time error at the call site instead of a runtime failure. The key correction over the paragraph above: a lift needs only the INPUT type (often written, and reasonably required), not the full signature, because the wrapper hides the output type behind local inference.

## Generics

A generic `fn double<T>` is a family, not one task: each monomorphization (`double::<u32>`, `double::<i64>`) is a distinct task with its own `type_name` key and its own serialization. The agent must carry a separate handler per concrete `T`, so the set of instantiations must be enumerated at build/link time.

Today this does not work: the scanner's `path_string` joins only the identifier segments and drops the turbofish, so `net_map(double::<u32>)` is recorded as `double` and the generated `with_fn(double)` fails to compile (ambiguous `T`); and when `T` is inferred (`net_map(double)`), build.rs has no type information at all. Practical workaround now: a concrete monomorphic wrapper, `fn double_u32(x: u32) -> u32 { double(x) }`, registered and passed to `net_map`.

Under an inventory design, generics fit only with explicit enumeration: `#[rayonette::task]` on the generic plus `rayonette::register_task!(double::<u32>);` per concrete type used. Inferred-type instantiations still cannot auto-register, because no compiler can guess which monomorphizations will be used at runtime.

## State of type analysis (research complete, June 2026)

The question driving this: is there a viable way to get type information at metaprogramming time, which would unblock lambdas and inferred-type generics? Bottom line: no path is viable for a consumer's build today. The only mechanism that could ever make a derive/reflection registry ergonomic (stable reflection) is years away. Detailed findings below.

### 1. The ordering limitation is architectural, not a missing feature

Macro expansion (both `macro_rules!` and proc-macros) and build scripts all run before name resolution and type inference. A proc-macro is by definition a `fn(TokenStream) -> TokenStream`: it sees only tokens and has no visibility into types, trait impls, or resolved names. This is a deliberate property of the compilation model. Two consequences matter for us:

- A macro cannot ask "what type is this expression," so it cannot synthesize the typed `fn(Vec<u8>) -> Vec<u8>` wrapper for an inferred-type call site.
- Macro invocations are isolated: there is no way to pass information from one proc-macro invocation to another. The sanctioned pattern is to emit independent syntactic macros that expand to code which makes the *compiler* correlate things after expansion. That is exactly what link-time registration (inventory/linkme) does, and it is the reason Level 2 is the only "see all the tasks" mechanism that works inside the standard toolchain: each `#[task]` emits a self-contained static, and the linker (not the macro) gathers them.

So nothing in the standard macro/build.rs toolchain can recover types. Confirmed; this closes the door on a token-level fix for lambdas and inferred generics. (RFC 1560 name resolution; proc-macro token model.)

### 2. rust-analyzer as a library (`ra_ap_*`) gives types, but is not embeddable in a consumer build

rust-analyzer's internals are auto-published to crates.io as `ra_ap_*` crates; the umbrella `ra_ap_rust-analyzer` is at `0.0.326` as of June 2026. It is a real semantic engine: the `ide` crate is the intended API boundary, exposing `Analysis` / `AnalysisHost` (a Salsa snapshot of world state), and the underlying `hir` crate provides `hir::Semantics` to map syntax nodes to typed definitions. So the type information genuinely exists here.

Why it is still not viable for us:

- The `0.0.x` versioning is an explicit "no semver, expect breakage every release" signal; each publish tracks rust-analyzer's internal development directly. The deeper, type-bearing crates (`hir-def`, `hir-ty`, `base-db`) are documented as "not, and will never be, an API boundary." Type inference internally pulls from `ra-ap-rustc_type_ir`, so it tracks the compiler and shifts under you.
- Heavy dependency tree and build-time cost, plus we would have to run a near-full analysis of the consumer's crate inside their `build.rs`. That wrecks build time and reproducibility for every downstream user.
- The genuinely robust part (the `syntax` crate) is exactly the part with no semantic info, which is what our current scanner already uses. Reaching the typed layer means depending on the unstable layer.

Verdict: a powerful research tool, not a dependency we can put in a distributed library's build. Rejected.

### 3. rustc as a library (`rustc_private` + `rustc_driver`/`rustc_interface`)

This is the only way to get *the real compiler's* types programmatically (clippy, miri, and rustdoc use it). You get `TyCtxt` and can call `tcx.typeck(def_id).node_type(hir_id)` to read inferred types. But the costs are disqualifying for a library consumed by others:

- Nightly-only, behind `#![feature(rustc_private)]`, with `extern crate rustc_*` declarations and a hard requirement for the `rustc-dev`, `llvm-tools`, and `rust-src` rustup components.
- API churn: the dev guide states the internal APIs "are always going to be unstable"; real drivers pin an exact nightly (e.g. `nightly-2024-10-20`) and adjust per release.
- Distribution is the killer for our model: a custom driver links `librustc_driver-*.so` (~136 MB), needs `LD_LIBRARY_PATH`/`-rpath` wiring, and gets no rustup-managed versioning. rayonette already ships source to a remote and compiles it there, possibly under a different toolchain (the whole point of the `Toolchain` enum). Forcing every node to a pinned nightly with `rustc-dev` installed is incompatible with that design.

Verdict: technically gives types, practically incompatible with "compile the agent from shipped source on an arbitrary remote toolchain." Rejected.

### 4. Reflection / comptime (2026 Project Goal) is the only thing that could eventually make this ergonomic, and it is not ready

This is the long-term direction worth tracking. Status as of June 2026:

- It is an active Rust Project Goal (POC @oli-obk; compiler/lang/libs teams) scoped explicitly as "a reflection scheme based on const fn that can only be called at compile time," for producing const-eval values only, not for putting types back into the type system.
- The MVP has landed on nightly: `TypeId::info` returns a `Type` struct with a `kind` field (a `TypeKind`); surfaced as `std::mem::type_info` behind a feature gate (referred to in coverage as `#[feature(type_info)]`), with an experimental `#[compile_time_only]` attribute marking the const fns. "A new type kind every week"; today it is "super MVP," mostly tuples (field counts, offsets, recursion), `Leaf` for primitives; ADTs and `dyn` are still open PRs.
- Hard design constraint: these functions are compile-time only by construction, because a runtime `TypeId -> repr` table is "an obvious no-go." That is actually fine for our use (registration is a build/link-time act), but it is nowhere near stable.
- Timeline: explicit goal is to let facet/bevy-reflect/reflect drop their derives; an RFC and T-lang/T-libs-api buy-in are slated for the 2026-2028 window. So a stable, dependable reflection-based registry is realistically years out.

Verdict: promising, watch it, but not actionable now. If it stabilizes, it could replace both the scan and an inventory attribute with a derive-free reflective registry. Note for the future, not a basis for the item-2 decision.

### Sources

- Proc-macro token model / pre-inference ordering: https://rust-lang.github.io/rfcs/1560-name-resolution.html ; https://docs.rs/proc-macro2 ; https://users.rust-lang.org/t/proc-macro-expansion-order-and-name-resolution/61750
- rust-analyzer as a library: https://docs.rs/ra_ap_ide/latest/ra_ap_ide/ ; https://crates.io/crates/ra_ap_rust-analyzer ; https://rust-analyzer.github.io/book/contributing/architecture.html
- rustc as a library: https://rustc-dev-guide.rust-lang.org/rustc-driver/intro.html ; https://rustc-dev-guide.rust-lang.org/rustc-driver/interacting-with-the-ast.html ; https://jyn.dev/rustc-driver/
- Reflection/comptime 2026: https://rust-lang.github.io/rust-project-goals/2026/reflection-and-comptime.html ; https://github.com/rust-lang/rust-project-goals/issues/406 ; https://weeklyrust.substack.com/p/compile-time-reflection-is-finally

## State of the art: how the ecosystem actually registers and ships tasks (June 2026)

The previous section answered "can a macro get types?" (no). This section answers the more useful question: how do real Rust crates and distributed frameworks solve "name, register, and dispatch a typed task across separately built binaries," and which mechanism fits rayonette. The single most important framing it surfaced: rayonette is a homogeneous-binary / SPMD system. The coordinator and agent are the same source recompiled on each side, so both contain the same set of task items. That is the exact regime in which every robust ecosystem solution below operates, and it is why link-time registration is a natural fit rather than a hack.

### The portability problem and the four ecosystem answers

A function pointer is an address that is not valid on another machine, so every distributed system converts "which code to run" into a portable key and resolves it locally. The Rust ecosystem has four distinct patterns:

- Declarative-plan, no user-function registry (Ballista/DataFusion): ship a serialized physical plan (protobuf); every executor has the same engine compiled in and interprets it. There is no arbitrary-function registry at all. Not applicable to rayonette, which runs arbitrary user `fn`s, but it is the "avoid the problem" baseline.
- Ship the code itself (Lunatic): send the compiled WASM module over the wire and JIT it per architecture. Solves portability by moving bytecode, not keys. rayonette already chose the "ship source, recompile remotely" variant of this idea, which is why our two binaries agree by construction.
- SPMD / same-binary everywhere (Timely Dataflow, Constellation): every worker runs the identical binary and builds the identical operator graph locally; only data and progress cross the wire. rayonette is squarely in this family. Crucially, this is the assumption that makes a stable string key (`type_name::<F>()`, or a `#[task]` static) line up on both sides for free.
- Explicit named registry (classic RPC, and what rayonette does today): a table mapping a stable key to a local handler, populated before invocation. This is the right pattern for "invoke arbitrary user functions by name," which is exactly our requirement. The design question is only how the table gets populated: a bespoke build-time source scan (today), or link-time/`ctor` registration (Level 2).

### typetag is the production-proven analog of Level 2

`typetag` (dtolnay) solves almost exactly our sub-problem: serialize/deserialize `Box<dyn Trait>` by a stable string tag. Its mechanism is the Level 2 mechanism: it uses `inventory` to build a registry of impls (each `#[typetag::serde]` emits a registration), `erased-serde` to keep it object-safe, and it lazily builds the tag-to-deserializer map on first use. It is widely deployed and battle-tested. This is strong evidence that an `inventory`-based `#[rayonette::task]` registry is a well-trodden, low-risk design rather than a novel gamble. Its documented limitations are also instructive and apply to us verbatim: the attribute must be applied to every concrete type, it does not work on platforms without `ctor` support (notably WASM), and generic traits/impls are not supported (so per-monomorphization registration is required regardless). `serde_flexitos` exists as the manual-registry alternative for the WASM/generic cases.

### linkme vs inventory: a real tradeoff that corrects the earlier lean

The earlier notes leaned `linkme` over `inventory` for `--gc-sections` robustness. The wider research complicates that:

- `linkme` (distributed slices via `link_section`, no life-before-main, const-only, zero runtime cost) has a long history of linker-section fragility: `--gc-sections` stripping the start/stop symbols (linkme #49, #41; the lld/GNU-ld start-stop-gc behavior change), and, most relevant to us, cross-crate discarding: members of a distributed slice declared only in a dependency crate (with nothing in the root binary referencing them) can silently vanish (linkme #36, rust-lang/rust#67209). For a tool whose whole point is to avoid silent task-loss, that failure mode is ironic and disqualifying if consumers factor tasks into a library crate.
- `inventory` (registration via `ctor` life-before-main constructors) does not have the cross-crate discarding problem, because the constructors run regardless of where the item is defined. Its costs are life-before-main and no WASM. typetag chose `inventory` for exactly this generality.

For rayonette, target hosts are normal Linux/macOS servers reached over SSH, so WASM is irrelevant and life-before-main is acceptable, while a consumer defining tasks in a sub-crate is entirely plausible. That inverts the earlier preference: if we do Level 2, `inventory` is the safer default and `linkme` is the optimization to reach for only if life-before-main becomes a problem. This is a genuine research-driven correction to the design notes.

### Lambdas are not categorically impossible, just incompatible with our macro-typing approach

`serde_closure` (alecmocatta) plus `serde_traitobject` is the state-of-the-art for shipping closures, including capturing ones, across identical binaries: `Fn!(|x| x + captured)` wraps the closure into a struct that implements `Serialize`/`Deserialize` over a typed environment tuple, and `serde_traitobject` type-erases it into a `Box<dyn serde_traitobject::Fn()>` that serializes between processes running the same binary. This is the engine under Constellation/Amadeus. The honest correction to "lambdas are out of reach": they are out of reach for our current mechanism (a macro inferring the closure's types), but a different architecture (macro-wrap the closure, serialize its captures, type-erase, dispatch to the identical recompiled binary) does ship them. The costs are why it is still not attractive for v1: it is nightly for the full trait impls, it forces every closure through a `Fn!()` macro (so the bare-`|x| ...` ergonomic is lost anyway), it requires serializing captured state (reversing the current zero-capture invariant), and it adds a heavy dependency stack. Worth recording as a real option, not a flat impossibility.

### Two future directions that would obsolete the bespoke mechanism

Beyond reflection (previous section), there is an active effort to add "global registration" to the language/compiler itself, generating linkme-style distributed slices natively (this is essentially how `libtest` already collects tests). If it lands, it removes the platform fragility that is `linkme`'s main weakness and would be the clean long-term home for a `#[task]` registry. Together with the reflection MVP, the trajectory is clearly toward first-class support, which argues for keeping our own mechanism small and swappable now.

### Sources (state of the art)

- linkme/inventory mechanics and fragility: https://github.com/dtolnay/linkme ; https://docs.rs/linkme/latest/linkme/struct.DistributedSlice.html ; https://github.com/dtolnay/linkme/issues/36 ; https://github.com/dtolnay/linkme/issues/41 ; https://github.com/dtolnay/linkme/issues/49 ; https://crates.io/crates/inventory ; https://donsz.nl/blog/global-registration/
- typetag / erased-serde / serde_flexitos (string-keyed trait-object registry): https://docs.rs/typetag/latest/typetag/ ; https://github.com/dtolnay/erased-serde ; https://crates.io/crates/serde_flexitos/0.1.0
- serde_closure / serde_traitobject / constellation (shipping closures across identical binaries): https://github.com/alecmocatta/serde_closure ; https://github.com/constellation-rs/constellation ; https://constellation.rs/constellation
- distributed-framework task models: https://datafusion.apache.org/ballista/contributors-guide/architecture.html ; https://github.com/lunatic-solutions/lunatic-rs ; https://github.com/TimelyDataflow/timely-dataflow

## The decision space for item 2

- Level 1 (pragmatic, recommended so far): keep the syntactic scan, but (a) make the runtime "unknown fn_key" failure loud and self-explaining (name the missing key, list the registered keys, and hint that a task must be a named `fn` passed directly), and (b) tighten the `net_map` const-assert / message so a closure is rejected at compile time rather than compiling and failing at runtime. Cheap, contained, honest about the limitation.
- Level 2 (robust redesign): replace the scan with registration via `#[rayonette::task]` + `inventory`, with `register_task!(f::<T>)` for generic instances. This removes the scan and the whole runtime-mismatch class and makes indirection (bindings, re-exports, sub-crate definitions) work. Research correction: prefer `inventory` over `linkme` here. `linkme`'s cross-crate distributed-slice discarding (linkme #36 / rust#67209) would silently drop tasks defined in a consumer's library crate, reintroducing the exact silent-loss failure we are trying to kill; `inventory`'s `ctor` constructors run regardless of definition site. WASM is irrelevant for an SSH-targeting tool, so `inventory`'s only real cost (life-before-main, no WASM) does not bite us. This is the same mechanism `typetag` uses in production, so it is low-risk. A real but self-contained redesign.
- Level 3 (call-site attribute macro with best-effort lambda lifting): a `#[rayonette::tasks]` attribute on each scope (fn or module) that contains `net_map` calls. The macro replaces the build.rs scanner: it sees every call site as tokens, registers named-fn arguments directly, lifts annotated non-capturing closures via the wrapper trick above (only the input type is needed; the output type stays inferred), assigns deterministic keys that line up across the SPMD recompile, and turns every unhandled case (unannotated param it cannot recover, capture, generic) into a compile error at the call site. This is the only level that gives transparent lambdas for the common annotated case, and it eliminates the silent-failure class the same way Level 2 does. Cost: a real proc-macro that parses and rewrites call sites (more than Level 2's per-item attribute), and the attribute must be applied to each scope containing `net_map` calls (its main ergonomic and coverage constraint versus Level 2's decentralized `#[task]`). Generics still need explicit per-monomorphization handling. Note: choosing transparent lambdas commits us to a call-site-aware macro, because the lift must rewrite the call and emit the wrapper in place; decentralized `#[task]` registration cannot do it.
- Inferred-type generics: out of reach. Which monomorphizations run at runtime is unguessable at build time; require explicit `register_task!(f::<T>)`. Capturing closures with serializable state are shippable in principle via a `serde_closure`/`serde_traitobject` architecture, but that is nightly, forces every closure through a `Fn!()` macro, reverses the zero-capture invariant, and adds a heavy dep stack, so it is out of scope for v1.

Three corrections from the research: (1) Level 1's "reject closures at compile time" is only half-achievable: the existing `const { assert!(size_of::<F>() == 0) }` rejects only capturing closures; a non-capturing closure passes it, is skipped by the scanner, and still fails at runtime, and no trait bound distinguishes a named `fn` from a non-capturing closure. (2) rayonette is an SPMD / homogeneous-binary system, the regime where both link-time registration (Level 2) and call-site key generation (Level 3) line up for free. (3) Transparent lambdas are NOT impossible: a call-site macro covers annotated non-capturing closures by lifting to a wrapper that only names the input type. This is Level 3.

Updated recommendation after the finished research: the decision now has a third axis, do we want transparent lambdas. If lambdas matter (the user's stated preference), Level 3 is the target: it is the only option that delivers them for the common case while still killing the silent-failure class, and it subsumes the scanner. If lambdas are out of scope, Level 2 with `inventory` is the leanest robust close-out (de-risked by `typetag`). Level 1 stays the cheap defer-it option. Level 3 and Level 2 are not exclusive: a call-site `#[tasks]` macro can coexist with decentralized `#[task]` registration for functions defined far from their call sites, at the cost of two mechanisms.

Decision: build Level 3. The target shape and the ergonomics targets it must hit are recorded below.

## Target shape (Level 3, the chosen design)

The current mechanism makes three pieces agree on one `type_name::<F>()` string: the coordinator key (`fleet.rs:635`, `agent.rs:71`), the build-time source scan (`rayonette-build/src/lib.rs:53-83`), and the runtime registry lookup (`agent.rs:185`). It works only when the scanned token and the runtime type are the same named item, and it fails in two ways: non-capturing closures are skipped and fail at runtime with `unknown fn_key`, while generics-with-turbofish, local bindings, re-exports, and module-relative paths emit code that fails to compile inside generated `OUT_DIR` text. The const-assert (`fleet.rs:753`) catches only capturing closures, not non-capturing ones.

The target replaces the source scan with a `#[rayonette::tasks]` attribute macro on the scope (a fn or a module) that contains the `net_map` call sites. It transforms the token stream the compiler actually compiles, which is the one thing a build script cannot do, so it can both rewrite the call and emit a sibling registration where the call site's paths, `use`s, and annotations resolve.

What the consumer writes:

```rust
use rayonette::prelude::*;

#[rayonette::tasks]                       // the one annotation; scopes the task call sites
fn main() -> anyhow::Result<()> {
    rayonette::install_fleet(Fleet::builder().add_node(node).build()?);

    let scores = players.net_map(score).collect()?;              // named fn
    let doubled = (0u32..100).net_map(|x: u32| x * 2).collect()?; // annotated closure (Tier A)
    let v: Vec<u32> = load();
    let halved = v.net_map(|x| x / 2).collect()?;                // closure, type from receiver (Tier B/C)
    Ok(())
}

fn score(p: Player) -> u32 { /* ... */ }
```

What the macro expands to (illustrative):

```rust
fn main() -> anyhow::Result<()> {
    rayonette::install_fleet(/* ... */);
    // explicit deterministic key per call; the task expr stays inline so net_map's
    // `Fn(Self::Item) -> O` bound type-checks it and so verifies the input-type guess
    let scores  = players.net_map_task("myapp::score",       score).collect()?;
    let doubled = (0u32..100).net_map_task("myapp::main::task#0", |x: u32| x * 2).collect()?;
    let v: Vec<u32> = load();
    let halved  = v.net_map_task("myapp::main::task#1",      |x: u32| x / 2).collect()?;
    Ok(())
}
fn score(p: Player) -> u32 { /* ... */ }

// emitted as siblings in this module, so paths / use / annotations resolve here:
rayonette::register_task! { "myapp::score",       score }
rayonette::register_task! { "myapp::main::task#0", |x: u32| x * 2 }
rayonette::register_task! { "myapp::main::task#1", |x: u32| x / 2 }
```

Library pieces that change:

- Registration via an `inventory` of entries (replaces the generated `Registry::new().with_fn(..)` string). `register_task!(key, task)` submits a `TaskEntry { register: fn(&mut Registry) }`; the agent builds its registry by iterating `inventory::iter::<TaskEntry>` at boot. No hand-written decode/encode wrapper is needed: the existing generic registration (`with_fn` / `handler`, `agent.rs:149-156`) extracts `I`/`O` from the task without naming them, so an annotated closure registers for free and the output type stays inferred.
- `net_map` becomes key-carrying. The terminal stores the macro-supplied key and uses it directly (`let key = self.key;` in `NetMap::run`, replacing `fn_key(&self.map)` at `fleet.rs:635`). The `F: Fn(Self::Item) -> O` bound stays and pins both the input type (so a wrong guess is a compile error at the call site) and the output type; the task is never called on the coordinator. The capturing-closure const-assert stays as a backstop.
- `fn_key` / `type_name` leave the wire entirely (`agent.rs:65-73`). This is a robustness gain beyond closures: `type_name`'s output is not guaranteed stable across toolchains, and rayonette deliberately allows agents to build on a different toolchain, so a macro-assigned string key is safer across that boundary than the current scheme.
- `rayonette-build` shrinks to source bundling only. `find_net_map_calls`, `path_string`, `generate_registry`, and the `rayonette_registry.rs` write are removed (`lib.rs:29-83`, `:133-136`); the reproducible-tar bundling and path-dep check stay. The build script keeps the one job a build script is good at (emit files into `OUT_DIR`) and stops doing the one it cannot do soundly (reason about call sites).
- `serve` keeps the `unknown fn_key` error (`agent.rs:185`) as a backstop for "forgot the `#[tasks]` attribute," now made loud and self-explaining.

Input-type heuristic (best-effort, sound by construction). The macro recovers the closure parameter type in tiers: (A) explicit annotation `|x: u32|`; (B) a typed binding in scope, `let xs: Vec<u32> = ...`; (C) a literal or range receiver with an obvious element type, `vec![1u32, 2, 3]` or `(0u32..n)`; (D) give up (chained adapters, function-call receiver, cross-scope binding, unsuffixed integer) and emit a compile error at the call site asking for an annotation. The heuristic can be as aggressive as we like because correctness is not its job: the inline task expression is still checked by `net_map`'s `Fn(Self::Item) -> O` bound, so a wrong guess cannot compile-and-mis-decode, it produces a normal type-mismatch error at the user's call site. Three outcomes only: guess correct (or item unconstrained) compiles and works, guess wrong is a compile error at the call site, no guess is a compile error at the call site.

Out of scope for v1 (documented limitations, all compile-time, not silent): closures whose parameter type cannot be recovered (annotate it), closures that capture (const-assert), closures using the enclosing fn's generic params, and inferred-type generic instantiations (require explicit `register_task!(double::<u32>)`).

Open details to settle during planning: (1) the exact key scheme (module path plus per-call-site ordinal for closures, path string for named fns; must be deterministic and identical on both build sides), and (2) the attribute placement contract (per-fn vs per-module, and what happens to a bare `net_map` written outside any `#[tasks]` scope: backstop runtime error vs a compile-time nudge).

## Ergonomics targets

These are the user-facing properties the Level 3 implementation must deliver. Work items will be planned against this list.

1. One annotation, no boilerplate registry. A task is "a thing passed to `net_map` inside a `#[rayonette::tasks]` scope." No `build.rs` registry scan, no manual `Registry` building, no `register_task!` written by hand for the common case.
2. Transparent annotated closures. `xs.net_map(|x: u32| x * 2)` works with no named function and no wrapper, for both `net_map` and `net_map_with_fleet`.
3. Best-effort unannotated closures. When the input type is recoverable from the receiver (Tiers B/C), `xs.net_map(|x| ...)` also works with no annotation.
4. No silent failures, ever. Every unsupported or mistaken case is a compile error pointing at the user's own call site. The `unknown fn_key` runtime surface and the cryptic generated-`OUT_DIR` compile surface are both gone (the runtime error remains only as a "forgot the attribute" backstop, and it names the missing key, lists the registered keys, and explains the fix).
5. Indirection just works. Re-exports, aliases, `let`-bound function values, and module-relative paths register correctly because lifting happens in the call site's own scope.
6. Generics are honest and explicit. `net_map(double::<u32>)` round-trips (the turbofish is preserved); inferred-type generic instantiation gives a clear compile error directing the user to `register_task!(double::<u32>)`.
7. Wrong-guess safety is structural. A mistaken input-type heuristic can only ever produce a compile error at the call site, never a runtime mis-decode, because `net_map`'s existing type bound verifies the guess.
8. Toolchain-robust keys. Task identity does not depend on `std::any::type_name` stability, so a coordinator and an agent built on different toolchains still agree on keys.
9. Clear capture rejection. A capturing closure fails at compile time with a message that explains the no-capture rule and points at a named function or a non-capturing closure (covering the non-capturing-closure gap the current const-assert misses at runtime).
10. Stable public surface, smaller build. The build script no longer scans for tasks (only bundles source), and the curated public API gains `#[rayonette::tasks]` and `register_task!` while `fn_key` and the generated-registry include disappear.
