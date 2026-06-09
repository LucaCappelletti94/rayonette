# API review: footguns and ergonomics

A review of the consumer-facing surface (`fleet.rs`, `lib.rs`, `node.rs`, `process.rs`, the `embed_microcrates!` macro, and the build-time scanner in `rayonette-build`). The crate internals are already disciplined after the hardening epic (see `HARDENING.md`), so these are API-design footguns and consumer ergonomics, the things to settle before publishing under the rayonette name. Ordered by impact.

The core API is well built and these notes should not obscure that: `NetMap` is `#[must_use]` with a helpful message, the capturing-closure check is a `const` assert with a clear error, the no-global-fleet path returns an actionable error, and poisoned locks are recovered gracefully throughout.

## 1. Two divergent agent entry points (highest priority, a bug in waiting)

`process::run_agent(registry)` (`rayonette/src/process.rs`) serves a leaf only: it calls `agent::serve(...)` directly, never reads the children file, so a consumer that uses it can never become a relay. It also returns a `Result` without exiting the process, which is the exact `tokio::io::stdin` blocking-thread hang shape the hardening pass flagged (only exiting the process closes stdout promptly on a graceful self-termination).

`node::agent_main(NodeConfig)` (`rayonette/src/node.rs`) is the relay-capable entry point that exits the process when serving ends. The fixture consumer (`fixtures/consumer/src/main.rs`) uses `run_agent(...).expect(...)`, while `examples/ssh-run` uses `agent_main`, so a consumer copying the fixture silently loses relay support and inherits the hang risk.

Fix: standardize on a single blessed agent entry point that is both relay-capable and exits. Done: `process::run_agent` was removed, and the two consumers that used it (`fixtures/consumer` and `examples/montecarlo`) now call `node::agent_main` with a `NodeConfig`, so every agent entry point in the repo is now `agent_main` (the docker harness consumer, ssh-run, and the rayonette-test-agent bin already were). For montecarlo this is a genuine upgrade, since it runs over ssh and its agents are now relay-capable. The only remaining caller surface is `agent_main` itself.

## 2. Task registration is a syntactic scan

`rayonette_build::find_net_map_calls` registers a task only when it sees a literal `.net_map(IDENT)` or `.net_map_with_fleet(IDENT, ...)` at a call site. Any indirection (`let f = my_fn; xs.net_map(f)`, a re-exported function, a function reached through a generic) is skipped, so the agent never registers it and the run fails at runtime with an unknown-`fn_key` error rather than at compile time. The capturing-closure `const` assert is good, but nothing catches the registration mismatch.

Fix: at minimum make the runtime error name the missing key and hint that the function must be passed directly to `.net_map` (not via a binding). Better would be a compile-time link between use and registration (for example a registration macro the call site uses), so a missing registration cannot reach runtime.

## 3. Stringly-typed NodeConfig

`NodeConfig::new(registry, source, binary_name: String, toolchain: String)` (`rayonette/src/node.rs`) takes the binary name and toolchain as bare strings. A wrong binary name or a typo such as `"stabel"` compiles cleanly and fails late during remote provisioning, far from the call site.

Fix: done. A `Toolchain` enum (`Stable` default, `Nightly`, `Named` for a pinned version) replaces the toolchain string in `NodeConfig` and `Ssh::build`, so a typo is unrepresentable. `NodeConfig::new(registry, source)` is now two-arg: the binary name defaults to the running executable's file stem (every node runs the same binary, so `std::env::current_exe()` is correct at every tree position) and the toolchain to `Stable`, with `.binary_name(...)` / `.toolchain(...)` builder overrides. The remote commands built from these values are unchanged, so provisioning behavior is identical. The consumer-side `Ssh::build` still names the binary explicitly; folding that away is item 5.

## 4. The entire crate is `pub mod`

`lib.rs` exposes 16 public modules: the real API is roughly six names, but `coordinator`, `framing`, `protocol`, `provisioning`, `relay`, `graph`, `control`, `telemetry`, `observability`, and the rest are all public, and `testing` ships its test doubles in the public API unconditionally. That is a large and fragile semver surface for a crate about to be published.

Fix: curate the surface. Done. The engine modules with no external use (`graph`, `heartbeat`, `layout`, `protocol`, `relay`, `telemetry`) are now `pub(crate)`; the modules the build-time codegen or the integration tests still reach (`agent`, `coordinator`, `framing`, `provisioning`, `testing`) are `#[doc(hidden)] pub` so they keep working but leave the docs; and within the modules that stay public, internal items were demoted (`node::load_children`, `process::agent_connection`, `observability::parent_of` to `pub(crate)`; `process::{spawn, AgentProcess}` and `ssh::SshRemote` to `#[doc(hidden)]`, since they are reached by the integration tests or forced public by a `Launch` associated type). A `prelude` was added. The public module surface dropped from 16+ to 9 (`capability`, `control`, `fleet`, `node`, `observability`, `process`, `ssh`, `tui`, `prelude`). Per the chosen strategy this used `#[doc(hidden)]` rather than a `test-util` feature, so there were no test changes.

## 5. Consumer boilerplate is spread thin and easy to get subtly wrong

A consumer must wire `build.rs` to call `rayonette_build::extract()`, invoke `embed_microcrates!()`, hand-write an `if is_agent() { agent_main(NodeConfig::new(...)).await }` branch, and then `install_fleet` plus `net_map`. The mistakes (doing fleet setup before the `is_agent` check, not realizing the agent branch must exit, the binary-name string) all surface at runtime.

Fix: a single `rayonette::agent_entrypoint!()` macro, or a `run_agent_if_agent()` helper that assembles `NodeConfig` from the macro plus `CARGO_BIN_NAME`, would collapse the agent half to one line and remove the foot-guns in items 1 and 3 at the same time.

## Status

- Item 1: done (merged).
- Item 3: done (this change).
- Item 4: done (merged).
- Items 2 and 5: recorded here, not yet started.
