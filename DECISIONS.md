# Design Decisions

This file records the architecture decisions made for rayonet during design discussion. Each entry is a locked choice plus the reasoning. Items marked "v1" are deliberate first-version scoping, not permanent. The companion `PLAN.md` turns these decisions into phased, test-gated milestones.

## Core assumption

1. **Tasks are compute-bound: each task must cost far more to execute than to copy and launch.** If transport and launch dominate, distribution is not worthwhile and the user should run rayon locally instead. This assumption is what justifies the simple "serialize the input, ship it, run, serialize the output" model and lets us defer all data-locality optimization.

## Distribution and compilation

2. **Build-on-host is the default.** Each remote machine compiles the agent itself rather than receiving a prebuilt binary. Pure-Rust consumer code is the assumed and supported case. This makes heterogeneous architectures free, since every host builds for its own target, and it removes the cross-compilation toolchain burden (the spec's painful macOS-from-Linux case disappears).

3. **Whole-crate compile-on-remote.** We ship the entire consumer crate source to each host and compile it there, rather than extracting a curated micro-crate. This is the foundation for v1. It collapses the extraction problem: the closure's transitive dependencies are all present because the whole crate is present, so there is no closure analysis, no placement convention, and no self-containment rules. Refinements such as cfg-gating out coordinator-only code are deferred.

4. **One binary, two roles.** The same compiled source runs as the coordinator locally and as the agent remotely, selected at runtime. This is the spec's original model, restored as a consequence of whole-crate compilation.

## Public API

5. **The surface is the iterator adapter `inputs.net_map_with_fleet(f, &fleet)`,** modeled on rayon's `.map`. It is an extension method on `IntoIterator` (not a macro) so it chains left to right and is terminated by `.collect()` (results in input order) or `.net_reduce(op)`, the shapes map-reduce needs. A future `inputs.net_map(f)` will target a global fleet implicitly. A separate bridge adds `.net_map(f)` to a rayon `ParallelIterator`, so a distributed map can also sit inside a rayon pipeline. The function is generic over `F: Fn(T) -> U` (decision 12), not a `fn` pointer, so its fn-item type stays unique (decision 8).

6. **net_map is an ordered barrier bridge.** It drains the input iterator, runs the distributed job, and `.collect()` returns results in input order so a rayon chain can re-enter rayon. Internally it still streams to the progress view. A streaming form is a separate advanced API.

7. **v1 task model is `fn(Input) -> Output`, with no per-agent shared-state machinery.** Input is serialized and shipped per task, output serialized back. A task fails only by **panicking**: the agent wraps each task in `catch_unwind` and turns a panic into the `Failed { task_id, error }` message, so one task's panic cannot kill the agent (it survives to serve the next task). This keeps the signature exactly rayon's `map` shape; tasks wanting typed errors make `Output` a `Result<T, E>`. There is no `setup()` in the v1 API. A user who needs load-once-reuse state can use a plain `OnceLock` in their own code with zero framework support. Per-agent shared state, local-file availability, in-memory caching, and data staging are deferred optimizations, justified only when transport cost starts to rival compute cost (which the core assumption says it should not).

8. **v1 supports non-capturing functions only, enforced at compile time.** `.net_map` is generic over the function type `F` (so its `type_name` stays unique per function, decision 12) and asserts `const { size_of::<F>() == 0 }`. A named function or non-capturing closure is zero-sized; captured state is not, so a capturing closure (`|x| heavy(x, config)`) fails to compile. (Typing the argument as a `fn(T) -> U` pointer would also reject captures but would erase the unique fn-item type the key relies on, so the const size assert is used instead.) A `compile_fail` doctest verifies the rejection. Captures are the documented future boundary (they would have to cross the network as serialized data or be loaded remotely via setup).

## Build-time mechanism

9. **A downstream build.rs calls `rayonet_build::extract()`.** A plain library dependency cannot read the consumer's source at build time. A build script in the consumer crate can, and it runs on every cargo build. This is the standard non-proc-macro codegen pattern (same shape as prost-build and tonic-build).

10. **rayonet-build is a separate crate from rayonet.** The build side pulls in syn and the parser and belongs in `[build-dependencies]`. The runtime side pulls in tokio and openssh. Keeping them separate stops each from leaking into the other's build graph.

11. **`embed_microcrates!()` is a macro_rules macro, not a proc macro.** The build.rs writes the source blob into `OUT_DIR`, but only code compiled in the consumer crate can `include_bytes!` from the consumer's `OUT_DIR`. A declarative macro expands in the consumer's context and pulls the blob in.

12. **Functions are keyed by their type name, not by source location.** The selector that crosses the wire is `type_name` of the passed function (for example `"my_crate::evolve"`). At runtime `.net_map<F>(..)` computes `type_name::<F>()`; the build.rs generates a matching registry entry per distinct function passed to `.net_map`, keyed by the same name, whose glue does deserialize then call then serialize. Both sides come from the same compile, so the keys agree. No `track_caller` and no textual closure lifting are needed: whole-crate compile means the function is already present on the agent, so the registry just maps the name back to a real call. The build step does no type analysis; the remote compiler infers all types. (`track_caller` plus source-location keys was the considered alternative; it is only needed if inline closures must key nicely, since a closure's type name is an unhelpful `{{closure}}`. Rejected in favor of naming task functions.)

## Dependencies

13. **Dependencies are inherited from the consumer crate's Cargo.toml verbatim,** with the Cargo.lock copied for version agreement. cargo does not drop unused declared deps, so the full set is the known-good floor.

14. **No dependency trimming.** Dependencies are inherited and compiled as-is (decision 13). An earlier idea of a self-rolled machete-style unused-dependency scan is dropped entirely.

15. **v1 errors on any non-rayonet local path dependency.** Detected via `cargo metadata` source labels, where a local path crate has a null source. The error names the offending crate. The path-dependency copy cascade is a documented future enhancement.

## Configuration and provisioning

16. **Two configuration surfaces, split by when the information is known.** build.rs configures the *artifact* (compile-time facts baked into the binary: what is bundled, dependency trimming, the remote toolchain version to target). The `.net_map(..)` chain configures the *run* (per-invocation facts: which hosts, TUI on/off, progress reporting, retries). Knobs live on the side where their information actually exists.

17. **`net_map` takes the function (and, for now, the fleet); everything else is chained config on a lazy job builder.** `inputs.net_map_with_fleet(f, &fleet)` returns a `NetMap` builder; future knobs (`.tui()`, `.progress(..)` (indicatif-compatible), retries) are set on it, and the job runs when terminated by `.collect()` or `.net_reduce(op)`. A reusable `Fleet` is passed explicitly today; a global implicit fleet (`inputs.net_map(f)`) and inline host config are deferred.

18. **No provisioning consent gate: ssh access is the authorization.** If you can ssh into a host you are already authorized to run commands and write files there, so installing what is needed requires no separate consent. Provisioning is part of the normal flow. Policy: **attempt what a requirement needs, error out per host if blocked** (a host that cannot satisfy a requirement fails clearly and its tasks are requeued). Refinements: (a) the rust toolchain is installed **user-locally via rustup** by default because that needs no sudo, but **sudo is not forbidden**, provisioning may use sudo or system package managers for requirements that genuinely need them (for example a C toolchain), erroring out per host when it cannot; (b) transparency instead of consent, the TUI shows installs as they happen. build.rs pins which toolchain version, the runtime installs it if missing. A `.no_auto_install()` opt-out exists for shared machines, off by default.

## Observability

19. **Observability is a serializable event stream; renderers are pluggable views.** The core emits a structured stream of events (node state changes, task lifecycle, log lines, progress) and is the single source of truth. No renderer is privileged. Views include a terminal TUI, a Dioxus UI (desktop or web), a plain progress line for non-terminal runs, a headless log, and the test harness. Events are `Serialize` so a renderer can be in-process (terminal TUI, Dioxus desktop) or out-of-process and remote (Dioxus web over a socket).

20. **`NodeState` is an enum, started minimal and grown incrementally.** We do not try to enumerate the full lifecycle up front. A first set covers provisioning (Queued, Connecting, Probing, Installing, Syncing, Building, Ready), running (Working, Idle), terminal (Done), and faults (Unreachable, Failed, Lost, Reconnecting), and variants are added as needed.

## Transport and wire protocol

21. **The transport is the ssh-spawned process's stdio.** The coordinator launches the agent over ssh (`ssh host -- <built-binary> --rayonet-agent`), writes frames to its stdin, reads frames from its stdout, and forwards its stderr verbatim so remote panics and errors are visible locally. The `openssh` crate drives the system ssh, so `~/.ssh/config`, connection multiplexing, and ProxyJump work unchanged. No listening ports.

22. **Encoding is plain serde plus length-delimited framing; no custom Codec trait.** Message types are serde-derived enums. The wire uses `tokio_util::codec::LengthDelimitedCodec` for framing (the one thing serde does not provide on a byte stream) and **postcard** for the bytes. Swapping the serde format is a one-line change, so no abstraction trait is warranted. Binary (postcard) is the default rather than JSON because it constrains the user's `Input`/`Output` types the least (it round-trips the full serde data model, whereas JSON rejects non-string map keys, mangles NaN/Infinity floats, and bloats byte arrays), and wire efficiency is irrelevant under the compute-bound assumption. Debuggability comes from a `--rayonet-debug` decode-and-print layer over frames, not from a human-readable wire.

23. **v1 message set.** Coordinator to agent: `Hello { protocol_version, fn_key }`, `Assign { task_id, payload }`, `Shutdown`. Agent to coordinator: `Ready`, `Started { task_id }`, `Completed { task_id, output }`, `Failed { task_id, error }`. `Ready` carries no payload because an agent runs one task at a time (decision 24). `Hello` carries the single `fn_key` (the `type_name` selector) for the whole job, since one `.net_map` call is one job running one function, so it is sent once at handshake, not per `Assign`. `Failed` is terminal (the task ran and errored, not retried); agent death is the separate requeue path. `Heartbeat { task_id, note }` and structured `Log { level, message }` are deferred (stderr forwarding covers visibility in v1). The protocol is extensible: `protocol_version` plus adding enum variants makes growing the set cheap and backward-aware.

## Scheduling

24. **Scheduling is a simple pull model: an idle agent gets the next pending task, one at a time.** When an agent finishes its task it receives the next one. This is work-stealing for free, fast machines drain more pending tasks with no sharding and no explicit algorithm. A machine runs **one task at a time**: the task is the unit of distribution and is expected to saturate the machine itself (via rayon, a GPU, or its own threads), so there is no per-agent capacity knob and `Ready` advertises nothing. The pre-send / credit optimization (hiding the post-`Completed` round-trip idle gap) is **dropped from v1**: under the compute-bound assumption a task takes seconds to minutes while the round-trip is milliseconds, so the gap is negligible.

## Fault tolerance

25. **v1 fault handling is abandon-and-redistribute, with requeue and an idempotency contract.** When an ssh channel closes, the coordinator marks that agent dead and requeues its in-flight `task_id`s into the pending pool; survivors absorb the work and the run continues through the loss of any machine. v1 does **not** reconnect a dropped host (reconnect with backoff is a deferred fast-follow). Because a requeued task may run twice (it may have completed exactly as the link dropped), **tasks must be safe to re-run** (a documented hard contract, not enforceable), and result assembly dedups by `task_id`. `Failed` is terminal and never requeued; only agent death triggers requeue.

## Result collection and run state

26. **Task result payloads are returned to the program, not persisted by rayonet.** Results stream back over ssh and are assembled into the `.collect()` / `.reduce()` output, exactly like rayon. rayonet does not store results; persistence is the user's concern. Coordinator-restart **resume is a deferred future feature**, so v1 has no durability-for-resume requirement.

27. **v1 has no database; the coordinator's in-memory state is the source of truth.** The coordinator already holds authoritative live state (per-node state, task lifecycle, scheduler bookkeeping) in memory because it needs it to run. In-process renderers (the terminal TUI, the plain progress line) read that state directly through the event broadcast, so a database adds nothing for viewing a live run. The TUI does not need a DB.

28. **The SQLite/Diesel operational database is deferred, not part of v1.** Its value is out-of-process and after-the-fact: exporting state across a boundary to a remote Dioxus web UI, post-mortem querying once the process has exited, and eventual resume persistence. None of these are v1. Deferring it also keeps SQLite out of the shipped crate, so v1 hosts compile no C and stay pure-Rust-only (this is why the provisioning policy's C-toolchain path is not actually exercised in v1). When the DB does return, it is Diesel over SQLite (relational schema, migrations; `redb` was rejected as schemaless key-value), coordinator-only, holding operational metadata and a debugging record, never task payloads.

## Infrastructure

29. **CI gates through docker multi-host.** The test pyramid runs in-process, subprocess, ssh-to-localhost, and docker-compose multi-host fleets, all in CI.

30. **Tests-first per phase (TDD).** Each phase writes failing tests before implementation.

31. **No CLI crate.** rayonet is a library. The consumer's binary is both coordinator and agent. Diagnostics, if any, are an examples concern.

## Open and deferred

- cfg-gating coordinator-only code out of the agent build (whole-crate refinement; would also let SQLite and other coordinator-only deps stop compiling on hosts).
- Shipping rayonet itself when it is an unpublished `path` dependency of the consumer. The production path is a published rayonet (`rayonet = "0.x"`) so the consumer-only bundle builds remotely. The docker harness sidesteps this for now by shipping the whole workspace tar; a published-crate or path-dependency-copy story is the real fix.
- Capturing closures.
- Path-dependency copy cascade.
- Reconnect-with-backoff for a dropped host (v1 abandons and redistributes).
- Per-agent shared state / setup, local-file availability, in-memory caching, data staging.
- Coordinator-restart resume and any persistence of results / completed task ids.
- SQLite/Diesel operational database, for out-of-process UIs (Dioxus web), post-mortem querying, and persistence. v1 uses in-memory state only.
- Heartbeat and structured Log messages (v1 relies on stderr forwarding).
- Relay subtrees (rayonet itself forwarding tasks coordinator to relay to leaf). v1 is a flat depth-1 star. Note `ProxyJump` is already supported and tested (Phase 4c): ssh routes the coordinator to an otherwise-unreachable leaf through jump hosts, so the star reaches segmented networks without rayonet forwarding anything. Only rayonet-managed relaying is deferred.
- Binary-copy of built artifacts between identical-target hosts (build-on-host optimization).
