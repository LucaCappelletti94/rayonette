# Implementation Plan

This turns `DECISIONS.md` into phased, test-gated milestones. Each phase is built tests-first (decision 30): the listed tests are written and made to fail before any implementation, and the phase is "done" only when its whole suite is green (and `cargo fmt --all -- --check` and `cargo clippy` pass).

## The test pyramid

Four fidelity levels, cheapest first. Most logic is proven at level 1, where tests are deterministic and microsecond-fast. Real ssh, on-host builds, and docker appear only once the logic beneath them is already proven.

1. **In-process** - coordinator and agent in one process, wired by `tokio::io::duplex()` pipes through the same framing the real transport uses. A fault-injecting wrapper can drop a channel at a chosen point. Deterministic, no ssh, no build.
2. **Subprocess** - the real built binary, spawned locally, talking over its actual stdin/stdout. Proves framing, process lifecycle, and agent-mode detection. No network.
3. **ssh-to-localhost** - the real `openssh` path against `localhost`. Gated behind a feature/env flag because some CI images lack `sshd`.
4. **docker multi-host** - a docker-compose fleet of containers (some without rust) for provisioning, multi-host work-stealing, and fault injection (killing a container).

CI runs all four (decision 29).

---

## Phase 0 - Scaffolding and wire format

**Goal:** the workspace, CI, and the encoding/framing every later test depends on.

**Deliverables:** `rayonet` + `rayonet-build` workspace; CI skeleton; the `ToAgent`/`FromAgent` message enums (decision 23); postcard encoding + `tokio_util` length-delimited framing (decision 22); the in-process duplex transport and its fault-injecting wrapper.

**Tests**
- Unit/proptest: every message variant round-trips through postcard (`encode` then `decode` is identity), including edge payloads (empty, large, nested, non-string map keys, byte vectors).
- Framing: a decoder fed the encoded bytes split at *every* possible boundary (one byte at a time, multiple frames per read, a frame spanning two reads) yields exactly the original frame sequence; an oversized length prefix is rejected, not OOM.
- The in-process transport carries frames both directions; the fault-injecting wrapper closes the channel at a set byte offset and the read side observes a clean EOF/error.

**Done when:** messages and framing are proven robust against arbitrary stream chunking, and the in-process transport (the substrate for all later level-1 tests) works including induced drops.

---

## Phase 1 - Core protocol logic, over the in-process transport

**Goal:** prove the entire coordinator/agent logic with zero ssh and zero build.

**Deliverables:** agent loop (`Hello`->`Ready`, `Assign`->`Completed`/`Failed`, `Shutdown`) dispatching via a `fn_key` registry; coordinator (handshake, pull scheduling per decision 24, ordered result assembly, dedup by `task_id`).

**Tests (in-process)**
- Handshake then a single task round-trips end to end.
- N tasks return in input order via `collect`, regardless of completion order.
- A worker returning a mix of `Ok` and `Err` yields the right per-task `Completed`/`Failed`, and `Failed` is terminal (never re-sent).
- Capacity N: the coordinator keeps exactly N tasks in flight on an agent, never N+1, never idling a free slot while work is pending.
- Two agents with scripted fast/slow latencies: the fast one provably drains more tasks (work-stealing falls out), and every task still completes.
- Proptest invariants over random task counts/latencies/capacities: every task completes exactly once, output order equals input order, no task is lost or duplicated in the assembled result.
- Clean `Shutdown` drains in-flight work and closes without dropping a completed result.

**Done when:** the scheduling/assembly/dispatch core is proven correct and invariant under property testing, entirely in-process.

---

## Phase 2 - Real-process transport

**Goal:** the same logic over a real OS process boundary.

**Deliverables:** `is_agent()` detection (decision 4); frames over a spawned binary's real stdin/stdout; stderr forwarding (decision 21).

**Tests (subprocess)**
- A task round-trips through the actual built binary launched as a subprocess.
- `is_agent()` correctly selects the agent role from the marker; the coordinator role is taken otherwise.
- Agent stderr (including a deliberate panic and an arbitrary `eprintln!`) is forwarded verbatim to the coordinator and surfaced.
- Killing the subprocess mid-task is observed by the coordinator as a transport drop (sets up Phase 6).

**Done when:** the protocol works unchanged across a real process boundary and remote stderr is visible locally.

---

## Phase 3 - The `.netmap` API and build-time extraction

**Goal:** the public surface and the codegen that makes it work, validated without shipping anything.

**Deliverables:** `rayonet_build::extract()` (parse the consumer crate via `cargo metadata`/syn, bundle whole-crate source + Cargo.lock, build the `type_name` registry, write the `OUT_DIR` blob, emit rerun-if-changed); `embed_microcrates!()` (decision 11); the `.netmap` lazy job builder with `fn(T)->U` typing and `type_name` keying (decisions 5, 12); rayon composition (decision 6).

**Tests (fixture consumer crate + in-process/subprocess)**
- `extract()` on a fixture crate produces a blob containing the crate source and a copied lockfile; the generated registry maps `type_name::<evolve>()` to glue that deserializes, calls, and re-serializes.
- A capturing closure fails to compile against `.netmap` (a `compile_fail` trybuild test), confirming decision 8.
- `.netmap(evolve)` end to end (over the in-process or subprocess transport) returns an ordered `Vec` equal to a local `map`.
- `.netmap` composes inside a rayon chain: `into_par_iter().map(..).netmap(..).map(..).reduce(..)` produces the correct reduction.
- A fixture crate with a non-rayonet local `path = ".."` dependency makes `extract()` error with a message naming the offending crate (decision 15).

**Done when:** a user can write `.netmap(f)` plus the one-line build.rs, and the produced bundle compiles and runs locally with correct ordered results and clear errors on the unsupported cases.

---

## Phase 4 - ssh transport and the provisioning ladder

**Goal:** get a cold host from ssh-only to a running agent.

**Deliverables:** launch over ssh via `openssh` (decision 21); `uname` probe; rustup user-local install when rust is missing (decision 18); ship the source blob; `cargo build` on the host with a content-addressed cache; launch the agent.

**Tests (ssh-localhost + docker)**
- ssh-localhost: a one-host job runs end to end through the real `openssh` path.
- docker, bare image with no rust: the host is probed, rust is installed user-locally, source shipped, built, and the job completes.
- docker: a second run on the same host hits the content-addressed cache and skips the rebuild.
- docker: a host that cannot satisfy a requirement (for example no network to fetch the toolchain) fails clearly for that host with its tasks left pending, per decision 18.
- The provisioning steps emit the expected node-state transitions (Probing -> Installing -> Syncing -> Building -> Ready), asserted on the event stream.

**Done when:** a fresh machine reachable only by ssh is provisioned and runs tasks, the cache makes re-runs cheap, and failures are legible per-host.

---

## Phase 5 - Multi-host, observability, and the TUI

**Goal:** real fleets and the live view.

**Deliverables:** multiple concurrent agents (work-stealing from Phase 1); the in-memory run-state model (decision 27); the lossy event broadcast (decision 19); the terminal TUI and the plain-progress renderer.

**Tests (docker multi-host + in-process)**
- docker: a fleet with fast and slow hosts completes every task, and the fast hosts provably take a larger share.
- The event broadcast is non-blocking: a deliberately stalled observer drops/coalesces events and never backpressures the run (assert task throughput is unaffected with a frozen consumer).
- Golden/snapshot test: feeding a scripted event sequence to the TUI renderer produces the expected frame (node rows, states, counts).
- The plain-progress renderer produces the expected line sequence for the same event script (headless-friendly).

**Done when:** multi-host runs are correct and observable through at least two renderers, and observers provably cannot slow the compute.

---

## Phase 6 - Fault tolerance

**Goal:** survive losing any machine.

**Deliverables:** drop detection -> mark dead -> requeue in-flight `task_id`s -> redistribute to survivors; dedup by `task_id` (decision 25).

**Tests (in-process fault-injection + docker)**
- In-process: a channel dropped mid-task requeues that task and it completes exactly once at the sink (the idempotency/dedup contract holds even though it may have run twice).
- docker: killing a container mid-run leaves the survivors to finish every task, with a complete, correct result set.
- A `Failed` result is never requeued (terminal), only agent death is.
- Proptest: random kill timing across the run still yields every task completed exactly once in the assembled output.

**Done when:** any single host can vanish at any time and the run still produces the complete, deduplicated, correctly ordered result.

---

## Phase 7 - Harden, document, port the first consumer

**Goal:** ship-ready v1.

**Deliverables:** the SMARTS-classifier batch ported onto `.netmap`; user docs (the one-line build.rs, the `.netmap` surface, the idempotency contract); CI green across all four pyramid levels; clippy/fmt gates enforced.

**Tests**
- The ported consumer runs a real batch across a docker fleet and produces results matching a local rayon baseline.
- Doc examples compile and run (`cargo test --doc`).
- A soak test: a long multi-host run with periodic induced host kills finishes complete and correct.

**Done when:** the motivating workload runs on rayonet end to end, docs match behavior, and the full pyramid is green in CI.

---

## Cross-phase success criteria

- Every phase is TDD: failing tests first, shown failing, then implemented to green.
- No phase is "done" with a red suite, a clippy warning, or unformatted code.
- Deferred items (`DECISIONS.md` "Open and deferred") are out of scope for v1 phases and must not be smuggled in mid-phase; note them and move on.
