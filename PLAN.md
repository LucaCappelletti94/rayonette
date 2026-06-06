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
- Proptest invariants over random task counts and latencies: every task completes exactly once, output order equals input order, no task is lost or duplicated in the assembled result.
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

## Phase 3 - The `.net_map` API and build-time extraction

**Goal:** the public surface and the codegen that makes it work, validated without shipping anything.

**Deliverables:** `rayonet_build::extract()` (parse the consumer crate via `cargo metadata`/syn, bundle whole-crate source + Cargo.lock, build the `type_name` registry, write the `OUT_DIR` blob, emit rerun-if-changed); `embed_microcrates!()` (decision 11); the `.net_map` lazy job builder with `fn(T)->U` typing and `type_name` keying (decisions 5, 12); rayon composition (decision 6).

**Tests (fixture consumer crate + in-process/subprocess)**
- `extract()` on a fixture crate produces a blob containing the crate source and a copied lockfile; the generated registry maps `type_name::<evolve>()` to glue that deserializes, calls, and re-serializes.
- A capturing closure fails to compile against `.net_map` (a `compile_fail` trybuild test), confirming decision 8.
- `.net_map(evolve)` end to end (over the in-process or subprocess transport) returns an ordered `Vec` equal to a local `map`.
- `.net_map` composes inside a rayon chain: `into_par_iter().map(..).net_map(..).map(..).reduce(..)` produces the correct reduction.
- A fixture crate with a non-rayonet local `path = ".."` dependency makes `extract()` error with a message naming the offending crate (decision 15).

**Done when:** a user can write `.net_map(f)` plus the one-line build.rs, and the produced bundle compiles and runs locally with correct ordered results and clear errors on the unsupported cases.

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

## Phase 7 - Harden, document, flagship example

**Goal:** ship-ready v1.

**Deliverables:** a flagship map-reduce example (Monte Carlo estimation of pi: each task draws millions of random samples, the reduce sums the hits), which exercises the `.net_map` surface and the rayon composition end to end; user docs (the one-line build.rs, the `.net_map` surface, the idempotency contract); CI green across all four pyramid levels; clippy/fmt gates enforced.

**Tests**
- The example runs across a fleet and produces a value matching a local single-machine baseline (the same chunks summed without distribution).
- Doc examples compile and run (`cargo test --doc`).
- A soak test: a long multi-host run with periodic induced host kills finishes complete and correct.

**Done when:** the Monte Carlo example runs on rayonet end to end, docs match behavior, and the full pyramid is green in CI.

---

## Cross-phase success criteria

- Every phase is TDD: failing tests first, shown failing, then implemented to green.
- No phase is "done" with a red suite, a clippy warning, or unformatted code.
- Deferred items (`DECISIONS.md` "Open and deferred") are out of scope for v1 phases and must not be smuggled in mid-phase; note them and move on.

---

## Relay tree (v2): phases R1-R7

Builds the relay tree of `DECISIONS.md` 32-43. The cross-phase criteria above still hold. v1 (Phases 0-7) is the degenerate depth-1 case and stays working throughout, and each phase below keeps the flat-star path green.

### R1 - Capability profiles and the role filter (still flat)

**Goal:** the coordinator picks workers by capability, no tree yet.

**Deliverables:**
- A `NodeProfile` with a stable core (OS, cores, RAM) plus extensible capability lists, notably `gpus: Vec<Gpu>` carrying vendor + runtime (`Cuda` / `Rocm` / `Metal`) + model + VRAM. Probed cross-platform (`uname`, core count, total RAM, GPU via `nvidia-smi` / `rocminfo` / `system_profiler`).
- A first-class composable `Predicate` (an `Arc`-boxed `Fn(&NodeProfile) -> bool` with `and` / `or` / `negate` and constructors `cuda()`, `rocm()`, `metal()`, `os_is`, `ram_at_least_gb`, `vram_at_least_gb`, `cores_at_least`).
- A `Role` enum (`Compute` / `RelayOnly` / `Excluded`) and a `Filter` rule-builder over the predicates (`Filter::new().relay_only(..).compute(..).otherwise(Role)`, first-match-wins, default `Excluded`), applied to a fleet with `Fleet::with_filter`.
- A per-run `net_map(task).requires(predicate)` that narrows a run to the compute hosts whose profile satisfies the predicate, sharing the same vocabulary as the fleet filter.
- The `Launch` trait split into connect / probe / activate, so a host is probed and filtered before the expensive provisioning step.
- An `Event::Profiled { host, profile, role }`, surfaced by the plain renderer and the TUI alongside each host's state.
- `Excluded` (and `RelayOnly`) hosts get no tasks in the flat star; a job requirement no host meets fails with a no-eligible-host error.

**Tests**
- Profile parsing for linux and macOS fixture command outputs.
- `with_filter` assigns roles, a non-`Compute` host receives no work, and a compute-only fleet still completes.
- A capability predicate (RAM/GPU threshold) keeps and drops the expected hosts, both as a fleet `Filter` and as a per-run `requires`.

**Done when:** a run across named hosts keeps or drops each by a capability predicate, still over the flat star.

### R2 - Single relay level (depth-2)

**Goal:** one tier of relays, decentralized children, central DAG.

**Deliverables (delivered):**
- The relay role (`relay.rs`): a node is an agent to its parent and a coordinator to its own children, splicing work down and `Started`/`Completed`/`Failed` straight up (task ids pass through), with a child's loss requeued onto its surviving siblings.
- Per-agent capacity in the protocol: `Ready { slots }` plus a capacity-filled coordinator scheduler, so a relay keeps its whole subtree fed (demand-pull at the relay boundary).
- The children file (`~/.config/rayonet/children`, or `$RAYONET_CHILDREN`) and boot-time role dispatch (`node::run_node`): a node with no children serves as a leaf, one with children runs the relay.
- The provisioning cascade: a relay re-ships the `__rayonet_source()` bundle it was built from to its children and builds them with `Ssh::build`.
- `RelayOnly` honored: a relay forwards, it runs no tasks of its own.
- No redundancy yet, so a relay whose subtree dies fails it (until R5).

**Scoped out of R2 (deferred, agreed with the design):**
- The coordinator-side `geometric-traits` CSR DAG and the rich discovery-reporting pass. In a depth-2 tree the coordinator sees each relay as one opaque agent, so the graph has nothing to validate, order, or reroute. The CSR DAG and Kahn ordering land in R3 (arbitrary depth), the tree view in R4, redundancy in R5.
- A node that both relays and computes (decision 35). R2 relays are pure forwarders; relay+compute will be modeled later as a relay whose child fleet includes a local in-process worker.
- A dynamic `Demand` message for capacity changing mid-run; advertised initial capacity suffices for a fixed child set (the dynamic case is the elastic-membership work).

**Tests**
- In-process recursive transport: coordinator -> one relay -> two leaves returns correct ordered results.
- A relay runs zero tasks itself, and its leaves run them all.
- Demand-pull keeps a relay's subtree busy (more than one task in flight beneath it).
- A lost child has its work absorbed by a sibling; a relay whose whole subtree dies fails its subtree.

**Done when:** a job runs coordinator -> relay -> leaves with the relay's children defined only on the relay.

### R3 - Arbitrary depth

**Goal:** recursion to any depth.

**Deliverables (delivered by R2's design, confirmed here):**
- A relay's child may itself be a relay. This falls out of R2 with no new code: a relay launches each child with `Ssh::build` and the child boots through `node::run_node`, which reads its own children file and becomes a relay if it has one. Capacity (the `slots` sum) and results pass through each hop transparently, and a relay re-ships the `__rayonet_source()` it was built from, so the cascade recurses to any depth.
- Provisioning order is respected for free: the cascade is recursively decentralized, so a node is built and running before it builds its own children. There is no central build DAG to order.
- Nothing in R2 assumes depth 2 (the relay and node code are depth-agnostic).

**Tests**
- An in-process three-level tree (coordinator -> relay -> sub-relay -> two leaves) returns correct ordered results.
- A mid-tree relay (the sub-relay has no registry of its own) forwards through to the deeper leaves, which do all the compute.

**Scoped out (deferred):**
- The coordinator-side `geometric-traits` CSR DAG and `Kahn` ordering. With the decentralized cascade there is nothing central to order for a run; the CSR graph is a coordinator-side view/policy structure, so it lands with the tree view (R4) and redundancy (R5) that actually consume it.

**Done when:** depth is unbounded and proven at three levels in-process. (Met: the depth-three relay test passes against the R2 implementation unchanged.)

### R4 - Observability and the TUI tree

**Goal:** the coordinator's view and the TUI show the whole graph.

**Deliverables (delivered):**
- A `FromAgent::Observe(Event)` uplink: a relay reports each child's `Profiled` and `Node` lifecycle up, and a grandchild's event is prefixed by the child's label at each hop, so node ids are paths from the root (`relay/leaf`) and the parent is the path prefix. No node-id handshake.
- `RunState` keyed by path id is a tree: `parent_of` / `depth` / `leaf_of` free functions and `RunState::roots` / `children_of` read the structure off the ids. The coordinator drains trailing subtree observability on teardown so the final view is complete.
- The TUI and the plain renderer indent each node by its depth, showing the leaf label, role, and live state.

**Scoped out (deferred):**
- Per-task attribution to the exact deep leaf (the flat-star tally stays at the coordinator's direct children); a later refinement.
- The `geometric-traits` CSR graph and roots/sinks layout: the path-id tree renders directly, and the CSR graph lands in R5 where redundancy/articulation analysis consume it.

**Tests**
- A scripted relay-like agent's `Observe` is re-emitted at the coordinator with a prefixed host; an in-process coordinator -> relay -> leaves run leaves the deep leaves in `RunState` with role and a terminal state.
- Path ids form a tree (`parent_of` / `roots` / `children_of`).
- A structure test of the rendered tree (plain and TUI), indented by depth with roles and states.

**Done when:** a multi-level run shows its full topology and live state in the TUI. (Met: the in-process tree tests, plus a real coordinator -> relay -> leaf run printing the indented tree.)

### R5 - Redundant paths and reroute

**Goal:** the strong fault guarantee back, via alternate paths.

**Deliverables:**
- Multi-parent topology (a node listed under two relays) with identity-based dedup.
- Primary and standby path selection by the per-run metric (widest-path bandwidth by default, or shortest-path latency), with the bandwidth probe run during discovery only when that metric is selected.
- Articulation-point / bridge analysis flagging single points of failure, and an optional filter that requires redundancy for compute.
- On a relay's death, reroute its orphaned-but-reachable subtree onto standbys (`ConnectedComponents` + path recompute), exactly-once preserved by dedup.
- Tasks stranded behind a dead articulation relay fail clearly.

**Tests**
- Proptest over random DAGs: every task completes exactly once, in input order, despite an induced relay kill, as long as a path survives.
- A stranded subtree (articulation relay killed) reports its tasks failed, never lost or duplicated.
- Dedup: a node reached by two paths is provisioned and assigned exactly once.

**Done when:** any node a survivor can still reach keeps working through any single relay's death.

### R6 - Elastic membership (join mid-run)

**Goal:** a long run absorbs machines that come online after it started, the mirror of R5's leave path.

**Deliverables:**
- Re-entrant discovery: the coordinator retries unreachable candidates and re-reads children files on a backoff, not only once up front.
- A newly-answering node is probed, role-assigned, provisioned (cache hit if previously built), spliced into the live DAG (CSR rebuilt), and begins pulling pending tasks.
- Speculative re-execution, opt-in per run and off by default: when no tasks are pending but some straggle, an idle or fresh node may race a straggler, first result wins via dedup.
- The TUI shows nodes appearing live.

**Tests**
- In-process: a node unavailable at start, made available mid-run, picks up pending tasks and the run completes correctly.
- A join neither duplicates nor loses results (dedup holds).
- Speculative re-run: a straggler raced by a faster node yields exactly one recorded result.

**Done when:** a run started with N nodes finishes having used N+k after k machines joined mid-run, with correct dedup.

### R7 - Real-world validation

**Goal:** prove it on segmented, multi-level, redundant, elastic hardware.

**Deliverables:**
- A docker (and a real-host) scenario: coordinator -> a `RelayOnly` gateway -> compute leaves on the gateway's private network defined only on the gateway, with a redundant second gateway path.
- The TUI showing the live DAG.
- An induced gateway kill mid-run that reroutes, and a machine joining mid-run that is picked up, both completing with correct deduplicated results.

**Tests**
- The docker segmented-network scenario runs in CI and asserts correct results through a gateway kill and a mid-run join.
- A documented real run: coordinator -> Mac (`RelayOnly`) -> Mac-LAN leaves (`Compute`), with a node brought online mid-run.

**Done when:** a segmented, multi-level, redundant, elastic fleet completes a job correctly through a relay failure and a mid-run join, end to end.
