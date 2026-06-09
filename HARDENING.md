# Hardening writeup: footgun audit and lint policy

This is the Phase 1 deliverable of the hardening epic described in `HANDOFF.md`. It is research and policy only, no behavior change. It audits the seven footgun areas the handoff names, grounds each in the current code, and records a decision (adopt, decline, or scope) for every clippy restriction lint that was evaluated. The later phases (lint adoption, unwrap-family scoping, lint inheritance, final tidy) execute the decisions written here, one gated commit at a time.

The point of writing it down first: if a later mechanical step goes wrong, this document is the record of what was decided and why, so the work can be resumed or reverted without re-deriving the analysis.

## How the evidence was gathered

A throwaway discovery pass enabled the full restriction family at `deny` in the workspace lint table, then `cargo clippy --workspace --all-features` (without `--all-targets`, so the inline `#[cfg(test)]` modules are excluded) gave the non-test fallout. The table was reverted immediately, so the tree is back at its clean baseline. The counts below are that non-test tally. Test-code fallout is reported separately where it matters, because the unwrap family fires heavily in tests and that drives the scoping decision.

Non-test tally from the discovery pass:

| Lint | Non-test hits |
|---|---|
| `indexing_slicing` | 138 |
| `expect_used` | 29 |
| `let_underscore_must_use` | 21 |
| `as_conversions` | 12 |
| `integer_division_remainder_used` | 8 |
| `allow_attributes` | 8 |
| `allow_attributes_without_reason` | 8 |
| `integer_division` | 4 |
| `string_slice` | 1 |
| `clone_on_ref_ptr` | 1 |

Every other restriction lint that was enabled found zero non-test hits: `panic`, `unreachable`, `unwrap_used`, `unwrap_in_result`, `panic_in_result_fn`, `get_unwrap`, `mem_forget`, `mutex_atomic`, `rc_buffer`, `path_buf_push_overwrite`, `verbose_file_reads`, `lossy_float_literal`, `missing_assert_message`, and `self_named_module_files`. That clean result is itself a finding: the panic, leak, and resource surface of non-test code is already disciplined.

## The gate (unchanged, must stay green at every step)

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo llvm-cov --workspace --all-features --ignore-filename-regex 'src/bin/|fixtures/|harness/|examples/' --fail-under-functions 100 --fail-under-lines 99 -- --include-ignored
```

Line coverage sits at 99.03 percent against a 99 percent floor, so any step that deletes a covered line or adds an uncovered branch can tip it under. Watch the total after every step.

## Footgun audit

### 1. Half-open connections at teardown

What the hazard is: a reader task parked on a read that will never complete (a peer that died without sending EOF, the half-open case the heartbeat introduced) keeps its event-channel sender alive, so any teardown path that waits for all readers to finish, or waits for the event channel to close, hangs forever.

The two declared-lost paths already handle it. The coordinator's `lose` (`rayonet/src/coordinator.rs:584`) aborts the node's reader at `coordinator.rs:592` before requeue and reroute, with a comment that says exactly why. The relay's heartbeat-stale arm (`rayonet/src/relay.rs:706`) aborts a stale child's reader at `relay.rs:729` for the same reason. So a node that goes silent and is declared lost has its reader torn down, not awaited.

The two trailing drains are the ones to scrutinize, and both are safe under one invariant. The coordinator drain drops the loop's own event sender (`coordinator.rs:934`), sends `Shutdown` to every live agent (`coordinator.rs:941`), drains trailing observability with `while let Some(event) = events_rx.recv().await` (`coordinator.rs:946`), then awaits every reader (`coordinator.rs:954`). The `recv` loop ends only when every event sender has dropped, and a sender drops only when its reader finishes, so the real wait is on the readers. The relay mirrors this: drop the child-event sender, `sched.shutdown()`, `flush_uplink`, then await every reader (`relay.rs:746`). A third path, `discard_children` (`relay.rs:427`), also awaits readers but is documented safe because it runs only when the parent left before naming an active set, so the children are still serving and EOF on `Shutdown`.

The invariant that makes the trailing drains safe: by the time a drain runs, every reader is either already aborted (its node was declared lost while half-open) or belongs to a live node that has just been sent `Shutdown` and will reach EOF. Under that invariant the drains cannot hang.

Residual edge, documented not fixed: if a live node goes half-open during the final shutdown window itself, after the run loop has exited and before its reader is awaited, and the heartbeat is off (so nothing declares it lost), the trailing drain can still hang on that one reader. The heartbeat closes this in practice because it would declare the node lost and abort the reader, but a run with `.no_heartbeat()` has no such backstop at the very end.

Decision: document the invariant inline at both trailing drains (a comment, no behavior change) when the surrounding step touches that code. Track a follow-up to bound the trailing drain with a short grace and then abort any still-parked reader, instead of awaiting unconditionally, so the residual edge is closed even with the heartbeat off. The follow-up changes teardown timing, so it is a separate, deliberate step, not part of this cleanup. Do not implement it as a side effect of a lint pass.

### 2. The stdin blocking-thread footgun (run_node versus agent_main)

What the hazard is: an agent reads its parent over `tokio::io::stdin`, which tokio backs with a blocking thread that cannot be cancelled while a read is outstanding. A graceful runtime shutdown blocks on that thread, so the process never closes its stdout, and the parent waiting on that stdout for end-of-stream hangs too. The fix in the codebase is `agent_main` (`rayonet/src/node.rs`), which runs the node and then calls `std::process::exit`, closing stdout at once. Its doc comment explains the rationale in full.

The latent trap: `examples/ssh-run/src/main.rs:90` still uses `run_node(config).await.expect("agent failed")` followed by a plain `return`, rather than `agent_main`. It is fine today because that example never kills an interior relay, so no graceful self-termination of a relay with a live parent ever happens, but it is the exact shape that hung the tree before.

`run_node` cannot be made safe by construction. It has to return a result, because the node and relay tests drive it directly and assert on the outcome. The safety comes from exiting after it returns, which is what `agent_main` adds.

Decision: standardize every agent binary entry point on `agent_main`. `run_node` stays as the lower-level form for tests and library composition, and its doc comment is sharpened from "Most agent binaries should call agent_main" to a plain statement that a binary `main` must call `agent_main`, never `run_node`, with the one-line reason. Update `examples/ssh-run` to call `agent_main` (a small swap that also drops the `.expect`). This lands in a later step, not Phase 1, because it is a code change. The `rayonet-test-agent` binary and the docker harness consumer already use `agent_main`, so only the one example is out of step.

### 3. The monomorphization coverage trap

What it is: a generic helper monomorphizes per stream type, and the monomorphizations that no test exercises show up as uncovered functions that fail the 100 percent function gate. The established workaround is to inline the logic into the one call site the `DuplexStream` test covers, trading a `too_many_lines` allow for coverage. Two sites carry this: the relay splice `relay_with_source` (`rayonet/src/relay.rs:521`, with the comment "the splice is inlined to avoid an uncovered per-stream helper") and the coordinator core `run_job_raw_with_joins` (`rayonet/src/coordinator.rs:788`).

Decision: keep the inlining. When the allow-to-expect conversion runs (Phase 2), turn these into `#[expect(clippy::too_many_lines, reason = "...")]` (and `clippy::too_many_arguments` where present) carrying the coverage reason, so the trade is self-documenting and the build fails if the inlining is ever undone and the allow becomes stale. Do not extract helpers to satisfy the line-count lint without first checking coverage.

### 4. The private-helper expect coverage idiom

What it is: a private helper uses `.expect()` on a path that is logically unreachable, because the panic branch is std or library code that the coverage gate does not count, which keeps the function at 100 percent without a contrived test for the impossible case.

The audit found that this idiom accounts for all 29 non-test `expect_used` hits, and there is nothing else in the non-test panic surface (see item 5). They fall into three families:

- Builder results that cannot fail on valid input: the `graph` and algebra builders in `rayonet/src/graph.rs` (lines 371, 383, 397, 437, 447, 465), for example `.build().expect("the edges are vertex-index pairs within the vertex count")`, where the inputs are constructed to satisfy the builder's contract.
- Invariant lookups for a selected vertex: the chain in `node_detail_lines` (`rayonet/src/tui.rs:609` through `628`), each `.expect("a selected vertex is a labelled node")` and similar, reached only for a node the UI has already established is selectable, with the precondition spelled out in the function's own doc comment.
- Just-configured resources: the pipe takes in `rayonet/src/process.rs:107` to `109` (`.expect("piped stdin")` right after `Stdio::piped()`), the ssh pipe takes in `rayonet/src/ssh.rs:175`, `323`, and `327`, and the blocking-join in `rayonet/src/agent.rs:232` (`.expect("task handler cannot panic")`, where the handler catches panics internally so the join cannot fail).

Decision: see item 5 and the lint table. In short, decline `expect_used` (every hit is a documented, genuinely-unreachable invariant, so the lint would be all false positives and 29 annotations of pure noise), and instead adopt the bans that have zero fallout (`unwrap_used`, `panic`, `unreachable`, and the related result-context lints), which lock in the discipline that the only sanctioned panic surface in non-test code is a documented `expect()`. Do not blanket-rewrite these `expect` calls.

### 5. unwrap, expect, and panic in non-test code

The audit result is cleaner than the handoff's earlier counts suggested. In non-test code there are zero `unwrap`, zero `panic!`, and zero `unreachable!`. The entire non-test panic surface is the 29 `expect()` calls catalogued in item 4, every one a documented invariant. None of them is convertible to a propagated error without contortion, because each guards a case that genuinely cannot occur given the surrounding construction. The earlier "coordinator 3, ssh 3, relay 2, agent 1" figures map onto subsets of these expects as the code stood at the time, and the current exact tally above is what governs.

Decision: no conversions are needed or warranted. Adopt the unwrap, panic, and unreachable bans (zero fallout in non-test, see the table) and keep the 29 expects as the sanctioned idiom. The bans must be scoped to non-test, because the unwrap and expect families fire in roughly 530 test sites. The cleanest scoping is a crate-root attribute, `#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::panic, clippy::unreachable, clippy::unwrap_in_result, clippy::panic_in_result_fn, clippy::get_unwrap, clippy::missing_assert_message))]` in both `rayonet/src/lib.rs` and `rayonet-build/src/lib.rs`, rather than per-test-module allow attributes. The crate attribute leaves the inline `#[cfg(test)]` modules exempt automatically, does not touch the integration tests under `tests/` (separate crates, restriction lints off by default), and does not reach the binaries (also separate crates; `rayonet-test-agent` uses no unwrap or expect today in any case). This keeps `expect_used` out of the set so the documented idiom stays clean.

### 6. as casts

There are 12 non-test `as` casts. The lone one outside the display and geometry code is `idx as TaskId` in the coordinator assign loop (`rayonet/src/coordinator.rs:388`), a widening cast on the task-index hot path. The other 11 are the TUI, graph, and layout math (`rayonet/src/tui.rs` x7, `rayonet/src/layout.rs:86` x2, `rayonet/src/graph.rs:51`, `rayonet/src/telemetry.rs` x2), and most already sit under the existing `cast_precision_loss`, `cast_possible_truncation`, and `cast_sign_loss` carve-outs that the pedantic group enforces.

Decision: decline `as_conversions`. The lossy casts are already gated by the pedantic cast lints at their specific sites, which is where the real correctness check lives. Forcing `try_from` in their place would add a fallible conversion plus an `expect` (or an `unwrap`) at each site, which is panic surface the bans in item 5 are meant to remove, traded for a cast pedantic already vetted. For the genuinely lossy display casts (an `f64` coordinate to a `u16` terminal cell) `From` does not even apply, so the only alternative is `try_from` plus a panic on a value the layout guarantees is in range. The cast is the honest expression of the operation. Record the decline with this reason.

### 7. Indexing and slicing

There are 138 non-test `indexing_slicing` hits, concentrated in the scheduler: roughly 82 in the coordinator and 60 in the relay, indexing per-agent and per-child vectors such as `self.senders[agent]`, `self.last_activity[child]`, and `job.senders[agent]`. These vectors grow in lockstep as the schedule grows (agents are only appended, so existing indices stay valid), so the indices are invariant-checked by construction.

Decision: decline `indexing_slicing`. Converting 138 sites to `.get(...)` would relocate the same logically-impossible panic into an `unwrap` or `expect` with more ceremony, against invariants the code already maintains, for no real safety gain. There is one related single-site lint, `string_slice` at `rayonet/src/provisioning.rs:127` (`content_hash(...)[..16]`), which slices the first 16 bytes of an ASCII hex digest that is always longer than 16 bytes, so it is char-boundary-safe and length-safe by construction. Decline `string_slice` too, on the same reasoning. The `clone_on_ref_ptr` single site is unrelated to indexing and is handled in the table below.

## Clippy restriction lint verdicts

Each verdict carries a one-line reason, in the spirit of the existing `multiple_crate_versions` and `future_not_send` carve-outs. Adopt-with-fallout lints land in the phase noted. Decline verdicts are recorded as reasoned `allow` entries (or simply documented here when there is nothing to silence).

Adopt:

- `allow_attributes` and `allow_attributes_without_reason`: forces every `#[allow]` to become an `#[expect(..., reason = "...")]`, which makes the existing carve-outs self-documenting and fails the build when one goes stale. Fallout is the ten existing allows (`rayonet/src/lib.rs:41` and `46` dead_code, `rayonet/src/layout.rs:82`, `rayonet/src/graph.rs:49`, `rayonet/src/coordinator.rs:788`, `rayonet/src/tui.rs:294`, `1045`, `1053`, `rayonet/src/relay.rs:521`, `rayonet/src/fleet.rs:322`). Phase 2.
- `unwrap_used`, `panic`, `unreachable`, `unwrap_in_result`, `panic_in_result_fn`, `get_unwrap`, `missing_assert_message`: zero non-test fallout, scoped to non-test via the crate-root `cfg_attr` in item 5. Locks in the no-unwrap, no-panic, expect-only discipline. `missing_assert_message` belongs here, not the workspace table, because test code is full of message-less asserts. Adopted in Phase 3 in both `rayonet` and `rayonet-build`.
- `mem_forget`, `mutex_atomic`, `rc_buffer`, `path_buf_push_overwrite`, `verbose_file_reads`, `lossy_float_literal`, `self_named_module_files`: all zero fallout everywhere (test included). Pure guardrails that cost nothing now and catch a future regression. Adopted in Phase 3 in the workspace lint table.

Decline (with reason):

- `expect_used`: every non-test hit is a documented, genuinely-unreachable invariant (item 4), so the lint would be all false positives and 29 annotations of noise. The no-unwrap and no-panic bans already give the safety the family is for.
- `as_conversions`: the 12 casts are display and geometry math already gated by the pedantic cast lints at their sites; replacing them with `try_from` adds fallible conversions and panic surface for casts pedantic has vetted (item 6).
- `indexing_slicing`: 138 invariant-checked scheduler indices; `.get(...)` would relocate impossible panics with more ceremony and no safety gain (item 7).
- `integer_division` and `integer_division_remainder_used`: the divisions are intended integer math (percent rollups, telemetry, layout cell math) in `rayonet/src/capability.rs`, `telemetry.rs`, `tui.rs`, `coordinator.rs`, `relay.rs`, and `fleet.rs`; the lint is situational and would only add noise here.
- `string_slice`: one site (`rayonet/src/provisioning.rs:127`) slicing a fixed-length ASCII hex digest, safe by construction (item 7).
- `let_underscore_must_use`: the 21 sites are all deliberate fire-and-forget (best-effort `send` during teardown, `remove_file` cleanup, `await` on a task that errors once its peer read is cut), for example `rayonet/src/coordinator.rs:607` and `rayonet/src/relay.rs:226`. No bug, and annotating each as intentional would be churn for documentation the `let _ =` already conveys.
- `clone_on_ref_ptr`: declined on attempt. The only non-test hit was `rayonet/src/agent.rs:229`, but the lint also fires on 16 test sites, five of which (`Fleet::observed(.., sink.clone())` and the like) rely on the `Arc<EventRecorder>` to `Arc<dyn EventSink>` unsize coercion that `.clone()` performs at the call. `Arc::clone(&sink)` blocks that coercion, so satisfying the lint there needs a typed intermediate binding plus an `EventSink` trait import in each test, which is less clean than the `.clone()` it replaces. The one non-test site is not worth forcing that workaround across the tests, so the lint stays off.

Already in place, no change: `dbg_macro` (deny), `todo` and `unimplemented` (warn). Consider promoting `todo` and `unimplemented` to deny in the final tidy, since neither should survive in committed code.

## Lint inheritance for the consumer-style members

Only `rayonet` and `rayonet-build` carry `[lints] workspace = true`. The five consumer-style members (`fixtures/consumer`, `harness/docker/consumer`, `examples/montecarlo`, `examples/ssh-run`, `examples/tui-replay`) do not, so the pedantic, nursery, and cargo groups never run there, even though `cargo clippy --workspace -D warnings` still denies their default-level warnings.

Decision: add `[lints] workspace = true` to the five members and clean up whatever the groups surface. The workspace table holds the group lints and the few cheap restriction lints; the non-test unwrap and panic bans live in `rayonet`'s crate-root `cfg_attr`, not the table, so they do not reach example or harness code (which legitimately uses unwrap and expect in demo paths). Inheriting the table therefore gives the examples the valuable pedantic and nursery coverage without imposing the core's stricter panic discipline on demo code. Phase 4. If the fallout in a given member turns out to be large or genuinely inappropriate for demo code, exempt that member explicitly with a written reason rather than silencing individual lints one by one.

## Adoption order

1. Phase 1 (this document). Done.
2. `allow_attributes` and `allow_attributes_without_reason`, converting the eight clippy allows to reasoned expects (carrying the coverage reasons for the two monomorphization sites). The two `dead_code` allows inside the `embed_microcrates!` macro stay as `#[allow]`, because they are expanded only in a consumer crate where the functions may or may not be used, so `#[expect]` could go unfulfilled there. One commit, gate green. (`clone_on_ref_ptr` was attempted here and declined, see the verdicts above.)
3. Done. The unwrap, panic, and unreachable bans (plus `missing_assert_message`) via a crate-root `#![cfg_attr(not(test), deny(...))]` in both `rayonet/src/lib.rs` and `rayonet-build/src/lib.rs`, scoped to non-test, plus the zero-fallout guardrail group in the workspace table. Zero code fallout. A pre-existing flaky control test (`kill_after_current_behind_a_relay_drains_then_loses_the_leaf`) that fails under full-suite parallel load was made deterministic here by gating leaf-a's task on a condvar the test releases only after it observes the `Draining` state, so the after-current kill always lands while the task is in flight.
4. Standardize agent binaries on `agent_main`: swap `examples/ssh-run` and sharpen the `run_node` doc. Add the trailing-drain invariant comments from item 1. Track the bounded-drain follow-up separately.
5. Done. `[lints] workspace = true` on the five consumer-style members, with the group fallout fixed rather than exempted (it was small and the fixes were genuine improvements): three `double` helpers made `const fn`, the montecarlo random-float and pi casts and the tui-replay elapsed-ms cast given reasoned `#[expect(clippy::cast_precision_loss)]`, a montecarlo distance test switched to `mul_add`, the docker share print switched to snapshot-under-lock then print (significant_drop_tightening), the tui-replay `to_input` taking its event by reference and made `const fn`, and a crate doc added to each build script (missing_docs). This step also caught and fixed a nursery `significant_drop_tightening` in the Phase 3 condvar-gated relay test that had slipped through (clippy was not re-run after that test was added); the guard is now dropped explicitly after the wait loop.
6. Done. Final tidy: `examples/ssh-run` now calls `agent_main` instead of `run_node().expect()` (footgun item 2), and the `run_node` doc states plainly that a binary `main` must use `agent_main`. The two trailing-drain sites (coordinator and relay) carry the invariant comment from item 1 explaining why awaiting every reader cannot hang, with the residual heartbeat-off edge and the bounded-drain follow-up noted. `todo` and `unimplemented` promoted from warn to deny (no usages exist). Dead code, missing docs, and naming are already enforced by the existing warn-level lints, so nothing further surfaced. Final full gate green.

## Follow-ups tracked (not part of this epic)

- Bound the trailing drain in the coordinator and relay teardown so a node going half-open during the final shutdown window with the heartbeat off cannot hang it (abort any reader still parked after a short grace). Noted at both sites (footgun item 1).

Enable and fix each lint in the same commit, never enable in one and fix in another, so the gate is green at every commit.
