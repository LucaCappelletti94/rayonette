# Handoff: API-review epic, item 2 (task registration)

## Where the project is

The crate is `rayonette` (renamed from `rayonet`, which is a company name; the GitHub repo is also `rayonette`). It distributes task-parallel work across machines over SSH, the way rayon does across cores. Branch `main` is current; everything below is merged into it. CI is green on `main`.

The API-review epic (recorded in `API-REVIEW.md`) is on its last item. Five of six items are merged (items 1, 3, 4, 5 plus the earlier whole-crate and hardening work). Item 2 (task registration) is the only one left, and it is now fully planned and ready to implement.

## Status of item 2: research done, design chosen, plan approved

This session finished the work that the previous handoff left open. In order:

1. The web research on the state of Rust type analysis is complete and written into `TASK-REGISTRATION-DESIGN.md` (the "State of type analysis" and "State of the art" sections, with sources). Conclusion: no way to get types at metaprogramming time that we can ship (proc-macro/build.rs run pre-inference; rust-analyzer and rustc-as-a-library are unstable and unembeddable; reflection MVP is years from stable).

2. The decision is made. We are building **Level 3**: replace the build-time source scanner with a `#[rayonette::tasks]` proc-macro attribute plus `inventory`-based registration. It delivers transparent annotated closures, turns every unsupported case into a compile error at the user's call site, and removes the `type_name` wire-key dependency. The target shape and the 10 ergonomics targets are in `TASK-REGISTRATION-DESIGN.md`.

3. A full multi-phase, test-driven implementation plan is written and APPROVED, at:
   `~/.claude/plans/let-s-start-preparing-a-shimmering-sunset.md`

## Next action: begin Phase 1

Start Phase 1 on a branch, then run the full gate before reporting at the phase checkpoint. The phases (all gate-green at each end) are:

- Phase 1: library plumbing, backward compatible. `inventory` + `TaskEntry`, `Registry::add` / `Registry::from_inventory`, `NetMap` explicit-key field + `net_map_task`, `register_task!`. Scanner untouched.
- Phase 2: new `rayonette-macros-core` (instrumented logic) + thin `rayonette-macros` proc-macro shell (excluded from coverage), Tier A (named fns + annotated closures). trybuild dev-dep, UI tests, and the end-to-end in-process closure test. The coverage `--ignore-filename-regex` edit for `rayonette-macros/src/` lands atomically here.
- Phase 3 (deferrable): heuristic Tiers B and C, plus generics and turbofish.
- Phase 4a: migrate the four consumers to `#[rayonette::tasks]` + `Registry::from_inventory()`, scanner still alive.
- Phase 4b: delete the scanner and the `embed_microcrates!` registry half. The riskiest edit is rewriting the scanner-coupled tests in `rayonette-build`.
- Phase 5: loud backstop error, docs, prelude exports, coverage top-up.

Two standing constraints baked into every phase: never land an uncalled `pub fn` (no consumer counts toward the 100-percent-functions gate, all excluded by the coverage regex), and make every deletion atomic with its tests.

Key design facts to keep in mind: rayonette is SPMD (same source recompiled both sides), the coordinator never calls the task (only derives the key and pins types via the `Fn(Self::Item) -> O` bound, so a wrong input-type guess is a compile error at the call site, never a runtime mis-decode), and the macro must emit the SAME key literal in both the rewritten call and the `register_task!`.

## The gate (must stay green at every phase)

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo llvm-cov --workspace --all-features --ignore-filename-regex 'src/bin/|fixtures/|harness/|examples/' --fail-under-functions 100 --fail-under-lines 99 -- --include-ignored
```

Coverage is 100 percent functions and about 99.12 percent lines (floor 99, razor-thin, watch it). The coverage command needs an ssh-localhost self-login key at `~/.ssh/rayonette_localhost_ed25519` (created in a prior session, generated not copied). CI sets its own up. CI also has a `docker` job (level-4 ssh scenarios plus the montecarlo flagship); the relay-tree bestiarium is intentionally NOT gated in CI. From Phase 2 on, the local gate command gains `|rayonette-macros/src/` in the ignore regex, matching the CI edit.

## Process rules (the user is strict on these)

- PR bodies and any PR/issue post: SHORT, 2-3 paragraphs, prose only, no lists/headings. Plain ASCII only (no em-dashes, en-dashes, semicolons, emoji). See the `pr-posts-short-no-lists` memory.
- Never post comments/reviews on PR or issue threads. Creating a PR the user asked for and editing its body are allowed; thread content is not. Use `gh api -X PATCH .../pulls/N` to edit a PR body (the GraphQL path errors on this repo).
- No `Co-Authored-By` lines. No DECISIONS.md decision-number citations in code/docs.
- Commit and push only when asked. Force-push is gated by the permission classifier; avoid it (use a follow-up commit instead).
- When committing, `git add` each path explicitly. A `git add rayonette/src/` once missed `rayonette/tests/`, which would have broken CI.
- Run the FULL four-command gate after every edit before declaring a step done.
- Work step by step: after each gated phase, report and wait for confirmation.

## Memory pointers

See `~/.claude/projects/.../memory/`: `crate-renamed-to-rayonette`, `rayonette-api-review-epic`, `rayonette-hardening-epic`, `pr-posts-short-no-lists`, `prefer-cleanest-design`, `prefer-methods-over-pub-fields`, `no-decision-number-citations`. The `rayonette-api-review-epic` memory should be updated when item 2 lands.

## Untracked

`TASK-REGISTRATION-DESIGN.md` and this `HANDOFF.md` are untracked. Commit both alongside the item-2 work when implementation begins (the approved plan says so). The plan file itself lives under `~/.claude/plans/` and is not part of the repo.
