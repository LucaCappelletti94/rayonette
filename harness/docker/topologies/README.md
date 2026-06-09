# Relay-tree topology bestiarium

Docker scenarios that stress the relay tree on real, segmented networks. The
topology is enforced by docker networks (a relay bridges its parent-side and
child-side networks, so killing it genuinely partitions) plus a children file
written onto each relay. The harness consumer is relay-capable (it runs
`node::run_node`, so a children file makes it a relay). The R5 scenarios kill a
node mid-run (the fleet shrinks and recovers); the R6 `elastic` scenario brings a
node online mid-run (the fleet grows and absorbs it).

## Topologies

| topology            | shape                                   | experiment                                                              |
|---------------------|-----------------------------------------|------------------------------------------------------------------------|
| `line2`             | coordinator -> relay -> two leaves      | kill the sole relay: subtree stranded, run fails legibly               |
| `line3`             | coordinator -> relay1 -> relay2 -> leaf | depth-3 cascade completes, kill the interior relay strands the leaf    |
| `diamond`           | leaf reachable via relayA and relayB    | dedup to one primary, kill the primary bridge, standby finishes        |
| `articulation`      | diamond plus a solo leaf under relayA   | kill relayA: solo's work reroutes onto the surviving compute           |
| `require_redundancy`| line2 (no redundancy) and diamond       | refuse a non-redundant topology, admit a redundant one                 |
| `elastic`           | coordinator -> two leaves               | start with one leaf, bring the second up mid-run, the rejoin driver absorbs it |
| `relay-grow`        | coordinator -> relay -> two leaves      | add a leaf to the relay's children file mid-run, the relay re-reads and absorbs it |
| `capstone`          | two gateways -> shared private leaf + a leaf joining mid-run | segmented, multi-level, redundant, and elastic at once: a node joins the standby gateway mid-run, then the primary gateway is killed and reroutes, all deduped |
| `metropolis`        | two gateways (each with a leaf) -> redundant shared, plus a third compute node joining mid-run | a richer, growing network: watch it scale up as gw3 joins the fleet, then the primary gateway is killed and shared reroutes. Demo topology, run with a high task count so the growth and kill land mid-run; not part of `run-all` (its timing needs the longer run) |

## Usage

```sh
../up.sh                 # build the harness images (once)
./run-all.sh             # warm the cache, then run every topology
./diamond/run.sh         # or run one topology
KEEP=1 ./diamond/run.sh  # keep the containers up afterward (fast re-runs)

# For a faster pass, run the kill/join scenarios with the wall-clock `dawdle`
# task at a modest count, so an event lands mid-run in a couple of seconds
# instead of the heavy CPU-bound `crunch` used by default:
RAYONET_HEAVY_TASK=dawdle RAYONET_HEAVY_COUNT=300 ./run-all.sh
```

## How it stays fast

Every container mounts a shared, architecture-keyed cache volume
(`rayonet-topo-cache`). `topo_warm` builds the agent once, sequentially, into
that volume; afterward every node provisions by cache hit instead of
recompiling. This is safe because all the containers share the host CPU, so the
`target-cpu=native` binary is valid on all of them, which is exactly what the
arch-keyed cache guarantees (a different microarchitecture would get a different
cache key).

A note on what the per-host completed counts show: the coordinator attributes a
finished task to the direct agent it scheduled to (the relay), so dedup and
reroute show in the relays' `share` lines, not the deep leaf paths.

## Real-host run (R7)

The bestiarium runs over real openssh and real cargo provisioning inside docker;
the same code was also exercised across real, physically separate machines over
Tailscale ssh, using the `ssh-run` example (`examples/ssh-run`).

Flat, cross-architecture (this Linux box -> a macOS arm64 host as a compute leaf):

```sh
RAYONET_HOSTS="mac" cargo run -p ssh-run --release
# mac: Compute (MacOs, 18 cores, 24576 MB RAM, 1 GPUs)
# mac: Probing -> Syncing -> Building -> Ready -> Working -> Done
# results: [Ok(0), Ok(2), ...]  8/8 tasks succeeded
```

A real multi-level relay tree (coordinator -> macOS gateway -> Linux leaf), each
hop reaching the next with its own credentials (decentralized discovery): set the
gateway's children file on the gateway itself, then drive through it.

```sh
ssh mac 'mkdir -p ~/.config/rayonet && printf "pippo\n" > ~/.config/rayonet/children'
RAYONET_HOSTS="mac" cargo run -p ssh-run --release
ssh mac 'rm -f ~/.config/rayonet/children'   # always remove it afterward
# mac: Compute (MacOs, ...) -> Probing -> Ready -> Working          (the gateway)
#   pippo: Compute (Linux, 64 cores, ...) -> Probing -> Syncing -> Building -> Ready
#   pippo: Working ... Done                                          (the leaf the gateway built)
# 8/8 tasks succeeded
```

The gateway here is relay-only by construction (a relay forwards work, it never
runs tasks). Redundant reroute and mid-run elastic membership are proven over
real openssh by the `diamond`, `elastic`, `relay-grow`, and `capstone` docker
scenarios above; the same children-file edit shown here, applied mid-run, is what
a real gateway re-reads to absorb a node. Always delete a real children file when
done, or later agent runs on that host will try to relay.

## Watching and refining the TUI

The consumer records its full event stream when `RAYONET_EVENT_LOG` is set, and
`topo_drive` forwards it, so any scenario can be captured or watched. The
`tui-replay` example (`examples/tui-replay`) renders a recording through the same
interactive `rayonet::tui` dashboard the live run uses: the relay tree drawn as a
node-link graph (nodes coloured by state, active versus standby links, single
points of failure flagged), a progress header, a per-node table, an event log,
and an info panel. Tab or the arrow keys (and the mouse) select a node to see its
detail, including live CPU, memory, and GPU use that agents self-report; hovering
a link shows its latency and whether it is the primary or a standby. Esc clears,
`p` pauses, `q` quits.

Watch a finished trace (a committed capstone recording lives at
`rayonet/tests/fixtures/capstone.jsonl`), paced by its own timestamps at 4x:

```sh
cargo run -p tui-replay -- rayonet/tests/fixtures/capstone.jsonl 4
```

The capstone trace is recorded with the build cache warm, so every node provisions
by cache hit and jumps straight to Ready. To watch the full provisioning ladder
(probe, install the toolchain, ship the source, compile the agent), replay the
cold recording, where the relay was built from scratch:

```sh
cargo run -p tui-replay -- rayonet/tests/fixtures/cold-provisioning.jsonl 1
```

To watch a richer network grow, replay the metropolis recording: two gateways
front a redundant shared leaf and a leaf each, a third compute node joins the
fleet mid-run, and then the primary gateway is killed so the shared leaf
reroutes. Re-record it with `RAYONET_EVENT_LOG=/tmp/m.jsonl RAYONET_HEAVY_COUNT=600 ./metropolis/run.sh`.

```sh
cargo run -p tui-replay -- rayonet/tests/fixtures/metropolis.jsonl 4
```

Watch a run live: set the log, run the scenario, and follow the log from another
terminal (it renders events as they are written):

```sh
RAYONET_EVENT_LOG=/tmp/run.jsonl KEEP=1 ./capstone/run.sh    # one terminal
cargo run -p tui-replay -- --follow /tmp/run.jsonl           # another terminal
```

Steer a run live: also set a control socket, then attach the viewer with
`--control`. Select a node (Tab / click) and press `p` to pause or resume a
compute leaf, `k` to kill a node now, or `d` to kill it after its current tasks
drain (the buttons for these sit at the bottom of the node detail panel). The
coordinator runs on the host, so the socket is a host path:

```sh
RAYONET_EVENT_LOG=/tmp/run.jsonl RAYONET_CONTROL_SOCKET=/tmp/run.sock \
  RAYONET_HEAVY_COUNT=1400 KEEP=1 ./metropolis/run.sh                  # one terminal
cargo run -p tui-replay -- --follow /tmp/run.jsonl --control /tmp/run.sock  # another
```

Refine the TUI against a real run: edit `rayonet/src/tui.rs`, run the snapshot
test, read the text diff of how the captured capstone now renders at 25 / 50 / 75
/ 100% of the run, and re-bless the golden when the change is intended:

```sh
cargo test -p rayonet --features tui --test tui_snapshot      # diff against the golden
RAYONET_TUI_BLESS=1 cargo test -p rayonet --features tui --test tui_snapshot  # accept changes
```
