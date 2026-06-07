# Relay-tree topology bestiarium

Docker scenarios that stress the R5 relay tree on real, segmented networks. The
topology is enforced by docker networks (a relay bridges its parent-side and
child-side networks, so killing it genuinely partitions) plus a children file
written onto each relay. The harness consumer is relay-capable (it runs
`node::run_node`, so a children file makes it a relay), and each run kills a node
mid-run to check the system behaves as expected.

## Topologies

| topology            | shape                                   | experiment                                                              |
|---------------------|-----------------------------------------|------------------------------------------------------------------------|
| `line2`             | coordinator -> relay -> two leaves      | kill the sole relay: subtree stranded, run fails legibly               |
| `line3`             | coordinator -> relay1 -> relay2 -> leaf | depth-3 cascade completes, kill the interior relay strands the leaf    |
| `diamond`           | leaf reachable via relayA and relayB    | dedup to one primary, kill the primary bridge, standby finishes        |
| `articulation`      | diamond plus a solo leaf under relayA   | kill relayA: solo's work reroutes onto the surviving compute           |
| `require_redundancy`| line2 (no redundancy) and diamond       | refuse a non-redundant topology, admit a redundant one                 |

## Usage

```sh
../up.sh                 # build the harness images (once)
./run-all.sh             # warm the cache, then run every topology
./diamond/run.sh         # or run one topology
KEEP=1 ./diamond/run.sh  # keep the containers up afterward (fast re-runs)
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
