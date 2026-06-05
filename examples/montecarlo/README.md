# Monte Carlo with rayonet

Estimate pi by Monte Carlo, distributed across a small local docker "swarm" with
rayonet. Each task draws millions of random points and counts how many land in
the unit quarter circle; summing the hits gives pi. The tasks are independent
and compute-bound, exactly what rayonet is for.

This one program is the whole rayonet contract:

- **One binary, two roles.** Run normally it is the coordinator; rayonet launches
  the same binary on each worker as the agent.
- **One line of build glue.** `build.rs` calls `rayonet_build::extract()`, which
  finds the `.netmap(sample)` call and generates the agent's task registry;
  `rayonet::embed_microcrates!()` pulls it in.
- **Blank hosts, no manual deploy.** The workers are bare ssh containers with no
  rust. rayonet provisions each one from cold: install rust, ship the source,
  build the agent, launch it.

## Run it

The swarm is three blank ssh containers managed by `docker compose`. Docker is
required.

```sh
# 1. Start the swarm: builds the worker image, starts the workers, and writes a
#    throwaway key plus the fleet list the example reads.
examples/montecarlo/cluster/up.sh

# 2. Run the coordinator. It ships this workspace to each worker, rayonet
#    provisions and builds the agent there, then distributes the tasks. You will
#    watch each host climb the ladder Probing -> Installing -> Syncing ->
#    Building -> Ready, then run the work, and finally print the estimate.
cargo run -p montecarlo

# 3. Tear the swarm down.
examples/montecarlo/cluster/down.sh
```

Expected tail of the output:

```text
pi ~= 3.14159 (from 160000000 samples across 32 tasks on 3 workers)
```

## The swarm

`cluster/compose.yml` defines three identical workers built from `cluster/Dockerfile`,
a `debian` image with `sshd` and the tools rustup needs, but **no rust**, that is
what makes this a real cold-provisioning demo. Each worker publishes its ssh port
(`2201`, `2202`, `2203`) so the coordinator can reach `root@localhost:<port>`.

To use more or fewer workers, add or remove services in `compose.yml` (with
matching published ports) and the corresponding `host port` lines that
`cluster/up.sh` writes to `cluster/fleet`.

## What gets shipped

rayonet ships the whole workspace as the source bundle (because rayonet itself is
an unpublished path dependency here) and builds only this package on each worker
(`cargo build -p montecarlo`). A real consumer that depends on a published
rayonet would ship just its own crate.
