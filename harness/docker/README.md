# rayonet docker harness (test pyramid level 4)

A deliberately segmented multi-host network for exercising the real provisioning
ladder and ssh transport (PLAN.md Phase 4). The point is partial connectivity:
the coordinator cannot reach most nodes directly, so work is forced through ssh
`ProxyJump` chains, the way decision 21 says it should be.

## Topology

```
host (coordinator, runs on your machine)
  |
  |  ssh 127.0.0.1:2201   (the ONLY published port)
  v
bastion        on frontnet + backnet + blockednet
  |- backnet ----> relay, leaf-a (no rust), leaf-b (rust preinstalled)
  |- blockednet -> leaf-blocked (no rust, NO egress)
relay          on backnet + deepnet
  |- deepnet ---> leaf-deep (no rust)
```

Reachability (enforced by docker DNS: the coordinator can only name-resolve a
node from inside a network it shares, so it must hop):

| node         | from host        | rust  | egress | exercises                    |
|--------------|------------------|-------|--------|------------------------------|
| bastion      | direct (:2201)   | no    | yes    | jump host                    |
| leaf-a       | `ProxyJump bastion`        | no  | yes  | install -> build           |
| leaf-b       | `ProxyJump bastion`        | yes | yes  | skip-install -> build      |
| leaf-deep    | `ProxyJump bastion,relay`  | no  | yes  | 2-hop propagation          |
| leaf-blocked | `ProxyJump bastion`        | no  | no   | blocked host fails legibly |

## Usage

```sh
./up.sh      # build images, start containers, write secrets/ssh_config, verify
./down.sh    # stop and remove containers
```

`up.sh` generates a throwaway ed25519 key into `secrets/` (gitignored) and a
coordinator-side `secrets/ssh_config`. Point rayonet at a node with that config
plus the node name, for example `leaf-deep`, and ssh handles the jump chain.

## Notes

- Images carry no rust except `leaf-b`; the ladder installs it where missing.
- `leaf-blocked` is on an `internal` network with no egress, so rustup cannot
  fetch the toolchain there: that is the per-host failure case (decision 18).
- The whole workspace source is shipped and built on each host (so the agent's
  path dependency on rayonet resolves without rayonet being published).
