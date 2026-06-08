# Shared helpers for the docker topology bestiarium.
#
# Topology is set by docker networks (a relay bridges its parent-side and
# child-side networks, so killing it genuinely partitions) plus children files
# written per node. A shared, architecture-keyed cache volume is warmed once so
# every topology after the first hits the cache instead of recompiling: all the
# containers share the host CPU, so the native binary is valid on all of them,
# which is exactly what the arch-keyed cache guarantees.

set -uo pipefail

TOPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$TOPO_DIR/../../.." && pwd)"
SECRETS="$(cd "$TOPO_DIR/../secrets" && pwd)"
BIN="$ROOT/target/release/rayonet-docker-consumer"
CACHE_VOLUME=rayonet-topo-cache

# The kill and join scenarios need a run long enough for the event to land
# mid-run. Locally that is a heavy CPU task (`crunch`) with a high count; CI
# overrides these with the wall-clock `dawdle` task and a modest count, so the
# timing holds on a slow shared runner without minutes of compute. Set
# RAYONET_HEAVY_TASK / RAYONET_HEAVY_COUNT (the CI workflow does) to switch.
HEAVY_TASK="${RAYONET_HEAVY_TASK:-crunch}"
HEAVY_COUNT="${RAYONET_HEAVY_COUNT:-400}"

# A child file entry pointing at <name> over the shared key (the bare label keeps
# the path segment clean: relay/<name>, not relay/rayonet@<name>).
child() { echo "$1=/secrets/id_ed25519"; }

topo_setup() {
  ( cd "$ROOT" && cargo build --release -p rayonet-docker-consumer >/dev/null 2>&1 ) \
    || { echo "consumer build failed"; exit 1; }
  docker volume create "$CACHE_VOLUME" >/dev/null
}

# Write a coordinator ssh_config reaching each "host=port" over 127.0.0.1.
topo_write_config() { # file host=port...
  local file="$1"; shift
  {
    echo "Host *"
    echo "  User rayonet"
    echo "  IdentityFile $SECRETS/id_ed25519"
    echo "  IdentitiesOnly yes"
    echo "  StrictHostKeyChecking no"
    echo "  UserKnownHostsFile /dev/null"
    echo "  LogLevel ERROR"
    for hp in "$@"; do
      printf 'Host %s\n  HostName 127.0.0.1\n  Port %s\n' "${hp%%=*}" "${hp##*=}"
    done
  } > "$file"
}

topo_wait() { # config host...
  local config="$1"; shift
  for _ in $(seq 1 80); do
    local ok=1 h
    for h in "$@"; do ssh -F "$config" -o ConnectTimeout=2 "$h" true 2>/dev/null || ok=0; done
    [ "$ok" = 1 ] && return 0
    sleep 0.5
  done
  echo "timed out waiting for sshd on: $*"; return 1
}

# A stable machine id per container makes node identity deterministic and
# race-free (a leaf reached two ways reads one id, so the coordinator dedups it).
topo_seed_ids() { # proj container...
  local proj="$1"; shift
  local c
  for c in "$@"; do
    docker exec "$proj-$c-1" sh -c \
      "head -c16 /dev/urandom | od -An -tx1 | tr -d ' \n' > /etc/machine-id" 2>/dev/null
  done
}

# Give a container a children file (one decentralized relay), naming its children.
topo_children() { # proj container child-entry...
  local proj="$1" container="$2"; shift 2
  local body=""; local e
  for e in "$@"; do body="${body}${e}\n"; done
  docker exec "$proj-$container-1" sh -c \
    "mkdir -p /home/rayonet/.config/rayonet \
     && printf '%b' '$body' > /home/rayonet/.config/rayonet/children \
     && chown -R rayonet:rayonet /home/rayonet/.config"
}

topo_drive() { # config leaves task count [require]
  local config="$1" leaves="$2" task="$3" count="$4" require="${5:-}"
  local -a vars=(RAYONET_SSH_CONFIG="$config" RAYONET_LEAVES="$leaves"
                 RAYONET_TOOLCHAIN=stable RAYONET_TASK="$task" RAYONET_COUNT="$count")
  [ -n "$require" ] && vars+=(RAYONET_REQUIRE_REDUNDANCY=1)
  # Forward the event-recording path if the caller set one, so a run can be
  # captured for TUI replay (see examples/tui-replay) without changing scenarios.
  [ -n "${RAYONET_EVENT_LOG:-}" ] && vars+=(RAYONET_EVENT_LOG="$RAYONET_EVENT_LOG")
  # Forward the control socket if set, so a TUI attached with `--control` can
  # pause or kill nodes in this live run (the coordinator runs on the host, so the
  # socket is a host path the viewer can connect to).
  [ -n "${RAYONET_CONTROL_SOCKET:-}" ] && vars+=(RAYONET_CONTROL_SOCKET="$RAYONET_CONTROL_SOCKET")
  # Line-buffer stdout so the per-node state lines reach a piped log promptly,
  # which is what lets a kill scenario detect "Working" and fire mid-run. The
  # timeout is a backstop: a correct run finishes or fails in seconds, so if a
  # kill scenario ever hangs (a node that does not tear down when its subtree
  # dies) the suite reports a failure instead of blocking forever.
  timeout 300 stdbuf -oL -eL env "${vars[@]}" "$BIN"
}

# Warm the shared cache once with a single sequential build, so every topology
# afterward provisions by cache hit (no recompiling, no concurrent-build races).
# Idempotent: once the volume holds the binary this is a fast cache hit.
topo_warm() {
  local proj=rayonet-warm config=/tmp/rayonet-warm-config
  echo "warming the shared build cache (first time compiles, then cached)..."
  docker compose -p "$proj" -f "$TOPO_DIR/warm/compose.yml" up -d >/dev/null 2>&1
  topo_write_config "$config" "warm=2299"
  topo_wait "$config" warm || return 1
  topo_seed_ids "$proj" warm
  topo_drive "$config" warm double 1 >/tmp/rayonet-warm.log 2>&1
  local rc=$?
  docker compose -p "$proj" -f "$TOPO_DIR/warm/compose.yml" down -t 2 >/dev/null 2>&1
  [ "$rc" = 0 ] || { echo "warm-up failed (see /tmp/rayonet-warm.log)"; return 1; }
}
