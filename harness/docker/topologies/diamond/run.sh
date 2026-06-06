#!/usr/bin/env bash
# Diamond topology scenario: a leaf reachable through two relays is deduped to a
# single primary path, and when the primary relay (a bridge to the leaf) is
# killed mid-run the standby relay takes over and every task still completes once.
#
# Run from anywhere; needs the harness images (../../up.sh builds them once).
set -uo pipefail
cd "$(dirname "$0")"

SECRETS="$(cd ../../secrets && pwd)"
CONFIG=/tmp/rayonet-diamond-config
ROOT="$(cd ../../../.. && pwd)"
BIN="$ROOT/target/release/rayonet-docker-consumer"
PROJ=rayonet-diamond
fails=0

cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" down -t 2 >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "building the consumer..."
( cd "$ROOT" && cargo build --release -p rayonet-docker-consumer >/dev/null 2>&1 ) \
  || { echo "consumer build failed"; exit 1; }

echo "starting the diamond..."
# Reuse running containers (warm caches) for fast iteration; set FRESH=1 to test
# the cold-build cascade from scratch.
recreate=""; [ "${FRESH:-0}" = 1 ] && recreate="--force-recreate"
docker compose -p "$PROJ" up -d $recreate >/dev/null 2>&1

cat > "$CONFIG" <<EOF
Host *
  User rayonet
  IdentityFile $SECRETS/id_ed25519
  IdentitiesOnly yes
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
Host relayA
  HostName 127.0.0.1
  Port 2211
Host relayB
  HostName 127.0.0.1
  Port 2212
EOF

echo "waiting for the relays' sshd..."
for _ in $(seq 1 60); do
  ssh -F "$CONFIG" -o ConnectTimeout=2 relayA true 2>/dev/null \
    && ssh -F "$CONFIG" -o ConnectTimeout=2 relayB true 2>/dev/null && break
  sleep 0.5
done

# A stable machine id per container makes node identity deterministic and
# race-free (the leaf is one container, so both relays read the same id and the
# coordinator dedups the two paths to it).
for c in relayA relayB leaf; do
  docker exec "$PROJ-$c-1" sh -c \
    "head -c16 /dev/urandom | od -An -tx1 | tr -d ' \n' > /etc/machine-id" 2>/dev/null
done

# Each relay reaches the shared leaf over its own child network with the shared
# key: the decentralized children file is what makes it a relay.
for r in relayA relayB; do
  docker exec "$PROJ-$r-1" sh -c \
    'mkdir -p /home/rayonet/.config/rayonet \
     && echo "leaf=/secrets/id_ed25519" > /home/rayonet/.config/rayonet/children \
     && chown -R rayonet:rayonet /home/rayonet/.config'
done

drive() { # task count logfile
  RAYONET_SSH_CONFIG="$CONFIG" RAYONET_LEAVES="relayA,relayB" \
    RAYONET_TOOLCHAIN=stable RAYONET_TASK="$1" RAYONET_COUNT="$2" "$BIN"
}

echo "=== warm-up run (builds the leaf via both relays, proves dedup) ==="
drive double 6 2>&1 | tee /tmp/diamond-warm.log
# The leaf is reached through both relays but deduped to one: the coordinator
# schedules every task through the primary relay and holds the other's leaf idle,
# so one relay's completed share is 6 and the other's is 0 (not 6 and 6).
sa=$(grep -E '^share relayA ' /tmp/diamond-warm.log | awk '{print $NF}')
sb=$(grep -E '^share relayB ' /tmp/diamond-warm.log | awk '{print $NF}')
sa=${sa:-0}; sb=${sb:-0}
if grep -q 'ok: 6 results' /tmp/diamond-warm.log \
   && [ $((sa + sb)) -eq 6 ] && { [ "$sa" -eq 0 ] || [ "$sb" -eq 0 ]; }; then
  echo "  PASS: leaf deduped to one primary relay (relayA=$sa relayB=$sb completed)"
else
  echo "  FAIL: dedup (ok? / completed relayA=$sa relayB=$sb, want sum 6 and one 0)"
  fails=$((fails + 1))
fi

echo "=== reroute run (kill the primary relay mid-run) ==="
drive crunch 80 >/tmp/diamond-kill.log 2>&1 &
pid=$!
# The primary is the relay that starts working first (the standby stays idle
# until it is promoted on reroute).
primary=""
for _ in $(seq 1 120); do
  primary=$(grep -m1 -oE 'state relay[AB] Working' /tmp/diamond-kill.log \
            | grep -oE 'relay[AB]' | head -1)
  [ -n "$primary" ] && break
  sleep 0.25
done
if [ -z "$primary" ]; then
  echo "  FAIL: never saw a leaf path start working"; fails=$((fails + 1)); primary=relayA
fi
sleep 1
echo "  primary is $primary; killing it mid-run"
docker kill "$PROJ-$primary-1" >/dev/null 2>&1
wait "$pid"

if grep -q 'ok: 80 results' /tmp/diamond-kill.log; then
  echo "  PASS: every task completed once despite the primary relay's death"
else
  echo "  FAIL: run did not complete all 80 tasks after the kill"; fails=$((fails + 1))
fi
if grep -qE "state $primary Lost" /tmp/diamond-kill.log; then
  echo "  PASS: the killed primary relay was marked Lost"
else
  echo "  FAIL: the killed relay was not marked Lost"; fails=$((fails + 1))
fi

echo
if [ "$fails" -eq 0 ]; then echo "DIAMOND: ALL CHECKS PASSED"; else echo "DIAMOND: $fails CHECK(S) FAILED"; fi
exit "$fails"
