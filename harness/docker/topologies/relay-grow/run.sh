#!/usr/bin/env bash
# relay-grow: a relay absorbs a child added to its children file mid-run. The
# relay starts coordinating leaf-a; leaf-b is then started and appended to the
# relay's file, and the relay's re-read launches it and splices it into the
# subtree. This is R6 elastic membership one level below the coordinator's rejoin.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonet-relay-grow
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonet-relay-grow-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
# Start the relay and leaf-a; leaf-b stays down until mid-run.
docker compose -p "$PROJ" -f "$COMPOSE" up -d relay leaf-a >/dev/null 2>&1
topo_write_config "$config" relay=2230
topo_wait "$config" relay || exit 1
topo_seed_ids "$PROJ" relay leaf-a
topo_children "$PROJ" relay "$(child leaf-a)"

echo "=== relay-grow: a child added to the relay's file mid-run is absorbed ==="
topo_drive "$config" relay crunch 400 >/tmp/relay-grow.log 2>&1 &
pid=$!
for _ in $(seq 1 240); do
  grep -qE 'state relay Working' /tmp/relay-grow.log && break
  sleep 0.25
done
sleep 0.3
echo "  starting leaf-b and adding it to the relay's children file"
docker compose -p "$PROJ" -f "$COMPOSE" up -d leaf-b >/dev/null 2>&1
topo_seed_ids "$PROJ" leaf-b
# Re-read picks this up: the relay launches leaf-b (retrying until its sshd is up).
topo_children "$PROJ" relay "$(child leaf-a)" "$(child leaf-b)"
wait "$pid"

grep -q 'ok: 400 results' /tmp/relay-grow.log \
  && echo "  PASS: every task completed" \
  || { echo "  FAIL: not all tasks completed"; fails=$((fails + 1)); }
grep -qE 'state relay/leaf-b' /tmp/relay-grow.log \
  && echo "  PASS: the relay re-read its file and absorbed leaf-b" \
  || { echo "  FAIL: leaf-b never joined the subtree"; fails=$((fails + 1)); }

echo
[ $fails = 0 ] && echo "RELAY-GROW: PASS" || echo "RELAY-GROW: $fails CHECK(S) FAILED"
exit $fails
