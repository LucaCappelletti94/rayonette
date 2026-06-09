#!/usr/bin/env bash
# metropolis: a richer network that grows mid-run. It starts with two gateways
# (gw1, gw2) fronting a redundant shared leaf plus a leaf each, then a whole third
# gateway subtree (gw3 -> leafC, leafD) is brought up mid-run and absorbed by the
# coordinator's rejoin driver, so the network visibly grows. Then the primary
# gateway for shared is killed and shared reroutes onto the standby. Everything
# completes once.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonette-metropolis
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonette-metropolis-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
# Start gw1, gw2 and their leaves; gw3's whole subtree stays down until mid-run.
docker compose -p "$PROJ" -f "$COMPOSE" up -d gw1 gw2 shared leafA leafB >/dev/null 2>&1
# All three gateways are in the fleet from the start; gw3 is down, so the
# coordinator's rejoin driver retries it until it comes online.
topo_write_config "$config" gw1=2250 gw2=2251 gw3=2252
topo_wait "$config" gw1 gw2 || exit 1
topo_seed_ids "$PROJ" gw1 gw2 shared leafA leafB
topo_children "$PROJ" gw1 "$(child leafA)" "$(child shared)"
topo_children "$PROJ" gw2 "$(child leafB)" "$(child shared)"

echo "=== metropolis: a growing, redundant, multi-gateway network ==="
topo_drive "$config" gw1,gw2,gw3 "$HEAVY_TASK" "$HEAVY_COUNT" >/tmp/metropolis.log 2>&1 &
pid=$!

# Whichever of gw1/gw2 runs shared first is its primary; the other is the standby.
primary=""
for _ in $(seq 1 240); do
  primary=$(grep -m1 -oE 'state gw[12] Working' /tmp/metropolis.log | grep -oE 'gw[12]' | head -1)
  [ -n "$primary" ] && break
  sleep 0.25
done
[ -z "$primary" ] && { echo "  FAIL: no gateway started working"; fails=$((fails + 1)); primary=gw1; }
standby=gw2
[ "$primary" = gw2 ] && standby=gw1
echo "  primary=$primary standby=$standby"

# Grow the network: bring up a new top-level compute node mid-run. The rejoin
# driver absorbs gw3 and it starts pulling work, so the network visibly grows.
echo "  growing: a new compute node gw3 joins mid-run"
docker compose -p "$PROJ" -f "$COMPOSE" up -d gw3 >/dev/null 2>&1
topo_seed_ids "$PROJ" gw3
for _ in $(seq 1 200); do
  grep -qE 'state gw3 Working' /tmp/metropolis.log && break
  sleep 0.25
done
sleep 0.3

# Kill the primary gateway for shared: it reroutes onto the standby.
echo "  killing primary $primary (shared reroutes onto $standby)"
docker kill "$PROJ-$primary-1" >/dev/null 2>&1
wait "$pid"

grep -q "ok: $HEAVY_COUNT results" /tmp/metropolis.log \
  && echo "  PASS: every task completed once through the growth and the kill" \
  || { echo "  FAIL: not all tasks completed"; fails=$((fails + 1)); }
grep -qE 'state gw3 Working' /tmp/metropolis.log \
  && echo "  PASS: a new compute node joined the network mid-run" \
  || { echo "  FAIL: gw3 did not join"; fails=$((fails + 1)); }
grep -qE "state $primary Lost" /tmp/metropolis.log \
  && echo "  PASS: the killed primary rerouted onto the standby" \
  || { echo "  FAIL: primary not marked Lost"; fails=$((fails + 1)); }

echo
[ $fails = 0 ] && echo "METROPOLIS: PASS" || echo "METROPOLIS: $fails CHECK(S) FAILED"
exit $fails
