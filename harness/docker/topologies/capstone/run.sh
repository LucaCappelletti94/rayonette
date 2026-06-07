#!/usr/bin/env bash
# capstone (R7): one run that is segmented, multi-level, redundant, AND elastic.
# Two gateways front a shared compute leaf on their private nets (deduped to one
# node: one primary, one standby). A distinct compute leaf joins the standby
# gateway mid-run (the standby re-reads its children file), then the primary
# gateway is killed and its shared subtree reroutes onto the standby. Everything
# completes once, deduplicated, through both the join and the kill.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonet-capstone
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonet-capstone-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
# Start both gateways and the shared leaf; extra stays down until mid-run.
docker compose -p "$PROJ" -f "$COMPOSE" up -d gatewayA gatewayB shared >/dev/null 2>&1
topo_write_config "$config" gatewayA=2240 gatewayB=2241
topo_wait "$config" gatewayA gatewayB || exit 1
topo_seed_ids "$PROJ" gatewayA gatewayB shared
topo_children "$PROJ" gatewayA "$(child shared)"
topo_children "$PROJ" gatewayB "$(child shared)"

echo "=== capstone: redundant + multi-level + elastic, through a gateway kill ==="
topo_drive "$config" gatewayA,gatewayB "$HEAVY_TASK" "$HEAVY_COUNT" >/tmp/capstone.log 2>&1 &
pid=$!
# Whichever gateway runs shared first is the primary; the other is the standby.
primary=""
for _ in $(seq 1 240); do
  primary=$(grep -m1 -oE 'state gateway[AB] Working' /tmp/capstone.log | grep -oE 'gateway[AB]' | head -1)
  [ -n "$primary" ] && break
  sleep 0.25
done
[ -z "$primary" ] && { echo "  FAIL: no gateway started working"; fails=$((fails + 1)); primary=gatewayA; }
standby=gatewayB
[ "$primary" = gatewayB ] && standby=gatewayA
echo "  primary=$primary standby=$standby"

# Elastic: a distinct compute leaf joins the standby gateway mid-run.
echo "  joining 'extra' to $standby mid-run"
docker compose -p "$PROJ" -f "$COMPOSE" up -d extra >/dev/null 2>&1
topo_seed_ids "$PROJ" extra
topo_children "$PROJ" "$standby" "$(child shared)" "$(child extra)"
for _ in $(seq 1 160); do
  grep -qE "state $standby/extra" /tmp/capstone.log && break
  sleep 0.25
done
sleep 0.3

# Kill the primary gateway: its shared subtree reroutes onto the standby.
echo "  killing primary $primary"
docker kill "$PROJ-$primary-1" >/dev/null 2>&1
wait "$pid"

grep -q "ok: $HEAVY_COUNT results" /tmp/capstone.log \
  && echo "  PASS: every task completed once (redundant + elastic, through the kill)" \
  || { echo "  FAIL: not all tasks completed"; fails=$((fails + 1)); }
grep -qE "state $primary Lost" /tmp/capstone.log \
  && echo "  PASS: the killed primary gateway rerouted onto the standby" \
  || { echo "  FAIL: primary not marked Lost"; fails=$((fails + 1)); }
grep -qE "state $standby/extra" /tmp/capstone.log \
  && echo "  PASS: a node joined the standby gateway mid-run" \
  || { echo "  FAIL: the mid-run join was not picked up"; fails=$((fails + 1)); }

echo
[ $fails = 0 ] && echo "CAPSTONE: PASS" || echo "CAPSTONE: $fails CHECK(S) FAILED"
exit $fails
