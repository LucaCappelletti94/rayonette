#!/usr/bin/env bash
# articulation: a diamond (shared leaf via relayA and relayB) plus a solo leaf
# reachable only through relayA. Killing relayA loses the only path to solo, but
# because tasks are not pinned to a node its work reroutes onto the surviving
# compute (the shared leaf via relayB), so the run still completes: losing an
# articulation relay degrades gracefully as long as some compute survives.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonet-articulation
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonet-articulation-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
docker compose -p "$PROJ" -f "$COMPOSE" up -d >/dev/null 2>&1
topo_write_config "$config" relayA=2211 relayB=2212
topo_wait "$config" relayA relayB || exit 1
topo_seed_ids "$PROJ" relayA relayB shared solo
topo_children "$PROJ" relayA "$(child shared)" "$(child solo)"
topo_children "$PROJ" relayB "$(child shared)"

echo "=== articulation: kill relayA (the only path to solo) mid-run ==="
topo_drive "$config" relayA,relayB crunch 400 >/tmp/articulation-kill.log 2>&1 &
pid=$!
for _ in $(seq 1 160); do
  grep -qE 'state relay[AB] Working' /tmp/articulation-kill.log && break; sleep 0.25
done
sleep 0.3
echo "  killing relayA"
docker kill "$PROJ-relayA-1" >/dev/null 2>&1
wait "$pid"
grep -q 'ok: 400 results' /tmp/articulation-kill.log \
  && echo "  PASS: every task completed via the surviving compute (relayB)" \
  || { echo "  FAIL: not all tasks completed after the articulation relay died"; fails=$((fails + 1)); }
grep -qE 'state relayA Lost' /tmp/articulation-kill.log \
  && echo "  PASS: the killed articulation relay was marked Lost" \
  || { echo "  FAIL: relayA not marked Lost"; fails=$((fails + 1)); }

echo
[ $fails = 0 ] && echo "ARTICULATION: PASS" || echo "ARTICULATION: $fails CHECK(S) FAILED"
exit $fails
