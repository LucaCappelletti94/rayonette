#!/usr/bin/env bash
# Diamond: a leaf reachable through two relays is deduped to one primary, and
# killing that primary (a bridge to the leaf) mid-run reroutes onto the standby.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonet-diamond
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonet-diamond-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
docker compose -p "$PROJ" -f "$COMPOSE" up -d >/dev/null 2>&1
topo_write_config "$config" relayA=2211 relayB=2212
topo_wait "$config" relayA relayB || exit 1
topo_seed_ids "$PROJ" relayA relayB leaf
topo_children "$PROJ" relayA "$(child leaf)"
topo_children "$PROJ" relayB "$(child leaf)"

echo "=== diamond: dedup ==="
topo_drive "$config" relayA,relayB double 6 | tee /tmp/diamond-dedup.log
sa=$(grep -E '^share relayA ' /tmp/diamond-dedup.log | awk '{print $NF}'); sa=${sa:-0}
sb=$(grep -E '^share relayB ' /tmp/diamond-dedup.log | awk '{print $NF}'); sb=${sb:-0}
if grep -q 'ok: 6 results' /tmp/diamond-dedup.log && [ $((sa + sb)) -eq 6 ] \
   && { [ "$sa" = 0 ] || [ "$sb" = 0 ]; }; then
  echo "  PASS: leaf deduped to one primary relay (relayA=$sa relayB=$sb)"
else
  echo "  FAIL: dedup (relayA=$sa relayB=$sb, want sum 6 and one 0)"; fails=$((fails + 1))
fi

echo "=== diamond: reroute (kill the primary bridge) ==="
topo_drive "$config" relayA,relayB "$HEAVY_TASK" "$HEAVY_COUNT" >/tmp/diamond-kill.log 2>&1 &
pid=$!
primary=""
for _ in $(seq 1 160); do
  primary=$(grep -m1 -oE 'state relay[AB] Working' /tmp/diamond-kill.log | grep -oE 'relay[AB]' | head -1)
  [ -n "$primary" ] && break; sleep 0.25
done
[ -z "$primary" ] && { echo "  FAIL: no relay started working"; fails=$((fails + 1)); primary=relayA; }
sleep 0.3
echo "  killing primary $primary"
docker kill "$PROJ-$primary-1" >/dev/null 2>&1
wait "$pid"
grep -q "ok: $HEAVY_COUNT results" /tmp/diamond-kill.log \
  && echo "  PASS: all tasks completed once via the standby" \
  || { echo "  FAIL: not all 80 tasks completed"; fails=$((fails + 1)); }
grep -qE "state $primary Lost" /tmp/diamond-kill.log \
  && echo "  PASS: the killed primary was marked Lost" \
  || { echo "  FAIL: primary not marked Lost"; fails=$((fails + 1)); }

echo
[ $fails = 0 ] && echo "DIAMOND: PASS" || echo "DIAMOND: $fails CHECK(S) FAILED"
exit $fails
