#!/usr/bin/env bash
# line3: coordinator -> relay1 -> relay2 -> leaf. A depth-3 cascade completes,
# and killing the interior relay (relay2) strands the leaf so the run fails.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonette-line3
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonette-line3-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
docker compose -p "$PROJ" -f "$COMPOSE" up -d >/dev/null 2>&1
topo_write_config "$config" relay1=2210
topo_wait "$config" relay1 || exit 1
topo_seed_ids "$PROJ" relay1 relay2 leaf
topo_children "$PROJ" relay1 "$(child relay2)"
topo_children "$PROJ" relay2 "$(child leaf)"

echo "=== line3: the depth-3 cascade runs ==="
topo_drive "$config" relay1 double 8 | tee /tmp/line3-run.log
grep -q 'ok: 8 results' /tmp/line3-run.log \
  && echo "  PASS: coordinator -> relay1 -> relay2 -> leaf completed every task" \
  || { echo "  FAIL: depth-3 cascade did not complete"; fails=$((fails + 1)); }

echo "=== line3: kill the interior relay (relay2) ==="
topo_drive "$config" relay1 "$HEAVY_TASK" "$HEAVY_COUNT" >/tmp/line3-kill.log 2>&1 &
pid=$!
for _ in $(seq 1 160); do
  grep -qE 'state relay1 Working' /tmp/line3-kill.log && break; sleep 0.25
done
sleep 0.3
echo "  killing relay2"
docker kill "$PROJ-relay2-1" >/dev/null 2>&1
wait "$pid"
if grep -q 'error:' /tmp/line3-kill.log && ! grep -q "ok: $HEAVY_COUNT results" /tmp/line3-kill.log; then
  echo "  PASS: stranding the leaf behind a dead interior relay fails the run"
else
  echo "  FAIL: expected a clear failure after relay2 died"; fails=$((fails + 1))
fi

echo
[ $fails = 0 ] && echo "LINE3: PASS" || echo "LINE3: $fails CHECK(S) FAILED"
exit $fails
