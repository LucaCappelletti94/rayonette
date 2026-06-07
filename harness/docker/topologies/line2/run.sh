#!/usr/bin/env bash
# line2: coordinator -> relay -> {leaf-a, leaf-b}. The cascade runs, and killing
# the sole relay strands the subtree (no alternate path) so the run fails clearly.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonet-line2
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonet-line2-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
docker compose -p "$PROJ" -f "$COMPOSE" up -d >/dev/null 2>&1
topo_write_config "$config" relay=2210
topo_wait "$config" relay || exit 1
topo_seed_ids "$PROJ" relay leaf-a leaf-b
topo_children "$PROJ" relay "$(child leaf-a)" "$(child leaf-b)"

echo "=== line2: the cascade runs ==="
topo_drive "$config" relay double 8 | tee /tmp/line2-run.log
grep -q 'ok: 8 results' /tmp/line2-run.log \
  && echo "  PASS: coordinator -> relay -> two leaves completed every task" \
  || { echo "  FAIL: cascade did not complete"; fails=$((fails + 1)); }

echo "=== line2: kill the sole relay (no redundant path) ==="
topo_drive "$config" relay "$HEAVY_TASK" "$HEAVY_COUNT" >/tmp/line2-kill.log 2>&1 &
pid=$!
for _ in $(seq 1 160); do
  grep -qE 'state relay Working' /tmp/line2-kill.log && break; sleep 0.25
done
sleep 0.3
echo "  killing the relay"
docker kill "$PROJ-relay-1" >/dev/null 2>&1
wait "$pid"
if grep -q 'error:' /tmp/line2-kill.log && ! grep -q "ok: $HEAVY_COUNT results" /tmp/line2-kill.log; then
  echo "  PASS: stranded subtree fails the run legibly"
else
  echo "  FAIL: expected a clear failure after the relay died"; fails=$((fails + 1))
fi

echo
[ $fails = 0 ] && echo "LINE2: PASS" || echo "LINE2: $fails CHECK(S) FAILED"
exit $fails
