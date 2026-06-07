#!/usr/bin/env bash
# elastic: a node absent at launch joins mid-run (R6 elastic membership). The
# coordinator starts with one leaf; a second leaf is started after the run is
# underway, and the rejoin driver discovers it, provisions it by cache hit, and
# it pulls pending work. Mirrors R5's leave path: here the fleet grows.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonet-elastic
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonet-elastic-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
# Start only leaf-a; leaf-b stays down until mid-run.
docker compose -p "$PROJ" -f "$COMPOSE" up -d leaf-a >/dev/null 2>&1
topo_write_config "$config" leaf-a=2220 leaf-b=2221
topo_wait "$config" leaf-a || exit 1
topo_seed_ids "$PROJ" leaf-a

echo "=== elastic: run starts with one leaf, a second joins mid-run ==="
# Both leaves are named, but leaf-b is unreachable at launch, so it becomes a
# rejoin candidate the driver retries.
topo_drive "$config" leaf-a,leaf-b "$HEAVY_TASK" "$HEAVY_COUNT" >/tmp/elastic.log 2>&1 &
pid=$!
for _ in $(seq 1 240); do
  grep -qE 'state leaf-a Working' /tmp/elastic.log && break
  sleep 0.25
done
sleep 0.3
echo "  starting leaf-b mid-run"
docker compose -p "$PROJ" -f "$COMPOSE" up -d leaf-b >/dev/null 2>&1
topo_wait "$config" leaf-b || echo "  warning: leaf-b sshd slow to answer"
topo_seed_ids "$PROJ" leaf-b
wait "$pid"

grep -q "ok: $HEAVY_COUNT results" /tmp/elastic.log \
  && echo "  PASS: every task completed" \
  || { echo "  FAIL: not all tasks completed"; fails=$((fails + 1)); }
shareb=$(grep -E '^share leaf-b ' /tmp/elastic.log | awk '{print $NF}')
shareb=${shareb:-0}
if [ "$shareb" -gt 0 ] 2>/dev/null; then
  echo "  PASS: the late leaf joined and ran $shareb tasks"
else
  echo "  FAIL: the late leaf ran no tasks (share=$shareb)"
  fails=$((fails + 1))
fi

echo
[ $fails = 0 ] && echo "ELASTIC: PASS" || echo "ELASTIC: $fails CHECK(S) FAILED"
exit $fails
