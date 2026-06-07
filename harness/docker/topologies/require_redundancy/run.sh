#!/usr/bin/env bash
# require_redundancy: a run that requires redundancy refuses, before scheduling,
# any compute reachable through only one relay, and admits a topology where every
# compute node has a redundant path. Reuses the line2 (no redundancy) and diamond
# (redundant) topologies.
source "$(dirname "$0")/../lib.sh"
TOP="$(cd "$(dirname "$0")/.." && pwd)"
fails=0
cleanup() {
  [ "${KEEP:-0}" = 1 ] && return
  docker compose -p rayonet-line2 -f "$TOP/line2/compose.yml" down -t 2 >/dev/null 2>&1
  docker compose -p rayonet-diamond -f "$TOP/diamond/compose.yml" down -t 2 >/dev/null 2>&1
}
trap cleanup EXIT

topo_setup
topo_warm || exit 1

echo "=== require_redundancy: refuses compute behind a lone relay (line2) ==="
docker compose -p rayonet-line2 -f "$TOP/line2/compose.yml" up -d >/dev/null 2>&1
cfg2=/tmp/rayonet-rr-line2-config
topo_write_config "$cfg2" relay=2210
topo_wait "$cfg2" relay || exit 1
topo_seed_ids rayonet-line2 relay leaf-a leaf-b
topo_children rayonet-line2 relay "$(child leaf-a)" "$(child leaf-b)"
topo_drive "$cfg2" relay double 8 require | tee /tmp/rr-line2.log
if grep -q 'require_redundancy' /tmp/rr-line2.log && ! grep -q 'ok:' /tmp/rr-line2.log; then
  echo "  PASS: refused (leaves reachable through only one relay)"
else
  echo "  FAIL: expected a require_redundancy refusal"; fails=$((fails + 1))
fi

echo "=== require_redundancy: admits a redundant leaf (diamond) ==="
docker compose -p rayonet-diamond -f "$TOP/diamond/compose.yml" up -d >/dev/null 2>&1
cfgd=/tmp/rayonet-rr-diamond-config
topo_write_config "$cfgd" relayA=2211 relayB=2212
topo_wait "$cfgd" relayA relayB || exit 1
topo_seed_ids rayonet-diamond relayA relayB leaf
topo_children rayonet-diamond relayA "$(child leaf)"
topo_children rayonet-diamond relayB "$(child leaf)"
topo_drive "$cfgd" relayA,relayB double 6 require | tee /tmp/rr-diamond.log
grep -q 'ok: 6 results' /tmp/rr-diamond.log \
  && echo "  PASS: admitted (the leaf is reachable through two relays)" \
  || { echo "  FAIL: a redundant topology should be admitted"; fails=$((fails + 1)); }

echo
[ $fails = 0 ] && echo "REQUIRE_REDUNDANCY: PASS" || echo "REQUIRE_REDUNDANCY: $fails CHECK(S) FAILED"
exit $fails
