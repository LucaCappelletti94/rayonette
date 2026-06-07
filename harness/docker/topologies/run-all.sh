#!/usr/bin/env bash
# Run the whole topology bestiarium in sequence and tally the result. Needs the
# harness images (../up.sh builds them once). The shared cache is warmed once, so
# only the first build compiles; every topology afterward provisions by cache hit.
cd "$(dirname "$0")"
source ./lib.sh

topo_setup
topo_warm || { echo "warm-up failed"; exit 1; }

total=0
for t in line2 line3 diamond articulation require_redundancy; do
  echo
  echo "############################## $t ##############################"
  bash "$t/run.sh"
  total=$((total + $?))
done

echo
if [ "$total" = 0 ]; then
  echo "BESTIARIUM: ALL TOPOLOGIES PASS"
else
  echo "BESTIARIUM: $total CHECK(S) FAILED"
fi
exit "$total"
