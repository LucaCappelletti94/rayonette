#!/usr/bin/env bash
# task-forms: a single leaf, driven once per macro-supported task shape. Every
# shape doubles its input, so each must come back as `ok: 8 results` with the
# doubled values (the consumer asserts the values internally). This proves the
# `#[rayonette::tasks]` output for a named function, an annotated closure, an
# unannotated closure recovered from a typed binding, and a turbofished generic
# instance all survive the ship-to-a-blank-host, build, and run round trip, not
# just a local compile.
source "$(dirname "$0")/../lib.sh"
PROJ=rayonette-task-forms
COMPOSE="$(dirname "$0")/compose.yml"
config=/tmp/rayonette-task-forms-config
fails=0
cleanup() { [ "${KEEP:-0}" = 1 ] || docker compose -p "$PROJ" -f "$COMPOSE" down -t 2 >/dev/null 2>&1; }
trap cleanup EXIT

topo_setup
topo_warm || exit 1
docker compose -p "$PROJ" -f "$COMPOSE" up -d >/dev/null 2>&1
topo_write_config "$config" forms=2280
topo_wait "$config" forms || exit 1
topo_seed_ids "$PROJ" forms

# The leaf's registry is built from inventory, so it carries every task shape the
# consumer registered; the coordinator selects one per run with RAYONETTE_TASK.
for form in double closure inferred generic; do
  echo "=== task-forms: $form ==="
  topo_drive "$config" forms "$form" 8 | tee "/tmp/task-forms-$form.log"
  if grep -q 'ok: 8 results' "/tmp/task-forms-$form.log"; then
    echo "  PASS: $form shipped, built on the leaf, and returned the right answer"
  else
    echo "  FAIL: $form did not complete correctly"
    fails=$((fails + 1))
  fi
done

echo
[ $fails = 0 ] && echo "TASK-FORMS: PASS" || echo "TASK-FORMS: $fails CHECK(S) FAILED"
exit $fails
