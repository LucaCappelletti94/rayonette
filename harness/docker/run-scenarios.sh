#!/usr/bin/env bash
# Level-4 functional scenarios: drive the docker consumer (coordinator role)
# against the segmented network and assert behavior. Requires `./up.sh` first.
#
# These are functional assertions, not part of the coverage gate (the logic is
# covered at levels 1-3); they prove the real ladder over real ssh.
set -uo pipefail
cd "$(dirname "$0")"
ROOT=../..
CONFIG="$(pwd)/secrets/ssh_config"
BIN="$ROOT/target/release/rayonette-docker-consumer"

# Start from fresh containers so "cold host" means cold (no rust, no cache).
# leaf-b keeps the rust baked into its image; the rest start bare.
echo "recreating fresh containers..."
docker compose up -d --force-recreate >/dev/null 2>&1
for _ in $(seq 1 60); do
  ssh -F "$CONFIG" -o ConnectTimeout=2 bastion true 2>/dev/null && break
  sleep 0.5
done

# The consumer's build.rs bundles the workspace source (embedded via
# __rayonette_source), so the script no longer needs to tar anything.
( cd "$ROOT" && cargo build --release -p rayonette-docker-consumer >/dev/null 2>&1 )

fails=0
run() { # name, leaves, [task=double], [count=10]
  local name="$1" leaves="$2" task="${3:-double}" count="${4:-10}"
  echo "=== scenario: $name (leaves=$leaves task=$task count=$count) ==="
  RAYONETTE_SSH_CONFIG="$CONFIG" RAYONETTE_LEAVES="$leaves" \
    RAYONETTE_TOOLCHAIN=stable \
    RAYONETTE_TASK="$task" RAYONETTE_COUNT="$count" \
    "$BIN" 2>&1 | tee "/tmp/rayonette-scenario-$name.log"
}
expect() { # name, pattern, description
  if grep -qE "$2" "/tmp/rayonette-scenario-$1.log"; then
    echo "  PASS: $3"
  else
    echo "  FAIL: $3 (expected /$2/)"; fails=$((fails + 1))
  fi
}

run cold leaf-a
expect cold 'state leaf-a Installing' 'cold host installs rust'
expect cold 'state leaf-a Building'   'cold host builds'
expect cold 'state leaf-a Ready'      'cold host becomes ready'
expect cold 'ok: 10 results'          'job completed'

run cache leaf-a
expect cache 'state leaf-a Ready' 'second run reaches ready'
if grep -qE 'state leaf-a Building' "/tmp/rayonette-scenario-cache.log"; then
  echo "  FAIL: cache hit should skip Building"; fails=$((fails + 1))
else
  echo "  PASS: cache hit skips Building"
fi
expect cache 'ok: 10 results' 'cached job completed'

run skip-install leaf-b
if grep -qE 'state leaf-b Installing' "/tmp/rayonette-scenario-skip-install.log"; then
  echo "  FAIL: host with rust should skip Installing"; fails=$((fails + 1))
else
  echo "  PASS: host with rust skips Installing"
fi
expect skip-install 'state leaf-b Building' 'builds on host with rust'
expect skip-install 'ok: 10 results'        'job completed'

run multihop leaf-deep
expect multihop 'state leaf-deep Ready' 'reached 2-hop host'
expect multihop 'ok: 10 results'        'multi-hop job completed'

run blocked "leaf-b,leaf-blocked"
expect blocked 'ok: 10 results'             'survivor completes despite blocked host'
expect blocked 'state leaf-blocked Installing' 'blocked host reached install'
if grep -qE 'state leaf-blocked Ready' "/tmp/rayonette-scenario-blocked.log"; then
  echo "  FAIL: blocked host should never become Ready"; fails=$((fails + 1))
else
  echo "  PASS: blocked host never becomes Ready"
fi

# Work-share: a CPU-bound batch across a full-speed and a throttled host. Warm
# both build caches with a cheap pass first so the cap bites task execution.
run fastslow-warmup "leaf-fast,leaf-slow" double 1
# A CPU-bound batch across a full-speed host and a 0.5-cpu-capped one. The cap
# can only slow leaf-slow down, never speed it up, so the unthrottled leaf-fast
# must take at least as large a share. We assert that invariant rather than a
# strict majority: on a shared, overprovisioned CI runner the cgroup quota often
# does not bite a short CPU burst, so a full run splits roughly evenly there.
run fastslow "leaf-fast,leaf-slow" crunch 40
expect fastslow 'ok: 40 results' 'every task completed'
fast=$(grep 'share leaf-fast ' "/tmp/rayonette-scenario-fastslow.log" | awk '{print $NF}')
slow=$(grep 'share leaf-slow ' "/tmp/rayonette-scenario-fastslow.log" | awk '{print $NF}')
if [ -n "${fast:-}" ] && [ -n "${slow:-}" ] && [ "$fast" -ge "$slow" ]; then
  echo "  PASS: throttled host took no larger a share than the fast host ($fast vs $slow)"
else
  echo "  FAIL: throttled host out-produced the fast host (fast=${fast:-?} slow=${slow:-?})"
  fails=$((fails + 1))
fi

# Fault tolerance: a host is killed mid-run; the survivor must finish every
# task (the caches are already warm from the fast/slow run above).
echo "=== scenario: kill (leaf-slow dies mid-run) ==="
RAYONETTE_SSH_CONFIG="$CONFIG" RAYONETTE_LEAVES="leaf-fast,leaf-slow" \
  RAYONETTE_TOOLCHAIN=stable RAYONETTE_TASK=crunch RAYONETTE_COUNT=120 \
  "$BIN" >/tmp/rayonette-scenario-kill.log 2>&1 &
consumer_pid=$!
sleep 3 # provisioning is a cache hit, so the run is underway by now
docker kill rayonette-harness-leaf-slow-1 >/dev/null 2>&1 && echo "  killed leaf-slow"
wait "$consumer_pid"
grep -E 'state leaf-slow|ok:|share' /tmp/rayonette-scenario-kill.log | tail -5
expect kill 'state leaf-slow Lost' 'killed host is marked Lost'
expect kill 'ok: 120 results'      'survivor finished every task'

echo
if [ "$fails" -eq 0 ]; then echo "ALL SCENARIOS PASSED"; else echo "$fails CHECK(S) FAILED"; fi
exit "$fails"
