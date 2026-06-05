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
TAR=/tmp/rayonet-workspace.tar
BIN="$ROOT/target/release/rayonet-docker-consumer"

# Start from fresh containers so "cold host" means cold (no rust, no cache).
# leaf-b keeps the rust baked into its image; the rest start bare.
echo "recreating fresh containers..."
docker compose up -d --force-recreate >/dev/null 2>&1
for _ in $(seq 1 60); do
  ssh -F "$CONFIG" -o ConnectTimeout=2 bastion true 2>/dev/null && break
  sleep 0.5
done

# One tar, reused across scenarios, so the content-addressed cache can hit.
tar -cf "$TAR" --exclude=./target --exclude=./.git --exclude=harness/docker/secrets \
  -C "$(cd "$ROOT" && pwd)" .
( cd "$ROOT" && cargo build --release -p rayonet-docker-consumer >/dev/null 2>&1 )

fails=0
run() { # name, leaves, [task=double], [count=10]
  local name="$1" leaves="$2" task="${3:-double}" count="${4:-10}"
  echo "=== scenario: $name (leaves=$leaves task=$task count=$count) ==="
  RAYONET_SSH_CONFIG="$CONFIG" RAYONET_LEAVES="$leaves" \
    RAYONET_SOURCE_TAR="$TAR" RAYONET_TOOLCHAIN=stable \
    RAYONET_TASK="$task" RAYONET_COUNT="$count" \
    "$BIN" 2>&1 | tee "/tmp/rayonet-scenario-$name.log"
}
expect() { # name, pattern, description
  if grep -qE "$2" "/tmp/rayonet-scenario-$1.log"; then
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
if grep -qE 'state leaf-a Building' "/tmp/rayonet-scenario-cache.log"; then
  echo "  FAIL: cache hit should skip Building"; fails=$((fails + 1))
else
  echo "  PASS: cache hit skips Building"
fi
expect cache 'ok: 10 results' 'cached job completed'

run skip-install leaf-b
if grep -qE 'state leaf-b Installing' "/tmp/rayonet-scenario-skip-install.log"; then
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
if grep -qE 'state leaf-blocked Ready' "/tmp/rayonet-scenario-blocked.log"; then
  echo "  FAIL: blocked host should never become Ready"; fails=$((fails + 1))
else
  echo "  PASS: blocked host never becomes Ready"
fi

# Work-share: a CPU-bound batch across a full-speed and a throttled host. Warm
# both build caches with a cheap pass first so the cap bites task execution.
run fastslow-warmup "leaf-fast,leaf-slow" double 1
run fastslow "leaf-fast,leaf-slow" crunch 40
expect fastslow 'ok: 40 results' 'every task completed'
fast=$(grep 'share leaf-fast ' "/tmp/rayonet-scenario-fastslow.log" | awk '{print $NF}')
slow=$(grep 'share leaf-slow ' "/tmp/rayonet-scenario-fastslow.log" | awk '{print $NF}')
if [ -n "${fast:-}" ] && [ -n "${slow:-}" ] && [ "$fast" -gt "$slow" ]; then
  echo "  PASS: fast host took a larger share ($fast vs $slow)"
else
  echo "  FAIL: expected fast share > slow share (fast=${fast:-?} slow=${slow:-?})"
  fails=$((fails + 1))
fi

echo
if [ "$fails" -eq 0 ]; then echo "ALL SCENARIOS PASSED"; else echo "$fails CHECK(S) FAILED"; fi
exit "$fails"
