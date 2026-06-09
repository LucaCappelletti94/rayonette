#!/usr/bin/env bash
# Build the harness images, generate the throwaway ssh key, bring the network
# up, write the coordinator-side ssh config, and verify the topology. Idempotent.
set -euo pipefail
cd "$(dirname "$0")"

SECRETS=./secrets
KEY="$SECRETS/id_ed25519"
CONFIG="$SECRETS/ssh_config"

mkdir -p "$SECRETS"
if [ ! -f "$KEY" ]; then
  ssh-keygen -t ed25519 -N "" -C rayonette-harness -f "$KEY" >/dev/null
fi
cp "$KEY.pub" "$SECRETS/authorized_keys"

# Base image first (the rust image derives from it), then the rest via compose.
docker build -f Dockerfile.base -t rayonette-harness-base:latest .
docker compose build
docker compose up -d

# The coordinator only has a real address for the bastion; every other node is
# reached by name through a ProxyJump chain.
cat > "$CONFIG" <<EOF
Host *
  User rayonette
  IdentityFile $(cd "$SECRETS" && pwd)/id_ed25519
  IdentitiesOnly yes
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR

Host bastion
  HostName 127.0.0.1
  Port 2201

Host leaf-a leaf-b leaf-blocked leaf-fast leaf-slow
  ProxyJump bastion

Host leaf-deep
  ProxyJump bastion,relay
EOF
echo "coordinator ssh config: $CONFIG"

echo "waiting for bastion sshd..."
for _ in $(seq 1 40); do
  if ssh -F "$CONFIG" -o ConnectTimeout=2 bastion true 2>/dev/null; then break; fi
  sleep 0.5
done

echo "=== connectivity (each line proves the jump chain to that node) ==="
for h in bastion leaf-a leaf-b leaf-deep leaf-blocked; do
  ssh -F "$CONFIG" -o ConnectTimeout=10 "$h" 'echo "  reached $(hostname)"' \
    || echo "  $h: UNREACHABLE"
done

echo "=== rust presence and egress ==="
ssh -F "$CONFIG" leaf-a 'command -v cargo >/dev/null && echo "  leaf-a: rust present" || echo "  leaf-a: no rust (expected)"'
ssh -F "$CONFIG" leaf-b 'command -v ~/.cargo/bin/cargo >/dev/null && echo "  leaf-b: rust present (expected)" || echo "  leaf-b: no rust"'
ssh -F "$CONFIG" leaf-a 'curl -sSf -m 15 https://static.rust-lang.org >/dev/null 2>&1 && echo "  leaf-a: has egress (expected)" || echo "  leaf-a: no egress"'
ssh -F "$CONFIG" leaf-blocked 'curl -sSf -m 8 https://static.rust-lang.org >/dev/null 2>&1 && echo "  leaf-blocked: has egress (UNEXPECTED)" || echo "  leaf-blocked: no egress (expected)"'

echo "harness up."
