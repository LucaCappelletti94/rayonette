#!/usr/bin/env bash
# Tear the swarm down.
set -euo pipefail
cd "$(dirname "$0")"
docker compose down --remove-orphans
rm -f fleet
