#!/usr/bin/env bash
# Tear the harness network down. Pass --volumes to also drop named volumes.
set -euo pipefail
cd "$(dirname "$0")"
docker compose down --remove-orphans "$@"
