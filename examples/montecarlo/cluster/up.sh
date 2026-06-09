#!/usr/bin/env bash
# Bring up the Monte Carlo swarm: generate a throwaway key, build the blank
# worker image, start the workers, and write the fleet list the example reads.
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p secrets
[ -f secrets/id ] || ssh-keygen -t ed25519 -N "" -C montecarlo -q -f secrets/id
cp secrets/id.pub secrets/authorized_keys

docker build -t montecarlo-worker:latest .
docker compose up -d

# One "host port" line per worker, matching the published ports in compose.yml.
{
  echo "localhost 2201"
  echo "localhost 2202"
  echo "localhost 2203"
} > fleet

echo "swarm up. now run:  cargo run -p montecarlo"
