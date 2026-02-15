#!/bin/bash
set -euo pipefail

if docker compose version >/dev/null 2>&1; then
  DC=(docker compose -f docker/docker-compose.yml --env-file .env)
else
  DC=(docker-compose -f docker/docker-compose.yml --env-file .env)
fi

echo "Stopping trading services (data preserved)..."
"${DC[@]}" stop

echo "Services stopped."
