#!/bin/bash
set -euo pipefail

if docker compose version >/dev/null 2>&1; then
  DC=(docker compose -f docker/docker-compose.yml --env-file .env)
else
  DC=(docker-compose -f docker/docker-compose.yml --env-file .env)
fi

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

echo "Starting trading services (preserving existing data)..."
"${DC[@]}" up -d postgres

echo "Waiting for PostgreSQL health..."
until "${DC[@]}" exec -T postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; do
  sleep 2
done

TABLES=$("${DC[@]}" exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'" 2>/dev/null | tr -d '[:space:]')
if [ -z "${TABLES}" ] || [ "${TABLES}" = "0" ]; then
  echo "Database is empty. Initializing schema/data..."
  "${DC[@]}" run --rm --entrypoint trading_init_db api
fi

"${DC[@]}" up -d api webapp pgadmin

echo "Services started."
echo "API: http://localhost:${API_PORT:-8000}"
echo "Webapp: http://localhost:1234"
echo "pgAdmin: http://localhost:5050"
