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

echo "WARNING: reset will delete ALL persisted Postgres and pgAdmin data."
read -r -p "Type 'yes' to continue: " confirm
if [ "$confirm" != "yes" ]; then
  echo "Reset cancelled."
  exit 0
fi

echo "Removing containers and volumes..."
"${DC[@]}" down -v --remove-orphans

echo "Rebuilding API image..."
"${DC[@]}" build api

echo "Starting Postgres..."
"${DC[@]}" up -d postgres

if [ -z "${POSTGRES_USER:-}" ] || [ -z "${POSTGRES_DB:-}" ]; then
  echo "POSTGRES_USER/POSTGRES_DB must be defined in .env"
  exit 1
fi

until "${DC[@]}" exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
  sleep 2
done

echo "Initializing database schema and seed data..."
"${DC[@]}" run --rm --entrypoint trading_init_db api

echo "Starting full stack..."
"${DC[@]}" up -d api webapp pgadmin

echo "Reset complete."
