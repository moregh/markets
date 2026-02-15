# Market Postgres (Rust)

This repository is organized into four deployable parts:

- `docker/`: Docker build and compose configuration
- `rust_api/`: Rust API + in-memory order matcher
- `webapp/`: static web client
- `testing/`: soak/stress and helper scripts

All user-configurable runtime settings are centralized in **`.env`** at the repository root.

## Prerequisites

- Docker + Docker Compose
- Bash
- (Optional) Python 3 for scripts in `testing/`

## Configuration

Edit `.env` before running:

- Postgres connection and credentials
- JWT settings
- CORS
- admin seed credentials
- initial market seed data
- matcher/read-cache tuning knobs

If you need a template, copy `.env.example`.

## Lifecycle Commands

### Start (preserves data)

```bash
./start.sh
```

What it does:

1. Starts Postgres
2. Waits for DB health
3. Initializes DB only if empty
4. Starts API, webapp, and pgAdmin

### Stop (preserves data)

```bash
./stop.sh
```

This stops containers without deleting volumes.

### Reset (deletes data)

```bash
./reset.sh
```

This is the **only** command that removes persisted data (`docker compose down -v`), then reinitializes schema and seed data.

## Service Endpoints

- API: `http://localhost:8000`
- Webapp: `http://localhost:1234`
- pgAdmin: `http://localhost:5050`
- Postgres: `localhost:5432`

## Basic Interaction (curl)

### 1. Login

```bash
curl -sS -X POST http://localhost:8000/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"ChangeMe123!"}'
```

### 2. List markets

```bash
curl -sS http://localhost:8000/markets
```

### 3. Submit async order

```bash
curl -sS -X POST http://localhost:8000/orders/async \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H 'Content-Type: application/json' \
  -d '{"market_id":2,"buy":true,"quantity":10,"price":"1000.00","expires_in_seconds":30}'
```

### 4. Read order book and trades

```bash
curl -sS http://localhost:8000/markets/2/orderbook
curl -sS http://localhost:8000/markets/2/trades?limit=50
```

## Testing Scripts

Located in `testing/`:

- `testing/test_users.py`: soak-load multi-user trading simulation
- `testing/test_create_users.py`: bulk test user creation
- `testing/scripts/stress_api_contention.py`: contention stress helper
- `testing/scripts/test_isolated_matcher.py`: isolated matcher checks

Examples:

```bash
python3 testing/test_users.py
python3 testing/test_create_users.py
```

## Logs

```bash
docker compose -f docker/docker-compose.yml --env-file .env logs -f api
docker compose -f docker/docker-compose.yml --env-file .env logs -f postgres
```
