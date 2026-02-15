-- Clean event-sourced schema (authoritative append-only ledger + snapshots).
-- Derived market state (orderbooks, open orders, balances, fees, trades) lives in Rust memory.

-- =========================================================
-- Extensions
-- =========================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =========================================================
-- Users & Authentication
-- =========================================================

CREATE TABLE users (
    id              BIGSERIAL PRIMARY KEY,
    username        VARCHAR(50) NOT NULL UNIQUE,
    email           VARCHAR(100) NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    is_admin        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_email ON users(email);

-- Refresh token rotation: store hash only.
CREATE TABLE refresh_tokens (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      TEXT NOT NULL UNIQUE,
    issued_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    revoked         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens(user_id);
CREATE INDEX idx_refresh_tokens_expires_at ON refresh_tokens(expires_at);

CREATE TABLE jwt_blacklist (
    jti             UUID PRIMARY KEY,
    revoked_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- Products, Currencies, Markets (static reference data)
-- =========================================================

CREATE TABLE currencies (
    id      SMALLSERIAL PRIMARY KEY,
    name    VARCHAR(5) NOT NULL UNIQUE
);

CREATE TABLE products (
    id      SMALLSERIAL PRIMARY KEY,
    name    VARCHAR(5) NOT NULL UNIQUE
);

CREATE TABLE markets (
    id          SMALLSERIAL PRIMARY KEY,
    product_id  SMALLINT NOT NULL REFERENCES products(id),
    currency_id SMALLINT NOT NULL REFERENCES currencies(id),
    UNIQUE (product_id, currency_id)
);

CREATE INDEX idx_markets_product_currency ON markets(product_id, currency_id);

-- =========================================================
-- Event Ledger (authoritative append-only log)
-- =========================================================

CREATE TABLE event_types (
    id      SMALLSERIAL PRIMARY KEY,
    code    VARCHAR(50) NOT NULL UNIQUE
);

-- Keep this list aligned with Rust.
INSERT INTO event_types (code) VALUES
('USER_CREATED'),
('USER_UPDATED'),
('USER_PASSWORD_CHANGED'),
('FUNDS_DEPOSITED'),
('FUNDS_WITHDRAWN'),
('PRODUCTS_DEPOSITED'),
('PRODUCTS_WITHDRAWN'),
('FEE_RATE_SET'),
('ORDER_ACCEPTED'),
('ORDER_CANCELLED'),
('TRADE_EXECUTED')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE events (
    event_id        BIGSERIAL PRIMARY KEY,
    event_type_id   SMALLINT NOT NULL REFERENCES event_types(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Correlation / request tracing (optional but useful for 202 submit flows)
    request_id      UUID,

    -- Common dimensions (nullable depending on event_type)
    user_id         BIGINT REFERENCES users(id),
    market_id       SMALLINT REFERENCES markets(id),
    order_id        BIGINT,

    -- Order fields
    side            BOOLEAN,           -- true=buy, false=sell
    price_cents     BIGINT,
    qty_units       BIGINT,
    expires_at      TIMESTAMPTZ,

    -- Trade fields
    maker_order_id  BIGINT,
    taker_order_id  BIGINT,
    buy_user_id     BIGINT REFERENCES users(id),
    sell_user_id    BIGINT REFERENCES users(id),
    fee_cents       BIGINT,

    -- Optional structured payload for event-type-specific fields
    payload         JSONB
);

-- Narrow indexes to support replay & queries.
CREATE INDEX idx_events_type_id ON events(event_type_id, event_id);
CREATE INDEX idx_events_created_at ON events(created_at);
CREATE INDEX idx_events_market_id ON events(market_id, event_id);
CREATE INDEX idx_events_order_id ON events(order_id, event_id);
CREATE INDEX idx_events_user_id ON events(user_id, event_id);
CREATE INDEX idx_events_request_id ON events(request_id);

-- =========================================================
-- Snapshots (append-only checkpoints of derived engine state)
-- =========================================================

CREATE TABLE snapshots (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    event_id_hi     BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    format          VARCHAR(20) NOT NULL,   -- e.g. "bincode"
    compression     VARCHAR(20) NOT NULL,   -- e.g. "zstd"
    state           BYTEA NOT NULL
);

CREATE INDEX idx_snapshots_event_id_hi ON snapshots(event_id_hi DESC);
