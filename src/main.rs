use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use bcrypt::{hash, verify, DEFAULT_COST};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Postgres, QueryBuilder, Row};
use tokio::sync::{mpsc, Mutex, RwLock};
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

mod auth;
mod config;
mod engine;
mod error;
mod state;
mod store;
mod tasks;

use crate::auth::{admin_user, auth_user, make_access_token, make_refresh_token, sha256_hex, Claims};
use crate::config::load_config;
use crate::engine::{EngineState, Wallet};
use crate::error::ApiError;
use crate::state::{lock_read, lock_write, try_lock_read, AppState, CancelJob, CancelStatus, OrderIntent, PerfCounters, SubmitJob, SubmitStatus, SystemReadCache};
use crate::store::{load_event_type_ids, reload_market_cache, reload_name_caches, reload_user_cache, replay_from_db};
use crate::tasks::{prime_read_caches, start_background_tasks};

// ===== Schema-aligned event types (codes stored in DB; ids loaded at startup) =====
const EVT_USER_CREATED: &str = "USER_CREATED";
const EVT_USER_UPDATED: &str = "USER_UPDATED";
const EVT_USER_PASSWORD_CHANGED: &str = "USER_PASSWORD_CHANGED";
const EVT_FUNDS_DEPOSITED: &str = "FUNDS_DEPOSITED";
const EVT_FUNDS_WITHDRAWN: &str = "FUNDS_WITHDRAWN";
const EVT_PRODUCTS_DEPOSITED: &str = "PRODUCTS_DEPOSITED";
const EVT_PRODUCTS_WITHDRAWN: &str = "PRODUCTS_WITHDRAWN";
const EVT_FEE_RATE_SET: &str = "FEE_RATE_SET";
const EVT_ORDER_ACCEPTED: &str = "ORDER_ACCEPTED";
const EVT_ORDER_CANCELLED: &str = "ORDER_CANCELLED";
const EVT_TRADE_EXECUTED: &str = "TRADE_EXECUTED";

const MAX_MY_ORDERS: usize = 400;

// Async submit (202) writer batching.
const ASYNC_SUBMIT_QUEUE_CAP: usize = 200_000;
const ASYNC_SUBMIT_BATCH_MAX: usize = 24;
const ASYNC_SUBMIT_FLUSH_MS: u64 = 1;
const ASYNC_SUBMIT_STATUS_TTL_MS: i64 = 5 * 60 * 1000;
const ASYNC_CANCEL_QUEUE_CAP: usize = 100_000;

// Snapshotting.
const SNAPSHOT_EVERY_EVENTS: i64 = 20_000_000;
const SNAPSHOT_MIN_INTERVAL_SECS: i64 = 3600;
const ENGINE_EXPIRY_POLL_MS: u64 = 100;
const ORDERBOOK_DEFAULT_LIMIT_PER_SIDE: usize = 300;
const ORDERBOOK_MAX_LIMIT_PER_SIDE: usize = 300;
const MARKET_LOCK_SHARDS: usize = 1024;

#[derive(Debug, Serialize, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    token_type: String,
}

#[derive(Debug, Deserialize)]
struct UserRegister {
    username: String,
    email: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct UserLogin {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct RefreshTokenRequest {
    refresh_token: String,
}

#[derive(Debug, Deserialize)]
struct PasswordChangeRequest {
    current_password: String,
    new_password: String,
}

#[derive(Debug, Deserialize)]
struct PasswordAdminResetRequest {
    new_password: String,
}

#[derive(Debug, Deserialize)]
struct OrderCreate {
    market_id: i16,
    buy: bool,
    quantity: i64,
    price: String,
    expires_in_seconds: Option<i64>,
}

#[derive(Debug, Serialize, Clone)]
struct OrderResponse {
    id: i64,
    user_id: i64,
    market_id: i16,
    state: String,
    price: Decimal,
    quantity: i64,
    remaining_quantity: i64,
    buy: bool,
    created_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct TradesQuery {
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OrderBookQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct TxJournalQuery {
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct StatsOrder {
    total_active: i64,
    active_buy_orders: i64,
    active_sell_orders: i64,
}

#[derive(Debug, Serialize)]
struct StatsMatching {
    last_hour_count: i64,
}

#[derive(Debug, Serialize)]
struct SystemStats {
    orders: StatsOrder,
    matching: StatsMatching,
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn sign_order_action_token(secret: &str, user_id: i64, order_id: i64, action: &str, ttl_seconds: i64) -> String {
    let now = now_epoch_secs();
    let expiry = ((now / ttl_seconds) + 1) * ttl_seconds;
    let payload = serde_json::json!({"uid": user_id, "oid": order_id, "act": action, "exp": expiry});
    let payload_bytes = serde_json::to_vec(&payload).expect("json");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac");
    mac.update(&payload_bytes);
    let sig = mac.finalize().into_bytes();
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload_bytes),
        URL_SAFE_NO_PAD.encode(sig)
    )
}

fn verify_order_action_token(secret: &str, token: &str, expected_action: &str, expected_user_id: i64) -> Result<i64, ApiError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 2 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Invalid action token"));
    }
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[0])
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid action token"))?;
    let sig_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid action token"))?;

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac");
    mac.update(&payload_bytes);
    mac.verify_slice(&sig_bytes)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid action token"))?;

    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid action token"))?;

    let uid = payload.get("uid").and_then(|v| v.as_i64()).unwrap_or(0);
    let oid = payload.get("oid").and_then(|v| v.as_i64()).unwrap_or(0);
    let act = payload.get("act").and_then(|v| v.as_str()).unwrap_or("");
    let exp = payload.get("exp").and_then(|v| v.as_i64()).unwrap_or(0);
    if uid != expected_user_id || act != expected_action || oid <= 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Invalid action token"));
    }
    if now_epoch_secs() > exp {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Action token expired"));
    }
    Ok(oid)
}

fn parse_price_to_cents(price: &str) -> Result<i64, ApiError> {
    let d = Decimal::from_str(price).map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid price"))?;
    if d <= Decimal::ZERO {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Invalid price"));
    }
    let cents = (d.round_dp(2) * Decimal::from(100)).to_i64().unwrap_or(0);
    if cents <= 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Invalid price"));
    }
    Ok(cents)
}

fn parse_signed_price_to_cents(price: &str) -> Result<i64, ApiError> {
    let d = Decimal::from_str(price).map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid amount"))?;
    if d == Decimal::ZERO {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Amount must be non-zero"));
    }
    let cents = (d.round_dp(2) * Decimal::from(100)).to_i64().unwrap_or(0);
    if cents == 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Amount must be non-zero"));
    }
    Ok(cents)
}

fn cents_to_decimal(cents: i64) -> Decimal {
    (Decimal::from(cents) / Decimal::from(100)).round_dp(2)
}

#[derive(Debug, Serialize)]
struct AsyncSubmitResponse {
    request_id: String,
    order_id: i64,
    status: String,
}

#[derive(Debug, Serialize)]
struct AsyncCancelResponse {
    request_id: String,
    order_id: i64,
    status: String,
}

// ===== HTTP handlers =====

async fn health_check(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    sqlx::query("SELECT 1")
        .fetch_one(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, format!("db error: {e}")))?;
    Ok(Json(serde_json::json!({
        "status":"healthy",
        "database":"connected",
        "engine_ready": state.engine_ready.load(Ordering::Acquire)
    })))
}

async fn require_engine_ready(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if !state.engine_ready.load(Ordering::Acquire) {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "engine warming up",
        ));
    }
    Ok(next.run(req).await)
}

async fn get_system_stats(State(state): State<AppState>) -> Result<Json<SystemStats>, ApiError> {
    let snap = lock_read(&state.system_read_cache, "main.get_system_stats.system_read").await.clone();
    Ok(Json(SystemStats {
        orders: StatsOrder {
            total_active: snap.total_active,
            active_buy_orders: snap.active_buy_orders,
            active_sell_orders: snap.active_sell_orders,
        },
        matching: StatsMatching { last_hour_count: snap.last_hour_count },
    }))
}

async fn list_markets(State(state): State<AppState>) -> Result<Json<Vec<serde_json::Value>>, ApiError> {
    let markets = lock_read(&state.markets, "main.list_markets.markets_read").await;
    let mut out = Vec::new();
    for m in markets.values() {
        out.push(serde_json::json!({
            "id": m.id,
            "product": m.product,
            "currency": m.currency,
            "ticker": m.ticker
        }));
    }
    out.sort_by(|a, b| a.get("ticker").and_then(|v| v.as_str()).cmp(&b.get("ticker").and_then(|v| v.as_str())));
    Ok(Json(out))
}

async fn get_market(State(state): State<AppState>, Path(market_id): Path<i16>) -> Result<Json<serde_json::Value>, ApiError> {
    let markets = lock_read(&state.markets, "main.get_market.markets_read").await;
    let m = markets.get(&market_id).ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Market not found"))?;
    Ok(Json(serde_json::json!({"id": m.id, "product": m.product, "currency": m.currency, "ticker": m.ticker})))
}

async fn get_market_stats(State(state): State<AppState>, Path(market_id): Path<i16>) -> Result<Json<serde_json::Value>, ApiError> {
    // Prioritize cache refresh for markets with active viewers to reduce UI jitter.
    state.mark_market_activity(market_id);
    let (best_bid, best_ask, buys, sells) = if let Some(cache) = state.market_read_cache.get(&market_id) {
        (
            cache.best_bid_cents,
            cache.best_ask_cents,
            cache.active_buy_orders,
            cache.active_sell_orders,
        )
    } else {
        (0, 0, 0, 0)
    };
    let crossed = best_bid > 0 && best_ask > 0 && best_bid >= best_ask;
    Ok(Json(serde_json::json!({
        "best_bid": if best_bid > 0 { (best_bid as f64) / 100.0 } else { 0.0 },
        "best_ask": if best_ask > 0 { (best_ask as f64) / 100.0 } else { 0.0 },
        "active_buy_orders": buys,
        "active_sell_orders": sells,
        "crossed": crossed
    })))
}

async fn get_order_book(
    State(state): State<AppState>,
    Path(market_id): Path<i16>,
    Query(q): Query<OrderBookQuery>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let maybe_user_id = auth_user(&state, &headers).await.ok().map(|u| u.user_id);
    {
        let markets = lock_read(&state.markets, "main.get_order_book.markets_read").await;
        if !markets.contains_key(&market_id) {
            return Err(ApiError::new(StatusCode::NOT_FOUND, "Market not found"));
        }
    }
    // Active orderbook readers should drive cache refresh priority.
    state.mark_market_activity(market_id);
    let per_side_limit = q
        .limit
        .unwrap_or(ORDERBOOK_DEFAULT_LIMIT_PER_SIDE)
        .max(1)
        .min(ORDERBOOK_MAX_LIMIT_PER_SIDE);

    #[derive(Clone)]
    struct ObRow {
        order_id: i64,
        price_cents: i64,
        remaining: i64,
        mine: bool,
        expires_in_seconds: Option<i64>,
    }

    let (buy_rows, sell_rows, buys_truncated, sells_truncated) = if let Some(cache) = state.market_read_cache.get(&market_id) {
        let buys_truncated = cache.buys.len() > per_side_limit;
        let sells_truncated = cache.sells.len() > per_side_limit;
        let buy_rows: Vec<ObRow> = cache
            .buys
            .iter()
            .take(per_side_limit)
            .map(|o| ObRow {
                order_id: o.order_id,
                price_cents: o.price_cents,
                remaining: o.remaining,
                mine: maybe_user_id.map(|uid| o.user_id == uid).unwrap_or(false),
                expires_in_seconds: o.expires_in_seconds,
            })
            .collect();
        let sell_rows: Vec<ObRow> = cache
            .sells
            .iter()
            .take(per_side_limit)
            .map(|o| ObRow {
                order_id: o.order_id,
                price_cents: o.price_cents,
                remaining: o.remaining,
                mine: maybe_user_id.map(|uid| o.user_id == uid).unwrap_or(false),
                expires_in_seconds: o.expires_in_seconds,
            })
            .collect();
        (buy_rows, sell_rows, buys_truncated, sells_truncated)
    } else {
        (Vec::new(), Vec::new(), false, false)
    };
    let crossed = matches!(
        (buy_rows.first(), sell_rows.first()),
        (Some(b), Some(s)) if b.price_cents >= s.price_cents
    );

    let buys: Vec<serde_json::Value> = buy_rows
        .into_iter()
        .map(|o| {
            let price_f = (o.price_cents as f64) / 100.0;
            let cancel = if o.mine {
                Some(sign_order_action_token(
                    &state.cfg.jwt.secret_key,
                    maybe_user_id.unwrap_or_default(),
                    o.order_id,
                    "cancel",
                    900,
                ))
            } else {
                None
            };
            serde_json::json!({
                "order_id": o.order_id,
                "price": price_f,
                "quantity": o.remaining,
                "is_mine": o.mine,
                "cancel_token": cancel,
                "expires_in_seconds": o.expires_in_seconds,
            })
        })
        .collect();

    let sells: Vec<serde_json::Value> = sell_rows
        .into_iter()
        .map(|o| {
            let price_f = (o.price_cents as f64) / 100.0;
            let cancel = if o.mine {
                Some(sign_order_action_token(
                    &state.cfg.jwt.secret_key,
                    maybe_user_id.unwrap_or_default(),
                    o.order_id,
                    "cancel",
                    900,
                ))
            } else {
                None
            };
            serde_json::json!({
                "order_id": o.order_id,
                "price": price_f,
                "quantity": o.remaining,
                "is_mine": o.mine,
                "cancel_token": cancel,
                "expires_in_seconds": o.expires_in_seconds,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "meta": {
            "limit_per_side": per_side_limit,
            "buys_truncated": buys_truncated,
            "sells_truncated": sells_truncated,
            "crossed": crossed
        },
        "buys": buys,
        "sells": sells
    })))
}

async fn get_recent_trades(State(state): State<AppState>, Path(market_id): Path<i16>, Query(q): Query<TradesQuery>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let maybe_user_id = auth_user(&state, &headers).await.ok().map(|u| u.user_id);
    {
        let markets = lock_read(&state.markets, "main.get_recent_trades.markets_read").await;
        if !markets.contains_key(&market_id) {
            return Err(ApiError::new(StatusCode::NOT_FOUND, "Market not found"));
        }
    }
    // Active trades readers should drive cache refresh priority.
    state.mark_market_activity(market_id);

    let limit = q.limit.unwrap_or(50).max(1).min(250) as usize;
    let rows: Vec<(i64, i64, i64, i64, i64)> = state
        .market_read_cache
        .get(&market_id)
        .map(|cache| {
            cache
                .trades
                .iter()
                .take(limit)
                .map(|t| (t.ts_ms, t.qty_units, t.price_cents, t.buy_user_id, t.sell_user_id))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut out = Vec::with_capacity(rows.len());
    for (ts_ms, qty_units, price_cents, buy_uid, sell_uid) in rows {
        let mine = maybe_user_id
            .map(|uid| buy_uid == uid || sell_uid == uid)
            .unwrap_or(false);
        let my_side = if !mine {
            "NONE"
        } else if Some(buy_uid) == maybe_user_id {
            "BUY"
        } else {
            "SELL"
        };
        out.push(serde_json::json!({
            "timestamp": (ts_ms as f64) / 1000.0,
            "quantity": qty_units,
            "price": (price_cents as f64) / 100.0,
            "my_side": my_side,
        }));
    }
    Ok(Json(serde_json::json!({"trades": out})))
}

async fn get_my_wallet(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let uid = user.user_id;
    if let Some(cached) = state.wallet_read_cache.get(&uid) {
        return Ok(Json(cached.value().clone()));
    }
    let eng = if let Some(eng) = try_lock_read(&state.engine, "main.get_my_wallet.engine_try_read") {
        Some(eng)
    } else {
        tokio::time::timeout(
            Duration::from_millis(30),
            lock_read(&state.engine, "main.get_my_wallet.engine_read_fallback"),
        )
        .await
        .ok()
    };
    let (w, cn, pn) = match (
        eng,
        try_lock_read(&state.currency_names, "main.get_my_wallet.currency_names_try_read"),
        try_lock_read(&state.product_names, "main.get_my_wallet.product_names_try_read"),
    ) {
        (Some(eng), Some(cn), Some(pn)) => (
            eng.wallets.get(&uid).cloned().unwrap_or_else(Wallet::new),
            cn,
            pn,
        ),
        _ => {
            if let Some(cached) = state.wallet_read_cache.get(&uid) {
                return Ok(Json(cached.value().clone()));
            }
            return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "engine busy"));
        }
    };

    let mut currencies = serde_json::Map::new();
    let mut currencies_reserved = serde_json::Map::new();
    let mut currencies_available = serde_json::Map::new();
    for (cid, cents) in &w.tot_cents {
        if let Some(name) = cn.get(cid) {
            let tot = *cents;
            let res = *w.res_cents.get(cid).unwrap_or(&0);
            let avail = (tot - res).max(0);
            currencies.insert(name.clone(), serde_json::Value::String(cents_to_decimal(tot).to_string()));
            currencies_reserved.insert(name.clone(), serde_json::Value::String(cents_to_decimal(res).to_string()));
            currencies_available.insert(name.clone(), serde_json::Value::String(cents_to_decimal(avail).to_string()));
        }
    }
    let mut products = serde_json::Map::new();
    let mut products_reserved = serde_json::Map::new();
    let mut products_available = serde_json::Map::new();
    for (pid, units) in &w.tot_units {
        if let Some(name) = pn.get(pid) {
            let tot = *units;
            let res = *w.res_units.get(pid).unwrap_or(&0);
            let avail = (tot - res).max(0);
            products.insert(name.clone(), serde_json::Value::Number(serde_json::Number::from(tot)));
            products_reserved.insert(name.clone(), serde_json::Value::Number(serde_json::Number::from(res)));
            products_available.insert(name.clone(), serde_json::Value::Number(serde_json::Number::from(avail)));
        }
    }
    let out = serde_json::json!({
        "currencies": currencies,
        "products": products,
        "reserved": {
            "currencies": currencies_reserved,
            "products": products_reserved
        },
        "available": {
            "currencies": currencies_available,
            "products": products_available
        }
    });
    state.wallet_read_cache.insert(uid, out.clone());
    Ok(Json(out))
}

async fn get_my_wallet_details(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let uid = user.user_id;
    if let Some(cached) = state.wallet_details_read_cache.get(&uid) {
        return Ok(Json(cached.value().clone()));
    }
    let eng = if let Some(eng) = try_lock_read(&state.engine, "main.get_my_wallet_details.engine_try_read") {
        Some(eng)
    } else {
        tokio::time::timeout(
            Duration::from_millis(30),
            lock_read(&state.engine, "main.get_my_wallet_details.engine_read_fallback"),
        )
        .await
        .ok()
    };
    let (w, user_order_ids, orders_by_id, cn, pn) = match (
        eng,
        try_lock_read(&state.currency_names, "main.get_my_wallet_details.currency_names_try_read"),
        try_lock_read(&state.product_names, "main.get_my_wallet_details.product_names_try_read"),
    ) {
        (Some(eng), Some(cn), Some(pn)) => {
            let w = eng.wallets.get(&uid).cloned().unwrap_or_else(Wallet::new);
            let order_ids = eng.user_orders.get(&uid).cloned().unwrap_or_default();
            let mut order_map = HashMap::new();
            for oid in &order_ids {
                if let Some(o) = eng.orders.get(oid).cloned() {
                    order_map.insert(*oid, o);
                }
            }
            (w, order_ids, order_map, cn, pn)
        }
        _ => {
            if let Some(cached) = state.wallet_details_read_cache.get(&uid) {
                return Ok(Json(cached.value().clone()));
            }
            return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "engine busy"));
        }
    };

    let mut currencies = serde_json::Map::new();
    for (cid, total) in &w.tot_cents {
        if let Some(name) = cn.get(cid) {
            let reserved = *w.res_cents.get(cid).unwrap_or(&0);
            let available = (*total - reserved).max(0);
            let deposited = *w.dep_cents.get(cid).unwrap_or(&0);
            let withdrawn = *w.wdr_cents.get(cid).unwrap_or(&0);
            let fees_paid = *w.fees_paid_cents.get(cid).unwrap_or(&0);
            currencies.insert(
                name.clone(),
                serde_json::json!({
                    "total": cents_to_decimal(*total).to_string(),
                    "reserved": cents_to_decimal(reserved).to_string(),
                    "available": cents_to_decimal(available).to_string(),
                    "deposited": cents_to_decimal(deposited).to_string(),
                    "withdrawn": cents_to_decimal(withdrawn).to_string(),
                    "fees_paid": cents_to_decimal(fees_paid).to_string(),
                    "net_flow": cents_to_decimal(deposited - withdrawn).to_string()
                }),
            );
        }
    }

    let mut products = serde_json::Map::new();
    for (pid, total) in &w.tot_units {
        if let Some(name) = pn.get(pid) {
            let reserved = *w.res_units.get(pid).unwrap_or(&0);
            let available = (*total - reserved).max(0);
            let deposited = *w.dep_units.get(pid).unwrap_or(&0);
            let withdrawn = *w.wdr_units.get(pid).unwrap_or(&0);
            products.insert(
                name.clone(),
                serde_json::json!({
                    "total": total,
                    "reserved": reserved,
                    "available": available,
                    "deposited": deposited,
                    "withdrawn": withdrawn,
                    "net_flow": deposited - withdrawn
                }),
            );
        }
    }

    let mut active_orders = 0i64;
    let mut completed_orders = 0i64;
    for oid in &user_order_ids {
        let Some(o) = orders_by_id.get(oid) else { continue; };
        if o.status == "OPEN" || o.status == "PARTIAL" || o.status == "PENDING_SUBMIT" {
            active_orders += 1;
        } else {
            completed_orders += 1;
        }
    }

    let out = serde_json::json!({
        "balances": {
            "currencies": currencies,
            "products": products
        },
        "orders": {
            "active": active_orders,
            "completed": completed_orders
        }
    });
    state.wallet_details_read_cache.insert(uid, out.clone());
    Ok(Json(out))
}

async fn get_my_orders(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let uid = user.user_id;
    if let Some(cached) = state.my_orders_read_cache.get(&uid) {
        return Ok(Json(cached.value().clone()));
    }
    let eng = if let Some(eng) = try_lock_read(&state.engine, "main.get_my_orders.engine_try_read") {
        Some(eng)
    } else {
        tokio::time::timeout(
            Duration::from_millis(30),
            lock_read(&state.engine, "main.get_my_orders.engine_read_fallback"),
        )
        .await
        .ok()
    };
    let rows: Vec<OrderResponse> = if let Some(eng) = eng {
        let mut active = Vec::new();
        let mut historical = Vec::new();
        if let Some(order_ids) = eng.user_orders.get(&uid) {
            for oid in order_ids.iter().rev() {
                let Some(o) = eng.orders.get(oid) else { continue; };
                let row = OrderResponse {
                    id: o.order_id,
                    user_id: o.user_id,
                    market_id: o.market_id,
                    state: o.status.clone(),
                    price: cents_to_decimal(o.price_cents),
                    quantity: o.quantity,
                    remaining_quantity: o.remaining,
                    buy: o.buy,
                    created_at: o.created_at,
                    expires_at: o.expires_at,
                };
                if o.status == "OPEN" || o.status == "PARTIAL" || o.status == "PENDING_SUBMIT" {
                    active.push(row);
                } else {
                    historical.push(row);
                }
            }
        }
        if historical.len() > MAX_MY_ORDERS {
            historical.truncate(MAX_MY_ORDERS);
        }
        active.extend(historical);
        active
    } else if let Some(cached) = state.my_orders_read_cache.get(&uid) {
        return Ok(Json(cached.value().clone()));
    } else {
        return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "engine busy"));
    };
    let out = serde_json::to_value(&rows)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("serialize error: {e}")))?;
    state.my_orders_read_cache.insert(uid, out.clone());
    Ok(Json(out))
}

async fn get_order(State(state): State<AppState>, Path(order_id): Path<i64>, headers: HeaderMap) -> Result<Json<OrderResponse>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let eng = lock_read(&state.engine, "main.get_order.engine_read").await;
    let o = eng.orders.get(&order_id).cloned().ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Order not found"))?;
    if o.user_id != user.user_id && !user.is_admin {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "Access denied"));
    }
    Ok(Json(OrderResponse {
        id: o.order_id,
        user_id: o.user_id,
        market_id: o.market_id,
        state: o.status.clone(),
        price: cents_to_decimal(o.price_cents),
        quantity: o.quantity,
        remaining_quantity: o.remaining,
        buy: o.buy,
        created_at: o.created_at,
        expires_at: o.expires_at,
    }))
}

async fn get_order_matches(State(state): State<AppState>, Path(order_id): Path<i64>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let eng = lock_read(&state.engine, "main.get_order_matches.engine_read").await;
    let o = eng.orders.get(&order_id).cloned().ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Order not found"))?;
    if o.user_id != user.user_id && !user.is_admin {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "Access denied"));
    }
    let list = eng.order_matches.get(&order_id).cloned().unwrap_or_default();
    let mut matches = Vec::new();
    for t in list {
        matches.push(serde_json::json!({
            "timestamp": (t.ts_ms as f64) / 1000.0,
            "quantity": t.qty_units,
            "price": cents_to_decimal(t.price_cents).to_f64().unwrap_or(0.0),
        }));
    }
    Ok(Json(serde_json::json!({
        "order_id": order_id,
        "side": if o.buy { "BUY" } else { "SELL" },
        "matches": matches
    })))
}

async fn create_order_async(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<OrderCreate>) -> Result<(StatusCode, Json<AsyncSubmitResponse>), ApiError> {
    let user = auth_user(&state, &headers).await?;
    state.perf.submit_received.fetch_add(1, Ordering::Relaxed);
    if req.quantity <= 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "quantity must be > 0"));
    }
    let price_cents = parse_price_to_cents(&req.price)?;
    let expires_at = req.expires_in_seconds.filter(|v| *v > 0).map(|sec| Utc::now() + ChronoDuration::seconds(sec));

    let order_id = state.next_order_id.fetch_add(1, Ordering::Relaxed);
    let request_id = Uuid::new_v4();
    let created_at = Utc::now();
    let created_at_ms = created_at.timestamp_millis();

    state.submit_status.insert(request_id.to_string(), SubmitStatus::Received { order_id, created_at_ms });

    let job = SubmitJob {
        request_id,
        order_id,
        user_id: user.user_id,
        market_id: req.market_id,
        buy: req.buy,
        price_cents,
        qty_units: req.quantity,
        expires_at,
    };

    let intent = OrderIntent { job, created_at, created_at_ms };
    state.perf.order_intent_queue_len.fetch_add(1, Ordering::Relaxed);
    if let Err(_e) = state.order_intent_tx.try_send(intent) {
        state.perf.order_intent_queue_len.fetch_sub(1, Ordering::Relaxed);
        state.perf.submit_rejected_queue_full.fetch_add(1, Ordering::Relaxed);
        state.submit_status.insert(request_id.to_string(), SubmitStatus::Failed { order_id, error: "submit worker unavailable".to_string(), created_at_ms });
        return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "submit unavailable, please retry"));
    }

    Ok((StatusCode::ACCEPTED, Json(AsyncSubmitResponse { request_id: request_id.to_string(), order_id, status: "RECEIVED".to_string() })))
}

async fn get_order_request_status(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = auth_user(&state, &headers).await?;
    if let Some(v) = state.submit_status.get(&request_id) {
        return Ok(Json(serde_json::json!({"request_id": request_id, "status": v.value()})));
    }
    Err(ApiError::new(StatusCode::NOT_FOUND, "request not found"))
}

// Sync endpoint: wait briefly for confirmation to keep webapp usable.
async fn create_order_sync(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<OrderCreate>) -> Result<Json<OrderResponse>, ApiError> {
    let (code, Json(resp)) = create_order_async(State(state.clone()), headers.clone(), Json(req)).await?;
    if code != StatusCode::ACCEPTED {
        return Err(ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "submit failed"));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if tokio::time::Instant::now() > deadline {
            return Err(ApiError::new(StatusCode::ACCEPTED, "Order received; still confirming"));
        }
        if let Some(st) = state.submit_status.get(&resp.request_id) {
            if let SubmitStatus::Confirmed { order_id, .. } = st.value() {
                // Keep sync submit from contributing to read-lock starvation under load.
                if let Some(eng) = try_lock_read(&state.engine, "main.create_order_sync.engine_try_read") {
                    if let Some(o) = eng.orders.get(order_id) {
                        return Ok(Json(OrderResponse {
                            id: o.order_id,
                            user_id: o.user_id,
                            market_id: o.market_id,
                            state: o.status.clone(),
                            price: cents_to_decimal(o.price_cents),
                            quantity: o.quantity,
                            remaining_quantity: o.remaining,
                            buy: o.buy,
                            created_at: o.created_at,
                            expires_at: o.expires_at,
                        }));
                    }
                }
            }
            if let SubmitStatus::Failed { error, .. } = st.value() {
                return Err(ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("submit failed: {error}")));
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn cancel_order(State(state): State<AppState>, Path(order_id): Path<i64>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let (market_id, owner) = {
        let eng = lock_read(&state.engine, "main.cancel_order.lookup_engine_read").await;
        let o = eng.orders.get(&order_id).cloned().ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Order not found"))?;
        (o.market_id, o.user_id)
    };
    if owner != user.user_id && !user.is_admin {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "Access denied"));
    }
    let request_id = Uuid::new_v4();
    let created_at_ms = now_epoch_ms();
    state.cancel_status.insert(
        request_id.to_string(),
        CancelStatus::Received {
            order_id,
            created_at_ms,
        },
    );
    let job = CancelJob {
        request_id,
        order_id,
        market_id,
        owner_user_id: owner,
        requester_user_id: user.user_id,
    };
    if let Err(_e) = state.cancel_tx.send(job).await {
        state.cancel_status.insert(
            request_id.to_string(),
            CancelStatus::Failed {
                order_id,
                error: "cancel worker unavailable".to_string(),
                created_at_ms,
            },
        );
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "cancel unavailable, please retry",
        ));
    }
    Ok(Json(serde_json::json!(AsyncCancelResponse {
        request_id: request_id.to_string(),
        order_id,
        status: "RECEIVED".to_string(),
    })))
}

#[derive(Debug, Deserialize)]
struct OrderActionRequest {
    token: String,
}

async fn cancel_order_from_action(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<OrderActionRequest>) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let oid = verify_order_action_token(&state.cfg.jwt.secret_key, &req.token, "cancel", user.user_id)?;
    cancel_order(State(state), Path(oid), headers).await
}

async fn get_cancel_request_status(State(state): State<AppState>, Path(request_id): Path<String>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = auth_user(&state, &headers).await?;
    if let Some(v) = state.cancel_status.get(&request_id) {
        return Ok(Json(serde_json::json!({"request_id": request_id, "status": v.value()})));
    }
    Err(ApiError::new(StatusCode::NOT_FOUND, "request not found"))
}

async fn change_my_password(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PasswordChangeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    if req.new_password.len() < 8 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "new password must be at least 8 characters"));
    }
    let row = sqlx::query("SELECT password_hash FROM users WHERE id = $1")
        .bind(user.user_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    let Some(r) = row else {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "User not found"));
    };
    let current_hash: String = r.get("password_hash");
    let ok = verify(&req.current_password, &current_hash).unwrap_or(false);
    if !ok {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Current password is incorrect"));
    }
    let new_hash = hash(&req.new_password, DEFAULT_COST)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("hash error: {e}")))?;

    let type_id = {
        let ids = lock_read(&state.event_type_ids, "main.change_my_password.event_type_ids_read").await;
        *ids
            .get(EVT_USER_PASSWORD_CHANGED)
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };
    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    sqlx::query("UPDATE users SET password_hash = $1 WHERE id = $2")
        .bind(&new_hash)
        .bind(user.user_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(type_id)
        .bind(user.user_id)
        .bind(serde_json::json!({"changed_by_user_id": user.user_id}))
        .execute(&mut *tx)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    Ok(Json(serde_json::json!({"success": true})))
}

async fn withdraw_currency(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CurrencyWithdrawRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let amount_cents = parse_price_to_cents(&req.amount)?;

    let currency_id = {
        let cn = lock_read(&state.currency_names, "main.withdraw_currency.currency_names_read").await;
        cn.iter()
            .find(|(_, name)| name.eq_ignore_ascii_case(req.currency.trim()))
            .map(|(id, _)| *id)
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Currency not found"))?
    };
    let type_id = {
        let ids = lock_read(&state.event_type_ids, "main.withdraw_currency.event_type_ids_read").await;
        *ids
            .get(EVT_FUNDS_WITHDRAWN)
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };

    let has_funds = state
        .with_engine_write_profile("main.withdraw_currency.check", |eng| {
            let w = eng.wallet_mut(user.user_id);
            w.avail_cents(currency_id) >= amount_cents
        })
        .await;
    if !has_funds {
        return Err(ApiError::new(StatusCode::PAYMENT_REQUIRED, "Insufficient funds"));
    }

    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(type_id)
        .bind(user.user_id)
        .bind(serde_json::json!({
            "currency_id": currency_id,
            "currency": req.currency.trim().to_uppercase(),
            "amount_cents": amount_cents
        }))
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    state
        .with_engine_write_profile("main.withdraw_currency.apply", |eng| {
            let w = eng.wallet_mut(user.user_id);
            *w.tot_cents.entry(currency_id).or_insert(0) -= amount_cents;
            *w.wdr_cents.entry(currency_id).or_insert(0) += amount_cents;
        })
        .await;
    state.invalidate_user_read_caches(user.user_id);

    Ok(Json(serde_json::json!({"success": true})))
}

async fn withdraw_product(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ProductWithdrawRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let user = auth_user(&state, &headers).await?;
    let amount_units = req.amount_units.max(0);
    if amount_units <= 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "amount_units must be > 0"));
    }

    let product_id = {
        let pn = lock_read(&state.product_names, "main.withdraw_product.product_names_read").await;
        pn.iter()
            .find(|(_, name)| name.eq_ignore_ascii_case(req.product.trim()))
            .map(|(id, _)| *id)
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Product not found"))?
    };
    let type_id = {
        let ids = lock_read(&state.event_type_ids, "main.withdraw_product.event_type_ids_read").await;
        *ids
            .get(EVT_PRODUCTS_WITHDRAWN)
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };

    let has_assets = state
        .with_engine_write_profile("main.withdraw_product.check", |eng| {
            let w = eng.wallet_mut(user.user_id);
            w.avail_units(product_id) >= amount_units
        })
        .await;
    if !has_assets {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Insufficient assets"));
    }

    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(type_id)
        .bind(user.user_id)
        .bind(serde_json::json!({
            "product_id": product_id,
            "product": req.product.trim().to_uppercase(),
            "amount_units": amount_units
        }))
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    state
        .with_engine_write_profile("main.withdraw_product.apply", |eng| {
            let w = eng.wallet_mut(user.user_id);
            *w.tot_units.entry(product_id).or_insert(0) -= amount_units;
            *w.wdr_units.entry(product_id).or_insert(0) += amount_units;
        })
        .await;
    state.invalidate_user_read_caches(user.user_id);

    Ok(Json(serde_json::json!({"success": true})))
}

async fn admin_list_users(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Vec<serde_json::Value>>, ApiError> {
    let _ = admin_user(&state, &headers).await?;
    let users = lock_read(&state.user_cache, "main.admin_list_users.user_cache_read").await;
    let eng = lock_read(&state.engine, "main.admin_list_users.engine_read").await;
    let mut out = Vec::new();
    for u in users.values() {
        let mut active_orders = 0i64;
        let mut completed_orders = 0i64;
        if let Some(order_ids) = eng.user_orders.get(&u.user_id) {
            for oid in order_ids {
                let Some(o) = eng.orders.get(oid) else { continue; };
                if o.status == "OPEN" || o.status == "PARTIAL" || o.status == "PENDING_SUBMIT" {
                    active_orders += 1;
                } else {
                    completed_orders += 1;
                }
            }
        }
        out.push(serde_json::json!({
            "user_id": u.user_id,
            "username": u.username,
            "email": u.email,
            "is_admin": u.is_admin,
            "created_at": u.created_at,
            "active_orders": active_orders,
            "completed_orders": completed_orders
        }));
    }
    out.sort_by(|a, b| a.get("user_id").and_then(|v| v.as_i64()).cmp(&b.get("user_id").and_then(|v| v.as_i64())));
    Ok(Json(out))
}

async fn admin_get_user_account(
    State(state): State<AppState>,
    Path(user_id): Path<i64>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = admin_user(&state, &headers).await?;
    let users = lock_read(&state.user_cache, "main.admin_get_user_account.user_cache_read").await;
    let u = users.get(&user_id).cloned().ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "User not found"))?;
    drop(users);

    let read_started = std::time::Instant::now();
    let (w, order_ids, order_rows, all_trades) = {
        let eng = lock_read(&state.engine, "main.admin_get_user_account.engine_read").await;
        let w = eng.wallets.get(&user_id).cloned().unwrap_or_else(Wallet::new);
        let order_ids = eng.user_orders.get(&user_id).cloned().unwrap_or_default();
        let mut order_rows = HashMap::new();
        for oid in &order_ids {
            if let Some(o) = eng.orders.get(oid).cloned() {
                order_rows.insert(*oid, o);
            }
        }
        let mut trades = Vec::new();
        for list in eng.trades.values() {
            for t in list {
                trades.push(t.clone());
            }
        }
        (w, order_ids, order_rows, trades)
    };
    let read_ms = read_started.elapsed().as_millis();
    if read_ms >= 200 {
        eprintln!(
            "[read] slow_endpoint endpoint=/admin/users/{}/account lock_ms={}",
            user_id, read_ms
        );
    }
    let cn = lock_read(&state.currency_names, "main.admin_get_user_account.currency_names_read").await;
    let pn = lock_read(&state.product_names, "main.admin_get_user_account.product_names_read").await;

    let mut currencies = serde_json::Map::new();
    for (cid, total) in &w.tot_cents {
        if let Some(name) = cn.get(cid) {
            let reserved = *w.res_cents.get(cid).unwrap_or(&0);
            let available = (*total - reserved).max(0);
            currencies.insert(
                name.clone(),
                serde_json::json!({
                    "total": cents_to_decimal(*total).to_string(),
                    "reserved": cents_to_decimal(reserved).to_string(),
                    "available": cents_to_decimal(available).to_string(),
                    "deposited": cents_to_decimal(*w.dep_cents.get(cid).unwrap_or(&0)).to_string(),
                    "withdrawn": cents_to_decimal(*w.wdr_cents.get(cid).unwrap_or(&0)).to_string(),
                    "fees_paid": cents_to_decimal(*w.fees_paid_cents.get(cid).unwrap_or(&0)).to_string()
                }),
            );
        }
    }
    let mut products = serde_json::Map::new();
    for (pid, total) in &w.tot_units {
        if let Some(name) = pn.get(pid) {
            let reserved = *w.res_units.get(pid).unwrap_or(&0);
            let available = (*total - reserved).max(0);
            products.insert(
                name.clone(),
                serde_json::json!({
                    "total": total,
                    "reserved": reserved,
                    "available": available,
                    "deposited": *w.dep_units.get(pid).unwrap_or(&0),
                    "withdrawn": *w.wdr_units.get(pid).unwrap_or(&0)
                }),
            );
        }
    }

    let mut outstanding = Vec::new();
    let mut completed = Vec::new();
    for oid in order_ids.iter().rev() {
        let Some(o) = order_rows.get(oid) else { continue; };
        let item = serde_json::json!({
                "order_id": o.order_id,
                "market_id": o.market_id,
                "buy": o.buy,
                "price": cents_to_decimal(o.price_cents),
                "quantity": o.quantity,
                "remaining": o.remaining,
                "status": o.status,
                "created_at": o.created_at,
                "expires_at": o.expires_at
            });
        if o.status == "OPEN" || o.status == "PARTIAL" || o.status == "PENDING_SUBMIT" {
            outstanding.push(item);
        } else {
            completed.push(item);
        }
    }
    if outstanding.len() > 400 { outstanding.truncate(400); }
    if completed.len() > 400 { completed.truncate(400); }

    let mut recent_trades = Vec::new();
    for t in &all_trades {
        if t.buy_user_id != user_id && t.sell_user_id != user_id {
            continue;
        }
        recent_trades.push(serde_json::json!({
            "timestamp": (t.ts_ms as f64) / 1000.0,
            "market_id": t.market_id,
            "buy_order_id": t.buy_order_id,
            "sell_order_id": t.sell_order_id,
            "price": cents_to_decimal(t.price_cents),
            "quantity": t.qty_units,
            "fee": cents_to_decimal(t.fee_cents),
            "side": if t.buy_user_id == user_id { "BUY" } else { "SELL" }
        }));
    }
    recent_trades.sort_by(|a, b| {
        b.get("timestamp")
            .and_then(|v| v.as_f64())
            .partial_cmp(&a.get("timestamp").and_then(|v| v.as_f64()))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if recent_trades.len() > 500 {
        recent_trades.truncate(500);
    }

    Ok(Json(serde_json::json!({
        "user": u,
        "wallet": {
            "currencies": currencies,
            "products": products
        },
        "orders": {
            "outstanding": outstanding,
            "completed": completed
        },
        "recent_trades": recent_trades
    })))
}

async fn admin_update_user(
    State(state): State<AppState>,
    Path(user_id): Path<i64>,
    headers: HeaderMap,
    Json(req): Json<AdminUserUpdateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let admin = admin_user(&state, &headers).await?;
    let mut users = lock_write(&state.user_cache, "main.admin_update_user.user_cache_write").await;
    let current = users.get(&user_id).cloned().ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "User not found"))?;

    let username = req.username.unwrap_or(current.username.clone());
    let email = req.email.unwrap_or(current.email.clone());
    let is_admin = req.is_admin.unwrap_or(current.is_admin);

    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    let row = sqlx::query("UPDATE users SET username = $1, email = $2, is_admin = $3 WHERE id = $4 RETURNING id, username, email, is_admin, created_at")
        .bind(&username)
        .bind(&email)
        .bind(is_admin)
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, format!("update failed: {e}")))?;
    let Some(r) = row else {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "User not found"));
    };
    let type_id = {
        let ids = lock_read(&state.event_type_ids, "main.admin_update_user.event_type_ids_read").await;
        *ids
            .get(EVT_USER_UPDATED)
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };
    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(type_id)
        .bind(user_id)
        .bind(serde_json::json!({
            "updated_by_user_id": admin.user_id,
            "username": username,
            "email": email,
            "is_admin": is_admin
        }))
        .execute(&mut *tx)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let updated = crate::state::UserProfile {
        user_id: r.get("id"),
        username: r.get("username"),
        email: r.get("email"),
        is_admin: r.get("is_admin"),
        created_at: r.get("created_at"),
    };
    users.insert(user_id, updated.clone());
    Ok(Json(serde_json::json!({"success": true, "user": updated})))
}

async fn admin_reset_user_password(
    State(state): State<AppState>,
    Path(user_id): Path<i64>,
    headers: HeaderMap,
    Json(req): Json<PasswordAdminResetRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let admin = admin_user(&state, &headers).await?;
    if req.new_password.len() < 8 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "new password must be at least 8 characters"));
    }
    let new_hash = hash(&req.new_password, DEFAULT_COST)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("hash error: {e}")))?;
    let type_id = {
        let ids = lock_read(&state.event_type_ids, "main.admin_reset_user_password.event_type_ids_read").await;
        *ids
            .get(EVT_USER_PASSWORD_CHANGED)
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };
    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    let changed = sqlx::query("UPDATE users SET password_hash = $1 WHERE id = $2")
        .bind(&new_hash)
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    if changed.rows_affected() == 0 {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "User not found"));
    }
    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(type_id)
        .bind(user_id)
        .bind(serde_json::json!({"changed_by_user_id": admin.user_id}))
        .execute(&mut *tx)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    Ok(Json(serde_json::json!({"success": true})))
}

async fn admin_adjust_user_currency(
    State(state): State<AppState>,
    Path(user_id): Path<i64>,
    headers: HeaderMap,
    Json(req): Json<AdminAdjustCurrencyRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let admin = admin_user(&state, &headers).await?;
    let delta_cents = parse_signed_price_to_cents(&req.amount)?;
    let currency_id = {
        let cn = lock_read(&state.currency_names, "main.admin_adjust_user_currency.currency_names_read").await;
        cn.iter()
            .find(|(_, name)| name.eq_ignore_ascii_case(req.currency.trim()))
            .map(|(id, _)| *id)
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Currency not found"))?
    };
    let (type_code, amount_abs) = if delta_cents > 0 {
        (EVT_FUNDS_DEPOSITED, delta_cents)
    } else {
        (EVT_FUNDS_WITHDRAWN, delta_cents.abs())
    };
    let type_id = {
        let ids = lock_read(&state.event_type_ids, "main.admin_adjust_user_currency.event_type_ids_read").await;
        *ids
            .get(type_code)
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };

    if delta_cents < 0 {
        let has_funds = state
            .with_engine_write_profile("main.admin_adjust_currency.check", |eng| {
                let w = eng.wallet_mut(user_id);
                w.avail_cents(currency_id) >= amount_abs
            })
            .await;
        if !has_funds {
            return Err(ApiError::new(StatusCode::PAYMENT_REQUIRED, "Insufficient funds for withdrawal"));
        }
    }

    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(type_id)
        .bind(user_id)
        .bind(serde_json::json!({
            "currency_id": currency_id,
            "currency": req.currency.trim().to_uppercase(),
            "amount_cents": amount_abs,
            "admin_user_id": admin.user_id,
            "reason": req.reason
        }))
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    state
        .with_engine_write_profile("main.admin_adjust_currency.apply", |eng| {
            let w = eng.wallet_mut(user_id);
            if delta_cents > 0 {
                *w.tot_cents.entry(currency_id).or_insert(0) += amount_abs;
                *w.dep_cents.entry(currency_id).or_insert(0) += amount_abs;
            } else {
                *w.tot_cents.entry(currency_id).or_insert(0) -= amount_abs;
                *w.wdr_cents.entry(currency_id).or_insert(0) += amount_abs;
            }
        })
        .await;
    state.invalidate_user_read_caches(user_id);

    Ok(Json(serde_json::json!({"success": true})))
}

async fn admin_adjust_user_product(
    State(state): State<AppState>,
    Path(user_id): Path<i64>,
    headers: HeaderMap,
    Json(req): Json<AdminAdjustProductRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let admin = admin_user(&state, &headers).await?;
    if req.amount_units == 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "amount_units must be non-zero"));
    }
    let product_id = {
        let pn = lock_read(&state.product_names, "main.admin_adjust_user_product.product_names_read").await;
        pn.iter()
            .find(|(_, name)| name.eq_ignore_ascii_case(req.product.trim()))
            .map(|(id, _)| *id)
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Product not found"))?
    };
    let (type_code, amount_abs) = if req.amount_units > 0 {
        (EVT_PRODUCTS_DEPOSITED, req.amount_units)
    } else {
        (EVT_PRODUCTS_WITHDRAWN, req.amount_units.abs())
    };
    let type_id = {
        let ids = lock_read(&state.event_type_ids, "main.admin_adjust_user_product.event_type_ids_read").await;
        *ids
            .get(type_code)
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };

    if req.amount_units < 0 {
        let has_assets = state
            .with_engine_write_profile("main.admin_adjust_product.check", |eng| {
                let w = eng.wallet_mut(user_id);
                w.avail_units(product_id) >= amount_abs
            })
            .await;
        if !has_assets {
            return Err(ApiError::new(StatusCode::BAD_REQUEST, "Insufficient assets for withdrawal"));
        }
    }

    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(type_id)
        .bind(user_id)
        .bind(serde_json::json!({
            "product_id": product_id,
            "product": req.product.trim().to_uppercase(),
            "amount_units": amount_abs,
            "admin_user_id": admin.user_id,
            "reason": req.reason
        }))
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    state
        .with_engine_write_profile("main.admin_adjust_product.apply", |eng| {
            let w = eng.wallet_mut(user_id);
            if req.amount_units > 0 {
                *w.tot_units.entry(product_id).or_insert(0) += amount_abs;
                *w.dep_units.entry(product_id).or_insert(0) += amount_abs;
            } else {
                *w.tot_units.entry(product_id).or_insert(0) -= amount_abs;
                *w.wdr_units.entry(product_id).or_insert(0) += amount_abs;
            }
        })
        .await;
    state.invalidate_user_read_caches(user_id);

    Ok(Json(serde_json::json!({"success": true})))
}

// ===== Admin endpoints (topup + fees) =====

#[derive(Debug, Deserialize)]
struct DevTopupRequest {
    min_user_id: i64,
    currency_amount: String,
    #[serde(alias = "product_amount_units", alias = "product_units")]
    product_amount: i64,
}

async fn admin_dev_topup(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<DevTopupRequest>) -> Result<Json<serde_json::Value>, ApiError> {
    let admin = admin_user(&state, &headers).await?;
    let _ = admin;

    let amount_cents = parse_price_to_cents(&req.currency_amount)?;
    let product_units = req.product_amount.max(0);

    // Fetch all users >= min_user_id.
    let users: Vec<i64> = sqlx::query_scalar("SELECT id FROM users WHERE id >= $1 ORDER BY id")
        .bind(req.min_user_id)
        .fetch_all(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;

    let currencies: Vec<(i16, String)> = sqlx::query("SELECT id, name FROM currencies ORDER BY id")
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.get::<i16, _>("id"), r.get::<String, _>("name")))
        .collect();
    let products: Vec<(i16, String)> = sqlx::query("SELECT id, name FROM products ORDER BY id")
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.get::<i16, _>("id"), r.get::<String, _>("name")))
        .collect();

    let funds_type = {
        let ids = lock_read(&state.event_type_ids, "main.admin_dev_topup.funds_event_type_ids_read").await;
        *ids.get(EVT_FUNDS_DEPOSITED).ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };
    let prod_type = {
        let ids = lock_read(&state.event_type_ids, "main.admin_dev_topup.products_event_type_ids_read").await;
        *ids.get(EVT_PRODUCTS_DEPOSITED).ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };

    // Batch insert deposit events.
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO events (event_type_id, user_id, payload) ",
    );
    qb.push_values(users.iter().flat_map(|uid| {
        currencies.iter().map(move |(cid, cname)| (uid, cid, cname))
    }), |mut b, (uid, cid, cname)| {
        b.push_bind(funds_type)
            .push_bind(*uid)
            .push_bind(serde_json::json!({"currency": cname, "currency_id": *cid, "amount_cents": amount_cents}));
    });
    qb.build().execute(&state.db).await.ok();

    let mut qb2: QueryBuilder<Postgres> = QueryBuilder::new("INSERT INTO events (event_type_id, user_id, payload) ");
    qb2.push_values(users.iter().flat_map(|uid| {
        products.iter().map(move |(pid, pname)| (uid, pid, pname))
    }), |mut b, (uid, pid, pname)| {
        b.push_bind(prod_type)
            .push_bind(*uid)
            .push_bind(serde_json::json!({"product": pname, "product_id": *pid, "amount_units": product_units}));
    });
    qb2.build().execute(&state.db).await.ok();

    // Apply locally in small chunks to avoid monopolizing engine write lock.
    const TOPUP_APPLY_CHUNK_USERS: usize = 1;
    for user_chunk in users.chunks(TOPUP_APPLY_CHUNK_USERS.max(1)) {
        let lock_wait_started = std::time::Instant::now();
        let mut eng = lock_write(&state.engine, "main.admin_dev_topup.engine_write").await;
        let lock_wait_ms = lock_wait_started.elapsed().as_millis();
        let hold_started = std::time::Instant::now();
        for uid in user_chunk {
            let w = eng.wallet_mut(*uid);
            for (cid, _name) in &currencies {
                *w.tot_cents.entry(*cid).or_insert(0) += amount_cents;
                *w.dep_cents.entry(*cid).or_insert(0) += amount_cents;
            }
            for (pid, _name) in &products {
                *w.tot_units.entry(*pid).or_insert(0) += product_units;
                *w.dep_units.entry(*pid).or_insert(0) += product_units;
            }
        }
        let hold_ms = hold_started.elapsed().as_millis();
        if lock_wait_ms >= 200 || hold_ms >= 200 {
            eprintln!(
                "[admin] topup_apply_lock_profile users_chunk={} lock_wait_ms={} hold_ms={}",
                user_chunk.len(),
                lock_wait_ms,
                hold_ms
            );
        }
        drop(eng);
        for uid in user_chunk {
            state.invalidate_user_read_caches(*uid);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let events_written =
        (users.len() as i64) * ((currencies.len() as i64) + (products.len() as i64));
    Ok(Json(serde_json::json!({"success": true, "events_written": events_written})))
}

#[derive(Debug, Deserialize)]
struct FeeConfigUpdateRequest {
    seller_fee_rate: String,
}

#[derive(Debug, Deserialize)]
struct CurrencyWithdrawRequest {
    currency: String,
    amount: String,
}

#[derive(Debug, Deserialize)]
struct ProductWithdrawRequest {
    product: String,
    amount_units: i64,
}

#[derive(Debug, Deserialize)]
struct AdminUserUpdateRequest {
    username: Option<String>,
    email: Option<String>,
    is_admin: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AdminAdjustCurrencyRequest {
    currency: String,
    amount: String,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AdminAdjustProductRequest {
    product: String,
    amount_units: i64,
    reason: Option<String>,
}

async fn get_fee_config(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = admin_user(&state, &headers).await?;
    let eng = lock_read(&state.engine, "main.get_fee_config.engine_read").await;
    let rate = (eng.fee_rate_ppm as f64) / 1_000_000.0;
    Ok(Json(serde_json::json!({"seller_fee_rate": rate})))
}

async fn set_fee_config(State(state): State<AppState>, headers: HeaderMap, Json(req): Json<FeeConfigUpdateRequest>) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = admin_user(&state, &headers).await?;
    let rate = f64::from_str(&req.seller_fee_rate).map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid fee rate"))?;
    if !(0.0..=1.0).contains(&rate) {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Invalid fee rate"));
    }
    let type_id = {
        let ids = lock_read(&state.event_type_ids, "main.set_fee_config.event_type_ids_read").await;
        *ids.get(EVT_FEE_RATE_SET).ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };
    sqlx::query("INSERT INTO events (event_type_id, payload) VALUES ($1,$2)")
        .bind(type_id)
        .bind(serde_json::json!({"seller_fee_rate": rate}))
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    state
        .with_engine_write_profile("main.set_fee_config.apply", |eng| {
            eng.fee_rate_ppm = (rate * 1_000_000.0).round() as i64;
        })
        .await;
    Ok(Json(serde_json::json!({"success": true})))
}

async fn get_fees_collected(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = admin_user(&state, &headers).await?;
    let eng = lock_read(&state.engine, "main.get_fees_collected.engine_read").await;
    let cn = lock_read(&state.currency_names, "main.get_fees_collected.currency_names_read").await;
    let mut totals = serde_json::Map::new();
    for (cid, cents) in &eng.fees_collected {
        if let Some(name) = cn.get(cid) {
            totals.insert(
                name.clone(),
                serde_json::Value::Number(serde_json::Number::from_f64(cents_to_decimal(*cents).to_f64().unwrap_or(0.0)).unwrap_or_else(|| serde_json::Number::from(0))),
            );
        }
    }
    Ok(Json(serde_json::json!({
        "totals": totals,
        "updated_at": now_epoch_secs()
    })))
}

// ===== Auth =====

async fn register(State(state): State<AppState>, Json(req): Json<UserRegister>) -> Result<Json<TokenResponse>, ApiError> {
    if req.username.len() < 3 || req.password.len() < 8 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Invalid registration payload"));
    }
    let existing: Option<(i64,)> = sqlx::query_as("SELECT id FROM users WHERE username = $1 OR email = $2")
        .bind(&req.username)
        .bind(&req.email)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    if existing.is_some() {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Username or email already exists"));
    }
    let password_hash = hash(&req.password, DEFAULT_COST).map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("hash error: {e}")))?;
    let row = sqlx::query("INSERT INTO users (username, email, password_hash, is_admin) VALUES ($1,$2,$3,false) RETURNING id")
        .bind(&req.username)
        .bind(&req.email)
        .bind(&password_hash)
        .fetch_one(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    let user_id: i64 = row.get("id");

    // Emit USER_CREATED event.
    if let Some(type_id) = lock_read(&state.event_type_ids, "main.register.event_type_ids_read").await.get(EVT_USER_CREATED).copied() {
        let _ = sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
            .bind(type_id)
            .bind(user_id)
            .bind(serde_json::json!({"username": req.username, "email": req.email, "is_admin": false}))
            .execute(&state.db)
            .await;
    }
    lock_write(&state.user_cache, "main.register.user_cache_write").await.insert(
        user_id,
        crate::state::UserProfile {
            user_id,
            username: req.username.clone(),
            email: req.email.clone(),
            is_admin: false,
            created_at: Utc::now(),
        },
    );

    let access = make_access_token(&state.cfg, user_id, false, &req.username, &req.email).map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("token error: {e}")))?;
    let refresh = make_refresh_token(&state.cfg, user_id, false).map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("token error: {e}")))?;
    let hash_hex = sha256_hex(&refresh);
    let expires_at = Utc::now() + ChronoDuration::days(state.cfg.jwt.refresh_token_expire_days);
    sqlx::query("INSERT INTO refresh_tokens (user_id, token_hash, expires_at) VALUES ($1,$2,$3)")
        .bind(user_id)
        .bind(&hash_hex)
        .bind(expires_at)
        .execute(&state.db)
        .await
        .ok();

    Ok(Json(TokenResponse { access_token: access, refresh_token: refresh, token_type: "bearer".to_string() }))
}

async fn login(State(state): State<AppState>, Json(req): Json<UserLogin>) -> Result<Json<TokenResponse>, ApiError> {
    let row = sqlx::query("SELECT id, username, email, password_hash, is_admin FROM users WHERE username = $1 OR email = $1")
        .bind(&req.username)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    let Some(r) = row else {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Invalid credentials"));
    };
    let user_id: i64 = r.get("id");
    let username: String = r.get("username");
    let email: String = r.get("email");
    let pass_hash: String = r.get("password_hash");
    let is_admin: bool = r.get("is_admin");
    let ok = verify(&req.password, &pass_hash).unwrap_or(false);
    if !ok {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Invalid credentials"));
    }
    let access = make_access_token(&state.cfg, user_id, is_admin, &username, &email).map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("token error: {e}")))?;
    let refresh = make_refresh_token(&state.cfg, user_id, is_admin).map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("token error: {e}")))?;
    let hash_hex = sha256_hex(&refresh);
    let expires_at = Utc::now() + ChronoDuration::days(state.cfg.jwt.refresh_token_expire_days);
    sqlx::query("INSERT INTO refresh_tokens (user_id, token_hash, expires_at) VALUES ($1,$2,$3)")
        .bind(user_id)
        .bind(&hash_hex)
        .bind(expires_at)
        .execute(&state.db)
        .await
        .ok();
    Ok(Json(TokenResponse { access_token: access, refresh_token: refresh, token_type: "bearer".to_string() }))
}

async fn refresh_token(State(state): State<AppState>, Json(req): Json<RefreshTokenRequest>) -> Result<Json<TokenResponse>, ApiError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    let decoded = decode::<Claims>(
        &req.refresh_token,
        &DecodingKey::from_secret(state.cfg.jwt.secret_key.as_bytes()),
        &validation,
    )
    .map_err(|_| ApiError::new(StatusCode::UNAUTHORIZED, "Invalid refresh token"))?;
    if decoded.claims.r#type.as_deref().unwrap_or("") != "refresh" {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Invalid refresh token"));
    }
    let user_id = decoded.claims.sub.parse::<i64>().unwrap_or(0);
    if user_id <= 0 {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Invalid refresh token"));
    }
    let hash_hex = sha256_hex(&req.refresh_token);
    let row = sqlx::query("SELECT revoked, expires_at FROM refresh_tokens WHERE token_hash = $1")
        .bind(&hash_hex)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    let Some(r) = row else {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Invalid refresh token"));
    };
    let revoked: bool = r.get("revoked");
    let expires_at: DateTime<Utc> = r.get("expires_at");
    if revoked || expires_at < Utc::now() {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Invalid refresh token"));
    }
    // Rotate: revoke old hash, issue new refresh.
    sqlx::query("UPDATE refresh_tokens SET revoked = true WHERE token_hash = $1")
        .bind(&hash_hex)
        .execute(&state.db)
        .await
        .ok();

    let row2 = sqlx::query("SELECT username, email, is_admin FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")))?;
    let username: String = row2.get("username");
    let email: String = row2.get("email");
    let is_admin: bool = row2.get("is_admin");

    let access = make_access_token(&state.cfg, user_id, is_admin, &username, &email).map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("token error: {e}")))?;
    let refresh = make_refresh_token(&state.cfg, user_id, is_admin).map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("token error: {e}")))?;
    let new_hash = sha256_hex(&refresh);
    let new_expires = Utc::now() + ChronoDuration::days(state.cfg.jwt.refresh_token_expire_days);
    sqlx::query("INSERT INTO refresh_tokens (user_id, token_hash, expires_at) VALUES ($1,$2,$3)")
        .bind(user_id)
        .bind(&new_hash)
        .bind(new_expires)
        .execute(&state.db)
        .await
        .ok();

    Ok(Json(TokenResponse { access_token: access, refresh_token: refresh, token_type: "bearer".to_string() }))
}

async fn get_current_user_info(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let u = auth_user(&state, &headers).await?;
    Ok(Json(serde_json::json!({
        "id": u.user_id,
        "username": u.username.unwrap_or_default(),
        "email": u.email.unwrap_or_default(),
        "is_admin": u.is_admin
    })))
}

async fn admin_get_tx_journal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TxJournalQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = admin_user(&state, &headers).await?;
    let limit = q.limit.unwrap_or(200).clamp(1, 2000);
    let mut rows: Vec<serde_json::Value> = state
        .tx_journal
        .iter()
        .map(|kv| serde_json::to_value(kv.value().clone()).unwrap_or_else(|_| serde_json::json!({})))
        .collect();
    rows.sort_by(|a, b| {
        b.get("updated_at_ms")
            .and_then(|v| v.as_i64())
            .cmp(&a.get("updated_at_ms").and_then(|v| v.as_i64()))
    });
    if rows.len() > limit {
        rows.truncate(limit);
    }
    Ok(Json(serde_json::json!({
        "count": rows.len(),
        "items": rows
    })))
}

async fn admin_get_perf(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = admin_user(&state, &headers).await?;
    Ok(Json(state.perf.snapshot_json(
        state.submit_inflight.load(Ordering::Relaxed),
    )))
}

#[tokio::main(worker_threads = 32)]
async fn main() -> Result<()> {
    let cfg = Arc::new(load_config()?);

    let db = PgPoolOptions::new()
        .min_connections(cfg.database.min_pool_size)
        .max_connections(cfg.database.max_pool_size)
        .acquire_timeout(Duration::from_secs(cfg.database.acquire_timeout_seconds))
        .max_lifetime(Duration::from_secs(cfg.database.max_lifetime_seconds))
        .connect(&cfg.database.url)
        .await
        .context("failed to connect to postgres")?;

    let (submit_tx, submit_rx) = mpsc::channel::<SubmitJob>(ASYNC_SUBMIT_QUEUE_CAP);
    let (order_intent_tx, order_intent_rx) = mpsc::channel::<OrderIntent>(ASYNC_SUBMIT_QUEUE_CAP);
    let (cancel_tx, cancel_rx) = mpsc::channel::<CancelJob>(ASYNC_CANCEL_QUEUE_CAP);
    let (match_tx, match_rx) = mpsc::unbounded_channel::<i16>();
    let submit_status: Arc<DashMap<String, SubmitStatus>> = Arc::new(DashMap::new());
    let cancel_status: Arc<DashMap<String, CancelStatus>> = Arc::new(DashMap::new());

    // Allocate order ids from max(order_id) in events.
    let max_order_id: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(order_id), 0)::bigint FROM events")
        .fetch_one(&db)
        .await
        .unwrap_or(0);
    let next_order_id = Arc::new(AtomicI64::new(max_order_id + 1));

    // Load markets and event type ids.
    let state = AppState {
        cfg: cfg.clone(),
        db: db.clone(),
        markets: Arc::new(RwLock::new(HashMap::new())),
        currency_names: Arc::new(RwLock::new(HashMap::new())),
        product_names: Arc::new(RwLock::new(HashMap::new())),
        event_type_ids: Arc::new(RwLock::new(HashMap::new())),
        engine: Arc::new(RwLock::new(EngineState::new(HashMap::new(), cfg.fees.default_seller_fee_rate))),
        next_order_id,
        submit_tx,
        order_intent_tx,
        cancel_tx,
        submit_inflight: Arc::new(AtomicU64::new(0)),
        submit_status,
        cancel_status,
        match_tx,
        match_pending: Arc::new(DashMap::new()),
        match_running: Arc::new(DashMap::new()),
        match_journal: Arc::new(DashMap::new()),
        tx_journal: Arc::new(DashMap::new()),
        perf: Arc::new(PerfCounters::new()),
        market_read_cache: Arc::new(DashMap::new()),
        market_activity_ms: Arc::new(DashMap::new()),
        system_read_cache: Arc::new(RwLock::new(SystemReadCache {
            total_active: 0,
            active_buy_orders: 0,
            active_sell_orders: 0,
            last_hour_count: 0,
            updated_at_ms: now_epoch_ms(),
        })),
        wallet_read_cache: Arc::new(DashMap::new()),
        wallet_details_read_cache: Arc::new(DashMap::new()),
        my_orders_read_cache: Arc::new(DashMap::new()),
        market_mutexes: Arc::new((0..MARKET_LOCK_SHARDS).map(|_| Arc::new(Mutex::new(()))).collect()),
        user_cache: Arc::new(RwLock::new(HashMap::new())),
        engine_ready: Arc::new(AtomicBool::new(false)),
    };

    load_event_type_ids(&state).await?;
    reload_market_cache(&state).await?;
    reload_name_caches(&state).await?;
    reload_user_cache(&state).await?;
    // CORS: explicitly allow Authorization.
    let allowed_headers = [AUTHORIZATION, CONTENT_TYPE, ACCEPT];
    let allowed_methods = [Method::GET, Method::POST, Method::PATCH, Method::DELETE, Method::OPTIONS];
    let cors = if cfg.api.cors_origins.iter().any(|x| x == "*") {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(allowed_methods)
            .allow_headers(allowed_headers)
    } else {
        let origins: Vec<HeaderValue> = cfg
            .api
            .cors_origins
            .iter()
            .filter_map(|origin| HeaderValue::from_str(origin).ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(allowed_methods)
            .allow_headers(allowed_headers)
    };

    // Serve immediately; heavy replay runs in background and flips readiness when done.
    let s_boot = state.clone();
    tokio::spawn(async move {
        match replay_from_db(&s_boot).await {
            Ok(()) => {
                prime_read_caches(&s_boot).await;
                start_background_tasks(s_boot.clone(), order_intent_rx, submit_rx, cancel_rx, match_rx);
                s_boot.engine_ready.store(true, Ordering::Release);
                eprintln!("[startup] engine_ready=true");
            }
            Err(e) => {
                eprintln!("[startup] replay_failed error={e}");
            }
        }
    });

    let protected_api = Router::new()
        .route("/stats", get(get_system_stats))
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh_token))
        .route("/auth/me", get(get_current_user_info))
        .route("/auth/change-password", post(change_my_password))
        .route("/users/me/wallet", get(get_my_wallet))
        .route("/users/me/wallet/details", get(get_my_wallet_details))
        .route("/users/me/wallet/withdraw/currency", post(withdraw_currency))
        .route("/users/me/wallet/withdraw/product", post(withdraw_product))
        .route("/markets", get(list_markets))
        .route("/markets/{market_id}", get(get_market))
        .route("/markets/{market_id}/stats", get(get_market_stats))
        .route("/markets/{market_id}/orderbook", get(get_order_book))
        .route("/markets/{market_id}/trades", get(get_recent_trades))
        .route("/orders", post(create_order_sync))
        .route("/orders/async", post(create_order_async))
        .route("/orders/requests/{request_id}", get(get_order_request_status))
        .route("/orders/cancel/requests/{request_id}", get(get_cancel_request_status))
        .route("/orders/me", get(get_my_orders))
        .route("/orders/{order_id}", get(get_order).delete(cancel_order))
        .route("/orders/{order_id}/matches", get(get_order_matches))
        .route("/orders/actions/cancel", post(cancel_order_from_action))
        .route("/admin/dev/topup", post(admin_dev_topup))
        .route("/admin/users", get(admin_list_users))
        .route("/admin/users/{user_id}", get(admin_get_user_account).patch(admin_update_user))
        .route("/admin/users/{user_id}/password", post(admin_reset_user_password))
        .route("/admin/users/{user_id}/wallet/currency", post(admin_adjust_user_currency))
        .route("/admin/users/{user_id}/wallet/product", post(admin_adjust_user_product))
        .route("/admin/fees", get(get_fee_config).post(set_fee_config))
        .route("/admin/fees/collected", get(get_fees_collected))
        .route("/admin/debug/tx-journal", get(admin_get_tx_journal))
        .route("/admin/debug/perf", get(admin_get_perf))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_engine_ready,
        ));

    let app = Router::new()
        .route("/health", get(health_check))
        .merge(protected_api)
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", cfg.api.host, cfg.api.port).parse()?;
    println!("Rust trading API listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
