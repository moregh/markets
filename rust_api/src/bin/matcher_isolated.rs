use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[path = "../error.rs"]
mod error;
#[allow(dead_code)]
#[path = "../engine.rs"]
mod engine;

use engine::{EngineState, MarketInfo, Order, Trade};
use error::ApiError;

const DEFAULT_LIMIT: usize = 200;
const MAX_LIMIT: usize = 2000;

#[derive(Clone)]
struct AppState {
    engine: Arc<RwLock<EngineState>>,
    next_order_id: Arc<AtomicI64>,
}

#[derive(Debug, Deserialize)]
struct FundCurrencyReq {
    user_id: i64,
    currency_id: i16,
    amount_cents: i64,
}

#[derive(Debug, Deserialize)]
struct FundProductReq {
    user_id: i64,
    product_id: i16,
    amount_units: i64,
}

#[derive(Debug, Deserialize)]
struct OrderReq {
    user_id: i64,
    market_id: i16,
    buy: bool,
    quantity: i64,
    price_cents: i64,
}

#[derive(Debug, Serialize)]
struct OrderResp {
    id: i64,
    state: String,
    remaining: i64,
}

#[derive(Debug, Deserialize)]
struct LimitQuery {
    limit: Option<usize>,
}

fn cents_to_f64(cents: i64) -> f64 {
    (Decimal::from(cents) / Decimal::from(100))
        .round_dp(2)
        .to_f64()
        .unwrap_or(0.0)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn fund_currency(
    State(state): State<AppState>,
    Json(req): Json<FundCurrencyReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if req.user_id <= 0 || req.currency_id <= 0 || req.amount_cents <= 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "invalid payload"));
    }
    let mut eng = state.engine.write().await;
    let w = eng.wallet_mut(req.user_id);
    *w.tot_cents.entry(req.currency_id).or_insert(0) += req.amount_cents;
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn fund_product(
    State(state): State<AppState>,
    Json(req): Json<FundProductReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if req.user_id <= 0 || req.product_id <= 0 || req.amount_units <= 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "invalid payload"));
    }
    let mut eng = state.engine.write().await;
    let w = eng.wallet_mut(req.user_id);
    *w.tot_units.entry(req.product_id).or_insert(0) += req.amount_units;
    Ok(Json(serde_json::json!({"ok": true})))
}

fn match_market_local(eng: &mut EngineState, market_id: i16) -> usize {
    let mut matched = 0usize;
    loop {
        let Some(best_bid) = eng.best_bid(market_id) else { break; };
        let Some(best_ask) = eng.best_ask(market_id) else { break; };
        if best_bid < best_ask {
            break;
        }
        let Some(bid_oid) = eng.pop_best_order_id(market_id, true) else { break; };
        let Some(ask_oid) = eng.pop_best_order_id(market_id, false) else {
            eng.restore_order_to_book_front(bid_oid);
            break;
        };
        let Some(bid) = eng.orders.get(&bid_oid).cloned() else {
            eng.restore_order_to_book_front(ask_oid);
            continue;
        };
        let Some(ask) = eng.orders.get(&ask_oid).cloned() else {
            eng.restore_order_to_book_front(bid_oid);
            continue;
        };
        if bid.price_cents < ask.price_cents {
            eng.restore_order_to_book_front(bid_oid);
            eng.restore_order_to_book_front(ask_oid);
            break;
        }
        let qty = bid.remaining.min(ask.remaining).max(0);
        if qty <= 0 {
            eng.restore_order_to_book_front(bid_oid);
            eng.restore_order_to_book_front(ask_oid);
            continue;
        }
        let price = ask.price_cents;
        let gross = qty.saturating_mul(price);
        let fee = (gross.saturating_mul(eng.fee_rate_ppm) / 1_000_000).max(0);
        let t = Trade {
            ts_ms: now_ms(),
            market_id,
            buy_user_id: bid.user_id,
            sell_user_id: ask.user_id,
            buy_order_id: bid_oid,
            sell_order_id: ask_oid,
            price_cents: price,
            qty_units: qty,
            fee_cents: fee,
        };
        eng.apply_trade(&t);
        if let Some(o) = eng.orders.get(&bid_oid).cloned() {
            if o.status == "OPEN" || o.status == "PARTIAL" {
                eng.add_order_to_book(&o);
            }
        }
        if let Some(o) = eng.orders.get(&ask_oid).cloned() {
            if o.status == "OPEN" || o.status == "PARTIAL" {
                eng.add_order_to_book(&o);
            }
        }
        matched += 1;
    }
    matched
}

async fn create_order(
    State(state): State<AppState>,
    Json(req): Json<OrderReq>,
) -> Result<Json<OrderResp>, ApiError> {
    if req.user_id <= 0 || req.market_id <= 0 || req.quantity <= 0 || req.price_cents <= 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "invalid payload"));
    }
    let mut eng = state.engine.write().await;
    eng.reserve_for_order(
        req.user_id,
        req.market_id,
        req.buy,
        req.quantity,
        req.price_cents,
    )?;
    let order_id = state.next_order_id.fetch_add(1, Ordering::Relaxed);
    let o = Order {
        order_id,
        user_id: req.user_id,
        market_id: req.market_id,
        buy: req.buy,
        price_cents: req.price_cents,
        quantity: req.quantity,
        remaining: req.quantity,
        created_at: Utc::now(),
        expires_at: None,
        status: "OPEN".to_string(),
    };
    eng.orders.insert(order_id, o.clone());
    eng.add_order_to_book(&o);
    let matched = match_market_local(&mut eng, req.market_id);
    if matched > 0 {
        eprintln!("[isolated-matcher] market_id={} matched={}", req.market_id, matched);
    }
    let out = eng.orders.get(&order_id).cloned().unwrap_or(o);
    Ok(Json(OrderResp {
        id: order_id,
        state: out.status,
        remaining: out.remaining,
    }))
}

async fn get_order(
    State(state): State<AppState>,
    Path(order_id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let eng = state.engine.read().await;
    let o = eng
        .orders
        .get(&order_id)
        .cloned()
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "order not found"))?;
    Ok(Json(serde_json::json!({
        "id": o.order_id,
        "user_id": o.user_id,
        "market_id": o.market_id,
        "buy": o.buy,
        "state": o.status,
        "remaining": o.remaining,
        "price_cents": o.price_cents
    })))
}

async fn get_orderbook(
    State(state): State<AppState>,
    Path(market_id): Path<i16>,
    Query(q): Query<LimitQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).max(1).min(MAX_LIMIT);
    let eng = state.engine.read().await;
    let book = eng.books.get(&market_id).cloned().unwrap_or_default();
    let mut buys = Vec::new();
    let mut sells = Vec::new();
    for (price, level) in book.buys.iter().rev() {
        for oid in &level.q {
            if let Some(o) = eng.orders.get(oid) {
                if o.status == "OPEN" || o.status == "PARTIAL" {
                    buys.push(serde_json::json!({
                        "order_id": o.order_id,
                        "price": cents_to_f64(*price),
                        "qty": o.remaining
                    }));
                    if buys.len() >= limit {
                        break;
                    }
                }
            }
        }
        if buys.len() >= limit {
            break;
        }
    }
    for (price, level) in book.sells.iter() {
        for oid in &level.q {
            if let Some(o) = eng.orders.get(oid) {
                if o.status == "OPEN" || o.status == "PARTIAL" {
                    sells.push(serde_json::json!({
                        "order_id": o.order_id,
                        "price": cents_to_f64(*price),
                        "qty": o.remaining
                    }));
                    if sells.len() >= limit {
                        break;
                    }
                }
            }
        }
        if sells.len() >= limit {
            break;
        }
    }
    Ok(Json(serde_json::json!({
        "market_id": market_id,
        "limit": limit,
        "buys": buys,
        "sells": sells
    })))
}

async fn get_trades(
    State(state): State<AppState>,
    Path(market_id): Path<i16>,
    Query(q): Query<LimitQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = q.limit.unwrap_or(100).max(1).min(MAX_LIMIT);
    let eng = state.engine.read().await;
    let list = eng.trades.get(&market_id).cloned().unwrap_or_default();
    let trades: Vec<serde_json::Value> = list
        .into_iter()
        .take(limit)
        .map(|t| {
            serde_json::json!({
                "ts_ms": t.ts_ms,
                "buy_order_id": t.buy_order_id,
                "sell_order_id": t.sell_order_id,
                "price_cents": t.price_cents,
                "qty_units": t.qty_units
            })
        })
        .collect();
    Ok(Json(serde_json::json!({
        "market_id": market_id,
        "count": trades.len(),
        "trades": trades
    })))
}

async fn debug_matcher(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let eng = state.engine.read().await;
    let mut open_buy = 0usize;
    let mut open_sell = 0usize;
    let mut crossed = 0usize;
    for book in eng.books.values() {
        open_buy += book.buys.values().map(|q| q.q.len()).sum::<usize>();
        open_sell += book.sells.values().map(|q| q.q.len()).sum::<usize>();
        let best_bid = book.buys.keys().next_back().copied().unwrap_or(0);
        let best_ask = book.sells.keys().next().copied().unwrap_or(i64::MAX);
        if best_bid >= best_ask && best_ask != i64::MAX {
            crossed += 1;
        }
    }
    Ok(Json(serde_json::json!({
        "open_buy_orders": open_buy,
        "open_sell_orders": open_sell,
        "crossed_markets": crossed,
        "match_sum_last_hour": eng.match_sum_last_hour,
        "orders_total": eng.orders.len()
    })))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok", "mode": "isolated"}))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut markets = HashMap::new();
    markets.insert(
        2,
        MarketInfo {
            id: 2,
            product_id: 1,
            currency_id: 2,
            product: "BTC".to_string(),
            currency: "EUR".to_string(),
            ticker: "BTC/EUR".to_string(),
        },
    );
    let state = AppState {
        engine: Arc::new(RwLock::new(EngineState::new(markets, 0.005))),
        next_order_id: Arc::new(AtomicI64::new(1)),
    };

    // Seed admin wallet so manual testing can start immediately.
    {
        let mut eng = state.engine.write().await;
        let w = eng.wallet_mut(1);
        *w.tot_cents.entry(2).or_insert(0) = 50_000_000_00;
        *w.tot_units.entry(1).or_insert(0) = 100_000;
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/wallets/fund-currency", post(fund_currency))
        .route("/wallets/fund-product", post(fund_product))
        .route("/orders", post(create_order))
        .route("/orders/{order_id}", get(get_order))
        .route("/markets/{market_id}/orderbook", get(get_orderbook))
        .route("/markets/{market_id}/trades", get(get_trades))
        .route("/debug/matcher", get(debug_matcher))
        .with_state(state);

    let addr = SocketAddr::from_str("0.0.0.0:18080")?;
    eprintln!("isolated matcher api listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
