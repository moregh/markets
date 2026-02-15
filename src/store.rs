use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use anyhow::Result;
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::engine::{EngineSnapshot, MarketInfo, Order, Trade};
use crate::error::ApiError;
use crate::state::{lock_read, lock_write, try_lock_read, AppState, MatchIntent, MatchIntentEntry, SubmitJob, SubmitStatus, UserProfile};

// Keep matcher write-lock slices short to avoid starving submit/apply writes under load.
const MATCH_BATCH_MAX: usize = 64;
const MATCH_LOCK_BUDGET_US: u128 = 6_000;
const MATCH_EXTRACT_LOCK_WAIT_MS: u64 = 4;
const SUBMIT_SLOW_WARN_MS: u128 = 200;
const MATCH_SLOW_WARN_MS: u128 = 250;

pub(crate) enum MatchPersistOutcome {
    Busy,
    Noop,
    Matched(usize),
}

pub(crate) async fn reload_market_cache(state: &AppState) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT m.id, m.product_id, m.currency_id, p.name AS product, c.name AS currency
        FROM markets m
        JOIN products p ON p.id = m.product_id
        JOIN currencies c ON c.id = m.currency_id
        ORDER BY m.id
        "#,
    )
    .fetch_all(&state.db)
    .await?;

    let mut map = HashMap::new();
    for r in rows {
        let id: i16 = r.get("id");
        let product_id: i16 = r.get("product_id");
        let currency_id: i16 = r.get("currency_id");
        let product: String = r.get("product");
        let currency: String = r.get("currency");
        map.insert(
            id,
            MarketInfo {
                id,
                product_id,
                currency_id,
                product: product.clone(),
                currency: currency.clone(),
                ticker: format!("{}/{}", product, currency),
            },
        );
    }

    *lock_write(&state.markets, "store.reload_market_cache.markets_write").await = map.clone();
    lock_write(&state.engine, "store.reload_market_cache.engine_write").await.markets = map;
    Ok(())
}

pub(crate) async fn reload_name_caches(state: &AppState) -> Result<()> {
    let crows = sqlx::query("SELECT id, name FROM currencies ORDER BY id")
        .fetch_all(&state.db)
        .await?;
    let mut cm = HashMap::new();
    for r in crows {
        let id: i16 = r.get("id");
        let name: String = r.get("name");
        cm.insert(id, name);
    }
    *lock_write(&state.currency_names, "store.reload_name_caches.currency_write").await = cm;

    let prows = sqlx::query("SELECT id, name FROM products ORDER BY id")
        .fetch_all(&state.db)
        .await?;
    let mut pm = HashMap::new();
    for r in prows {
        let id: i16 = r.get("id");
        let name: String = r.get("name");
        pm.insert(id, name);
    }
    *lock_write(&state.product_names, "store.reload_name_caches.product_write").await = pm;
    Ok(())
}

pub(crate) async fn load_event_type_ids(state: &AppState) -> Result<()> {
    // Keep DB event type catalog aligned with Rust handlers.
    for code in [
        crate::EVT_USER_CREATED,
        crate::EVT_USER_UPDATED,
        crate::EVT_USER_PASSWORD_CHANGED,
        crate::EVT_FUNDS_DEPOSITED,
        crate::EVT_FUNDS_WITHDRAWN,
        crate::EVT_PRODUCTS_DEPOSITED,
        crate::EVT_PRODUCTS_WITHDRAWN,
        crate::EVT_FEE_RATE_SET,
        crate::EVT_ORDER_ACCEPTED,
        crate::EVT_ORDER_CANCELLED,
        crate::EVT_TRADE_EXECUTED,
    ] {
        let _ = sqlx::query("INSERT INTO event_types (code) VALUES ($1) ON CONFLICT (code) DO NOTHING")
            .bind(code)
            .execute(&state.db)
            .await?;
    }

    let rows = sqlx::query("SELECT id, code FROM event_types ORDER BY id")
        .fetch_all(&state.db)
        .await?;
    let mut map = HashMap::new();
    for r in rows {
        let id: i16 = r.get("id");
        let code: String = r.get("code");
        map.insert(code, id);
    }
    *lock_write(&state.event_type_ids, "store.load_event_type_ids.write").await = map;
    Ok(())
}

pub(crate) async fn reload_user_cache(state: &AppState) -> Result<()> {
    let rows = sqlx::query("SELECT id, username, email, is_admin, created_at FROM users ORDER BY id")
        .fetch_all(&state.db)
        .await?;
    let mut map = HashMap::new();
    for r in rows {
        let user_id: i64 = r.get("id");
        let username: String = r.get("username");
        let email: String = r.get("email");
        let is_admin: bool = r.get("is_admin");
        let created_at: DateTime<Utc> = r.get("created_at");
        map.insert(
            user_id,
            UserProfile {
                user_id,
                username,
                email,
                is_admin,
                created_at,
            },
        );
    }
    *lock_write(&state.user_cache, "store.reload_user_cache.write").await = map;
    Ok(())
}

async fn latest_snapshot(state: &AppState) -> Result<Option<(i64, Vec<u8>)>> {
    let row = sqlx::query("SELECT event_id_hi, state FROM snapshots ORDER BY event_id_hi DESC LIMIT 1")
        .fetch_optional(&state.db)
        .await?;
    if let Some(r) = row {
        let hi: i64 = r.get("event_id_hi");
        let bytes: Vec<u8> = r.get("state");
        Ok(Some((hi, bytes)))
    } else {
        Ok(None)
    }
}

pub(crate) async fn replay_from_db(state: &AppState) -> Result<()> {
    if let Some((_hi, blob)) = latest_snapshot(state).await? {
        if let Ok(decompressed) = zstd::decode_all(blob.as_slice()) {
            if let Ok(snap) = bincode::deserialize::<EngineSnapshot>(&decompressed) {
                lock_write(&state.engine, "store.replay.restore_snapshot").await.restore_from_snapshot(snap);
            }
        }
    }

    let last = lock_read(&state.engine, "store.replay.last_event_id").await.last_event_id;
    let rows = sqlx::query(
        r#"
        SELECT e.event_id, et.code AS code, e.created_at, e.request_id,
               e.user_id, e.market_id, e.order_id,
               e.side, e.price_cents, e.qty_units, e.expires_at,
               e.maker_order_id, e.taker_order_id,
               e.buy_user_id, e.sell_user_id, e.fee_cents,
               e.payload
        FROM events e
        JOIN event_types et ON et.id = e.event_type_id
        WHERE e.event_id > $1
        ORDER BY e.event_id ASC
        "#,
    )
    .bind(last)
    .fetch_all(&state.db)
    .await?;

    for r in rows {
        apply_event_row(state, &r).await?;
    }
    state
        .with_engine_write_profile("store.replay.rebuild_indexes", |eng| {
            eng.rebuild_books_from_open_orders();
            eng.recompute_reservations_from_open_orders();
            eng.rebuild_expiry_heap();
        })
        .await;
    Ok(())
}

async fn apply_event_row(state: &AppState, row: &sqlx::postgres::PgRow) -> Result<()> {
    let event_id: i64 = row.get("event_id");
    let code: String = row.get("code");
    let created_at: DateTime<Utc> = row.get("created_at");

    state
        .with_engine_write_profile("store.apply_event_row", |eng| {
            eng.last_event_id = event_id;

            match code.as_str() {
        crate::EVT_FEE_RATE_SET => {
            let payload: serde_json::Value = row.get("payload");
            if let Some(v) = payload.get("seller_fee_rate").and_then(|x| x.as_f64()) {
                eng.fee_rate_ppm = (v * 1_000_000.0).round() as i64;
            }
        }
        crate::EVT_FUNDS_DEPOSITED => {
            let uid: i64 = row.get("user_id");
            let payload: serde_json::Value = row.get("payload");
            let amount_cents = payload.get("amount_cents").and_then(|x| x.as_i64()).unwrap_or(0);
            let currency_id = payload.get("currency_id").and_then(|x| x.as_i64()).unwrap_or(0) as i16;
            if uid > 0 && amount_cents > 0 && currency_id > 0 {
                let w = eng.wallet_mut(uid);
                *w.tot_cents.entry(currency_id).or_insert(0) += amount_cents;
                *w.dep_cents.entry(currency_id).or_insert(0) += amount_cents;
            }
        }
        crate::EVT_FUNDS_WITHDRAWN => {
            let uid: i64 = row.get("user_id");
            let payload: serde_json::Value = row.get("payload");
            let amount_cents = payload.get("amount_cents").and_then(|x| x.as_i64()).unwrap_or(0);
            let currency_id = payload.get("currency_id").and_then(|x| x.as_i64()).unwrap_or(0) as i16;
            if uid > 0 && amount_cents > 0 && currency_id > 0 {
                let w = eng.wallet_mut(uid);
                *w.tot_cents.entry(currency_id).or_insert(0) -= amount_cents;
                *w.wdr_cents.entry(currency_id).or_insert(0) += amount_cents;
            }
        }
        crate::EVT_PRODUCTS_DEPOSITED => {
            let uid: i64 = row.get("user_id");
            let payload: serde_json::Value = row.get("payload");
            let amount_units = payload.get("amount_units").and_then(|x| x.as_i64()).unwrap_or(0);
            let product_id = payload.get("product_id").and_then(|x| x.as_i64()).unwrap_or(0) as i16;
            if uid > 0 && amount_units > 0 && product_id > 0 {
                let w = eng.wallet_mut(uid);
                *w.tot_units.entry(product_id).or_insert(0) += amount_units;
                *w.dep_units.entry(product_id).or_insert(0) += amount_units;
            }
        }
        crate::EVT_PRODUCTS_WITHDRAWN => {
            let uid: i64 = row.get("user_id");
            let payload: serde_json::Value = row.get("payload");
            let amount_units = payload.get("amount_units").and_then(|x| x.as_i64()).unwrap_or(0);
            let product_id = payload.get("product_id").and_then(|x| x.as_i64()).unwrap_or(0) as i16;
            if uid > 0 && amount_units > 0 && product_id > 0 {
                let w = eng.wallet_mut(uid);
                *w.tot_units.entry(product_id).or_insert(0) -= amount_units;
                *w.wdr_units.entry(product_id).or_insert(0) += amount_units;
            }
        }
        crate::EVT_ORDER_ACCEPTED => {
            let uid: i64 = row.get("user_id");
            let market_id: i16 = row.get("market_id");
            let order_id: i64 = row.get("order_id");
            let buy: bool = row.get("side");
            let price_cents: i64 = row.get("price_cents");
            let qty_units: i64 = row.get("qty_units");
            let expires_at: Option<DateTime<Utc>> = row.try_get("expires_at").ok();

            if order_id > 0 && uid > 0 && qty_units > 0 && price_cents > 0 {
                let o = Order {
                    order_id,
                    user_id: uid,
                    market_id,
                    buy,
                    price_cents,
                    quantity: qty_units,
                    remaining: qty_units,
                    created_at,
                    expires_at,
                    status: "OPEN".to_string(),
                };
                eng.orders.insert(order_id, o.clone());
                eng.track_order_for_user(order_id, uid);
                eng.add_order_to_book(&o);
                eng.enqueue_expiry(&o);
            }
        }
        crate::EVT_ORDER_CANCELLED => {
            let order_id: i64 = row.get("order_id");
            let mut rel: Option<(i64, i16, bool, i64, i64)> = None;
            if let Some(o) = eng.orders.get_mut(&order_id) {
                if o.status == "OPEN" || o.status == "PARTIAL" {
                    o.status = "CANCELLED".to_string();
                    rel = Some((o.user_id, o.market_id, o.buy, o.remaining, o.price_cents));
                }
            }
            if let Some((uid, mid, buy, rem, px)) = rel {
                eng.release_reservation(uid, mid, buy, rem, px);
                let _ = eng.remove_order_from_book(order_id);
            }
        }
        crate::EVT_TRADE_EXECUTED => {
            let t = Trade {
                ts_ms: created_at.timestamp_millis(),
                market_id: row.get("market_id"),
                buy_user_id: row.get("buy_user_id"),
                sell_user_id: row.get("sell_user_id"),
                buy_order_id: row.get("maker_order_id"),
                sell_order_id: row.get("taker_order_id"),
                price_cents: row.get("price_cents"),
                qty_units: row.get("qty_units"),
                fee_cents: row.get("fee_cents"),
            };
            eng.apply_trade(&t);
        }
                _ => {}
            }
        })
        .await;

    Ok(())
}

pub(crate) async fn persist_order_accepted_batch(state: &AppState, batch: &[SubmitJob]) -> Result<(), ApiError> {
    let started = std::time::Instant::now();
    if batch.is_empty() {
        return Ok(());
    }
    for j in batch {
        state.tx_journal_pending(
            format!("submit:{}", j.request_id),
            "ORDER_ACCEPTED",
            Some(j.market_id),
            vec![j.order_id],
        );
    }
    let type_id = {
        let ids = lock_read(&state.event_type_ids, "store.persist_order_accepted.event_type_ids").await;
        *ids.get(crate::EVT_ORDER_ACCEPTED).ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO events (event_type_id, request_id, user_id, market_id, order_id, side, price_cents, qty_units, expires_at, payload) ",
    );
    qb.push_values(batch, |mut b, j| {
        b.push_bind(type_id)
            .push_bind(j.request_id)
            .push_bind(j.user_id)
            .push_bind(j.market_id)
            .push_bind(j.order_id)
            .push_bind(j.buy)
            .push_bind(j.price_cents)
            .push_bind(j.qty_units)
            .push_bind(j.expires_at)
            .push_bind(serde_json::json!({}));
    });
    qb.push(" RETURNING event_id, created_at, request_id, order_id");

    let db_started = std::time::Instant::now();
    let rows = match qb.build().fetch_all(&state.db).await {
        Ok(v) => v,
        Err(e) => {
            let err_msg = format!("db error: {e}");
            for j in batch {
                state.tx_journal_rolled_back(&format!("submit:{}", j.request_id), err_msg.clone());
            }
            return Err(ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err_msg));
        }
    };
    let db_ms = db_started.elapsed().as_millis();

    let mut persisted = Vec::with_capacity(rows.len());
    for r in rows {
        let event_id: i64 = r.get("event_id");
        let created_at: DateTime<Utc> = r.get("created_at");
        let request_id: Uuid = r.get("request_id");
        let order_id: i64 = r.get("order_id");

        state.submit_status.insert(
            request_id.to_string(),
            SubmitStatus::Confirmed { order_id, event_id, created_at_ms: created_at.timestamp_millis() },
        );
        state.perf.submit_confirmed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        state.tx_journal_confirmed(&format!("submit:{}", request_id), vec![event_id]);
        persisted.push((request_id, order_id, event_id, created_at));
    }

    let jobs_by_request: HashMap<Uuid, &SubmitJob> = batch.iter().map(|j| (j.request_id, j)).collect();
    let market_ids: Vec<i16> = batch
        .iter()
        .map(|j| j.market_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let _market_guards = state.lock_markets(&market_ids).await;

    let mut apply_lock_wait_ms = 0u128;
    let apply_started = std::time::Instant::now();
    let lock_wait_started = std::time::Instant::now();
    let mut users_to_invalidate = BTreeSet::new();
    let mut markets_to_refresh = BTreeSet::new();
    state
        .with_engine_write_profile("store.persist_order_accepted.apply", |eng| {
            for (request_id, order_id, event_id, created_at) in persisted.iter().copied() {
                if let Some(job) = jobs_by_request.get(&request_id) {
                    markets_to_refresh.insert(job.market_id);
                    if let Some(existing) = eng.orders.get_mut(&order_id) {
                        let should_index_book = existing.status == "PENDING_SUBMIT";
                        existing.status = "OPEN".to_string();
                        existing.created_at = created_at;
                        existing.expires_at = job.expires_at;
                        if should_index_book {
                            let o = existing.clone();
                            eng.add_order_to_book(&o);
                            eng.enqueue_expiry(&o);
                        }
                    } else {
                        let o = Order {
                            order_id,
                            user_id: job.user_id,
                            market_id: job.market_id,
                            buy: job.buy,
                            price_cents: job.price_cents,
                            quantity: job.qty_units,
                            remaining: job.qty_units,
                            created_at,
                            expires_at: job.expires_at,
                            status: "OPEN".to_string(),
                        };
                        eng.orders.insert(order_id, o.clone());
                        eng.track_order_for_user(order_id, job.user_id);
                        eng.add_order_to_book(&o);
                        eng.enqueue_expiry(&o);
                    }
                    users_to_invalidate.insert(job.user_id);
                    eng.last_event_id = eng.last_event_id.max(event_id);
                }
            }
        })
        .await;
    for uid in users_to_invalidate {
        state.invalidate_user_read_caches(uid);
    }
    for mid in markets_to_refresh {
        state.mark_market_activity(mid);
    }
    apply_lock_wait_ms += lock_wait_started.elapsed().as_millis();
    state
        .perf
        .observe_submit_apply_lock_wait_ms(apply_lock_wait_ms as u64);
    let apply_ms = apply_started.elapsed().as_millis();
    let total_ms = started.elapsed().as_millis();
    if total_ms >= SUBMIT_SLOW_WARN_MS {
        eprintln!(
            "[submit] slow_batch batch_size={} persisted={} total_ms={} db_ms={} apply_ms={} apply_lock_wait_ms={}",
            batch.len(),
            persisted.len(),
            total_ms,
            db_ms,
            apply_ms,
            apply_lock_wait_ms
        );
    }

    Ok(())
}

pub(crate) async fn match_market_to_db(state: &AppState, market_id: i16) -> Result<MatchPersistOutcome, ApiError> {
    if let Some(eng) = try_lock_read(&state.engine, "store.match_market_to_db.engine_try_read") {
        if !eng.market_maybe_crossed(market_id) {
            return Ok(MatchPersistOutcome::Noop);
        }
    }

    let type_id = {
        let ids = lock_read(&state.event_type_ids, "store.match_market_to_db.event_type_ids").await;
        *ids.get(crate::EVT_TRADE_EXECUTED).ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "missing event type"))?
    };

    #[derive(Clone)]
    struct PendingMatch {
        bid_oid: i64,
        ask_oid: i64,
        price_cents: i64,
        qty_units: i64,
        buy_uid: i64,
        sell_uid: i64,
        fee_cents: i64,
    }

    let started = std::time::Instant::now();
    let extract_lock_wait_ms;
    let extract_work_ms;
    let mut db_ms = 0u128;
    let wallet_lock_wait_ms = 0u128;
    let mut apply_lock_wait_ms = 0u128;
    let mut apply_work_ms = 0u128;

    let pending: Vec<PendingMatch> = {
        let _market_guard = state.lock_market(market_id).await;
        let extract_started = std::time::Instant::now();
        let lock_wait_started = std::time::Instant::now();
        let mut eng = match tokio::time::timeout(
            Duration::from_millis(MATCH_EXTRACT_LOCK_WAIT_MS),
            lock_write(&state.engine, "store.match_market_to_db.engine_write_extract"),
        )
        .await
        {
            Ok(guard) => guard,
            Err(_timeout) => return Ok(MatchPersistOutcome::Busy),
        };
        let lock_wait_ms = lock_wait_started.elapsed().as_millis();
        let mut out = Vec::with_capacity(MATCH_BATCH_MAX);
        let mut consumed_by_order = HashMap::<i64, i64>::new();
        for _ in 0..MATCH_BATCH_MAX {
            if extract_started.elapsed().as_micros() >= MATCH_LOCK_BUDGET_US {
                break;
            }
            let Some(best_bid) = eng.best_matchable_bid(market_id) else { break; };
            let Some(best_ask) = eng.best_matchable_ask(market_id) else { break; };
            if best_bid < best_ask {
                break;
            }

            let bid_oid = match eng.pop_best_order_id(market_id, true) {
                Some(v) => v,
                None => break,
            };
            let ask_oid = match eng.pop_best_order_id(market_id, false) {
                Some(v) => v,
                None => {
                    eng.restore_order_to_book_front(bid_oid);
                    break;
                }
            };

            let bid_meta = eng.orders.get(&bid_oid).map(|o| (o.user_id, o.price_cents, o.remaining));
            let ask_meta = eng.orders.get(&ask_oid).map(|o| (o.user_id, o.price_cents, o.remaining));
            let pair = match (bid_meta, ask_meta) {
                (Some(bid), Some(ask)) => Some((bid, ask)),
                _ => {
                    if eng.orders.contains_key(&bid_oid) {
                        eng.restore_order_to_book_front(bid_oid);
                    }
                    if eng.orders.contains_key(&ask_oid) {
                        eng.restore_order_to_book_front(ask_oid);
                    }
                    None
                }
            };
            if let Some(((bid_uid, bid_price, bid_remaining), (ask_uid, ask_price, ask_remaining))) = pair {
                if bid_price < ask_price {
                    eng.restore_order_to_book_front(bid_oid);
                    eng.restore_order_to_book_front(ask_oid);
                    break;
                }

                let bid_consumed = *consumed_by_order.get(&bid_oid).unwrap_or(&0);
                let ask_consumed = *consumed_by_order.get(&ask_oid).unwrap_or(&0);
                let bid_avail = (bid_remaining - bid_consumed).max(0);
                let ask_avail = (ask_remaining - ask_consumed).max(0);
                let price = ask_price;
                let qty = bid_avail.min(ask_avail).max(0);
                if qty <= 0 {
                    if bid_avail > 0 {
                        eng.restore_order_to_book_front(bid_oid);
                    }
                    if ask_avail > 0 {
                        eng.restore_order_to_book_front(ask_oid);
                    }
                    continue;
                }
                let Some(fee) = crate::engine::checked_fee_cents(qty, price, eng.fee_rate_ppm) else {
                    eng.restore_order_to_book_front(bid_oid);
                    eng.restore_order_to_book_front(ask_oid);
                    eprintln!(
                        "[matcher] fee_overflow market_id={} bid_oid={} ask_oid={} qty={} price_cents={} fee_rate_ppm={}",
                        market_id, bid_oid, ask_oid, qty, price, eng.fee_rate_ppm
                    );
                    continue;
                };
                consumed_by_order.insert(bid_oid, bid_consumed + qty);
                consumed_by_order.insert(ask_oid, ask_consumed + qty);
                if bid_avail > qty {
                    eng.restore_order_to_book_front(bid_oid);
                }
                if ask_avail > qty {
                    eng.restore_order_to_book_front(ask_oid);
                }

                out.push(PendingMatch {
                    bid_oid,
                    ask_oid,
                    price_cents: price,
                    qty_units: qty,
                    buy_uid: bid_uid,
                    sell_uid: ask_uid,
                    fee_cents: fee,
                });
            }
        }
        let work_ms = extract_started.elapsed().as_millis().saturating_sub(lock_wait_ms);
        extract_lock_wait_ms = lock_wait_ms;
        state
            .perf
            .observe_matcher_extract_lock_wait_ms(lock_wait_ms as u64);
        extract_work_ms = work_ms;
        out
    };
    if pending.is_empty() {
        let total_ms = started.elapsed().as_millis();
        if total_ms >= MATCH_SLOW_WARN_MS {
            eprintln!(
                "[matcher] phase_breakdown market_id={} matched=0 total_ms={} extract_lock_wait_ms={} extract_work_ms={} db_ms={} wallet_lock_wait_ms={} apply_lock_wait_ms={} apply_work_ms={}",
                market_id,
                total_ms,
                extract_lock_wait_ms,
                extract_work_ms,
                db_ms,
                wallet_lock_wait_ms,
                apply_lock_wait_ms,
                apply_work_ms
            );
        }
        return Ok(MatchPersistOutcome::Noop);
    }

    let intent_id = Uuid::new_v4().to_string();
    let tx_id = format!("match:{intent_id}");
    state.match_journal.insert(
        intent_id.clone(),
        MatchIntent {
            intent_id: intent_id.clone(),
            market_id,
            created_at_ms: crate::now_epoch_ms(),
            entries: pending
                .iter()
                .map(|m| MatchIntentEntry {
                    bid_order_id: m.bid_oid,
                    ask_order_id: m.ask_oid,
                    price_cents: m.price_cents,
                    qty_units: m.qty_units,
                })
                .collect(),
        },
    );
    state.tx_journal_pending(
        tx_id.clone(),
        "TRADE_EXECUTED_BATCH",
        Some(market_id),
        pending
            .iter()
            .flat_map(|m| [m.bid_oid, m.ask_oid])
            .collect(),
    );

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO events (event_type_id, market_id, maker_order_id, taker_order_id, buy_user_id, sell_user_id, price_cents, qty_units, fee_cents, payload) ",
    );
    qb.push_values(pending.iter(), |mut b, m| {
        b.push_bind(type_id)
            .push_bind(market_id)
            .push_bind(m.bid_oid)
            .push_bind(m.ask_oid)
            .push_bind(m.buy_uid)
            .push_bind(m.sell_uid)
            .push_bind(m.price_cents)
            .push_bind(m.qty_units)
            .push_bind(m.fee_cents)
            .push_bind(serde_json::json!({}));
    });
    qb.push(" RETURNING event_id, created_at");
    let db_started = std::time::Instant::now();
    let rows = match qb.build().fetch_all(&state.db).await {
        Ok(v) => v,
        Err(e) => {
            state
                .with_engine_write_profile("store.match.persist_error_restore", |eng| {
                    for m in pending.iter().rev() {
                        eng.restore_order_to_book_front(m.bid_oid);
                        eng.restore_order_to_book_front(m.ask_oid);
                    }
                })
                .await;
            state.tx_journal_rolled_back(&tx_id, format!("match persist failed: {e}"));
            state.match_journal.remove(&intent_id);
            return Err(ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("match persist failed: {e}"),
            ));
        }
    };
    db_ms = db_started.elapsed().as_millis();

    let persisted_count = rows.len().min(pending.len());
    let mut persisted: Vec<(usize, i64, DateTime<Utc>)> = Vec::with_capacity(persisted_count);
    for (idx, row) in rows.into_iter().enumerate().take(persisted_count) {
        let event_id: i64 = row.get("event_id");
        let created_at: DateTime<Utc> = row.get("created_at");
        persisted.push((idx, event_id, created_at));
    }
    if persisted.is_empty() {
        state.tx_journal_rolled_back(&tx_id, "no trade events returned".to_string());
    } else {
        state.tx_journal_confirmed(&tx_id, persisted.iter().map(|(_, eid, _)| *eid).collect());
    }

    if persisted_count < pending.len() {
        let _market_guard = state.lock_market(market_id).await;
        state
            .with_engine_write_profile("store.match.partial_restore", |eng| {
                for m in pending[persisted_count..].iter().rev() {
                    eng.restore_order_to_book_front(m.bid_oid);
                    eng.restore_order_to_book_front(m.ask_oid);
                }
            })
            .await;
    }

    let mut persisted_trades_total = 0usize;
    let _market_guard = state.lock_market(market_id).await;
    let apply_started = std::time::Instant::now();
    let engine_lock_started = std::time::Instant::now();
    let mut users_to_invalidate = BTreeSet::new();
    state
        .with_engine_write_profile("store.match.apply", |eng| {
            for (idx, event_id, created_at) in &persisted {
                let m = &pending[*idx];
                let t = Trade {
                    ts_ms: created_at.timestamp_millis(),
                    market_id,
                    buy_user_id: m.buy_uid,
                    sell_user_id: m.sell_uid,
                    buy_order_id: m.bid_oid,
                    sell_order_id: m.ask_oid,
                    price_cents: m.price_cents,
                    qty_units: m.qty_units,
                    fee_cents: m.fee_cents,
                };
                eng.last_event_id = eng.last_event_id.max(*event_id);
                eng.apply_trade(&t);
                users_to_invalidate.insert(m.buy_uid);
                users_to_invalidate.insert(m.sell_uid);
                persisted_trades_total += 1;
            }
        })
        .await;
    for uid in users_to_invalidate {
        state.invalidate_user_read_caches(uid);
    }
    if persisted_trades_total > 0 {
        state.mark_market_activity(market_id);
    }
    apply_lock_wait_ms += engine_lock_started.elapsed().as_millis();
    state
        .perf
        .observe_matcher_apply_lock_wait_ms(apply_lock_wait_ms as u64);
    apply_work_ms += apply_started.elapsed().as_millis();
    state.match_journal.remove(&intent_id);
    let total_ms = started.elapsed().as_millis();
    if total_ms >= MATCH_SLOW_WARN_MS {
        eprintln!(
            "[matcher] phase_breakdown market_id={} matched={} total_ms={} extract_lock_wait_ms={} extract_work_ms={} db_ms={} wallet_lock_wait_ms={} apply_lock_wait_ms={} apply_work_ms={}",
            market_id,
            persisted_trades_total,
            total_ms,
            extract_lock_wait_ms,
            extract_work_ms,
            db_ms,
            0,
            apply_lock_wait_ms,
            apply_work_ms
        );
    }
    Ok(MatchPersistOutcome::Matched(persisted_trades_total))
}
