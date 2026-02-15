use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::Semaphore;

use crate::engine::{EngineState, Order};
use crate::state::{
    lock_read, lock_write, try_lock_read, try_lock_write, AppState, CachedOrderLevel,
    CachedTradeRow, CancelJob, CancelStatus, MarketReadCache, OrderIntent, SubmitJob,
    SubmitStatus, BATCH_BUCKET_BOUNDS, LATENCY_BUCKET_BOUNDS_MS,
};
use crate::store::{match_market_to_db, persist_order_accepted_batch, MatchPersistOutcome};

const MATCHER_SLICE_MAX_BATCHES: usize = 4;
const MATCHER_SLICE_MAX_MS: u64 = 40;
const MATCHER_RETRY_DELAY_MS: u64 = 120;
const MATCHER_PROGRESS_DELAY_MS: u64 = 1;
const MATCHER_SLOW_WARN_MS: u128 = 250;
const READ_CACHE_TICK_MS: u64 = 50;
const READ_CACHE_SCAN_BUDGET_PER_SIDE: usize = 800;
const READ_CACHE_MIN_TICK_MS: u64 = 20;
const READ_CACHE_MAX_TICK_MS: u64 = 120;
const READ_CACHE_TARGET_P95_MS: u64 = 700;
const READ_CACHE_TARGET_MAX_MS: u64 = 1000;
const READ_CACHE_CTRL_STEP_DOWN_MS: u64 = 5;
const READ_CACHE_CTRL_STEP_UP_MS: u64 = 10;
const ORDER_INTENT_BATCH_MAX: usize = 64;
const ORDER_INTENT_COALESCE_MS_DEFAULT: u64 = 5;
const EXPIRY_MAX_PER_TICK: usize = 256;
const ORDER_INTENT_MAX_CONCURRENT_MARKETS: usize = 2;

static MATCH_RUNS: AtomicU64 = AtomicU64::new(0);
static MATCH_TRADES: AtomicU64 = AtomicU64::new(0);
static MATCH_ERRORS: AtomicU64 = AtomicU64::new(0);
static MATCH_NO_PROGRESS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
enum MarketCommand {
    Match,
}

#[derive(Debug, Clone, Copy)]
enum MarketIntentCommand {
    SubmitIntent,
}

fn matcher_max_concurrent_markets() -> usize {
    std::env::var("MATCHER_MAX_CONCURRENT_MARKETS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(2)
}

fn p95_from_hist_delta(bounds: &[u64], delta: &[u64]) -> Option<u64> {
    let total: u64 = delta.iter().copied().sum();
    if total == 0 {
        return None;
    }
    let target = ((total as f64) * 0.95).ceil() as u64;
    let mut acc = 0u64;
    for (i, c) in delta.iter().enumerate() {
        acc += *c;
        if acc >= target {
            if let Some(v) = bounds.get(i) {
                return Some(*v);
            }
            return bounds.last().copied();
        }
    }
    Some(*bounds.last().unwrap_or(&u64::MAX))
}

fn order_intent_coalesce_ms() -> u64 {
    std::env::var("ORDER_INTENT_COALESCE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(ORDER_INTENT_COALESCE_MS_DEFAULT)
        .min(20)
}

fn read_cache_tick_ms() -> u64 {
    std::env::var("READ_CACHE_TICK_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(READ_CACHE_TICK_MS)
        .clamp(5, 500)
}

fn read_cache_min_tick_ms() -> u64 {
    std::env::var("READ_CACHE_MIN_TICK_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(READ_CACHE_MIN_TICK_MS)
        .clamp(5, 500)
}

fn read_cache_max_tick_ms(min_tick: u64) -> u64 {
    std::env::var("READ_CACHE_MAX_TICK_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(READ_CACHE_MAX_TICK_MS)
        .clamp(min_tick, 1000)
}

fn read_cache_target_p95_ms() -> u64 {
    std::env::var("READ_CACHE_TARGET_P95_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(READ_CACHE_TARGET_P95_MS)
        .clamp(100, 5000)
}

fn read_cache_target_max_ms() -> u64 {
    std::env::var("READ_CACHE_TARGET_MAX_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(READ_CACHE_TARGET_MAX_MS)
        .clamp(200, 10_000)
}

fn p95_from_values(values: &mut [u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let idx = ((values.len() as f64) * 0.95).ceil() as usize;
    let pos = idx.saturating_sub(1).min(values.len() - 1);
    Some(values[pos])
}

fn build_market_read_cache(eng: &EngineState, market_id: i16, now_ms: i64) -> MarketReadCache {
    let mut buy_rows = Vec::<CachedOrderLevel>::new();
    let mut sell_rows = Vec::<CachedOrderLevel>::new();
    let mut buy_examined = 0usize;
    let mut sell_examined = 0usize;

    let (best_bid_cents, best_ask_cents, active_buy_orders, active_sell_orders) = if let Some(book) = eng.books.get(&market_id) {
        for (price, level) in book.buys.iter().rev() {
            for oid in &level.q {
                buy_examined += 1;
                if buy_examined > READ_CACHE_SCAN_BUDGET_PER_SIDE {
                    break;
                }
                let Some(o) = eng.orders.get(oid) else { continue; };
                if o.status != "OPEN" && o.status != "PARTIAL" {
                    continue;
                }
                if o.remaining <= 0 {
                    continue;
                }
                let exp_ms = o.expires_at.map(|t| t.timestamp_millis());
                if exp_ms.map(|ms| ms <= now_ms).unwrap_or(false) {
                    continue;
                }
                buy_rows.push(CachedOrderLevel {
                    order_id: o.order_id,
                    user_id: o.user_id,
                    price_cents: *price,
                    remaining: o.remaining,
                    has_non_expiring: o.expires_at.is_none(),
                    expires_in_seconds: exp_ms.map(|ms| ((ms - now_ms) / 1000).max(0)),
                });
                if buy_rows.len() >= crate::ORDERBOOK_MAX_LIMIT_PER_SIDE {
                    break;
                }
            }
            if buy_rows.len() >= crate::ORDERBOOK_MAX_LIMIT_PER_SIDE || buy_examined > READ_CACHE_SCAN_BUDGET_PER_SIDE {
                break;
            }
        }

        for (price, level) in &book.sells {
            for oid in &level.q {
                sell_examined += 1;
                if sell_examined > READ_CACHE_SCAN_BUDGET_PER_SIDE {
                    break;
                }
                let Some(o) = eng.orders.get(oid) else { continue; };
                if o.status != "OPEN" && o.status != "PARTIAL" {
                    continue;
                }
                if o.remaining <= 0 {
                    continue;
                }
                let exp_ms = o.expires_at.map(|t| t.timestamp_millis());
                if exp_ms.map(|ms| ms <= now_ms).unwrap_or(false) {
                    continue;
                }
                sell_rows.push(CachedOrderLevel {
                    order_id: o.order_id,
                    user_id: o.user_id,
                    price_cents: *price,
                    remaining: o.remaining,
                    has_non_expiring: o.expires_at.is_none(),
                    expires_in_seconds: exp_ms.map(|ms| ((ms - now_ms) / 1000).max(0)),
                });
                if sell_rows.len() >= crate::ORDERBOOK_MAX_LIMIT_PER_SIDE {
                    break;
                }
            }
            if sell_rows.len() >= crate::ORDERBOOK_MAX_LIMIT_PER_SIDE || sell_examined > READ_CACHE_SCAN_BUDGET_PER_SIDE {
                break;
            }
        }

        (
            book.buys.keys().next_back().copied().unwrap_or(0),
            book.sells.keys().next().copied().unwrap_or(0),
            book.buy_count,
            book.sell_count,
        )
    } else {
        (0, 0, 0, 0)
    };

    let trades = eng
        .trades
        .get(&market_id)
        .map(|list| {
            list.iter()
                .take(250)
                .map(|t| CachedTradeRow {
                    ts_ms: t.ts_ms,
                    qty_units: t.qty_units,
                    price_cents: t.price_cents,
                    buy_user_id: t.buy_user_id,
                    sell_user_id: t.sell_user_id,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let crossed = best_bid_cents > 0 && best_ask_cents > 0 && best_bid_cents >= best_ask_cents;
    MarketReadCache {
        market_id,
        best_bid_cents,
        best_ask_cents,
        active_buy_orders,
        active_sell_orders,
        crossed,
        buys: buy_rows,
        sells: sell_rows,
        trades,
        updated_at_ms: now_ms,
    }
}

pub(crate) async fn prime_read_caches(state: &AppState) {
    let market_ids: Vec<i16> = {
        let markets = lock_read(&state.markets, "tasks.prime_read_caches.markets_read").await;
        let mut ids: Vec<i16> = markets.keys().copied().collect();
        ids.sort_unstable();
        ids
    };
    let now_ms = crate::now_epoch_ms();
    let mut total_buy = 0i64;
    let mut total_sell = 0i64;
    if let Some(eng) = try_lock_read(&state.engine, "tasks.prime_read_caches.engine_try_read") {
        for market_id in market_ids {
            let cache = build_market_read_cache(&eng, market_id, now_ms);
            total_buy += cache.active_buy_orders;
            total_sell += cache.active_sell_orders;
            state.market_read_cache.insert(market_id, cache);
        }
        let mut sys = lock_write(&state.system_read_cache, "tasks.prime_read_caches.system_write").await;
        sys.total_active = total_buy + total_sell;
        sys.active_buy_orders = total_buy;
        sys.active_sell_orders = total_sell;
        sys.last_hour_count = eng.match_sum_last_hour as i64;
        sys.updated_at_ms = now_ms;
    }
}

pub(crate) fn enqueue_market_match(state: &AppState, market_id: i16) {
    if market_id <= 0 {
        return;
    }
    if state.match_pending.insert(market_id, ()).is_some() {
        return;
    }
    if state.match_tx.send(market_id).is_err() {
        state.match_pending.remove(&market_id);
    }
}

fn enqueue_market_match_after_delay(state: AppState, market_id: i16, delay_ms: u64) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms.max(1))).await;
        enqueue_market_match(&state, market_id);
    });
}

async fn run_market_match_slice(state: &AppState, market_id: i16) {
    let started = std::time::Instant::now();
    let mut matched_total = 0usize;
    let mut batches = 0usize;
    let mut busy_batches = 0usize;
    let mut db_error: Option<String> = None;

    while batches < MATCHER_SLICE_MAX_BATCHES && started.elapsed() < Duration::from_millis(MATCHER_SLICE_MAX_MS) {
        batches += 1;
        match match_market_to_db(state, market_id).await {
            Ok(MatchPersistOutcome::Noop) => break,
            Ok(MatchPersistOutcome::Busy) => {
                state.perf.match_busy.fetch_add(1, Ordering::Relaxed);
                busy_batches += 1;
                break;
            }
            Ok(MatchPersistOutcome::Matched(n)) => {
                state.perf.match_batches.fetch_add(1, Ordering::Relaxed);
                state.perf.match_trades.fetch_add(n as u64, Ordering::Relaxed);
                matched_total = matched_total.saturating_add(n);
                MATCH_TRADES.fetch_add(n as u64, Ordering::Relaxed);
            }
            Err(e) => {
                state.perf.match_errors.fetch_add(1, Ordering::Relaxed);
                db_error = Some(e.detail);
                MATCH_ERRORS.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
        tokio::task::yield_now().await;
    }

    MATCH_RUNS.fetch_add(1, Ordering::Relaxed);
    let elapsed_core = started.elapsed().as_millis();

    let (crossed, best_bid, best_ask) = if let Some(eng) = try_lock_read(&state.engine, "tasks.run_market_match_slice.engine_try_read") {
        (
            eng.market_is_crossed(market_id),
            eng.best_live_bid(market_id),
            eng.best_live_ask(market_id),
        )
    } else {
        (false, None, None)
    };

    if let Some(err) = db_error {
        eprintln!(
            "[matcher] db_error market_id={} matched_total={} batches={} elapsed_ms={} error={}",
            market_id,
            matched_total,
            batches,
            started.elapsed().as_millis(),
            err
        );
        enqueue_market_match_after_delay(state.clone(), market_id, MATCHER_RETRY_DELAY_MS);
        return;
    }

    if busy_batches > 0 {
        enqueue_market_match_after_delay(state.clone(), market_id, MATCHER_RETRY_DELAY_MS);
        return;
    }

    if crossed {
        if matched_total == 0 {
            if let Some(mut eng) = try_lock_write(&state.engine, "tasks.run_market_match_slice.engine_try_write") {
                let scrubbed = eng.scrub_market_front(market_id, 3);
                if scrubbed > 0 {
                    eprintln!(
                        "[matcher] front_scrub market_id={} passes={} best_bid={:?} best_ask={:?}",
                        market_id,
                        scrubbed,
                        eng.best_bid(market_id),
                        eng.best_ask(market_id)
                    );
                }
            }
            MATCH_NO_PROGRESS.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[matcher] crossed_without_progress market_id={} best_bid={:?} best_ask={:?} batches={} elapsed_ms={}",
                market_id,
                best_bid,
                best_ask,
                batches,
                elapsed_core
            );
            enqueue_market_match_after_delay(state.clone(), market_id, MATCHER_RETRY_DELAY_MS);
        } else {
            enqueue_market_match_after_delay(state.clone(), market_id, MATCHER_PROGRESS_DELAY_MS);
        }
    }

    if elapsed_core >= MATCHER_SLOW_WARN_MS {
        eprintln!(
            "[matcher] slow_slice market_id={} matched_total={} batches={} busy_batches={} elapsed_ms={} crossed={}",
            market_id, matched_total, batches, busy_batches, elapsed_core, crossed
        );
    }
}

pub(crate) fn start_background_tasks(
    state: AppState,
    mut order_intent_rx: mpsc::Receiver<OrderIntent>,
    mut submit_rx: mpsc::Receiver<SubmitJob>,
    mut cancel_rx: mpsc::Receiver<CancelJob>,
    mut match_rx: mpsc::UnboundedReceiver<i16>,
) {
    let min_tick_ms = read_cache_min_tick_ms();
    let max_tick_ms = read_cache_max_tick_ms(min_tick_ms);
    let target_p95_ms = read_cache_target_p95_ms();
    let target_max_ms = read_cache_target_max_ms();
    let initial_tick_ms = read_cache_tick_ms().clamp(min_tick_ms, max_tick_ms);
    let read_cache_tick_shared = Arc::new(AtomicU64::new(initial_tick_ms));
    state
        .perf
        .read_cache_tick_ms
        .store(initial_tick_ms, Ordering::Relaxed);

    // 1) Request status pruning.
    let s_clean = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let cutoff = crate::now_epoch_ms() - crate::ASYNC_SUBMIT_STATUS_TTL_MS;
            let keys: Vec<String> = s_clean
                .submit_status
                .iter()
                .filter_map(|kv| {
                    let too_old = match kv.value() {
                        SubmitStatus::Received { created_at_ms, .. } => *created_at_ms < cutoff,
                        SubmitStatus::Confirmed { created_at_ms, .. } => *created_at_ms < cutoff,
                        SubmitStatus::Failed { created_at_ms, .. } => *created_at_ms < cutoff,
                    };
                    if too_old { Some(kv.key().clone()) } else { None }
                })
                .collect();
            for k in keys { s_clean.submit_status.remove(&k); }
            let cancel_keys: Vec<String> = s_clean
                .cancel_status
                .iter()
                .filter_map(|kv| {
                    let too_old = match kv.value() {
                        CancelStatus::Received { created_at_ms, .. } => *created_at_ms < cutoff,
                        CancelStatus::Confirmed { created_at_ms, .. } => *created_at_ms < cutoff,
                        CancelStatus::Failed { created_at_ms, .. } => *created_at_ms < cutoff,
                    };
                    if too_old { Some(kv.key().clone()) } else { None }
                })
                .collect();
            for k in cancel_keys { s_clean.cancel_status.remove(&k); }

            let stale_intent_cutoff = crate::now_epoch_ms() - 30_000;
            let stale_intents: Vec<String> = s_clean
                .match_journal
                .iter()
                .filter_map(|kv| {
                    if kv.value().created_at_ms < stale_intent_cutoff {
                        Some(kv.key().clone())
                    } else {
                        None
                    }
                })
                .collect();
            for k in stale_intents {
                s_clean.match_journal.remove(&k);
            }

            let stale_tx_cutoff = crate::now_epoch_ms() - 60_000;
            let stale_txs: Vec<String> = s_clean
                .tx_journal
                .iter()
                .filter_map(|kv| {
                    if kv.value().updated_at_ms < stale_tx_cutoff {
                        Some(kv.key().clone())
                    } else {
                        None
                    }
                })
                .collect();
            for k in stale_txs {
                s_clean.tx_journal.remove(&k);
            }
        }
    });

    // 1b) Advance rolling match window even during inactivity.
    let s_match = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let now_sec = crate::now_epoch_secs();
            if let Some(mut eng) = try_lock_write(&s_match.engine, "tasks.match_window.engine_try_write") {
                let hold_started = std::time::Instant::now();
                eng.advance_match_window_to(now_sec);
                let hold_ms = hold_started.elapsed().as_millis();
                if hold_ms >= 200 {
                    eprintln!(
                        "[engine_lock] label=tasks.match_window.advance wait_ms=0 hold_ms={}",
                        hold_ms
                    );
                }
            }
        }
    });

    // 1bb) Precompute read model incrementally (one market per tick) to keep
    // HTTP reads off hot locks without long full-engine scans.
    let s_read = state.clone();
    let read_tick_worker = read_cache_tick_shared.clone();
    tokio::spawn(async move {
        let mut rr_idx: usize = 0;
        eprintln!(
            "[read_cache] worker=0 tick_ms={} min_tick_ms={} max_tick_ms={} target_p95_ms={} target_max_ms={}",
            initial_tick_ms, min_tick_ms, max_tick_ms, target_p95_ms, target_max_ms
        );
        loop {
            let tick_ms = read_tick_worker
                .load(Ordering::Relaxed)
                .clamp(min_tick_ms, max_tick_ms);
            tokio::time::sleep(Duration::from_millis(tick_ms)).await;
            s_read.perf.read_cache_ticks.fetch_add(1, Ordering::Relaxed);
            let now_ms = crate::now_epoch_ms();
            let market_ids: Vec<i16> = if let Some(markets) = try_lock_read(&s_read.markets, "tasks.read_cache.markets_try_read") {
                let mut ids: Vec<i16> = markets.keys().copied().collect();
                ids.sort_unstable();
                ids
            } else {
                s_read.perf.read_cache_markets_busy.fetch_add(1, Ordering::Relaxed);
                Vec::new()
            };
            if market_ids.is_empty() {
                continue;
            }
            // Stale-first selection: pick the market with the oldest cached snapshot.
            // This reduces user-visible jitter versus strict round-robin.
            let start_idx = rr_idx % market_ids.len();
            let mut market_id = market_ids[start_idx];
            let mut best_age = i64::MIN;
            for step in 0..market_ids.len() {
                let idx = (start_idx + step) % market_ids.len();
                let mid = market_ids[idx];
                let age = s_read
                    .market_read_cache
                    .get(&mid)
                    .map(|c| now_ms.saturating_sub(c.updated_at_ms))
                    .unwrap_or(i64::MAX / 4);
                if age > best_age {
                    best_age = age;
                    market_id = mid;
                }
            }
            rr_idx = rr_idx.wrapping_add(1);

            let cache_opt = if let Some(eng) = try_lock_read(&s_read.engine, "tasks.read_cache.engine_try_read") {
                let build_started = std::time::Instant::now();
                let cache = build_market_read_cache(&eng, market_id, now_ms);
                let build_ms = build_started.elapsed().as_millis() as u64;
                s_read.perf.observe_read_cache_build_ms(build_ms);
                if build_ms >= 200 {
                    eprintln!("[read_cache] slow_build market_id={} build_ms={}", market_id, build_ms);
                }
                Some(cache)
            } else {
                s_read.perf.read_cache_engine_busy.fetch_add(1, Ordering::Relaxed);
                None
            };
            if let Some(cache) = cache_opt {
                s_read.market_read_cache.insert(market_id, cache);
                s_read.perf.read_cache_updates.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    // 1bc) Aggregate system read cache from per-market caches.
    let s_sys = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let mut total_buy = 0i64;
            let mut total_sell = 0i64;
            for kv in s_sys.market_read_cache.iter() {
                total_buy += kv.value().active_buy_orders;
                total_sell += kv.value().active_sell_orders;
            }
            let last_hour = if let Some(eng) = try_lock_read(&s_sys.engine, "tasks.system_cache.engine_try_read") {
                eng.match_sum_last_hour as i64
            } else {
                lock_read(&s_sys.system_read_cache, "tasks.system_cache.aggregate_read").await.last_hour_count
            };
            let mut sys = lock_write(&s_sys.system_read_cache, "tasks.system_cache.aggregate_write").await;
            sys.total_active = total_buy + total_sell;
            sys.active_buy_orders = total_buy;
            sys.active_sell_orders = total_sell;
            sys.last_hour_count = last_hour;
            sys.updated_at_ms = crate::now_epoch_ms();
        }
    });

    // 1c) Expiry worker.
    let s_exp = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(crate::ENGINE_EXPIRY_POLL_MS.max(10))).await;
            let now_ms = crate::now_epoch_ms();
            let (expired, has_more_due, crossed_mids) = {
                if let Some(mut eng) = try_lock_write(&s_exp.engine, "tasks.expiry.engine_try_write") {
                    let hold_started = std::time::Instant::now();
                    let (expired, has_more_due) = eng.expire_due_orders_budget(now_ms, EXPIRY_MAX_PER_TICK);
                    let crossed_mids = if expired > 0 {
                        eng.books
                            .keys()
                            .copied()
                            .filter(|mid| eng.market_is_crossed(*mid))
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    let hold_ms = hold_started.elapsed().as_millis();
                    if hold_ms >= 200 {
                        eprintln!(
                            "[engine_lock] label=tasks.expiry.apply wait_ms=0 hold_ms={} expired={} has_more_due={}",
                            hold_ms, expired, has_more_due
                        );
                    }
                    (expired, has_more_due, crossed_mids)
                } else {
                    (0usize, false, Vec::new())
                }
            };
            if expired > 0 {
                for mid in crossed_mids {
                    enqueue_market_match(&s_exp, mid);
                }
            }
            if has_more_due {
                tokio::task::yield_now().await;
            }
        }
    });

    // 1d) Per-market market workers (actor-style; matcher command wired now).
    let s_matcher = state.clone();
    tokio::spawn(async move {
        let market_ids: Vec<i16> = {
            let markets = lock_read(&s_matcher.markets, "tasks.matcher_worker.markets_read").await;
            let mut ids: Vec<i16> = markets.keys().copied().collect();
            ids.sort_unstable();
            ids
        };
        let match_slots = Arc::new(Semaphore::new(matcher_max_concurrent_markets().max(1)));

        let mut routes: HashMap<i16, mpsc::UnboundedSender<MarketCommand>> = HashMap::new();
        for mid in market_ids {
            let (tx_mid, mut rx_mid) = mpsc::unbounded_channel::<MarketCommand>();
            routes.insert(mid, tx_mid);
            let s_mid = s_matcher.clone();
            let slots = match_slots.clone();
            tokio::spawn(async move {
                while let Some(cmd) = rx_mid.recv().await {
                    if s_mid.match_running.insert(mid, ()).is_some() {
                        continue;
                    }
                    let _permit = slots.acquire().await.ok();
                    match cmd {
                        MarketCommand::Match => run_market_match_slice(&s_mid, mid).await,
                    }
                    s_mid.match_running.remove(&mid);
                }
            });
        }

        while let Some(market_id) = match_rx.recv().await {
            s_matcher.match_pending.remove(&market_id);
            if let Some(tx_mid) = routes.get(&market_id) {
                let _ = tx_mid.send(MarketCommand::Match);
            }
        }
    });

    // 1e) Matcher telemetry.
    let s_match_stats = state.clone();
    tokio::spawn(async move {
        let mut last_runs = 0u64;
        let mut last_trades = 0u64;
        let mut last_errors = 0u64;
        let mut last_no_progress = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let runs = MATCH_RUNS.load(Ordering::Relaxed);
            let trades = MATCH_TRADES.load(Ordering::Relaxed);
            let errors = MATCH_ERRORS.load(Ordering::Relaxed);
            let no_progress = MATCH_NO_PROGRESS.load(Ordering::Relaxed);
            let pending = s_match_stats.match_pending.len();
            let running = s_match_stats.match_running.len();
            let intents = s_match_stats.match_journal.len();
            if runs != last_runs || trades != last_trades || errors != last_errors || no_progress != last_no_progress || pending > 0 || running > 0 || intents > 0 {
                eprintln!(
                    "[matcher] runs={} trades={} errors={} crossed_no_progress={} pending_markets={} running_markets={} pending_intents={}",
                    runs, trades, errors, no_progress, pending, running, intents
                );
                last_runs = runs;
                last_trades = trades;
                last_errors = errors;
                last_no_progress = no_progress;
            }
        }
    });

    // 1f) Performance telemetry (interval histograms + queue depths).
    let s_perf = state.clone();
    let read_tick_ctrl = read_cache_tick_shared.clone();
    tokio::spawn(async move {
        let mut prev_submit_intake = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
        let mut prev_submit_apply = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
        let mut prev_match_extract = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
        let mut prev_match_apply = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
        let mut prev_batch = vec![0u64; BATCH_BUCKET_BOUNDS.len() + 1];
        let mut prev_read_cache_build = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
        let mut prev_read_ticks = 0u64;
        let mut prev_read_updates = 0u64;
        let mut prev_read_engine_busy = 0u64;
        let mut prev_read_markets_busy = 0u64;
        let mut stale_hot_windows = 0u8;
        let mut busy_hot_windows = 0u8;
        let mut slack_windows = 0u8;
        let mut pressure_hot_windows = 0u8;

        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            let mut cur_submit_intake = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
            let mut cur_submit_apply = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
            let mut cur_match_extract = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
            let mut cur_match_apply = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];
            let mut cur_batch = vec![0u64; BATCH_BUCKET_BOUNDS.len() + 1];
            let mut cur_read_cache_build = vec![0u64; LATENCY_BUCKET_BOUNDS_MS.len() + 1];

            for (i, c) in s_perf.perf.submit_intake_lock_wait_hist.iter().enumerate() {
                cur_submit_intake[i] = c.load(Ordering::Relaxed);
            }
            for (i, c) in s_perf.perf.submit_apply_lock_wait_hist.iter().enumerate() {
                cur_submit_apply[i] = c.load(Ordering::Relaxed);
            }
            for (i, c) in s_perf.perf.matcher_extract_lock_wait_hist.iter().enumerate() {
                cur_match_extract[i] = c.load(Ordering::Relaxed);
            }
            for (i, c) in s_perf.perf.matcher_apply_lock_wait_hist.iter().enumerate() {
                cur_match_apply[i] = c.load(Ordering::Relaxed);
            }
            for (i, c) in s_perf.perf.submit_intake_batch_hist.iter().enumerate() {
                cur_batch[i] = c.load(Ordering::Relaxed);
            }
            for (i, c) in s_perf.perf.read_cache_build_hist.iter().enumerate() {
                cur_read_cache_build[i] = c.load(Ordering::Relaxed);
            }

            let delta_submit_intake: Vec<u64> = cur_submit_intake
                .iter()
                .zip(prev_submit_intake.iter())
                .map(|(a, b)| a.saturating_sub(*b))
                .collect();
            let delta_submit_apply: Vec<u64> = cur_submit_apply
                .iter()
                .zip(prev_submit_apply.iter())
                .map(|(a, b)| a.saturating_sub(*b))
                .collect();
            let delta_match_extract: Vec<u64> = cur_match_extract
                .iter()
                .zip(prev_match_extract.iter())
                .map(|(a, b)| a.saturating_sub(*b))
                .collect();
            let delta_match_apply: Vec<u64> = cur_match_apply
                .iter()
                .zip(prev_match_apply.iter())
                .map(|(a, b)| a.saturating_sub(*b))
                .collect();
            let delta_batch: Vec<u64> = cur_batch
                .iter()
                .zip(prev_batch.iter())
                .map(|(a, b)| a.saturating_sub(*b))
                .collect();
            let delta_read_cache_build: Vec<u64> = cur_read_cache_build
                .iter()
                .zip(prev_read_cache_build.iter())
                .map(|(a, b)| a.saturating_sub(*b))
                .collect();

            prev_submit_intake = cur_submit_intake;
            prev_submit_apply = cur_submit_apply;
            prev_match_extract = cur_match_extract;
            prev_match_apply = cur_match_apply;
            prev_batch = cur_batch;
            prev_read_cache_build = cur_read_cache_build;

            let read_ticks = s_perf.perf.read_cache_ticks.load(Ordering::Relaxed);
            let read_updates = s_perf.perf.read_cache_updates.load(Ordering::Relaxed);
            let read_engine_busy = s_perf.perf.read_cache_engine_busy.load(Ordering::Relaxed);
            let read_markets_busy = s_perf.perf.read_cache_markets_busy.load(Ordering::Relaxed);
            let delta_read_ticks = read_ticks.saturating_sub(prev_read_ticks);
            let delta_read_updates = read_updates.saturating_sub(prev_read_updates);
            let delta_read_engine_busy = read_engine_busy.saturating_sub(prev_read_engine_busy);
            let delta_read_markets_busy = read_markets_busy.saturating_sub(prev_read_markets_busy);
            prev_read_ticks = read_ticks;
            prev_read_updates = read_updates;
            prev_read_engine_busy = read_engine_busy;
            prev_read_markets_busy = read_markets_busy;

            let batch_total: u64 = delta_batch.iter().sum();
            let batch1 = *delta_batch.first().unwrap_or(&0);
            let batch_ge4 = delta_batch
                .iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    let upper = BATCH_BUCKET_BOUNDS.get(i).copied().unwrap_or(u64::MAX);
                    if upper >= 4 { Some(*v) } else { None }
                })
                .sum::<u64>();

            let order_q = s_perf.perf.order_intent_queue_len.load(Ordering::Relaxed);
            let submit_inflight = s_perf.submit_inflight.load(Ordering::Relaxed);
            let match_pending = s_perf.match_pending.len();
            let match_running = s_perf.match_running.len();
            let current_tick_ms = read_tick_ctrl
                .load(Ordering::Relaxed)
                .clamp(min_tick_ms, max_tick_ms);

            let now_ms = crate::now_epoch_ms();
            let mut stale_ages = Vec::new();
            for kv in s_perf.market_read_cache.iter() {
                let age = now_ms.saturating_sub(kv.value().updated_at_ms).max(0) as u64;
                stale_ages.push(age);
            }
            let stale_max = stale_ages.iter().copied().max().unwrap_or(0);
            let stale_avg = if stale_ages.is_empty() {
                0
            } else {
                stale_ages.iter().copied().sum::<u64>() / (stale_ages.len() as u64)
            };
            let stale_p95 = p95_from_values(&mut stale_ages);
            let stale_p95_v = stale_p95.unwrap_or(0);
            let busy_ratio = if delta_read_ticks == 0 {
                0.0
            } else {
                (delta_read_engine_busy + delta_read_markets_busy) as f64 / (delta_read_ticks as f64)
            };
            let matcher_wait_p95 = p95_from_hist_delta(&LATENCY_BUCKET_BOUNDS_MS, &delta_match_apply)
                .unwrap_or(0)
                .max(p95_from_hist_delta(&LATENCY_BUCKET_BOUNDS_MS, &delta_match_extract).unwrap_or(0));

            if stale_p95_v > target_p95_ms || stale_max > target_max_ms {
                stale_hot_windows = stale_hot_windows.saturating_add(1);
            } else {
                stale_hot_windows = 0;
            }
            if busy_ratio > 0.20 || matcher_wait_p95 >= 20 {
                busy_hot_windows = busy_hot_windows.saturating_add(1);
            } else {
                busy_hot_windows = 0;
            }
            if order_q >= 20 || submit_inflight >= 3 {
                pressure_hot_windows = pressure_hot_windows.saturating_add(1);
            } else {
                pressure_hot_windows = 0;
            }
            if stale_p95_v < (target_p95_ms / 2) && stale_max < target_p95_ms && busy_ratio < 0.05 && matcher_wait_p95 <= 2 {
                slack_windows = slack_windows.saturating_add(1);
            } else {
                slack_windows = 0;
            }

            let mut next_tick_ms = current_tick_ms;
            let mut ctrl_reason: Option<&'static str> = None;
            if pressure_hot_windows >= 1 {
                next_tick_ms = current_tick_ms.saturating_add(READ_CACHE_CTRL_STEP_UP_MS).min(max_tick_ms);
                ctrl_reason = Some("submit_pressure_backoff");
                pressure_hot_windows = 0;
                busy_hot_windows = 0;
                stale_hot_windows = 0;
                slack_windows = 0;
            } else if busy_hot_windows >= 2 {
                next_tick_ms = current_tick_ms.saturating_add(READ_CACHE_CTRL_STEP_UP_MS).min(max_tick_ms);
                ctrl_reason = Some("contention_backoff");
                busy_hot_windows = 0;
                stale_hot_windows = 0;
                slack_windows = 0;
            } else if stale_hot_windows >= 2 && busy_ratio < 0.25 && matcher_wait_p95 < 50 {
                next_tick_ms = current_tick_ms.saturating_sub(READ_CACHE_CTRL_STEP_DOWN_MS).max(min_tick_ms);
                ctrl_reason = Some("staleness_catchup");
                stale_hot_windows = 0;
                slack_windows = 0;
            } else if slack_windows >= 3 {
                next_tick_ms = current_tick_ms.saturating_add(READ_CACHE_CTRL_STEP_DOWN_MS).min(max_tick_ms);
                ctrl_reason = Some("slack_relax");
                slack_windows = 0;
            }

            if next_tick_ms != current_tick_ms {
                read_tick_ctrl.store(next_tick_ms, Ordering::Relaxed);
                s_perf
                    .perf
                    .read_cache_tick_ms
                    .store(next_tick_ms, Ordering::Relaxed);
                eprintln!(
                    "[read_cache_ctrl] reason={} tick_ms={}=>{} stale_p95_ms={} stale_max_ms={} busy_ratio={:.3} matcher_wait_p95_ms={} order_intent_q={} submit_inflight={}",
                    ctrl_reason.unwrap_or("unknown"),
                    current_tick_ms,
                    next_tick_ms,
                    stale_p95_v,
                    stale_max,
                    busy_ratio,
                    matcher_wait_p95,
                    order_q,
                    submit_inflight
                );
            } else {
                s_perf
                    .perf
                    .read_cache_tick_ms
                    .store(current_tick_ms, Ordering::Relaxed);
            }

            eprintln!(
                "[perf] submit_intake_p95_ms={:?} submit_apply_p95_ms={:?} matcher_extract_p95_ms={:?} matcher_apply_p95_ms={:?} read_cache_tick_ms={} read_cache_build_p95_ms={:?} read_cache_ticks={} read_cache_updates={} read_cache_engine_busy={} read_cache_markets_busy={} read_cache_stale_p95_ms={:?} read_cache_stale_avg_ms={} read_cache_stale_max_ms={} intake_batches={} batch1={} batch_ge4={} order_intent_q={} submit_inflight={} match_pending={} match_running={}",
                p95_from_hist_delta(&LATENCY_BUCKET_BOUNDS_MS, &delta_submit_intake),
                p95_from_hist_delta(&LATENCY_BUCKET_BOUNDS_MS, &delta_submit_apply),
                p95_from_hist_delta(&LATENCY_BUCKET_BOUNDS_MS, &delta_match_extract),
                p95_from_hist_delta(&LATENCY_BUCKET_BOUNDS_MS, &delta_match_apply),
                current_tick_ms,
                p95_from_hist_delta(&LATENCY_BUCKET_BOUNDS_MS, &delta_read_cache_build),
                delta_read_ticks,
                delta_read_updates,
                delta_read_engine_busy,
                delta_read_markets_busy,
                stale_p95,
                stale_avg,
                stale_max,
                batch_total,
                batch1,
                batch_ge4,
                order_q,
                submit_inflight,
                match_pending,
                match_running
            );
        }
    });

    // 2) Market-routed reserve/provisional writer (one channel per market, bounded global concurrency).
    let s_order = state.clone();
    tokio::spawn(async move {
        let base_coalesce_ms = order_intent_coalesce_ms();
        let market_ids: Vec<i16> = {
            let markets = lock_read(&s_order.markets, "tasks.intent_worker.markets_read").await;
            let mut ids: Vec<i16> = markets.keys().copied().collect();
            ids.sort_unstable();
            ids
        };
        let intent_slots = Arc::new(Semaphore::new(ORDER_INTENT_MAX_CONCURRENT_MARKETS.max(1)));
        let mut routes: HashMap<i16, mpsc::UnboundedSender<(MarketIntentCommand, OrderIntent)>> = HashMap::new();
        for mid in market_ids {
            let (tx_mid, mut rx_mid) = mpsc::unbounded_channel::<(MarketIntentCommand, OrderIntent)>();
            routes.insert(mid, tx_mid);
            let s_mid = s_order.clone();
            let slots = intent_slots.clone();
            tokio::spawn(async move {
                while let Some((cmd, first_intent)) = rx_mid.recv().await {
                    let _permit = slots.acquire().await.ok();
                    match cmd {
                        MarketIntentCommand::SubmitIntent => {
                            let mut batch = Vec::with_capacity(ORDER_INTENT_BATCH_MAX);
                            batch.push(first_intent);
                            // Short adaptive coalescing: increase batch size under light load to reduce
                            // lock churn, but avoid adding latency when backlog is already high.
                            let backlog = s_mid.perf.order_intent_queue_len.load(Ordering::Relaxed);
                            let coalesce_ms = if backlog > 1_000 {
                                0
                            } else if backlog > 100 {
                                base_coalesce_ms.min(2)
                            } else {
                                base_coalesce_ms
                            };
                            if coalesce_ms > 0 {
                                let coalesce_deadline =
                                    tokio::time::Instant::now() + Duration::from_millis(coalesce_ms);
                                while batch.len() < ORDER_INTENT_BATCH_MAX {
                                    match tokio::time::timeout_at(coalesce_deadline, rx_mid.recv()).await {
                                        Ok(Some((_, next_intent))) => batch.push(next_intent),
                                        _ => break,
                                    }
                                }
                            }
                            while batch.len() < ORDER_INTENT_BATCH_MAX {
                                let Ok((_, next_intent)) = rx_mid.try_recv() else {
                                    break;
                                };
                                batch.push(next_intent);
                            }
                            s_mid.perf.observe_submit_intake_batch_size(batch.len());

                            let mut submit_jobs = Vec::<SubmitJob>::with_capacity(batch.len());
                            let write_started = std::time::Instant::now();
                            let lock_wait_started = std::time::Instant::now();
                            let market_id = batch.first().map(|intent| intent.job.market_id).unwrap_or_default();
                            let wallet_user_ids: Vec<i64> = batch
                                .iter()
                                .map(|intent| intent.job.user_id)
                                .collect::<BTreeSet<_>>()
                                .into_iter()
                                .collect();
                            let _market_guard = s_mid.lock_market(market_id).await;
                            let _wallet_guards = s_mid.lock_wallet_users(&wallet_user_ids).await;
                            let mut eng = lock_write(&s_mid.engine, "tasks.order_intent.market_engine_write").await;
                            let lock_wait_ms = lock_wait_started.elapsed().as_millis();
                            s_mid
                                .perf
                                .observe_submit_intake_lock_wait_ms(lock_wait_ms as u64);
                            let hold_started = std::time::Instant::now();
                            for intent in &batch {
                                let j = &intent.job;
                                match eng.reserve_for_order(j.user_id, j.market_id, j.buy, j.qty_units, j.price_cents) {
                                    Ok(()) => {
                                        eng.orders.insert(j.order_id, Order {
                                            order_id: j.order_id,
                                            user_id: j.user_id,
                                            market_id: j.market_id,
                                            buy: j.buy,
                                            price_cents: j.price_cents,
                                            quantity: j.qty_units,
                                            remaining: j.qty_units,
                                            created_at: intent.created_at,
                                            expires_at: j.expires_at,
                                            status: "PENDING_SUBMIT".to_string(),
                                        });
                                        eng.track_order_for_user(j.order_id, j.user_id);
                                        submit_jobs.push(j.clone());
                                    }
                                    Err(e) => {
                                        s_mid.submit_status.insert(
                                            j.request_id.to_string(),
                                            SubmitStatus::Failed {
                                                order_id: j.order_id,
                                                error: e.detail,
                                                created_at_ms: intent.created_at_ms,
                                            },
                                        );
                                    }
                                }
                            }
                            let hold_ms = hold_started.elapsed().as_millis();
                            drop(eng);
                            if lock_wait_ms >= 200 || hold_ms >= 200 {
                                eprintln!(
                                    "[submit] intake_lock_profile batch_size={} reserved={} lock_wait_ms={} hold_ms={}",
                                    batch.len(),
                                    submit_jobs.len(),
                                    lock_wait_ms,
                                    hold_ms
                                );
                            }
                            let write_ms = write_started.elapsed().as_millis();
                            if write_ms >= 100 {
                                eprintln!(
                                    "[submit] intake_write_slow batch_size={} reserved={} elapsed_ms={}",
                                    batch.len(),
                                    submit_jobs.len(),
                                    write_ms
                                );
                            }

                            for j in submit_jobs {
                                if let Err(e) = s_mid.submit_tx.send(j).await {
                                    let j = e.0;
                                    let _market_guard = s_mid.lock_market(j.market_id).await;
                                    let _wallet_guard = s_mid.lock_wallet_user(j.user_id).await;
                                    s_mid
                                        .with_engine_write_profile("tasks.order_intent.submit_tx_failed", |eng| {
                                            eng.release_reservation(j.user_id, j.market_id, j.buy, j.qty_units, j.price_cents);
                                            eng.orders.remove(&j.order_id);
                                        })
                                        .await;
                                    s_mid.submit_status.insert(
                                        j.request_id.to_string(),
                                        SubmitStatus::Failed {
                                            order_id: j.order_id,
                                            error: "submit worker unavailable".to_string(),
                                            created_at_ms: crate::now_epoch_ms(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            });
        }

        loop {
            let first = match order_intent_rx.recv().await {
                Some(v) => v,
                None => break,
            };
            s_order.perf.order_intent_queue_len.fetch_sub(1, Ordering::Relaxed);
            if let Some(tx_mid) = routes.get(&first.job.market_id) {
                let _ = tx_mid.send((MarketIntentCommand::SubmitIntent, first));
                continue;
            }
            s_order.submit_status.insert(
                first.job.request_id.to_string(),
                SubmitStatus::Failed {
                    order_id: first.job.order_id,
                    error: format!("unknown market_id={}", first.job.market_id),
                    created_at_ms: first.created_at_ms,
                },
            );
        }
    });

    // 2c) Async cancel writer.
    let s_cancel = state.clone();
    tokio::spawn(async move {
        loop {
            let Some(job) = cancel_rx.recv().await else {
                break;
            };
            s_cancel.perf.cancel_received.fetch_add(1, Ordering::Relaxed);
            let tx_id = format!("cancel:{}", job.request_id);
            s_cancel.tx_journal_pending(
                tx_id.clone(),
                "ORDER_CANCELLED",
                Some(job.market_id),
                vec![job.order_id],
            );
            let type_id = {
                let ids = lock_read(&s_cancel.event_type_ids, "tasks.cancel.event_type_ids_read").await;
                match ids.get(crate::EVT_ORDER_CANCELLED) {
                    Some(v) => *v,
                    None => {
                        s_cancel.perf.cancel_failed.fetch_add(1, Ordering::Relaxed);
                        s_cancel.tx_journal_rolled_back(&tx_id, "missing event type".to_string());
                        s_cancel.cancel_status.insert(
                            job.request_id.to_string(),
                            CancelStatus::Failed {
                                order_id: job.order_id,
                                error: "missing event type".to_string(),
                                created_at_ms: crate::now_epoch_ms(),
                            },
                        );
                        continue;
                    }
                }
            };
            let db_result = sqlx::query("INSERT INTO events (event_type_id, user_id, market_id, order_id, payload) VALUES ($1,$2,$3,$4,$5)")
                .bind(type_id)
                .bind(job.requester_user_id)
                .bind(job.market_id)
                .bind(job.order_id)
                .bind(serde_json::json!({}))
                .execute(&s_cancel.db)
                .await;
            if let Err(e) = db_result {
                s_cancel.perf.cancel_failed.fetch_add(1, Ordering::Relaxed);
                s_cancel.tx_journal_rolled_back(&tx_id, format!("db error: {e}"));
                s_cancel.cancel_status.insert(
                    job.request_id.to_string(),
                    CancelStatus::Failed {
                        order_id: job.order_id,
                        error: format!("db error: {e}"),
                        created_at_ms: crate::now_epoch_ms(),
                    },
                );
                continue;
            }
            let _market_guard = s_cancel.lock_market(job.market_id).await;
            let _wallet_guard = s_cancel.lock_wallet_user(job.owner_user_id).await;
            s_cancel
                .with_engine_write_profile("tasks.cancel.apply", |eng| {
                    let mut rel: Option<(i64, i16, bool, i64, i64)> = None;
                    if let Some(o) = eng.orders.get_mut(&job.order_id) {
                        if o.user_id != job.owner_user_id {
                            return;
                        }
                        if o.status == "OPEN" || o.status == "PARTIAL" || o.status == "PENDING_SUBMIT" {
                            o.status = "CANCELLED".to_string();
                            rel = Some((o.user_id, o.market_id, o.buy, o.remaining, o.price_cents));
                        }
                    }
                    if let Some((uid, mid, buy, rem, px)) = rel {
                        eng.release_reservation(uid, mid, buy, rem, px);
                        let _ = eng.remove_order_from_book(job.order_id);
                        s_cancel.invalidate_user_read_caches(uid);
                    }
                })
                .await;
            s_cancel.tx_journal_confirmed(&tx_id, Vec::new());
            s_cancel.perf.cancel_confirmed.fetch_add(1, Ordering::Relaxed);
            s_cancel.cancel_status.insert(
                job.request_id.to_string(),
                CancelStatus::Confirmed {
                    order_id: job.order_id,
                    created_at_ms: crate::now_epoch_ms(),
                },
            );
            if job.market_id > 0 {
                enqueue_market_match(&s_cancel, job.market_id);
            }
        }
    });

    // 3) Async submit batch writer (ORDER_ACCEPTED).
    let s_submit = state.clone();
    tokio::spawn(async move {
        loop {
            let first = match submit_rx.recv().await {
                Some(v) => v,
                None => break,
            };
            let mut batch = Vec::with_capacity(crate::ASYNC_SUBMIT_BATCH_MAX);
            batch.push(first);
            let deadline = tokio::time::Instant::now() + Duration::from_millis(crate::ASYNC_SUBMIT_FLUSH_MS.max(1));
            while batch.len() < crate::ASYNC_SUBMIT_BATCH_MAX {
                let now = tokio::time::Instant::now();
                if now >= deadline { break; }
                let remaining = deadline - now;
                match tokio::time::timeout(remaining, submit_rx.recv()).await {
                    Ok(Some(v)) => batch.push(v),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            s_submit.submit_inflight.fetch_add(1, Ordering::Relaxed);
            let persist_res = persist_order_accepted_batch(&s_submit, &batch).await;
            s_submit.submit_inflight.fetch_sub(1, Ordering::Relaxed);
            if let Err(e) = persist_res {
                s_submit.perf.submit_failed_db.fetch_add(batch.len() as u64, Ordering::Relaxed);
                let market_ids: Vec<i16> = batch
                    .iter()
                    .map(|j| j.market_id)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let wallet_user_ids: Vec<i64> = batch
                    .iter()
                    .map(|j| j.user_id)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let _market_guards = s_submit.lock_markets(&market_ids).await;
                let _wallet_guards = s_submit.lock_wallet_users(&wallet_user_ids).await;
                {
                    s_submit
                        .with_engine_write_profile("tasks.submit.persist_failed_rollback", |eng| {
                        for j in &batch {
                            eng.release_reservation(j.user_id, j.market_id, j.buy, j.qty_units, j.price_cents);
                            eng.orders.remove(&j.order_id);
                        }
                    })
                    .await;
                }
                let created_at_ms = crate::now_epoch_ms();
                for j in &batch {
                    s_submit.tx_journal_rolled_back(
                        &format!("submit:{}", j.request_id),
                        e.detail.clone(),
                    );
                    s_submit.submit_status.insert(
                        j.request_id.to_string(),
                        SubmitStatus::Failed {
                            order_id: j.order_id,
                            error: e.detail.clone(),
                            created_at_ms,
                        },
                    );
                }
                eprintln!(
                    "[submit] persist_order_accepted_batch_failed batch_size={} error={}",
                    batch.len(),
                    e.detail
                );
                continue;
            }

            let mut touched = BTreeSet::<i16>::new();
            for j in &batch {
                touched.insert(j.market_id);
            }
            for mid in touched {
                enqueue_market_match(&s_submit, mid);
            }
        }
    });

    // 2b) Seed matcher for crossed markets found during startup replay.
    let s_seed = state.clone();
    tokio::spawn(async move {
        let startup_crossed: Vec<i16> = {
            let eng = lock_read(&s_seed.engine, "tasks.seed_matcher.engine_read").await;
            eng.books
                .keys()
                .copied()
                .filter(|mid| eng.market_is_crossed(*mid))
                .collect()
        };
        for mid in startup_crossed {
            enqueue_market_match(&s_seed, mid);
        }
    });

    // 4) Snapshot worker.
    let s_snap = state.clone();
    tokio::spawn(async move {
        let mut last_snapshot_at = crate::now_epoch_secs();
        let mut last_snapshot_event = 0i64;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if s_snap.match_running.len() > 0 || s_snap.match_pending.len() > 0 {
                continue;
            }
            let now = crate::now_epoch_secs();
            if (now - last_snapshot_at) < crate::SNAPSHOT_MIN_INTERVAL_SECS {
                continue;
            }
            let event_id = if let Some(eng) = try_lock_read(&s_snap.engine, "tasks.snapshot.event_id_try_read") {
                eng.last_event_id
            } else {
                continue;
            };
            if event_id == 0 || event_id <= last_snapshot_event {
                continue;
            }
            if (event_id - last_snapshot_event) < crate::SNAPSHOT_EVERY_EVENTS {
                continue;
            }
            let (snap_event_id, blob) = if let Some(eng) = try_lock_read(&s_snap.engine, "tasks.snapshot.capture_try_read") {
                (eng.last_event_id, eng.snapshot())
            } else {
                continue;
            };
            if snap_event_id <= last_snapshot_event {
                continue;
            }
            if let Ok(encoded) = bincode::serialize(&blob) {
                if let Ok(compressed) = zstd::encode_all(encoded.as_slice(), 1) {
                    let snapshot_result = sqlx::query(
                        "INSERT INTO snapshots (event_id_hi, format, compression, state) VALUES ($1, $2, $3, $4)",
                    )
                    .bind(snap_event_id)
                    .bind("bincode")
                    .bind("zstd")
                    .bind(compressed)
                    .execute(&s_snap.db)
                    .await;
                    if let Err(e) = snapshot_result {
                        eprintln!(
                            "[snapshot] persist_failed event_id={} error={}",
                            snap_event_id, e
                        );
                    } else {
                        last_snapshot_event = snap_event_id;
                    }
                    // Backoff regardless of DB outcome to avoid lock-heavy retry storms.
                    last_snapshot_at = now;
                }
            }
        }
    });
}
