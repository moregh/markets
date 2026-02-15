use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;

pub(crate) const MAX_TRADE_LIST: usize = 300;
pub(crate) const MAX_ORDER_MATCH_LIST: usize = 500;
pub(crate) const MATCH_WINDOW_SECS: i64 = 3600;

#[derive(Debug, Clone)]
pub(crate) struct MarketInfo {
    pub(crate) id: i16,
    pub(crate) product_id: i16,
    pub(crate) currency_id: i16,
    pub(crate) product: String,
    pub(crate) currency: String,
    pub(crate) ticker: String,
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Wallet {
    pub(crate) tot_cents: HashMap<i16, i64>,
    pub(crate) res_cents: HashMap<i16, i64>,
    pub(crate) dep_cents: HashMap<i16, i64>,
    pub(crate) wdr_cents: HashMap<i16, i64>,
    pub(crate) fees_paid_cents: HashMap<i16, i64>,
    pub(crate) tot_units: HashMap<i16, i64>,
    pub(crate) res_units: HashMap<i16, i64>,
    pub(crate) dep_units: HashMap<i16, i64>,
    pub(crate) wdr_units: HashMap<i16, i64>,
}

impl Wallet {
    pub(crate) fn new() -> Self {
        Self {
            tot_cents: HashMap::new(),
            res_cents: HashMap::new(),
            dep_cents: HashMap::new(),
            wdr_cents: HashMap::new(),
            fees_paid_cents: HashMap::new(),
            tot_units: HashMap::new(),
            res_units: HashMap::new(),
            dep_units: HashMap::new(),
            wdr_units: HashMap::new(),
        }
    }
    pub(crate) fn avail_cents(&self, currency_id: i16) -> i64 {
        let t = *self.tot_cents.get(&currency_id).unwrap_or(&0);
        let r = *self.res_cents.get(&currency_id).unwrap_or(&0);
        t - r
    }
    pub(crate) fn avail_units(&self, product_id: i16) -> i64 {
        let t = *self.tot_units.get(&product_id).unwrap_or(&0);
        let r = *self.res_units.get(&product_id).unwrap_or(&0);
        t - r
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Order {
    pub(crate) order_id: i64,
    pub(crate) user_id: i64,
    pub(crate) market_id: i16,
    pub(crate) buy: bool,
    pub(crate) price_cents: i64,
    pub(crate) quantity: i64,
    pub(crate) remaining: i64,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub(crate) ts_ms: i64,
    pub(crate) market_id: i16,
    pub(crate) buy_user_id: i64,
    pub(crate) sell_user_id: i64,
    pub(crate) buy_order_id: i64,
    pub(crate) sell_order_id: i64,
    pub(crate) price_cents: i64,
    pub(crate) qty_units: i64,
    pub(crate) fee_cents: i64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct LevelQueue {
    pub(crate) q: VecDeque<i64>, // order_ids
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OrderBook {
    // price -> queue(order_id); cancels are lazy (we skip dead orders when popping).
    pub(crate) buys: BTreeMap<i64, LevelQueue>,  // key=price_cents
    pub(crate) sells: BTreeMap<i64, LevelQueue>, // key=price_cents
    #[serde(default)]
    pub(crate) buy_count: i64,
    #[serde(default)]
    pub(crate) sell_count: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct ExpiryItem {
    at_ms: i64,
    order_id: i64,
}

impl Ord for ExpiryItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.at_ms.cmp(&other.at_ms).then(self.order_id.cmp(&other.order_id))
    }
}

impl PartialOrd for ExpiryItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EngineSnapshot {
    pub(crate) last_event_id: i64,
    pub(crate) fee_rate_ppm: i64, // fee rate in parts-per-million
    pub(crate) fees_collected: HashMap<i16, i64>, // currency_id -> cents
    pub(crate) wallets: HashMap<i64, Wallet>,
    pub(crate) orders: HashMap<i64, Order>,
    pub(crate) books: HashMap<i16, OrderBook>,
    pub(crate) trades: HashMap<i16, VecDeque<Trade>>,
    pub(crate) order_matches: HashMap<i64, VecDeque<Trade>>,
    pub(crate) match_last_sec: i64,
    pub(crate) match_sum_last_hour: u64,
    pub(crate) match_counts: Vec<u32>, // ring buffer indexed by (epoch_sec % MATCH_WINDOW_SECS)
}

#[derive(Debug)]
pub(crate) struct EngineState {
    pub(crate) last_event_id: i64,
    pub(crate) fee_rate_ppm: i64,
    pub(crate) fees_collected: HashMap<i16, i64>,
    pub(crate) wallets: HashMap<i64, Wallet>,
    pub(crate) orders: HashMap<i64, Order>,
    pub(crate) user_orders: HashMap<i64, Vec<i64>>,
    pub(crate) books: HashMap<i16, OrderBook>,
    pub(crate) trades: HashMap<i16, VecDeque<Trade>>,
    pub(crate) order_matches: HashMap<i64, VecDeque<Trade>>,
    pub(crate) markets: HashMap<i16, MarketInfo>,
    expiry_heap: BinaryHeap<Reverse<ExpiryItem>>, // min-heap via Reverse
    pub(crate) match_last_sec: i64,
    pub(crate) match_sum_last_hour: u64,
    pub(crate) match_counts: Vec<u32>,
}

impl EngineState {
    fn classify_book_front_order(
        &self,
        order_id: i64,
        now_ms: i64,
    ) -> (bool, Option<(i64, i16, bool, i64, i64)>) {
        let Some(o) = self.orders.get(&order_id) else {
            return (true, None);
        };
        if (o.status != "OPEN" && o.status != "PARTIAL") || o.remaining <= 0 {
            return (true, None);
        }
        if o.expires_at.map(|e| e.timestamp_millis() <= now_ms).unwrap_or(false) {
            return (true, Some((o.user_id, o.market_id, o.buy, o.remaining, o.price_cents)));
        }
        (false, None)
    }

    fn cleanup_best_price_level(&mut self, market_id: i16, buy_side: bool, now_ms: i64) -> Option<i64> {
        loop {
            let best_price = {
                let book = self.books.get(&market_id)?;
                if buy_side {
                    book.buys.keys().next_back().copied()
                } else {
                    book.sells.keys().next().copied()
                }
            }?;

            let front_oid = {
                let book = self.books.get(&market_id)?;
                let side = if buy_side { &book.buys } else { &book.sells };
                side.get(&best_price).and_then(|level| level.q.front().copied())
            };

            let Some(front_oid) = front_oid else {
                let book = self.book_mut(market_id);
                if buy_side {
                    book.buys.remove(&best_price);
                } else {
                    book.sells.remove(&best_price);
                }
                continue;
            };

            let (stale, expire_release) = self.classify_book_front_order(front_oid, now_ms);
            if !stale {
                return Some(best_price);
            }

            let became_empty = {
                let book = self.book_mut(market_id);
                let side = if buy_side { &mut book.buys } else { &mut book.sells };
                if let Some(level) = side.get_mut(&best_price) {
                    let _ = level.q.pop_front();
                    if buy_side {
                        book.buy_count = book.buy_count.saturating_sub(1);
                    } else {
                        book.sell_count = book.sell_count.saturating_sub(1);
                    }
                    level.q.is_empty()
                } else {
                    false
                }
            };
            if became_empty {
                let book = self.book_mut(market_id);
                if buy_side {
                    book.buys.remove(&best_price);
                } else {
                    book.sells.remove(&best_price);
                }
            }
            if let Some((uid, mid, buy, rem, px)) = expire_release {
                if let Some(o) = self.orders.get_mut(&front_oid) {
                    if o.status == "OPEN" || o.status == "PARTIAL" {
                        o.status = "EXPIRED".to_string();
                    }
                }
                self.release_reservation(uid, mid, buy, rem, px);
            }
        }
    }

    pub(crate) fn best_matchable_bid(&mut self, market_id: i16) -> Option<i64> {
        self.cleanup_best_price_level(market_id, true, now_epoch_ms())
    }

    pub(crate) fn best_matchable_ask(&mut self, market_id: i16) -> Option<i64> {
        self.cleanup_best_price_level(market_id, false, now_epoch_ms())
    }

    // Light maintenance pass for markets stuck in crossed/no-progress cycles.
    // This avoids full book rebuilds while still cleaning stale fronts.
    pub(crate) fn scrub_market_front(&mut self, market_id: i16, passes: usize) -> usize {
        let mut changed = 0usize;
        let now_ms = now_epoch_ms();
        for _ in 0..passes.max(1) {
            let before_bid = self.books.get(&market_id).and_then(|b| b.buys.keys().next_back().copied());
            let before_ask = self.books.get(&market_id).and_then(|b| b.sells.keys().next().copied());
            let _ = self.cleanup_best_price_level(market_id, true, now_ms);
            let _ = self.cleanup_best_price_level(market_id, false, now_ms);
            let after_bid = self.books.get(&market_id).and_then(|b| b.buys.keys().next_back().copied());
            let after_ask = self.books.get(&market_id).and_then(|b| b.sells.keys().next().copied());
            if before_bid == after_bid && before_ask == after_ask {
                break;
            }
            changed += 1;
        }
        changed
    }

    pub(crate) fn new(markets: HashMap<i16, MarketInfo>, default_fee_rate: f64) -> Self {
        let fee_rate_ppm = (default_fee_rate * 1_000_000.0).round() as i64;
        Self {
            last_event_id: 0,
            fee_rate_ppm,
            fees_collected: HashMap::new(),
            wallets: HashMap::new(),
            orders: HashMap::new(),
            user_orders: HashMap::new(),
            books: HashMap::new(),
            trades: HashMap::new(),
            order_matches: HashMap::new(),
            markets,
            expiry_heap: BinaryHeap::new(),
            match_last_sec: 0,
            match_sum_last_hour: 0,
            match_counts: vec![0u32; MATCH_WINDOW_SECS as usize],
        }
    }

    pub(crate) fn advance_match_window_to(&mut self, now_sec: i64) {
        if now_sec <= 0 {
            return;
        }
        if self.match_last_sec == 0 {
            self.match_last_sec = now_sec;
            return;
        }
        if now_sec <= self.match_last_sec {
            return;
        }
        let start = self.match_last_sec + 1;
        let end = now_sec;
        if (end - start) >= MATCH_WINDOW_SECS {
            for v in self.match_counts.iter_mut() {
                *v = 0;
            }
            self.match_sum_last_hour = 0;
            self.match_last_sec = now_sec;
            return;
        }
        for sec in start..=end {
            let idx = (sec % MATCH_WINDOW_SECS) as usize;
            let old = self.match_counts[idx] as u64;
            if old != 0 {
                self.match_sum_last_hour = self.match_sum_last_hour.saturating_sub(old);
                self.match_counts[idx] = 0;
            }
        }
        self.match_last_sec = now_sec;
    }

    pub(crate) fn bump_match_count(&mut self, ts_ms: i64) {
        let sec = ts_ms / 1000;
        if sec <= 0 {
            return;
        }
        self.advance_match_window_to(sec);
        let idx = (sec % MATCH_WINDOW_SECS) as usize;
        self.match_counts[idx] = self.match_counts[idx].saturating_add(1);
        self.match_sum_last_hour = self.match_sum_last_hour.saturating_add(1);
    }

    pub(crate) fn wallet_mut(&mut self, user_id: i64) -> &mut Wallet {
        self.wallets.entry(user_id).or_insert_with(Wallet::new)
    }

    pub(crate) fn book_mut(&mut self, market_id: i16) -> &mut OrderBook {
        self.books.entry(market_id).or_insert_with(OrderBook::default)
    }

    fn trades_mut(&mut self, market_id: i16) -> &mut VecDeque<Trade> {
        self.trades.entry(market_id).or_insert_with(VecDeque::new)
    }

    fn order_matches_mut(&mut self, order_id: i64) -> &mut VecDeque<Trade> {
        self.order_matches.entry(order_id).or_insert_with(VecDeque::new)
    }

    pub(crate) fn reserve_for_order(&mut self, user_id: i64, market_id: i16, buy: bool, qty: i64, price_cents: i64) -> Result<(), ApiError> {
        let Some(mkt) = self.markets.get(&market_id).cloned() else {
            return Err(ApiError::new(StatusCode::NOT_FOUND, "Market not found"));
        };
        let w = self.wallet_mut(user_id);
        if buy {
            let required = qty.saturating_mul(price_cents);
            if w.avail_cents(mkt.currency_id) < required {
                return Err(ApiError::new(StatusCode::PAYMENT_REQUIRED, "Insufficient funds"));
            }
            *w.res_cents.entry(mkt.currency_id).or_insert(0) += required;
        } else {
            if w.avail_units(mkt.product_id) < qty {
                return Err(ApiError::new(StatusCode::BAD_REQUEST, "Insufficient assets"));
            }
            *w.res_units.entry(mkt.product_id).or_insert(0) += qty;
        }
        Ok(())
    }

    pub(crate) fn release_reservation(&mut self, user_id: i64, market_id: i16, buy: bool, qty: i64, price_cents: i64) {
        if let Some(mkt) = self.markets.get(&market_id).cloned() {
            let w = self.wallet_mut(user_id);
            if buy {
                let required = qty.saturating_mul(price_cents);
                let e = w.res_cents.entry(mkt.currency_id).or_insert(0);
                *e = (*e - required).max(0);
            } else {
                let e = w.res_units.entry(mkt.product_id).or_insert(0);
                *e = (*e - qty).max(0);
            }
        }
    }

    pub(crate) fn add_order_to_book(&mut self, order: &Order) {
        let book = self.book_mut(order.market_id);
        if order.buy {
            book.buys.entry(order.price_cents).or_default().q.push_back(order.order_id);
            book.buy_count = book.buy_count.saturating_add(1);
        } else {
            book.sells.entry(order.price_cents).or_default().q.push_back(order.order_id);
            book.sell_count = book.sell_count.saturating_add(1);
        }
    }

    pub(crate) fn track_order_for_user(&mut self, order_id: i64, user_id: i64) {
        let list = self.user_orders.entry(user_id).or_default();
        if list.last().copied() == Some(order_id) {
            return;
        }
        if !list.iter().any(|oid| *oid == order_id) {
            list.push(order_id);
        }
    }

    pub(crate) fn rebuild_user_order_index(&mut self) {
        self.user_orders.clear();
        let mut rows: Vec<(DateTime<Utc>, i64, i64)> = self
            .orders
            .values()
            .map(|o| (o.created_at, o.order_id, o.user_id))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        for (_, order_id, user_id) in rows {
            self.user_orders.entry(user_id).or_default().push(order_id);
        }
    }

    pub(crate) fn remove_order_from_book(&mut self, order_id: i64) -> bool {
        let Some(o) = self.orders.get(&order_id).cloned() else {
            return false;
        };
        let book = self.book_mut(o.market_id);
        let side = if o.buy {
            &mut book.buys
        } else {
            &mut book.sells
        };
        let mut removed = false;
        let mut level_empty = false;
        if let Some(level) = side.get_mut(&o.price_cents) {
            while let Some(pos) = level.q.iter().position(|x| *x == order_id) {
                let _ = level.q.remove(pos);
                removed = true;
                if o.buy {
                    book.buy_count = book.buy_count.saturating_sub(1);
                } else {
                    book.sell_count = book.sell_count.saturating_sub(1);
                }
            }
            level_empty = level.q.is_empty();
        }
        if level_empty {
            side.remove(&o.price_cents);
        }
        removed
    }

    pub(crate) fn restore_order_to_book_front(&mut self, order_id: i64) {
        // Best-effort restore for transient failures (e.g. DB insert failure mid-match).
        // Avoids leaving OPEN/PARTIAL orders "lost" (missing from the book) indefinitely.
        let now_ms = now_epoch_ms();
        let (market_id, buy, price_cents, remaining, expires_at_ms, is_open) = match self.orders.get(&order_id) {
            Some(o) => (
                o.market_id,
                o.buy,
                o.price_cents,
                o.remaining,
                o.expires_at.map(|e| e.timestamp_millis()),
                o.status == "OPEN" || o.status == "PARTIAL",
            ),
            None => return,
        };
        if !is_open { return; }
        if remaining <= 0 { return; }
        if expires_at_ms.map(|ms| ms <= now_ms).unwrap_or(false) {
            let _ = self.maybe_expire_order(order_id, now_ms);
            return;
        }

        let book = self.book_mut(market_id);
        let level = if buy {
            book.buys.entry(price_cents).or_default()
        } else {
            book.sells.entry(price_cents).or_default()
        };
        // Prevent duplicates (slow path; only used for exceptional restores).
        if level.q.iter().any(|x| *x == order_id) {
            return;
        }
        level.q.push_front(order_id);
        if buy {
            book.buy_count = book.buy_count.saturating_add(1);
        } else {
            book.sell_count = book.sell_count.saturating_add(1);
        }
    }

    pub(crate) fn best_bid(&self, market_id: i16) -> Option<i64> {
        self.books.get(&market_id)?.buys.keys().next_back().copied()
    }

    pub(crate) fn best_ask(&self, market_id: i16) -> Option<i64> {
        self.books.get(&market_id)?.sells.keys().next().copied()
    }

    pub(crate) fn best_live_bid(&self, market_id: i16) -> Option<i64> {
        let now_ms = now_epoch_ms();
        let book = self.books.get(&market_id)?;
        for (price, level) in book.buys.iter().rev() {
            for oid in &level.q {
                let Some(o) = self.orders.get(oid) else { continue; };
                if (o.status == "OPEN" || o.status == "PARTIAL")
                    && o.remaining > 0
                    && o.expires_at.map(|e| e.timestamp_millis() > now_ms).unwrap_or(true)
                {
                    return Some(*price);
                }
            }
        }
        None
    }

    pub(crate) fn best_live_ask(&self, market_id: i16) -> Option<i64> {
        let now_ms = now_epoch_ms();
        let book = self.books.get(&market_id)?;
        for (price, level) in &book.sells {
            for oid in &level.q {
                let Some(o) = self.orders.get(oid) else { continue; };
                if (o.status == "OPEN" || o.status == "PARTIAL")
                    && o.remaining > 0
                    && o.expires_at.map(|e| e.timestamp_millis() > now_ms).unwrap_or(true)
                {
                    return Some(*price);
                }
            }
        }
        None
    }

    pub(crate) fn market_is_crossed(&self, market_id: i16) -> bool {
        matches!(
            (self.best_live_bid(market_id), self.best_live_ask(market_id)),
            (Some(b), Some(a)) if b >= a
        )
    }

    pub(crate) fn market_maybe_crossed(&self, market_id: i16) -> bool {
        matches!(
            (self.best_bid(market_id), self.best_ask(market_id)),
            (Some(b), Some(a)) if b >= a
        )
    }

    pub(crate) fn enqueue_expiry(&mut self, o: &Order) {
        if let Some(exp) = o.expires_at {
            self.expiry_heap.push(Reverse(ExpiryItem { at_ms: exp.timestamp_millis(), order_id: o.order_id }));
        }
    }

    pub(crate) fn rebuild_expiry_heap(&mut self) {
        self.expiry_heap.clear();
        for o in self.orders.values() {
            if o.status == "OPEN" || o.status == "PARTIAL" {
                if let Some(exp) = o.expires_at {
                    self.expiry_heap.push(Reverse(ExpiryItem { at_ms: exp.timestamp_millis(), order_id: o.order_id }));
                }
            }
        }
    }

    fn maybe_expire_order(&mut self, order_id: i64, now_ms: i64) -> bool {
        let mut rel: Option<(i64, i16, bool, i64, i64)> = None;
        if let Some(o) = self.orders.get_mut(&order_id) {
            if o.status != "OPEN" && o.status != "PARTIAL" {
                return false;
            }
            let Some(exp) = o.expires_at else { return false; };
            if exp.timestamp_millis() > now_ms {
                return false;
            }
            o.status = "EXPIRED".to_string();
            rel = Some((o.user_id, o.market_id, o.buy, o.remaining, o.price_cents));
        }
        if let Some((uid, mid, buy, rem, px)) = rel {
            self.release_reservation(uid, mid, buy, rem, px);
            let _ = self.remove_order_from_book(order_id);
            return true;
        }
        false
    }

    pub(crate) fn expire_due_orders_budget(&mut self, now_ms: i64, max_items: usize) -> (usize, bool) {
        let mut expired = 0usize;
        let mut scanned = 0usize;
        let limit = max_items.max(1);
        loop {
            if scanned >= limit {
                break;
            }
            let Some(Reverse(top)) = self.expiry_heap.peek().copied() else { break; };
            if top.at_ms > now_ms {
                break;
            }
            scanned += 1;
            let _ = self.expiry_heap.pop();
            let matches = self
                .orders
                .get(&top.order_id)
                .and_then(|o| o.expires_at.map(|e| e.timestamp_millis() == top.at_ms))
                .unwrap_or(false);
            if matches {
                if self.maybe_expire_order(top.order_id, now_ms) {
                    expired += 1;
                }
            }
        }
        let has_more_due = self
            .expiry_heap
            .peek()
            .map(|Reverse(top)| top.at_ms <= now_ms)
            .unwrap_or(false);
        (expired, has_more_due)
    }

    pub(crate) fn pop_best_order_id(&mut self, market_id: i16, buy_side: bool) -> Option<i64> {
        let now_ms = now_epoch_ms();
        loop {
            let oid = {
                let book = self.book_mut(market_id);
                if buy_side {
                    let price = *book.buys.keys().next_back()?;
                    let level = book.buys.get_mut(&price)?;
                    let oid = level.q.pop_front();
                    if oid.is_some() {
                        book.buy_count = book.buy_count.saturating_sub(1);
                    }
                    if level.q.is_empty() {
                        book.buys.remove(&price);
                    }
                    oid
                } else {
                    let price = *book.sells.keys().next()?;
                    let level = book.sells.get_mut(&price)?;
                    let oid = level.q.pop_front();
                    if oid.is_some() {
                        book.sell_count = book.sell_count.saturating_sub(1);
                    }
                    if level.q.is_empty() {
                        book.sells.remove(&price);
                    }
                    oid
                }
            };

            let Some(oid) = oid else { return None; };
            if let Some(o) = self.orders.get(&oid) {
                if (o.status == "OPEN" || o.status == "PARTIAL")
                    && o.expires_at.map(|e| e.timestamp_millis() <= now_ms).unwrap_or(false)
                {
                    let mut rel: Option<(i64, i16, bool, i64, i64)> = None;
                    if let Some(expired) = self.orders.get_mut(&oid) {
                        if expired.status == "OPEN" || expired.status == "PARTIAL" {
                            expired.status = "EXPIRED".to_string();
                            rel = Some((
                                expired.user_id,
                                expired.market_id,
                                expired.buy,
                                expired.remaining,
                                expired.price_cents,
                            ));
                        }
                    }
                    if let Some((uid, mid, buy, rem, px)) = rel {
                        self.release_reservation(uid, mid, buy, rem, px);
                    }
                    continue;
                }
                if (o.status == "OPEN" || o.status == "PARTIAL") && o.remaining > 0 {
                    return Some(oid);
                }
            }
        }
    }

    pub(crate) fn apply_trade(&mut self, t: &Trade) {
        let Some(mkt) = self.markets.get(&t.market_id).cloned() else { return; };

        // Buyer reservation is placed at the order's *limit* price. Trades can execute at a better
        // price than the buyer's limit, so reserved should be reduced by (qty * buy_limit_price),
        // while totals are debited by (qty * trade_price).
        let buy_limit_price = self
            .orders
            .get(&t.buy_order_id)
            .map(|o| o.price_cents)
            .unwrap_or(t.price_cents);

        if let Some(buy) = self.orders.get_mut(&t.buy_order_id) {
            buy.remaining = (buy.remaining - t.qty_units).max(0);
            buy.status = if buy.remaining == 0 { "FILLED".to_string() } else { "PARTIAL".to_string() };
        }
        if let Some(sell) = self.orders.get_mut(&t.sell_order_id) {
            sell.remaining = (sell.remaining - t.qty_units).max(0);
            sell.status = if sell.remaining == 0 { "FILLED".to_string() } else { "PARTIAL".to_string() };
        }

        let gross = t.qty_units.saturating_mul(t.price_cents);
        let fee = t.fee_cents.max(0);
        let seller_net = (gross - fee).max(0);

        {
            let w = self.wallet_mut(t.buy_user_id);
            let rc = w.res_cents.entry(mkt.currency_id).or_insert(0);
            let release = t.qty_units.saturating_mul(buy_limit_price);
            *rc = (*rc - release).max(0);
            *w.tot_units.entry(mkt.product_id).or_insert(0) += t.qty_units;
            *w.tot_cents.entry(mkt.currency_id).or_insert(0) -= gross;
        }

        {
            let w = self.wallet_mut(t.sell_user_id);
            let rp = w.res_units.entry(mkt.product_id).or_insert(0);
            *rp = (*rp - t.qty_units).max(0);
            *w.tot_units.entry(mkt.product_id).or_insert(0) -= t.qty_units;
            *w.tot_cents.entry(mkt.currency_id).or_insert(0) += seller_net;
            *w.fees_paid_cents.entry(mkt.currency_id).or_insert(0) += fee;
        }

        *self.fees_collected.entry(mkt.currency_id).or_insert(0) += fee;
        self.bump_match_count(t.ts_ms);

        let list = self.trades_mut(t.market_id);
        list.push_front(t.clone());
        while list.len() > MAX_TRADE_LIST { list.pop_back(); }

        for oid in [t.buy_order_id, t.sell_order_id] {
            let m = self.order_matches_mut(oid);
            m.push_front(t.clone());
            while m.len() > MAX_ORDER_MATCH_LIST { m.pop_back(); }
        }
    }

    pub(crate) fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            last_event_id: self.last_event_id,
            fee_rate_ppm: self.fee_rate_ppm,
            fees_collected: self.fees_collected.clone(),
            wallets: self.wallets.clone(),
            orders: self.orders.clone(),
            books: self.books.clone(),
            trades: self.trades.clone(),
            order_matches: self.order_matches.clone(),
            match_last_sec: self.match_last_sec,
            match_sum_last_hour: self.match_sum_last_hour,
            match_counts: self.match_counts.clone(),
        }
    }

    pub(crate) fn restore_from_snapshot(&mut self, snap: EngineSnapshot) {
        self.last_event_id = snap.last_event_id;
        self.fee_rate_ppm = snap.fee_rate_ppm;
        self.fees_collected = snap.fees_collected;
        self.wallets = snap.wallets;
        self.orders = snap.orders;
        self.rebuild_user_order_index();
        self.books = snap.books;
        self.recompute_book_counts();
        self.trades = snap.trades;
        self.order_matches = snap.order_matches;
        self.match_last_sec = snap.match_last_sec;
        self.match_sum_last_hour = snap.match_sum_last_hour;
        self.match_counts = snap.match_counts;
        self.rebuild_expiry_heap();
    }

    pub(crate) fn rebuild_books_from_open_orders(&mut self) {
        self.rebuild_user_order_index();
        self.books.clear();
        let now_ms = now_epoch_ms();

        let mut order_ids: Vec<i64> = self.orders.keys().copied().collect();
        order_ids.sort_by(|a, b| {
            let oa = self.orders.get(a);
            let ob = self.orders.get(b);
            match (oa, ob) {
                (Some(oa), Some(ob)) => oa.created_at.cmp(&ob.created_at).then(oa.order_id.cmp(&ob.order_id)),
                _ => a.cmp(b),
            }
        });

        for oid in order_ids {
            let Some(o) = self.orders.get(&oid).cloned() else { continue; };
            if o.status != "OPEN" && o.status != "PARTIAL" {
                continue;
            }
            if o.remaining <= 0 {
                continue;
            }
            if o.expires_at.map(|e| e.timestamp_millis() <= now_ms).unwrap_or(false) {
                let _ = self.maybe_expire_order(oid, now_ms);
                continue;
            }
            self.add_order_to_book(&o);
        }
    }

    pub(crate) fn recompute_reservations_from_open_orders(&mut self) {
        for w in self.wallets.values_mut() {
            w.res_cents.clear();
            w.res_units.clear();
        }

        let now_ms = now_epoch_ms();
        let open_orders: Vec<Order> = self.orders.values().cloned().collect();
        for o in open_orders {
            if o.status != "OPEN" && o.status != "PARTIAL" {
                continue;
            }
            if o.remaining <= 0 {
                continue;
            }
            if o.expires_at.map(|e| e.timestamp_millis() <= now_ms).unwrap_or(false) {
                continue;
            }
            let Some(mkt) = self.markets.get(&o.market_id).cloned() else { continue; };
            let w = self.wallet_mut(o.user_id);
            if o.buy {
                let required = o.remaining.saturating_mul(o.price_cents);
                *w.res_cents.entry(mkt.currency_id).or_insert(0) += required;
            } else {
                *w.res_units.entry(mkt.product_id).or_insert(0) += o.remaining;
            }
        }
    }

    pub(crate) fn recompute_book_counts(&mut self) {
        for book in self.books.values_mut() {
            book.buy_count = book
                .buys
                .values()
                .map(|level| level.q.len() as i64)
                .sum::<i64>();
            book.sell_count = book
                .sells
                .values()
                .map(|level| level.q.len() as i64)
                .sum::<i64>();
        }
    }
}
