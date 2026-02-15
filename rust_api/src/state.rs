use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use sqlx::{Pool, Postgres};
use tokio::sync::{mpsc, Mutex, OwnedMutexGuard, RwLock};

use crate::config::AppConfig;
use crate::engine::{EngineState, MarketInfo};
use chrono::{DateTime, Utc};

pub(crate) const LATENCY_BUCKET_BOUNDS_MS: [u64; 12] = [0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000];
pub(crate) const BATCH_BUCKET_BOUNDS: [u64; 7] = [1, 2, 4, 8, 16, 32, 64];

fn hist_bucket_idx(v: u64, bounds: &[u64]) -> usize {
    for (i, b) in bounds.iter().enumerate() {
        if v <= *b {
            return i;
        }
    }
    bounds.len()
}

#[derive(Debug, Clone)]
pub(crate) struct SubmitJob {
    pub(crate) request_id: uuid::Uuid,
    pub(crate) order_id: i64,
    pub(crate) user_id: i64,
    pub(crate) market_id: i16,
    pub(crate) buy: bool,
    pub(crate) price_cents: i64,
    pub(crate) qty_units: i64,
    pub(crate) expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone)]
pub(crate) struct OrderIntent {
    pub(crate) job: SubmitJob,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) created_at_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct CancelJob {
    pub(crate) request_id: uuid::Uuid,
    pub(crate) order_id: i64,
    pub(crate) market_id: i16,
    pub(crate) owner_user_id: i64,
    pub(crate) requester_user_id: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct TxJournalEntry {
    pub(crate) tx_id: String,
    pub(crate) tx_kind: String,
    pub(crate) market_id: Option<i16>,
    pub(crate) order_ids: Vec<i64>,
    pub(crate) state: String, // pending_db | confirmed | rolled_back
    pub(crate) created_at_ms: i64,
    pub(crate) updated_at_ms: i64,
    pub(crate) event_ids: Vec<i64>,
    pub(crate) error: Option<String>,
}

pub(crate) struct PerfCounters {
    pub(crate) submit_received: AtomicU64,
    pub(crate) submit_rejected_queue_full: AtomicU64,
    pub(crate) submit_confirmed: AtomicU64,
    pub(crate) submit_failed_db: AtomicU64,
    pub(crate) cancel_received: AtomicU64,
    pub(crate) cancel_confirmed: AtomicU64,
    pub(crate) cancel_failed: AtomicU64,
    pub(crate) match_batches: AtomicU64,
    pub(crate) match_trades: AtomicU64,
    pub(crate) match_busy: AtomicU64,
    pub(crate) match_errors: AtomicU64,
    pub(crate) order_intent_queue_len: AtomicI64,
    pub(crate) submit_intake_lock_wait_hist: [AtomicU64; LATENCY_BUCKET_BOUNDS_MS.len() + 1],
    pub(crate) submit_apply_lock_wait_hist: [AtomicU64; LATENCY_BUCKET_BOUNDS_MS.len() + 1],
    pub(crate) matcher_extract_lock_wait_hist: [AtomicU64; LATENCY_BUCKET_BOUNDS_MS.len() + 1],
    pub(crate) matcher_apply_lock_wait_hist: [AtomicU64; LATENCY_BUCKET_BOUNDS_MS.len() + 1],
    pub(crate) submit_intake_batch_hist: [AtomicU64; BATCH_BUCKET_BOUNDS.len() + 1],
    pub(crate) read_cache_build_hist: [AtomicU64; LATENCY_BUCKET_BOUNDS_MS.len() + 1],
    pub(crate) read_cache_ticks: AtomicU64,
    pub(crate) read_cache_updates: AtomicU64,
    pub(crate) read_cache_engine_busy: AtomicU64,
    pub(crate) read_cache_markets_busy: AtomicU64,
    pub(crate) read_cache_tick_ms: AtomicU64,
}

impl PerfCounters {
    pub(crate) fn new() -> Self {
        Self {
            submit_received: AtomicU64::new(0),
            submit_rejected_queue_full: AtomicU64::new(0),
            submit_confirmed: AtomicU64::new(0),
            submit_failed_db: AtomicU64::new(0),
            cancel_received: AtomicU64::new(0),
            cancel_confirmed: AtomicU64::new(0),
            cancel_failed: AtomicU64::new(0),
            match_batches: AtomicU64::new(0),
            match_trades: AtomicU64::new(0),
            match_busy: AtomicU64::new(0),
            match_errors: AtomicU64::new(0),
            order_intent_queue_len: AtomicI64::new(0),
            submit_intake_lock_wait_hist: std::array::from_fn(|_| AtomicU64::new(0)),
            submit_apply_lock_wait_hist: std::array::from_fn(|_| AtomicU64::new(0)),
            matcher_extract_lock_wait_hist: std::array::from_fn(|_| AtomicU64::new(0)),
            matcher_apply_lock_wait_hist: std::array::from_fn(|_| AtomicU64::new(0)),
            submit_intake_batch_hist: std::array::from_fn(|_| AtomicU64::new(0)),
            read_cache_build_hist: std::array::from_fn(|_| AtomicU64::new(0)),
            read_cache_ticks: AtomicU64::new(0),
            read_cache_updates: AtomicU64::new(0),
            read_cache_engine_busy: AtomicU64::new(0),
            read_cache_markets_busy: AtomicU64::new(0),
            read_cache_tick_ms: AtomicU64::new(0),
        }
    }

    pub(crate) fn observe_submit_intake_lock_wait_ms(&self, ms: u64) {
        let idx = hist_bucket_idx(ms, &LATENCY_BUCKET_BOUNDS_MS);
        self.submit_intake_lock_wait_hist[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_submit_apply_lock_wait_ms(&self, ms: u64) {
        let idx = hist_bucket_idx(ms, &LATENCY_BUCKET_BOUNDS_MS);
        self.submit_apply_lock_wait_hist[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_matcher_extract_lock_wait_ms(&self, ms: u64) {
        let idx = hist_bucket_idx(ms, &LATENCY_BUCKET_BOUNDS_MS);
        self.matcher_extract_lock_wait_hist[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_matcher_apply_lock_wait_ms(&self, ms: u64) {
        let idx = hist_bucket_idx(ms, &LATENCY_BUCKET_BOUNDS_MS);
        self.matcher_apply_lock_wait_hist[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_submit_intake_batch_size(&self, n: usize) {
        let idx = hist_bucket_idx(n as u64, &BATCH_BUCKET_BOUNDS);
        self.submit_intake_batch_hist[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_read_cache_build_ms(&self, ms: u64) {
        let idx = hist_bucket_idx(ms, &LATENCY_BUCKET_BOUNDS_MS);
        self.read_cache_build_hist[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot_json(&self, submit_inflight: u64) -> serde_json::Value {
        serde_json::json!({
            "submit": {
                "received": self.submit_received.load(Ordering::Relaxed),
                "rejected_queue_full": self.submit_rejected_queue_full.load(Ordering::Relaxed),
                "confirmed": self.submit_confirmed.load(Ordering::Relaxed),
                "failed_db": self.submit_failed_db.load(Ordering::Relaxed),
                "inflight_db_batches": submit_inflight,
                "order_intent_queue_len": self.order_intent_queue_len.load(Ordering::Relaxed),
            },
            "cancel": {
                "received": self.cancel_received.load(Ordering::Relaxed),
                "confirmed": self.cancel_confirmed.load(Ordering::Relaxed),
                "failed": self.cancel_failed.load(Ordering::Relaxed),
            },
            "matcher": {
                "batches": self.match_batches.load(Ordering::Relaxed),
                "trades": self.match_trades.load(Ordering::Relaxed),
                "busy": self.match_busy.load(Ordering::Relaxed),
                "errors": self.match_errors.load(Ordering::Relaxed),
            },
            "read_cache": {
                "tick_ms": self.read_cache_tick_ms.load(Ordering::Relaxed),
                "ticks": self.read_cache_ticks.load(Ordering::Relaxed),
                "updates": self.read_cache_updates.load(Ordering::Relaxed),
                "engine_busy": self.read_cache_engine_busy.load(Ordering::Relaxed),
                "markets_busy": self.read_cache_markets_busy.load(Ordering::Relaxed),
            }
        })
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct MatchIntentEntry {
    pub(crate) bid_order_id: i64,
    pub(crate) ask_order_id: i64,
    pub(crate) price_cents: i64,
    pub(crate) qty_units: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct MatchIntent {
    pub(crate) intent_id: String,
    pub(crate) market_id: i16,
    pub(crate) created_at_ms: i64,
    pub(crate) entries: Vec<MatchIntentEntry>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct CachedOrderLevel {
    pub(crate) order_id: i64,
    pub(crate) user_id: i64,
    pub(crate) price_cents: i64,
    pub(crate) remaining: i64,
    pub(crate) has_non_expiring: bool,
    pub(crate) expires_in_seconds: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct CachedTradeRow {
    pub(crate) ts_ms: i64,
    pub(crate) qty_units: i64,
    pub(crate) price_cents: i64,
    pub(crate) buy_user_id: i64,
    pub(crate) sell_user_id: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct MarketReadCache {
    pub(crate) market_id: i16,
    pub(crate) best_bid_cents: i64,
    pub(crate) best_ask_cents: i64,
    pub(crate) active_buy_orders: i64,
    pub(crate) active_sell_orders: i64,
    pub(crate) crossed: bool,
    pub(crate) buys: Vec<CachedOrderLevel>,
    pub(crate) sells: Vec<CachedOrderLevel>,
    pub(crate) trades: Vec<CachedTradeRow>,
    pub(crate) updated_at_ms: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SystemReadCache {
    pub(crate) total_active: i64,
    pub(crate) active_buy_orders: i64,
    pub(crate) active_sell_orders: i64,
    pub(crate) last_hour_count: i64,
    pub(crate) updated_at_ms: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "state", rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SubmitStatus {
    Received { order_id: i64, created_at_ms: i64 },
    Confirmed { order_id: i64, event_id: i64, created_at_ms: i64 },
    Failed { order_id: i64, error: String, created_at_ms: i64 },
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "state", rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum CancelStatus {
    Received { order_id: i64, created_at_ms: i64 },
    Confirmed { order_id: i64, created_at_ms: i64 },
    Failed { order_id: i64, error: String, created_at_ms: i64 },
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct UserProfile {
    pub(crate) user_id: i64,
    pub(crate) username: String,
    pub(crate) email: String,
    pub(crate) is_admin: bool,
    pub(crate) created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) cfg: Arc<AppConfig>,
    pub(crate) db: Pool<Postgres>,
    pub(crate) markets: Arc<RwLock<HashMap<i16, MarketInfo>>>,
    pub(crate) currency_names: Arc<RwLock<HashMap<i16, String>>>,
    pub(crate) product_names: Arc<RwLock<HashMap<i16, String>>>,
    pub(crate) event_type_ids: Arc<RwLock<HashMap<String, i16>>>,
    pub(crate) engine: Arc<RwLock<EngineState>>,
    pub(crate) next_order_id: Arc<AtomicI64>,
    pub(crate) submit_tx: mpsc::Sender<SubmitJob>,
    pub(crate) order_intent_tx: mpsc::Sender<OrderIntent>,
    pub(crate) cancel_tx: mpsc::Sender<CancelJob>,
    pub(crate) submit_inflight: Arc<AtomicU64>,
    pub(crate) submit_status: Arc<DashMap<String, SubmitStatus>>,
    pub(crate) cancel_status: Arc<DashMap<String, CancelStatus>>,
    pub(crate) match_tx: mpsc::UnboundedSender<i16>,
    pub(crate) match_pending: Arc<DashMap<i16, ()>>,
    pub(crate) match_running: Arc<DashMap<i16, ()>>,
    pub(crate) match_journal: Arc<DashMap<String, MatchIntent>>,
    pub(crate) tx_journal: Arc<DashMap<String, TxJournalEntry>>,
    pub(crate) perf: Arc<PerfCounters>,
    pub(crate) market_read_cache: Arc<DashMap<i16, MarketReadCache>>,
    pub(crate) system_read_cache: Arc<RwLock<SystemReadCache>>,
    pub(crate) wallet_read_cache: Arc<DashMap<i64, serde_json::Value>>,
    pub(crate) wallet_details_read_cache: Arc<DashMap<i64, serde_json::Value>>,
    pub(crate) my_orders_read_cache: Arc<DashMap<i64, serde_json::Value>>,
    pub(crate) market_mutexes: Arc<Vec<Arc<Mutex<()>>>>,
    pub(crate) wallet_mutexes: Arc<Vec<Arc<Mutex<()>>>>,
    pub(crate) user_cache: Arc<RwLock<HashMap<i64, UserProfile>>>,
    pub(crate) engine_ready: Arc<AtomicBool>,
}

const LOCK_PROFILE_WARN_MS: u128 = 500;
const LOCK_PROFILE_COOLDOWN_MS: i64 = 1000;
static LOCK_LOG_LAST_MS: Lazy<DashMap<&'static str, i64>> = Lazy::new(DashMap::new);
static ENGINE_LOG_LAST_MS: Lazy<DashMap<String, i64>> = Lazy::new(DashMap::new);

fn now_epoch_ms_i64() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

fn should_emit_lock_log(label: &'static str) -> bool {
    let now = now_epoch_ms_i64();
    if let Some(mut last) = LOCK_LOG_LAST_MS.get_mut(label) {
        if now - *last < LOCK_PROFILE_COOLDOWN_MS {
            return false;
        }
        *last = now;
        true
    } else {
        LOCK_LOG_LAST_MS.insert(label, now);
        true
    }
}

fn should_emit_engine_log(label: &str) -> bool {
    let now = now_epoch_ms_i64();
    if let Some(mut last) = ENGINE_LOG_LAST_MS.get_mut(label) {
        if now - *last < LOCK_PROFILE_COOLDOWN_MS {
            return false;
        }
        *last = now;
        true
    } else {
        ENGINE_LOG_LAST_MS.insert(label.to_string(), now);
        true
    }
}

pub(crate) struct ProfiledReadGuard<'a, T> {
    label: &'static str,
    wait_ms: u128,
    acquired_at: Instant,
    guard: tokio::sync::RwLockReadGuard<'a, T>,
}

impl<'a, T> Deref for ProfiledReadGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, T> Drop for ProfiledReadGuard<'a, T> {
    fn drop(&mut self) {
        let hold_ms = self.acquired_at.elapsed().as_millis();
        if (self.wait_ms >= LOCK_PROFILE_WARN_MS || hold_ms >= LOCK_PROFILE_WARN_MS)
            && should_emit_lock_log(self.label)
        {
            eprintln!(
                "[lock] kind=read label={} wait_ms={} hold_ms={}",
                self.label, self.wait_ms, hold_ms
            );
        }
    }
}

pub(crate) struct ProfiledWriteGuard<'a, T> {
    label: &'static str,
    wait_ms: u128,
    acquired_at: Instant,
    guard: tokio::sync::RwLockWriteGuard<'a, T>,
}

impl<'a, T> Deref for ProfiledWriteGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, T> DerefMut for ProfiledWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<'a, T> Drop for ProfiledWriteGuard<'a, T> {
    fn drop(&mut self) {
        let hold_ms = self.acquired_at.elapsed().as_millis();
        if (self.wait_ms >= LOCK_PROFILE_WARN_MS || hold_ms >= LOCK_PROFILE_WARN_MS)
            && should_emit_lock_log(self.label)
        {
            eprintln!(
                "[lock] kind=write label={} wait_ms={} hold_ms={}",
                self.label, self.wait_ms, hold_ms
            );
        }
    }
}

pub(crate) async fn lock_read<'a, T>(
    lock: &'a RwLock<T>,
    label: &'static str,
) -> ProfiledReadGuard<'a, T> {
    let wait_started = Instant::now();
    let guard = lock.read().await;
    ProfiledReadGuard {
        label,
        wait_ms: wait_started.elapsed().as_millis(),
        acquired_at: Instant::now(),
        guard,
    }
}

pub(crate) async fn lock_write<'a, T>(
    lock: &'a RwLock<T>,
    label: &'static str,
) -> ProfiledWriteGuard<'a, T> {
    let wait_started = Instant::now();
    let guard = lock.write().await;
    ProfiledWriteGuard {
        label,
        wait_ms: wait_started.elapsed().as_millis(),
        acquired_at: Instant::now(),
        guard,
    }
}

pub(crate) fn try_lock_read<'a, T>(
    lock: &'a RwLock<T>,
    label: &'static str,
) -> Option<ProfiledReadGuard<'a, T>> {
    let guard = lock.try_read().ok()?;
    Some(ProfiledReadGuard {
        label,
        wait_ms: 0,
        acquired_at: Instant::now(),
        guard,
    })
}

pub(crate) fn try_lock_write<'a, T>(
    lock: &'a RwLock<T>,
    label: &'static str,
) -> Option<ProfiledWriteGuard<'a, T>> {
    let guard = lock.try_write().ok()?;
    Some(ProfiledWriteGuard {
        label,
        wait_ms: 0,
        acquired_at: Instant::now(),
        guard,
    })
}

pub(crate) struct ProfiledMutexGuard {
    label: &'static str,
    wait_ms: u128,
    acquired_at: Instant,
    _guard: OwnedMutexGuard<()>,
}

impl Drop for ProfiledMutexGuard {
    fn drop(&mut self) {
        let hold_ms = self.acquired_at.elapsed().as_millis();
        if (self.wait_ms >= LOCK_PROFILE_WARN_MS || hold_ms >= LOCK_PROFILE_WARN_MS)
            && should_emit_lock_log(self.label)
        {
            eprintln!(
                "[lock] kind=mutex label={} wait_ms={} hold_ms={}",
                self.label, self.wait_ms, hold_ms
            );
        }
    }
}

impl AppState {
    pub(crate) fn invalidate_user_read_caches(&self, user_id: i64) {
        self.wallet_read_cache.remove(&user_id);
        self.wallet_details_read_cache.remove(&user_id);
        self.my_orders_read_cache.remove(&user_id);
    }

    pub(crate) async fn with_engine_write_profile<R>(
        &self,
        label: &str,
        f: impl FnOnce(&mut EngineState) -> R,
    ) -> R {
        let lock_wait_started = Instant::now();
        let mut eng = self.engine.write().await;
        let lock_wait_ms = lock_wait_started.elapsed().as_millis();
        let hold_started = Instant::now();
        let out = f(&mut eng);
        let hold_ms = hold_started.elapsed().as_millis();
        if (lock_wait_ms >= LOCK_PROFILE_WARN_MS || hold_ms >= LOCK_PROFILE_WARN_MS)
            && should_emit_engine_log(label)
        {
            eprintln!(
                "[engine_lock] label={} wait_ms={} hold_ms={}",
                label, lock_wait_ms, hold_ms
            );
        }
        out
    }

    pub(crate) fn tx_journal_pending(
        &self,
        tx_id: String,
        tx_kind: &str,
        market_id: Option<i16>,
        order_ids: Vec<i64>,
    ) {
        let now = now_epoch_ms_i64();
        self.tx_journal.insert(
            tx_id.clone(),
            TxJournalEntry {
                tx_id,
                tx_kind: tx_kind.to_string(),
                market_id,
                order_ids,
                state: "pending_db".to_string(),
                created_at_ms: now,
                updated_at_ms: now,
                event_ids: Vec::new(),
                error: None,
            },
        );
    }

    pub(crate) fn tx_journal_confirmed(&self, tx_id: &str, event_ids: Vec<i64>) {
        let now = now_epoch_ms_i64();
        if let Some(mut e) = self.tx_journal.get_mut(tx_id) {
            e.state = "confirmed".to_string();
            e.updated_at_ms = now;
            e.event_ids = event_ids;
            e.error = None;
        }
    }

    pub(crate) fn tx_journal_rolled_back(&self, tx_id: &str, error: String) {
        let now = now_epoch_ms_i64();
        if let Some(mut e) = self.tx_journal.get_mut(tx_id) {
            e.state = "rolled_back".to_string();
            e.updated_at_ms = now;
            e.error = Some(error);
        }
    }

    fn shard_index(id: i64, shard_count: usize) -> usize {
        if shard_count == 0 {
            return 0;
        }
        (id.unsigned_abs() as usize) % shard_count
    }

    pub(crate) async fn lock_market(&self, market_id: i16) -> ProfiledMutexGuard {
        let idx = Self::shard_index(market_id as i64, self.market_mutexes.len());
        let wait_started = Instant::now();
        let guard = self.market_mutexes[idx].clone().lock_owned().await;
        ProfiledMutexGuard {
            label: "state.lock_market",
            wait_ms: wait_started.elapsed().as_millis(),
            acquired_at: Instant::now(),
            _guard: guard,
        }
    }

    pub(crate) async fn lock_markets(&self, market_ids: &[i16]) -> Vec<ProfiledMutexGuard> {
        let mut shards: Vec<usize> = market_ids
            .iter()
            .map(|mid| Self::shard_index(*mid as i64, self.market_mutexes.len()))
            .collect();
        shards.sort_unstable();
        shards.dedup();
        let mut guards = Vec::with_capacity(shards.len());
        for idx in shards {
            let wait_started = Instant::now();
            let guard = self.market_mutexes[idx].clone().lock_owned().await;
            guards.push(ProfiledMutexGuard {
                label: "state.lock_markets",
                wait_ms: wait_started.elapsed().as_millis(),
                acquired_at: Instant::now(),
                _guard: guard,
            });
        }
        guards
    }

    pub(crate) async fn lock_wallet_user(&self, user_id: i64) -> ProfiledMutexGuard {
        let idx = Self::shard_index(user_id, self.wallet_mutexes.len());
        let wait_started = Instant::now();
        let guard = self.wallet_mutexes[idx].clone().lock_owned().await;
        ProfiledMutexGuard {
            label: "state.lock_wallet_user",
            wait_ms: wait_started.elapsed().as_millis(),
            acquired_at: Instant::now(),
            _guard: guard,
        }
    }

    pub(crate) async fn lock_wallet_users(&self, user_ids: &[i64]) -> Vec<ProfiledMutexGuard> {
        let mut shards: Vec<usize> = user_ids
            .iter()
            .map(|uid| Self::shard_index(*uid, self.wallet_mutexes.len()))
            .collect();
        shards.sort_unstable();
        shards.dedup();
        let mut guards = Vec::with_capacity(shards.len());
        for idx in shards {
            let wait_started = Instant::now();
            let guard = self.wallet_mutexes[idx].clone().lock_owned().await;
            guards.push(ProfiledMutexGuard {
                label: "state.lock_wallet_users",
                wait_ms: wait_started.elapsed().as_millis(),
                acquired_at: Instant::now(),
                _guard: guard,
            });
        }
        guards
    }
}
