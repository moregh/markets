#!/usr/bin/env python3
"""Load users from users.csv and run market-aware trading loops.

Goals:
- Use live order book + wallet on every decision.
- Reduce one-sided books by placing orders on the thinner side when possible.
- Size orders from available balances (not mostly qty=1).
- Keep prices near top-of-book and inside a reasonable band.
"""

from __future__ import annotations

import csv
import json
import random
import socket
import threading
import time
import urllib.error
import urllib.request
import statistics
import sys
import queue
import atexit
import gc
from dataclasses import dataclass, field
from datetime import datetime
import os
from pathlib import Path
from typing import Literal, SupportsFloat
from threading import Lock

BASE_URL = "http://localhost:8000"
USERS_CSV = Path(__file__).resolve().with_name("users.csv")
REQUEST_TIMEOUT_SECONDS = 10

RUN_SECONDS = 360000
# Traders should be very active: place an order every 0.1s to 1.0s.
MIN_SLEEP_SECONDS = 0.10
MAX_SLEEP_SECONDS = 1.00
BUSY_RETRY_SLEEP_MIN = 0.15
BUSY_RETRY_SLEEP_MAX = 0.45

# Per-order caps. Real size will be wallet-constrained and book-aware.
MAX_ORDER_QTY = 100
MIN_ORDER_QTY = 1

# If no market data exists yet, seed from this range.
DEFAULT_MIN_PRICE = 150.0
DEFAULT_MAX_PRICE = 1500.0

# Chance to cross at best price for immediate fills.
CROSS_PROBABILITY = 0.6

# Optional target market ticker. If None, trader scans all markets and picks best candidate.
PREFERRED_MARKET_TICKER: str | None = None

# ============================================================
# Agent strategies
# ============================================================

StrategyName = Literal[
    "market_maker",
    "arbitrage",
    "trend_long_short",
    "mean_reversion",
    "breakout_momentum",
]
STRATEGY_WEIGHTS: list[tuple[StrategyName, float]] = [
    ("market_maker", 0.20),
    ("arbitrage", 0.20),
    ("trend_long_short", 0.20),
    ("mean_reversion", 0.20),
    ("breakout_momentum", 0.20),
]

# Order expiry windows (seconds)
MM_MIN_EXPIRY = 10
MM_MAX_EXPIRY = 60
ARB_EXPIRY = 1

# How aggressive to seed empty books (keeps markets alive).
EMPTY_BOOK_SEED_PROBABILITY = 0.55

# Encourage liquidity in quiet/thin markets to avoid activity concentrating into a few.
THIN_MARKET_DEPTH_QTY = 30
THIN_MARKET_PREFERENCE_PROBABILITY = 0.70

# Arbitrage threshold (rough, since we don't have FX conversion).
ARBITRAGE_EDGE_FRACTION = 0.0075  # ~0.75%

# How many orderbooks to refresh per tick (we keep a cache so "scan all markets" is cheap).
# More refreshes => better global market awareness; adjust based on API capacity.
ORDERBOOK_REFRESH_PER_TICK = 8

# This project no longer uses Redis and does not support Redis wallet rebuild endpoints.
# Fund users ahead of time using the webapp admin console (Dev Topup Users).

# Logging: printing from 200 threads with flush=True will serialize on stdout and cause
# multi-second stalls. Use a single writer thread and a bounded queue to keep the
# load-generator from stuttering due to log backpressure.
LOG_QUEUE_MAX = int(os.getenv("LOG_QUEUE_MAX", "200000"))
LOG_FLUSH_INTERVAL_SECONDS = float(os.getenv("LOG_FLUSH_INTERVAL_SECONDS", "0.20"))
LOG_BATCH_MAX = int(os.getenv("LOG_BATCH_MAX", "500"))
LOG_DROP_WHEN_FULL = os.getenv("LOG_DROP_WHEN_FULL", "1") == "1"
_LOG_Q: queue.Queue[str] = queue.Queue(maxsize=max(1, LOG_QUEUE_MAX))
_LOG_DROPPED = 0
_LOG_STOP = threading.Event()


def _log_writer() -> None:
    global _LOG_DROPPED
    buf: list[str] = []
    last_flush = time.time()
    while not _LOG_STOP.is_set():
        timeout = max(0.01, LOG_FLUSH_INTERVAL_SECONDS - (time.time() - last_flush))
        try:
            line = _LOG_Q.get(timeout=timeout)
            buf.append(line)
            if len(buf) >= LOG_BATCH_MAX:
                sys.stdout.write("\n".join(buf) + "\n")
                sys.stdout.flush()
                buf.clear()
                last_flush = time.time()
        except queue.Empty:
            if buf:
                sys.stdout.write("\n".join(buf) + "\n")
                sys.stdout.flush()
                buf.clear()
            # Periodically emit a dropped-lines counter (keeps it visible but cheap).
            if _LOG_DROPPED:
                sys.stdout.write(f"{ts()} [log] dropped={_LOG_DROPPED}\n")
                sys.stdout.flush()
                _LOG_DROPPED = 0
            last_flush = time.time()

    # Drain any remaining lines on shutdown.
    try:
        while True:
            buf.append(_LOG_Q.get_nowait())
            if len(buf) >= LOG_BATCH_MAX:
                sys.stdout.write("\n".join(buf) + "\n")
                sys.stdout.flush()
                buf.clear()
    except queue.Empty:
        pass
    if buf:
        sys.stdout.write("\n".join(buf) + "\n")
        sys.stdout.flush()


_LOG_THREAD = threading.Thread(target=_log_writer, name="log-writer", daemon=True)
_LOG_THREAD.start()


def _shutdown_logging() -> None:
    _LOG_STOP.set()
    try:
        _LOG_THREAD.join(timeout=1.0)
    except Exception:
        pass


atexit.register(_shutdown_logging)


if os.getenv("DISABLE_GC", "0") == "1":
    # Optional: removes rare GC pauses for long-running stress tests.
    gc.disable()


@dataclass
class Session:
    username: str
    password: str
    token: str
    refresh_token: str


@dataclass
class MarketBook:
    market_id: int
    ticker: str
    product: str
    currency: str
    best_bid: float | None
    best_ask: float | None
    total_buy_qty: int
    total_sell_qty: int
    total_buy_value: float
    total_sell_value: float


@dataclass
class MarketSnapshot:
    market_id: int
    ticker: str
    product: str
    currency: str
    ts: float
    best_bid: float | None
    best_ask: float | None
    total_buy_qty: int
    total_sell_qty: int

    @property
    def mid(self) -> float | None:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2.0
        if self.best_bid is not None:
            return self.best_bid
        if self.best_ask is not None:
            return self.best_ask
        return None


@dataclass
class AgentState:
    strategy: StrategyName

    markets: list[dict] = field(default_factory=list)
    markets_fetched_at: float = 0.0

    wallet: dict = field(default_factory=lambda: {"currencies": {}, "products": {}})
    wallet_fetched_at: float = 0.0

    snapshots: dict[int, MarketSnapshot] = field(default_factory=dict)
    rr_idx: int = 0  # round-robin pointer for orderbook refresh

    last_mid_by_ticker: dict[str, float] = field(default_factory=dict)
    mid_history_by_market: dict[int, list[tuple[float, float]]] = field(default_factory=dict)
    mm_last_pair_ts_by_market_id: dict[int, float] = field(default_factory=dict)
    last_cancel_ts: float = 0.0
    empty_wallet_strikes: int = 0


def format_user_tag(username: str) -> str:
    parts = username.split("_")
    if len(parts) == 2 and parts[1].isdigit():
        return f"[{int(parts[1]):04}]"
    return f"[{username}]"


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def log(message: str) -> None:
    line = f"{ts()} {message}"
    try:
        _LOG_Q.put_nowait(line)
    except queue.Full:
        global _LOG_DROPPED
        if LOG_DROP_WHEN_FULL:
            _LOG_DROPPED += 1
        else:
            _LOG_Q.put(line)


def _wallet_totals(wallet: dict) -> tuple[float, int]:
    cur = wallet.get("currencies", {}) or {}
    prod = wallet.get("products", {}) or {}
    cur_total = sum(safe_float(v, 0.0) for v in cur.values())
    prod_total = sum(int(safe_float(v, 0.0)) for v in prod.values())
    return float(cur_total), int(prod_total)


def _log_empty_wallet_notice(user_tag: str, cur_total: float, prod_total: int) -> None:
    log(f"{user_tag} empty wallet (cur_total={cur_total:.2f}, prod_total={prod_total}) - fund via admin console")


def http_json(method: str, url: str, token: str | None = None, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8")
            try:
                return resp.status, (json.loads(body) if body else {})
            except json.JSONDecodeError:
                return resp.status, {"detail": "Invalid JSON response"}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            parsed = json.loads(body) if body else {}
        except json.JSONDecodeError:
            parsed = {"detail": body or f"HTTP {exc.code}"}
        return exc.code, parsed
    except urllib.error.URLError as exc:
        reason = str(getattr(exc, "reason", exc))
        return 0, {"detail": f"Network error: {reason}", "transient": True}
    except (TimeoutError, socket.timeout):
        return 0, {"detail": "Request timeout", "transient": True}
    except Exception as exc:
        return 0, {"detail": f"Unexpected request error: {exc}", "transient": True}


def read_users() -> list[tuple[str, str]]:
    if not USERS_CSV.exists():
        raise FileNotFoundError(f"Missing {USERS_CSV}")

    rows: list[tuple[str, str]] = []
    with USERS_CSV.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            username = (row.get("username") or "").strip()
            password = (row.get("password") or "").strip()
            if username and password:
                rows.append((username, password))
    return rows


def login(username: str, password: str) -> Session | None:
    status, body = http_json(
        "POST",
        f"{BASE_URL}/auth/login",
        payload={"username": username, "password": password},
    )
    if status != 200:
        detail = body.get("detail", f"HTTP {status}")
        log(f"[LOGIN FAIL] {username}: {detail}")
        return None

    token = body.get("access_token")
    refresh_token = body.get("refresh_token")
    if not token or not refresh_token:
        log(f"[LOGIN FAIL] {username}: missing tokens")
        return None

    return Session(username=username, password=password, token=token, refresh_token=refresh_token)


def refresh_session_token(session: Session) -> bool:
    if not session.refresh_token:
        return False
    status, body = http_json(
        "POST",
        f"{BASE_URL}/auth/refresh",
        payload={"refresh_token": session.refresh_token},
    )
    if status != 200:
        return False
    new_access = body.get("access_token")
    new_refresh = body.get("refresh_token")
    if not new_access or not new_refresh:
        return False
    session.token = new_access
    session.refresh_token = new_refresh
    return True


def should_retry_auth(status: int, body: object) -> bool:
    """Return True when auth should be refreshed/relogged.

    The Rust API can return different 401 details (expired/invalid/unauthorized)
    depending on token state and timing. Treat any 401 as recoverable first.
    """
    if status == 401:
        return True
    if not isinstance(body, dict):
        return False
    detail = str(body.get("detail", "")).lower()
    return any(token in detail for token in ("token", "unauthorized", "not authenticated"))


def request_with_auth(
    session: Session,
    method: str,
    path: str,
    payload: dict | None = None,
) -> tuple[int, dict]:
    status, body = http_json(method, f"{BASE_URL}{path}", token=session.token, payload=payload)
    if should_retry_auth(status, body):
        renewed = False
        refreshed = refresh_session_token(session)
        if not refreshed:
            renewed_session = login(session.username, session.password)
            if renewed_session:
                session.token = renewed_session.token
                session.refresh_token = renewed_session.refresh_token
                renewed = True
        if refreshed or renewed:
            status, body = http_json(method, f"{BASE_URL}{path}", token=session.token, payload=payload)
    return status, body


def is_transient_busy(status: int, body: object) -> bool:
    if status in (0, 429, 502, 503, 504):
        return True
    if not isinstance(body, dict):
        return False
    detail = str(body.get("detail", "")).lower()
    return any(token in detail for token in (
        "busy",
        "please retry",
        "timeout",
        "temporarily unavailable",
        "deadlock",
        "lock",
    ))


def get_markets(session: Session) -> list[dict]:
    status, body = request_with_auth(session, "GET", "/markets")
    if status != 200 or not isinstance(body, list):
        return []
    if PREFERRED_MARKET_TICKER:
        return [m for m in body if m.get("ticker") == PREFERRED_MARKET_TICKER]
    return body


def get_wallet(session: Session) -> dict:
    status, body = request_with_auth(session, "GET", "/users/me/wallet")
    if status != 200:
        return {"currencies": {}, "products": {}}
    return body


def get_orderbook(session: Session, market_id: int) -> dict:
    status, body = request_with_auth(session, "GET", f"/markets/{market_id}/orderbook")
    if status != 200:
        return {"buys": [], "sells": []}
    return body


def place_order(
    session: Session,
    market_id: int,
    buy: bool,
    qty: int,
    price: float,
    expires_in_seconds: int,
) -> tuple[int, dict]:
    # Async submit path: returns 202 immediately (status=RECEIVED) so bots are not gated by DB stalls.
    return request_with_auth(
        session,
        "POST",
        "/orders/async",
        payload={
            "market_id": market_id,
            "buy": buy,
            "quantity": int(qty),
            "price": f"{price:.2f}",
            "expires_in_seconds": int(expires_in_seconds),
        },
    )


def get_my_orders(session: Session) -> list[dict]:
    status, body = request_with_auth(session, "GET", "/orders/me")
    if status != 200 or not isinstance(body, list):
        return []
    return body


def cancel_order(session: Session, order_id: int) -> tuple[int, dict]:
    return request_with_auth(session, "DELETE", f"/orders/{int(order_id)}")


def safe_float(value: SupportsFloat, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def summarize_book(market: dict, raw: dict) -> MarketBook | None:
    ticker = str(market.get("ticker") or "")
    if "/" not in ticker:
        return None
    product, currency = ticker.split("/", 1)

    buys = raw.get("buys", []) or []
    sells = raw.get("sells", []) or []

    best_bid = safe_float(buys[0].get("price")) if buys else None
    best_ask = safe_float(sells[0].get("price")) if sells else None

    total_buy_qty = sum(int(safe_float(row.get("quantity"), 0.0)) for row in buys)
    total_sell_qty = sum(int(safe_float(row.get("quantity"), 0.0)) for row in sells)

    total_buy_value = 0.0
    total_sell_value = 0.0
    for row in buys:
        p = safe_float(row.get("price"), 0.0)
        q = int(safe_float(row.get("quantity"), 0.0))
        total_buy_value += p * q
    for row in sells:
        p = safe_float(row.get("price"), 0.0)
        q = int(safe_float(row.get("quantity"), 0.0))
        total_sell_value += p * q

    return MarketBook(
        market_id=int(market["id"]),
        ticker=ticker,
        product=product,
        currency=currency,
        best_bid=best_bid,
        best_ask=best_ask,
        total_buy_qty=total_buy_qty,
        total_sell_qty=total_sell_qty,
        total_buy_value=total_buy_value,
        total_sell_value=total_sell_value,
    )


def estimate_mid_price(book: MarketBook, last_mid: float | None) -> float:
    if book.best_bid is not None and book.best_ask is not None:
        return (book.best_bid + book.best_ask) / 2.0
    if book.best_ask is not None:
        return book.best_ask
    if book.best_bid is not None:
        return book.best_bid
    if last_mid is not None:
        return last_mid
    return random.uniform(DEFAULT_MIN_PRICE, DEFAULT_MAX_PRICE)


def choose_side_to_balance(
    book: MarketBook,
    currency_balance: float,
    product_balance: float,
    mid_price: float,
) -> bool | None:
    """Return True for BUY, False for SELL, or None if cannot trade."""
    max_buy_qty = int(currency_balance // max(mid_price, 0.01))
    max_sell_qty = int(product_balance)
    can_buy = max_buy_qty >= 1
    can_sell = max_sell_qty >= 1

    if not can_buy and not can_sell:
        return None

    # Hard rule: if one side of the book is empty, try to add that side.
    has_buys = book.total_buy_qty > 0
    has_sells = book.total_sell_qty > 0
    if has_buys and not has_sells and can_sell:
        return False
    if has_sells and not has_buys and can_buy:
        return True

    # Compute imbalance and choose the side that reduces it.
    imbalance = book.total_buy_qty - book.total_sell_qty
    if imbalance > 0 and can_sell:
        return False
    if imbalance < 0 and can_buy:
        return True

    # If roughly balanced, use wallet composition to avoid drifting to one side.
    product_notional = product_balance * mid_price
    if can_buy and can_sell:
        if currency_balance > product_notional * 1.15:
            return True
        if product_notional > currency_balance * 1.15:
            return False
        return random.choice([True, False])

    return True if can_buy else False


def choose_price(book: MarketBook, buy: bool, mid_price: float, aggressive: bool = False) -> float:
    # Reasonable band around current market signal.
    if book.best_bid is not None and book.best_ask is not None:
        spread = max(0.02, book.best_ask - book.best_bid)
        lower = max(0.01, book.best_bid - spread * 0.8)
        upper = book.best_ask + spread * 0.8
    elif book.best_ask is not None:
        band = max(0.5, book.best_ask * 0.05)
        lower = max(0.01, book.best_ask - band)
        upper = book.best_ask + band
    elif book.best_bid is not None:
        band = max(0.5, book.best_bid * 0.05)
        lower = max(0.01, book.best_bid - band)
        upper = book.best_bid + band
    else:
        band = max(1.0, mid_price * 0.08)
        lower = max(0.01, mid_price - band)
        upper = mid_price + band

    cross_prob = 0.92 if aggressive else CROSS_PROBABILITY
    cross = random.random() < cross_prob
    if buy:
        if cross and book.best_ask is not None:
            price = book.best_ask
        else:
            anchor = book.best_ask if book.best_ask is not None else mid_price
            price = anchor * random.uniform(0.992, 1.012)
    else:
        if cross and book.best_bid is not None:
            price = book.best_bid
        else:
            anchor = book.best_bid if book.best_bid is not None else mid_price
            price = anchor * random.uniform(0.988, 1.008)

    return round(clamp(price, lower, upper), 2)


def choose_quantity(
    buy: bool,
    price: float,
    currency_balance: float,
    product_balance: float,
    opposing_top_qty: int,
) -> int:
    if buy:
        max_qty = int(currency_balance // max(price, 0.01))
    else:
        max_qty = int(product_balance)

    max_qty = max(0, min(max_qty, MAX_ORDER_QTY))
    if max_qty <= 0:
        return 0

    # Primary target from wallet capacity so size isn't mostly 1.
    target_from_wallet = int(max_qty * random.uniform(0.10, 0.35))

    # Secondary target from visible opposing liquidity.
    target_from_book = int(opposing_top_qty * random.uniform(0.75, 1.6)) if opposing_top_qty > 0 else 0

    qty = max(target_from_wallet, target_from_book)
    qty_floor = min(max_qty, MIN_ORDER_QTY if max_qty >= MIN_ORDER_QTY else 1)
    qty = max(qty_floor, qty)
    return max(1, min(max_qty, qty))


def build_order_for_market(
    wallet: dict,
    market: dict,
    raw_book: dict,
    last_mid_by_ticker: dict[str, float],
) -> tuple[int, bool, int, float, str] | None:
    book = summarize_book(market, raw_book)
    if not book:
        return None

    currency_balance = safe_float(wallet.get("currencies", {}).get(book.currency, 0))
    product_balance = safe_float(wallet.get("products", {}).get(book.product, 0))

    mid = estimate_mid_price(book, last_mid_by_ticker.get(book.ticker))
    last_mid_by_ticker[book.ticker] = mid

    # Empty book bootstrap: place random seed pricing, preferring bids so
    # markets without any levels quickly gain a reference price ladder.
    if book.total_buy_qty == 0 and book.total_sell_qty == 0:
        can_buy = currency_balance >= max(mid, 1.0)
        can_sell = product_balance >= 1
        if not can_buy and not can_sell:
            return None

        if can_buy:
            buy = True
        else:
            buy = False

        base = max(1.0, mid)
        band = max(5.0, base * 0.25)
        price = round(random.uniform(max(0.01, base - band), base + band), 2)
        qty = choose_quantity(
            buy=buy,
            price=price,
            currency_balance=currency_balance,
            product_balance=product_balance,
            opposing_top_qty=0,
        )
        if qty <= 0:
            return None
        side = "BUY" if buy else "SELL"
        return book.market_id, buy, qty, price, f"{book.ticker} {side}"

    forced_buy: bool | None = None
    aggressive = False
    product_notional = product_balance * max(mid, 0.01)
    if currency_balance < max(10.0, mid * 0.5) and product_balance >= 1:
        # Cash poor and product rich: prioritize selling to free currency.
        forced_buy = False
        aggressive = True
    elif product_balance < 1 and currency_balance >= mid:
        # Product poor and cash rich: prioritize buying to restore inventory.
        forced_buy = True
        aggressive = True
    elif currency_balance < product_notional * 0.25 and product_balance > 0:
        forced_buy = False
    elif product_notional < currency_balance * 0.25 and currency_balance >= mid:
        forced_buy = True

    buy = forced_buy if forced_buy is not None else choose_side_to_balance(book, currency_balance, product_balance, mid)
    if buy is None:
        return None

    price = choose_price(book, buy, mid, aggressive=aggressive)

    buys = raw_book.get("buys", []) or []
    sells = raw_book.get("sells", []) or []
    opposing_top_qty = int(safe_float((sells[0] if buy and sells else buys[0] if (not buy and buys) else {}).get("quantity"), 0.0))

    qty = choose_quantity(
        buy=buy,
        price=price,
        currency_balance=currency_balance,
        product_balance=product_balance,
        opposing_top_qty=opposing_top_qty,
    )
    if qty <= 0:
        return None

    side = "BUY" if buy else "SELL"
    return book.market_id, buy, qty, price, f"{book.ticker} {side}"


def choose_strategy_for_user(username: str) -> StrategyName:
    # Stable per-user assignment to create persistent "personalities".
    r = random.Random(username).random()
    total = sum(w for _, w in STRATEGY_WEIGHTS)
    acc = 0.0
    for name, weight in STRATEGY_WEIGHTS:
        acc += (weight / total)
        if r <= acc:
            return name
    return STRATEGY_WEIGHTS[-1][0]


def snapshot_from_orderbook(
    now: float,
    market: dict,
    raw: dict,
    last_mid_by_ticker: dict[str, float],
) -> MarketSnapshot | None:
    book = summarize_book(market, raw)
    if not book:
        return None
    mid = estimate_mid_price(book, last_mid_by_ticker.get(book.ticker))
    last_mid_by_ticker[book.ticker] = mid
    return MarketSnapshot(
        market_id=book.market_id,
        ticker=book.ticker,
        product=book.product,
        currency=book.currency,
        ts=now,
        best_bid=book.best_bid,
        best_ask=book.best_ask,
        total_buy_qty=book.total_buy_qty,
        total_sell_qty=book.total_sell_qty,
    )


def _price_tick(price: float) -> float:
    return round(max(0.01, price), 2)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def size_from_edge(max_qty: int, edge_fraction: float, min_edge: float, max_edge: float) -> int:
    """Size orders based on margin/edge.

    - Thin edges => small size
    - Large edges => larger size (but still randomized)
    """
    if max_qty <= 0:
        return 0

    edge = max(0.0, float(edge_fraction))
    if max_edge <= min_edge:
        scaled = 0.25
    else:
        scaled = _clamp01((edge - min_edge) / (max_edge - min_edge))

    # Never go to zero; keep some flow. Randomize to avoid constant caps.
    mult = max(0.05, min(1.0, scaled))
    qty = int(max_qty * random.uniform(0.10, 0.85) * mult)
    return max(1, min(max_qty, qty))


def mm_quote_prices(snap: MarketSnapshot, mid_hint: float) -> tuple[float, float]:
    mid = snap.mid if snap.mid is not None else mid_hint
    mid = max(0.01, mid)

    if snap.best_bid is not None and snap.best_ask is not None and snap.best_ask > snap.best_bid:
        spread = max(0.02, snap.best_ask - snap.best_bid)
        half = spread * random.uniform(0.35, 0.85)
    else:
        half = max(0.10, mid * random.uniform(0.002, 0.01))

    buy_price = _price_tick(mid - half)
    sell_price = _price_tick(mid + half)

    if snap.best_bid is not None:
        buy_price = _price_tick(max(buy_price, snap.best_bid * random.uniform(0.995, 1.002)))
    if snap.best_ask is not None:
        sell_price = _price_tick(min(sell_price, snap.best_ask * random.uniform(0.998, 1.005)))
    return buy_price, sell_price


def mm_choose_pair_quantities(
    wallet: dict,
    snap: MarketSnapshot,
    buy_price: float,
    sell_price: float,
) -> tuple[int, int]:
    currency_balance = safe_float(wallet.get("currencies", {}).get(snap.currency, 0))
    product_balance = safe_float(wallet.get("products", {}).get(snap.product, 0))

    max_buy = int(currency_balance // max(buy_price, 0.01))
    max_sell = int(product_balance)
    if max_buy <= 0 and max_sell <= 0:
        return 0, 0

    mid = snap.mid
    if mid is None:
        mid = max(0.01, (buy_price + sell_price) / 2.0)
    mid = max(0.01, float(mid))
    edge = max(0.0, (sell_price - buy_price) / mid)

    buy_qty = 0
    sell_qty = 0
    if max_buy > 0:
        cap = min(MAX_ORDER_QTY, max_buy)
        buy_qty = size_from_edge(cap, edge, min_edge=0.001, max_edge=0.02)
    if max_sell > 0:
        cap = min(MAX_ORDER_QTY, max_sell)
        sell_qty = size_from_edge(cap, edge, min_edge=0.001, max_edge=0.02)
    return buy_qty, sell_qty


def find_arbitrage_opportunity(state: AgentState) -> tuple[MarketSnapshot, MarketSnapshot] | None:
    def build_fx_map() -> dict[tuple[str, str], float]:
        """Infer quote-currency conversion from same-product mids.

        fx[(a, b)] means: multiply a price quoted in currency b to express it in currency a.
        """
        now_fx = time.time()
        by_product: dict[str, list[MarketSnapshot]] = {}
        for s in state.snapshots.values():
            if (now_fx - s.ts) > 3.0:
                continue
            if s.mid is None or s.mid <= 0:
                continue
            by_product.setdefault(s.product, []).append(s)

        samples: dict[tuple[str, str], list[float]] = {}
        for snaps_fx in by_product.values():
            if len(snaps_fx) < 2:
                continue
            for i in range(len(snaps_fx)):
                for j in range(i + 1, len(snaps_fx)):
                    a = snaps_fx[i]
                    b = snaps_fx[j]
                    if a.currency == b.currency:
                        continue
                    a_mid = float(a.mid or 0.0)
                    b_mid = float(b.mid or 0.0)
                    if a_mid <= 0 or b_mid <= 0:
                        continue
                    # price_a / price_b converts b-quoted price into a currency units
                    ratio_ab = a_mid / b_mid
                    ratio_ba = b_mid / a_mid
                    if ratio_ab > 0:
                        samples.setdefault((a.currency, b.currency), []).append(ratio_ab)
                    if ratio_ba > 0:
                        samples.setdefault((b.currency, a.currency), []).append(ratio_ba)

        fx: dict[tuple[str, str], float] = {}
        for pair, vals in samples.items():
            if not vals:
                continue
            fx[pair] = float(statistics.median(vals))
        return fx

    def converted_bid_in_buy_currency(
        sell_snap: MarketSnapshot,
        buy_currency: str,
        fx: dict[tuple[str, str], float],
    ) -> float | None:
        raw_bid = float(sell_snap.best_bid or 0.0)
        if raw_bid <= 0:
            return None
        if sell_snap.currency == buy_currency:
            return raw_bid
        rate = fx.get((buy_currency, sell_snap.currency))
        if rate is None or rate <= 0:
            return None
        return raw_bid * rate

    now = time.time()
    by_product: dict[str, list[MarketSnapshot]] = {}
    for snap in state.snapshots.values():
        if (now - snap.ts) > 3.0:
            continue
        by_product.setdefault(snap.product, []).append(snap)

    fx = build_fx_map()
    best: tuple[float, MarketSnapshot, MarketSnapshot] | None = None
    for snaps in by_product.values():
        if len(snaps) < 2:
            continue
        buy_markets = [s for s in snaps if s.best_ask is not None]
        sell_markets = [s for s in snaps if s.best_bid is not None]
        if not buy_markets or not sell_markets:
            continue
        for low in buy_markets:
            low_ask = float(low.best_ask or 0.0)
            if low_ask <= 0:
                continue
            for high in sell_markets:
                if low.market_id == high.market_id:
                    continue
                high_bid_converted = converted_bid_in_buy_currency(high, low.currency, fx)
                if high_bid_converted is None or high_bid_converted <= 0:
                    continue
                mid_ref = max(0.01, (low_ask + high_bid_converted) / 2.0)
                edge = (high_bid_converted - low_ask) / mid_ref
                if edge <= ARBITRAGE_EDGE_FRACTION:
                    continue

                # Maximize expected absolute profit, not just edge.
                cur_bal = _wallet_currency_balance(state.wallet, low.currency)
                prod_bal = _wallet_product_balance(state.wallet, high.product)
                max_buy_qty = int(cur_bal // max(low_ask, 0.01))
                max_sell_qty = int(prod_bal)
                liq_cap = max(
                    0,
                    min(
                        int(low.total_sell_qty),
                        int(high.total_buy_qty),
                        int(MAX_ORDER_QTY),
                        int(max_buy_qty),
                        int(max_sell_qty),
                    ),
                )
                if liq_cap <= 0:
                    continue
                per_unit_profit = max(0.0, high_bid_converted - low_ask)
                expected_profit = per_unit_profit * float(liq_cap)

                if best is None or expected_profit > best[0]:
                    best = (expected_profit, low, high)

    if not best:
        return None
    return best[1], best[2]


def refresh_some_orderbooks(session: Session, state: AgentState) -> None:
    if not state.markets:
        return
    now = time.time()
    n = min(ORDERBOOK_REFRESH_PER_TICK, len(state.markets))
    for _ in range(n):
        idx = state.rr_idx % len(state.markets)
        state.rr_idx += 1
        market = state.markets[idx]
        raw = get_orderbook(session, int(market["id"]))
        snap = snapshot_from_orderbook(now, market, raw, state.last_mid_by_ticker)
        if snap:
            state.snapshots[int(market["id"])] = snap
            mid = snap.mid
            if mid is not None:
                series = state.mid_history_by_market.setdefault(int(snap.market_id), [])
                series.append((now, float(mid)))
                # Keep around ~3 minutes of per-market history.
                cutoff = now - 180.0
                state.mid_history_by_market[int(snap.market_id)] = [(t, p) for (t, p) in series if t >= cutoff]


def _wallet_currency_balance(wallet: dict, currency: str) -> float:
    return safe_float(wallet.get("currencies", {}).get(currency, 0.0), 0.0)


def _wallet_product_balance(wallet: dict, product: str) -> int:
    return int(safe_float(wallet.get("products", {}).get(product, 0.0), 0.0))


def _series_since(history: list[tuple[float, float]], now: float, lookback_seconds: float) -> list[tuple[float, float]]:
    cutoff = now - lookback_seconds
    return [(t, p) for (t, p) in history if t >= cutoff]


def _place_with_log(
    session: Session,
    state: AgentState,
    user_tag: str,
    label: str,
    market_id: int,
    ticker: str,
    buy: bool,
    qty: int,
    price: float,
    expiry: int,
) -> bool:
    if qty <= 0:
        return False
    status, body = place_order(session, int(market_id), bool(buy), int(qty), float(price), int(expiry))
    if status in (200, 202):
        side = "BUY" if buy else "SELL"
        log(f"{user_tag} [{label}] {ticker} {side} qty={qty} @ {price:.2f}")
        return True

    detail = body.get("detail", f"HTTP {status}") if isinstance(body, dict) else f"HTTP {status}"
    if is_transient_busy(status, body if isinstance(body, dict) else {}):
        log(f"{user_tag} transient: {detail}")
        time.sleep(random.uniform(BUSY_RETRY_SLEEP_MIN, BUSY_RETRY_SLEEP_MAX))
    elif "insufficient" in str(detail).lower():
        _maybe_cancel_some_orders(session, state, label.lower())
    return False


def trade_trend_long_short(session: Session, state: AgentState, user_tag: str) -> bool:
    now = time.time()
    best = None
    for snap in state.snapshots.values():
        mid_now = snap.mid
        if mid_now is None:
            continue
        hist = state.mid_history_by_market.get(int(snap.market_id), [])
        window = _series_since(hist, now, 45.0)
        if len(window) < 6:
            continue
        base = window[0][1]
        if base <= 0:
            continue
        momentum = (mid_now - base) / base
        if abs(momentum) < 0.0015:
            continue
        score = abs(momentum) * (1.0 + (0.25 if _is_thin_market(snap) else 0.0))
        if best is None or score > best[0]:
            best = (score, snap, momentum)

    if not best:
        return False

    _, snap, momentum = best
    buy = momentum > 0
    if buy:
        price = float(snap.best_ask or (snap.mid or random.uniform(DEFAULT_MIN_PRICE, DEFAULT_MAX_PRICE)))
    else:
        price = float(snap.best_bid or (snap.mid or random.uniform(DEFAULT_MIN_PRICE, DEFAULT_MAX_PRICE)))
    price = _price_tick(price)

    cur_bal = _wallet_currency_balance(state.wallet, snap.currency)
    prod_bal = _wallet_product_balance(state.wallet, snap.product)
    max_qty = int(cur_bal // max(price, 0.01)) if buy else int(prod_bal)
    if max_qty <= 0:
        return False

    cap = min(MAX_ORDER_QTY, max_qty)
    qty = size_from_edge(cap, abs(momentum), min_edge=0.0015, max_edge=0.03)
    if qty <= 0:
        return False

    label = "LS-LONG" if buy else "LS-SHORT"
    return _place_with_log(
        session, state, user_tag, label, snap.market_id, snap.ticker, buy, qty, price, random.randint(20, 90)
    )


def trade_mean_reversion(session: Session, state: AgentState, user_tag: str) -> bool:
    now = time.time()
    best = None
    for snap in state.snapshots.values():
        mid_now = snap.mid
        if mid_now is None:
            continue
        hist = state.mid_history_by_market.get(int(snap.market_id), [])
        window = _series_since(hist, now, 60.0)
        if len(window) < 8:
            continue
        mean = sum(p for _, p in window) / len(window)
        if mean <= 0:
            continue
        deviation = (mid_now - mean) / mean
        if abs(deviation) < 0.0025:
            continue
        score = abs(deviation) * (1.0 + (0.3 if _is_thin_market(snap) else 0.0))
        if best is None or score > best[0]:
            best = (score, snap, deviation, mean)

    if not best:
        return False

    _, snap, deviation, mean = best
    buy = deviation < 0  # below mean: buy; above mean: sell
    mid = snap.mid if snap.mid is not None else mean
    if buy:
        # Try to buy under mean; cross occasionally to ensure fills.
        base = float(snap.best_ask or mid)
        price = base if random.random() < 0.45 else (mid * 0.998)
    else:
        base = float(snap.best_bid or mid)
        price = base if random.random() < 0.45 else (mid * 1.002)
    price = _price_tick(price)

    cur_bal = _wallet_currency_balance(state.wallet, snap.currency)
    prod_bal = _wallet_product_balance(state.wallet, snap.product)
    max_qty = int(cur_bal // max(price, 0.01)) if buy else int(prod_bal)
    if max_qty <= 0:
        return False

    cap = min(MAX_ORDER_QTY, max_qty)
    qty = size_from_edge(cap, abs(deviation), min_edge=0.0025, max_edge=0.04)
    if qty <= 0:
        return False

    label = "MR-BUY" if buy else "MR-SELL"
    return _place_with_log(
        session, state, user_tag, label, snap.market_id, snap.ticker, buy, qty, price, random.randint(20, 80)
    )


def trade_breakout_momentum(session: Session, state: AgentState, user_tag: str) -> bool:
    now = time.time()
    best = None
    for snap in state.snapshots.values():
        mid_now = snap.mid
        if mid_now is None:
            continue
        hist = state.mid_history_by_market.get(int(snap.market_id), [])
        window = _series_since(hist, now, 50.0)
        if len(window) < 8:
            continue
        prices = [p for _, p in window]
        high = max(prices)
        low = min(prices)
        if high <= 0 or low <= 0:
            continue
        up_break = (mid_now - high) / high
        down_break = (low - mid_now) / low
        if up_break > 0.0015:
            score = up_break * (1.0 + (0.35 if _is_thin_market(snap) else 0.0))
            if best is None or score > best[0]:
                best = (score, snap, True, up_break)
        if down_break > 0.0015:
            score = down_break * (1.0 + (0.35 if _is_thin_market(snap) else 0.0))
            if best is None or score > best[0]:
                best = (score, snap, False, down_break)

    if not best:
        return False

    _, snap, buy, edge = best
    if buy:
        price = _price_tick(float(snap.best_ask or (snap.mid or random.uniform(DEFAULT_MIN_PRICE, DEFAULT_MAX_PRICE))))
    else:
        price = _price_tick(float(snap.best_bid or (snap.mid or random.uniform(DEFAULT_MIN_PRICE, DEFAULT_MAX_PRICE))))

    cur_bal = _wallet_currency_balance(state.wallet, snap.currency)
    prod_bal = _wallet_product_balance(state.wallet, snap.product)
    max_qty = int(cur_bal // max(price, 0.01)) if buy else int(prod_bal)
    if max_qty <= 0:
        return False

    cap = min(MAX_ORDER_QTY, max_qty)
    qty = size_from_edge(cap, edge, min_edge=0.0015, max_edge=0.03)
    if qty <= 0:
        return False

    label = "BO-BUY" if buy else "BO-SELL"
    return _place_with_log(
        session, state, user_tag, label, snap.market_id, snap.ticker, buy, qty, price, random.randint(10, 45)
    )


def _is_empty_market(snap: MarketSnapshot) -> bool:
    return snap.total_buy_qty == 0 and snap.total_sell_qty == 0


def _is_one_sided_market(snap: MarketSnapshot) -> bool:
    return (snap.total_buy_qty == 0 and snap.total_sell_qty > 0) or (snap.total_sell_qty == 0 and snap.total_buy_qty > 0)


def _is_thin_market(snap: MarketSnapshot) -> bool:
    return (snap.total_buy_qty + snap.total_sell_qty) <= THIN_MARKET_DEPTH_QTY


def speculate_on_thin_markets(session: Session, state: AgentState, user_tag: str) -> bool:
    """Speculate on thin/quiet markets so activity doesn't concentrate.

    Targets:
    - empty books
    - one-sided books
    - low-depth books
    """
    candidates = [
        s for s in state.snapshots.values()
        if _is_empty_market(s) or _is_one_sided_market(s) or _is_thin_market(s)
    ]

    if not candidates and state.markets:
        # Pull one fresh market into the snapshot cache and re-check.
        market = random.choice(state.markets)
        raw = get_orderbook(session, int(market["id"]))
        snap = snapshot_from_orderbook(time.time(), market, raw, state.last_mid_by_ticker)
        if snap:
            state.snapshots[int(market["id"])] = snap
            if _is_empty_market(snap) or _is_one_sided_market(snap) or _is_thin_market(snap):
                candidates = [snap]

    if not candidates:
        return False

    # Prefer empties and one-sided books.
    candidates.sort(
        key=lambda s: (
            2 if _is_empty_market(s) else 1 if _is_one_sided_market(s) else 0,
            -(s.total_buy_qty + s.total_sell_qty),
        ),
        reverse=True,
    )
    snap = random.choice(candidates[: max(1, min(12, len(candidates)))])

    mid_hint = state.last_mid_by_ticker.get(snap.ticker, random.uniform(DEFAULT_MIN_PRICE, DEFAULT_MAX_PRICE))
    buy_p, sell_p = mm_quote_prices(snap, mid_hint)
    buy_qty, sell_qty = mm_choose_pair_quantities(state.wallet, snap, buy_p, sell_p)

    # If one-sided, focus on filling the missing side first.
    if _is_one_sided_market(snap):
        if snap.total_buy_qty == 0:
            # Missing buys
            sell_qty = 0
        else:
            # Missing sells
            buy_qty = 0

    placed = False
    if buy_qty >= 1:
        status, _ = place_order(session, snap.market_id, True, buy_qty, buy_p, random.randint(MM_MIN_EXPIRY, MM_MAX_EXPIRY))
        if status in (200, 202):
            log(f"{user_tag} [THIN-BUY] {snap.ticker} qty={buy_qty} @ {buy_p:.2f}")
            placed = True
        else:
            # Useful when markets are empty and bots mysteriously "do nothing".
            log(f"{user_tag} thin buy failed: HTTP {status}")
    if sell_qty >= 1:
        status, _ = place_order(session, snap.market_id, False, sell_qty, sell_p, random.randint(MM_MIN_EXPIRY, MM_MAX_EXPIRY))
        if status in (200, 202):
            log(f"{user_tag} [THIN-SELL] {snap.ticker} qty={sell_qty} @ {sell_p:.2f}")
            placed = True
        else:
            log(f"{user_tag} thin sell failed: HTTP {status}")

    return placed


def choose_market_maker_target(state: AgentState) -> MarketSnapshot | None:
    snaps = list(state.snapshots.values())
    if not snaps:
        return None

    weights: list[float] = []
    for s in snaps:
        depth = s.total_buy_qty + s.total_sell_qty
        imbalance = abs(s.total_buy_qty - s.total_sell_qty)

        w = 1.0
        if _is_empty_market(s):
            w += 2500.0
        elif _is_one_sided_market(s):
            w += 1600.0
        elif _is_thin_market(s):
            w += 900.0

        # Still consider profitable "buy low sell high" in quiet markets:
        w += min(800.0, float(imbalance) * 3.0)
        # Mildly penalize already-deep markets so we don't crowd them.
        w *= 1.0 / (1.0 + (depth / 150.0))

        weights.append(max(0.01, float(w)))

    return random.choices(snaps, weights=weights, k=1)[0]


def _maybe_cancel_some_orders(session: Session, state: AgentState, reason: str) -> None:
    now = time.time()
    # Avoid spamming cancels; under load the DB row can be busy and we don't want to hammer retries.
    if state.last_cancel_ts and (now - state.last_cancel_ts) < 1.0:
        return

    active_orders = [
        o for o in get_my_orders(session)
        if str(o.get("state")) in ("OPEN", "PARTIAL")
    ]
    if not active_orders:
        return

    # Only cancel aggressively if we've piled up a lot of open exposure.
    cancel_cap = 2 if len(active_orders) >= 12 else 1
    # For low-pressure reasons, don't always cancel.
    if cancel_cap == 1 and random.random() < 0.75:
        return

    active_orders.sort(key=lambda o: int(o.get("id", 0)))
    for order in active_orders[: min(cancel_cap, len(active_orders))]:
        status, _body = cancel_order(session, int(order["id"]))
        # 200: cancelled/closed, 409: busy lock, 400: already closed/expired (idempotent server-side).
        if status in (200, 202, 400, 409):
            continue
    state.last_cancel_ts = now

def score_market_opportunity(wallet: dict, market: dict, raw_book: dict, last_mid_by_ticker: dict[str, float]) -> float:
    book = summarize_book(market, raw_book)
    if not book:
        return -1.0

    currency_balance = safe_float(wallet.get("currencies", {}).get(book.currency, 0))
    product_balance = safe_float(wallet.get("products", {}).get(book.product, 0))
    mid = estimate_mid_price(book, last_mid_by_ticker.get(book.ticker))

    max_buy_qty = int(currency_balance // max(mid, 0.01))
    max_sell_qty = int(product_balance)

    # Opportunity = imbalance size * ability to help on missing/thin side.
    imbalance = abs(book.total_buy_qty - book.total_sell_qty)

    # Explicitly prioritize completely empty books so the bot farm seeds all markets.
    if book.total_buy_qty == 0 and book.total_sell_qty == 0:
        bootstrap_ability = min(max(max_buy_qty, max_sell_qty), MAX_ORDER_QTY)
        if bootstrap_ability <= 0:
            return -1.0
        return 3000.0 + float(bootstrap_ability)

    if book.total_buy_qty > 0 and book.total_sell_qty == 0:
        return float(min(max_sell_qty, MAX_ORDER_QTY)) + 1000.0
    if book.total_sell_qty > 0 and book.total_buy_qty == 0:
        return float(min(max_buy_qty, MAX_ORDER_QTY)) + 1000.0

    if book.total_buy_qty > book.total_sell_qty:
        ability = min(max_sell_qty, MAX_ORDER_QTY)
    elif book.total_sell_qty > book.total_buy_qty:
        ability = min(max_buy_qty, MAX_ORDER_QTY)
    else:
        ability = min(max(max_buy_qty, max_sell_qty), MAX_ORDER_QTY)

    if ability <= 0:
        return -1.0

    return float(imbalance) + (ability * 0.5)


def trader_loop(session: Session, stop_at: float) -> None:
    user_tag = format_user_tag(session.username)
    state = AgentState(strategy=choose_strategy_for_user(session.username))

    # Quick readiness check
    ready_wallet = get_wallet(session)
    log(
        f"[READY] {session.username} strat={state.strategy}: "
        f"currencies={len(ready_wallet.get('currencies', {}))} "
        f"products={len(ready_wallet.get('products', {}))}"
    )

    while time.time() < stop_at:
        try:
            now = time.time()

            if (now - state.markets_fetched_at) > 1.0 or not state.markets:
                state.markets = get_markets(session)
                state.markets_fetched_at = now

            if not state.markets:
                time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
                continue

            if (now - state.wallet_fetched_at) > 1.0 or not state.wallet:
                state.wallet = get_wallet(session)
                state.wallet_fetched_at = now
                cur_total, prod_total = _wallet_totals(state.wallet)
                if cur_total <= 0.0 and prod_total <= 0:
                    state.empty_wallet_strikes += 1
                    if state.empty_wallet_strikes in (2, 6, 12):
                        _log_empty_wallet_notice(user_tag, cur_total, prod_total)
                else:
                    state.empty_wallet_strikes = 0

            refresh_some_orderbooks(session, state)

            placed_any = False

            if state.strategy == "arbitrage":
                opp = find_arbitrage_opportunity(state)
                if opp:
                    buy_snap, sell_snap = opp
                    # Revalidate both legs from live books to avoid acting on stale snapshots.
                    market_by_id = {int(m.get("id", 0)): m for m in state.markets}
                    buy_market = market_by_id.get(int(buy_snap.market_id))
                    sell_market = market_by_id.get(int(sell_snap.market_id))
                    if buy_market:
                        buy_raw = get_orderbook(session, int(buy_snap.market_id))
                        buy_live = snapshot_from_orderbook(time.time(), buy_market, buy_raw, state.last_mid_by_ticker)
                        if buy_live:
                            state.snapshots[int(buy_snap.market_id)] = buy_live
                            buy_snap = buy_live
                    if sell_market:
                        sell_raw = get_orderbook(session, int(sell_snap.market_id))
                        sell_live = snapshot_from_orderbook(time.time(), sell_market, sell_raw, state.last_mid_by_ticker)
                        if sell_live:
                            state.snapshots[int(sell_snap.market_id)] = sell_live
                            sell_snap = sell_live

                    buy_price = float(buy_snap.best_ask or 0.0)
                    sell_price = float(sell_snap.best_bid or 0.0)

                    if buy_price > 0 and sell_price > 0:
                        cur_bal = safe_float(state.wallet.get("currencies", {}).get(buy_snap.currency, 0))
                        prod_bal = safe_float(state.wallet.get("products", {}).get(sell_snap.product, 0))

                        max_buy_qty = int(cur_bal // max(buy_price, 0.01))
                        max_sell_qty = int(prod_bal)

                        # Normalize sell quote into buy currency using cross-product implied FX.
                        edge = 0.0
                        if buy_snap.currency == sell_snap.currency:
                            mid_ref = max(0.01, (buy_price + sell_price) / 2.0)
                            edge = max(0.0, (sell_price - buy_price) / mid_ref)
                        else:
                            ratio_samples: list[float] = []
                            for s1 in state.snapshots.values():
                                if s1.currency != buy_snap.currency or s1.mid is None or s1.mid <= 0:
                                    continue
                                for s2 in state.snapshots.values():
                                    if s2.currency != sell_snap.currency or s2.product != s1.product or s2.mid is None or s2.mid <= 0:
                                        continue
                                    ratio_samples.append(float(s1.mid) / float(s2.mid))
                            if ratio_samples:
                                fx_sell_to_buy = float(statistics.median(ratio_samples))
                                sell_in_buy_ccy = sell_price * fx_sell_to_buy
                                mid_ref = max(0.01, (buy_price + sell_in_buy_ccy) / 2.0)
                                edge = max(0.0, (sell_in_buy_ccy - buy_price) / mid_ref)

                        qty_both = 0
                        if max_sell_qty > 0:
                            cap = min(MAX_ORDER_QTY, max_buy_qty, max_sell_qty)
                            if cap > 0:
                                qty_both = size_from_edge(cap, edge, min_edge=ARBITRAGE_EDGE_FRACTION, max_edge=0.05)
                        if qty_both >= 1 and edge > ARBITRAGE_EDGE_FRACTION:
                            status1, body1 = place_order(session, buy_snap.market_id, True, qty_both, buy_price, ARB_EXPIRY)
                            status2, body2 = place_order(session, sell_snap.market_id, False, qty_both, sell_price, ARB_EXPIRY)
                            if status1 in (200, 202):
                                log(f"{user_tag} [ARB-BUY] {buy_snap.ticker} qty={qty_both} @ {buy_price:.2f}")
                                placed_any = True
                            else:
                                detail = body1.get("detail", f"HTTP {status1}")
                                if is_transient_busy(status1, body1) or body1.get("transient"):
                                    log(f"{user_tag} transient: {detail}")
                                    time.sleep(random.uniform(BUSY_RETRY_SLEEP_MIN, BUSY_RETRY_SLEEP_MAX))
                                elif "insufficient" in str(detail).lower():
                                    _maybe_cancel_some_orders(session, state, "arb-buy")
                            if status2 in (200, 202):
                                log(f"{user_tag} [ARB-SELL] {sell_snap.ticker} qty={qty_both} @ {sell_price:.2f}")
                                placed_any = True
                            else:
                                detail = body2.get("detail", f"HTTP {status2}")
                                if is_transient_busy(status2, body2) or body2.get("transient"):
                                    log(f"{user_tag} transient: {detail}")
                                    time.sleep(random.uniform(BUSY_RETRY_SLEEP_MIN, BUSY_RETRY_SLEEP_MAX))
                                elif "insufficient" in str(detail).lower():
                                    _maybe_cancel_some_orders(session, state, "arb-sell")

            if not placed_any and state.strategy == "market_maker":
                market_choice: MarketSnapshot | None = None

                # Prefer thin markets most of the time so quiet books get liquidity.
                if random.random() < THIN_MARKET_PREFERENCE_PROBABILITY:
                    thin = [s for s in state.snapshots.values() if _is_empty_market(s) or _is_one_sided_market(s) or _is_thin_market(s)]
                    if thin:
                        market_choice = random.choice(thin)

                if market_choice is None:
                    # General weighted choice across all markets.
                    market_choice = choose_market_maker_target(state)

                if market_choice:
                    mid_hint = state.last_mid_by_ticker.get(
                        market_choice.ticker,
                        random.uniform(DEFAULT_MIN_PRICE, DEFAULT_MAX_PRICE),
                    )
                    buy_p, sell_p = mm_quote_prices(market_choice, mid_hint)
                    buy_qty, sell_qty = mm_choose_pair_quantities(state.wallet, market_choice, buy_p, sell_p)

                    last_pair = state.mm_last_pair_ts_by_market_id.get(market_choice.market_id, 0.0)
                    if (now - last_pair) < 0.35:
                        buy_qty = 0
                        sell_qty = 0

                    if buy_qty >= 1:
                        status, body = place_order(
                            session,
                            market_choice.market_id,
                            True,
                            buy_qty,
                            buy_p,
                            random.randint(MM_MIN_EXPIRY, MM_MAX_EXPIRY),
                        )
                        if status in (200, 202):
                            log(f"{user_tag} [MM-BUY] {market_choice.ticker} qty={buy_qty} @ {buy_p:.2f}")
                            placed_any = True
                        else:
                            detail = body.get("detail", f"HTTP {status}")
                            if is_transient_busy(status, body) or body.get("transient"):
                                log(f"{user_tag} transient: {detail}")
                                time.sleep(random.uniform(BUSY_RETRY_SLEEP_MIN, BUSY_RETRY_SLEEP_MAX))
                            elif "insufficient" in str(detail).lower():
                                _maybe_cancel_some_orders(session, state, "mm-buy")

                    if sell_qty >= 1:
                        status, body = place_order(
                            session,
                            market_choice.market_id,
                            False,
                            sell_qty,
                            sell_p,
                            random.randint(MM_MIN_EXPIRY, MM_MAX_EXPIRY),
                        )
                        if status in (200, 202):
                            log(f"{user_tag} [MM-SELL] {market_choice.ticker} qty={sell_qty} @ {sell_p:.2f}")
                            placed_any = True
                        else:
                            detail = body.get("detail", f"HTTP {status}")
                            if is_transient_busy(status, body) or body.get("transient"):
                                log(f"{user_tag} transient: {detail}")
                                time.sleep(random.uniform(BUSY_RETRY_SLEEP_MIN, BUSY_RETRY_SLEEP_MAX))
                            elif "insufficient" in str(detail).lower():
                                _maybe_cancel_some_orders(session, state, "mm-sell")

                    if placed_any:
                        state.mm_last_pair_ts_by_market_id[market_choice.market_id] = now

            if not placed_any and state.strategy == "trend_long_short":
                placed_any = trade_trend_long_short(session, state, user_tag)

            if not placed_any and state.strategy == "mean_reversion":
                placed_any = trade_mean_reversion(session, state, user_tag)

            if not placed_any and state.strategy == "breakout_momentum":
                placed_any = trade_breakout_momentum(session, state, user_tag)

            # When markets are quiet/thin/empty, speculate there to spread activity.
            if not placed_any:
                placed_any = speculate_on_thin_markets(session, state, user_tag)

            if not placed_any and random.random() < 0.25:
                _maybe_cancel_some_orders(session, state, "no-op")
        except Exception as exc:
            # Keep trader thread alive even if one loop iteration fails unexpectedly.
            log(f"{user_tag} loop error: {exc}")
            time.sleep(random.uniform(0.2, 0.6))
            continue

        time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))


def main() -> None:
    users = read_users()
    if not users:
        log("No users found in users.csv")
        return

    sessions: list[Session] = []
    for username, password in users:
        sess = login(username, password)
        if sess:
            sessions.append(sess)

    if not sessions:
        log("No valid sessions; aborting.")
        return

    # If everyone looks empty, it usually means users were not funded yet.
    empty = 0
    for sess in sessions[: min(10, len(sessions))]:
        w = get_wallet(sess)
        cur_total, prod_total = _wallet_totals(w)
        if cur_total <= 0.0 and prod_total <= 0:
            empty += 1
    if empty >= max(1, min(10, len(sessions))):
        log("Many users appear unfunded. Use the webapp admin console to top up users, then rerun.")

    stop_at = time.time() + RUN_SECONDS
    threads: list[threading.Thread] = []
    for sess in sessions:
        t = threading.Thread(target=trader_loop, args=(sess, stop_at), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    log("Trading run complete.")


if __name__ == "__main__":
    main()
