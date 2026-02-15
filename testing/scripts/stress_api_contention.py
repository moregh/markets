#!/usr/bin/env python3
import argparse
import json
import random
import statistics
import string
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict


def now_ms() -> int:
    return int(time.time() * 1000)


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.latencies = defaultdict(list)
        self.counts = defaultdict(int)
        self.errors = defaultdict(int)
        self.error_status = defaultdict(lambda: defaultdict(int))

    def ok(self, key: str, latency_ms: float):
        with self.lock:
            self.counts[key] += 1
            self.latencies[key].append(latency_ms)

    def err(self, key: str, status=None):
        with self.lock:
            self.errors[key] += 1
            if status is not None:
                self.error_status[key][str(status)] += 1

    def snapshot(self):
        with self.lock:
            return (
                dict(self.counts),
                dict(self.errors),
                {k: list(v) for k, v in self.latencies.items()},
                {k: dict(v) for k, v in self.error_status.items()},
            )


def http_json(base: str, method: str, path: str, token=None, payload=None, timeout=2.5):
    data = None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(f"{base}{path}", method=method, data=data, headers=headers)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return resp.status, body, (time.perf_counter() - t0) * 1000.0
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {"detail": str(e)}
        return e.code, body, (time.perf_counter() - t0) * 1000.0
    except Exception as e:
        return 0, {"detail": str(e)}, (time.perf_counter() - t0) * 1000.0


def percentile(values, p):
    if not values:
        return 0.0
    arr = sorted(values)
    idx = max(0, min(len(arr) - 1, int(len(arr) * p) - 1))
    return arr[idx]


def random_user(prefix: str):
    suffix = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
    return f"{prefix}_{suffix}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--duration", type=int, default=120)
    ap.add_argument("--users", type=int, default=80)
    ap.add_argument("--order-workers", type=int, default=40)
    ap.add_argument("--read-workers", type=int, default=24)
    ap.add_argument("--burst-workers", type=int, default=8)
    args = ap.parse_args()

    metrics = Metrics()
    stop = threading.Event()

    # Admin auth
    s, body, lat = http_json(
        args.base,
        "POST",
        "/auth/login",
        payload={"username": "admin", "password": "ChangeMe123!"},
        timeout=5,
    )
    if s != 200 or "access_token" not in body:
        raise SystemExit(f"admin login failed: status={s} body={body}")
    metrics.ok("auth_login_admin", lat)
    admin_token = body["access_token"]

    # Discover markets
    s, mkts, lat = http_json(args.base, "GET", "/markets", timeout=5)
    if s != 200:
        raise SystemExit(f"markets failed: status={s} body={mkts}")
    metrics.ok("get_markets", lat)
    markets = mkts
    if not markets:
        raise SystemExit("no markets")

    print(f"[bootstrap] begin users={args.users}", flush=True)
    # Bootstrap users
    sessions = []
    for i in range(args.users):
        uname = random_user("stressu")
        email = f"{uname}@example.com"
        pw = "StressPass123!"
        s, body, lat = http_json(
            args.base,
            "POST",
            "/auth/register",
            payload={"username": uname, "email": email, "password": pw},
            timeout=6,
        )
        if s != 200:
            metrics.err("register", s)
            continue
        metrics.ok("register", lat)
        token = body["access_token"]
        s, me, lat = http_json(args.base, "GET", "/auth/me", token=token, timeout=5)
        if s != 200:
            metrics.err("auth_me", s)
            continue
        metrics.ok("auth_me", lat)
        uid = me["id"]
        # Fund both currency and product heavily to allow large swings.
        for currency in ["EUR", "USD", "JPY", "GBP"]:
            s, _, lat = http_json(
                args.base,
                "POST",
                f"/admin/users/{uid}/wallet/currency",
                token=admin_token,
                payload={"currency": currency, "amount": "20000000.00", "reason": "stress"},
                timeout=5,
            )
            if s == 200:
                metrics.ok("fund_currency", lat)
            else:
                metrics.err("fund_currency", s)
        for product in ["BTC", "ETH", "LTC", "XRP"]:
            s, _, lat = http_json(
                args.base,
                "POST",
                f"/admin/users/{uid}/wallet/product",
                token=admin_token,
                payload={"product": product, "amount_units": 200000, "reason": "stress"},
                timeout=5,
            )
            if s == 200:
                metrics.ok("fund_product", lat)
            else:
                metrics.err("fund_product", s)
        sessions.append({"user_id": uid, "token": token})
        if (i + 1) % 10 == 0:
            print(f"[bootstrap] created={i+1} sessions={len(sessions)}", flush=True)

    if len(sessions) < 10:
        raise SystemExit(f"insufficient sessions bootstrapped: {len(sessions)}")
    print(f"[bootstrap] done sessions={len(sessions)}", flush=True)

    price_centers = {int(m["id"]): 100_000 for m in markets}

    print("[bootstrap] seeding market centers", flush=True)
    # Seed centers from market stats
    for m in markets:
        mid = int(m["id"])
        s, st, lat = http_json(args.base, "GET", f"/markets/{mid}/stats", timeout=4)
        if s == 200:
            metrics.ok("market_stats", lat)
            best_bid = float(st.get("best_bid", 0.0) or 0.0)
            best_ask = float(st.get("best_ask", 0.0) or 0.0)
            center = best_ask if best_ask > 0 else best_bid
            if center <= 0:
                center = 1000.0
            price_centers[mid] = int(center * 100)

    def choose_qty():
        r = random.random()
        if r < 0.50:
            return random.randint(1, 80)
        if r < 0.85:
            return random.randint(100, 1500)
        return random.randint(2000, 20000)

    def choose_price_cents(mid):
        base = max(100, price_centers.get(mid, 100_000))
        swing = random.choice([0.15, 0.4, 0.7, 1.0, 1.4, 2.0, 3.0, 6.0])
        jitter = random.uniform(0.92, 1.08)
        px = int(base * swing * jitter)
        return max(1, px)

    def order_worker():
        while not stop.is_set():
            sess = random.choice(sessions)
            m = random.choice(markets)
            mid = int(m["id"])
            buy = random.random() < 0.5
            qty = choose_qty()
            price_cents = choose_price_cents(mid)
            payload = {
                "market_id": mid,
                "buy": buy,
                "quantity": qty,
                "price": f"{price_cents / 100.0:.2f}",
                "expires_in_seconds": random.choice([3, 5, 10, 30, 60, 120]),
            }
            s, body, lat = http_json(
                args.base, "POST", "/orders/async", token=sess["token"], payload=payload, timeout=3
            )
            if s in (200, 202):
                metrics.ok("orders_async", lat)
            else:
                metrics.err("orders_async", s)
            if s == 202 and random.random() < 0.25:
                rid = body.get("request_id")
                if rid:
                    s2, _, lat2 = http_json(
                        args.base, "GET", f"/orders/requests/{rid}", token=sess["token"], timeout=2
                    )
                    if s2 == 200:
                        metrics.ok("orders_request_status", lat2)
                    else:
                        metrics.err("orders_request_status", s2)
            time.sleep(random.uniform(0.005, 0.03))

    def read_worker():
        while not stop.is_set():
            sess = random.choice(sessions)
            m = random.choice(markets)
            mid = int(m["id"])
            op = random.random()
            if op < 0.45:
                limit = random.choice([80, 150, 300, 600, 1200])
                s, _, lat = http_json(
                    args.base,
                    "GET",
                    f"/markets/{mid}/orderbook?limit={limit}",
                    token=sess["token"],
                    timeout=3,
                )
                key = "get_orderbook"
            elif op < 0.70:
                s, _, lat = http_json(
                    args.base,
                    "GET",
                    f"/markets/{mid}/trades?limit={random.choice([20, 50, 100, 200])}",
                    token=sess["token"],
                    timeout=3,
                )
                key = "get_trades"
            elif op < 0.88:
                s, _, lat = http_json(args.base, "GET", "/orders/me", token=sess["token"], timeout=3)
                key = "get_orders_me"
            else:
                s, _, lat = http_json(args.base, "GET", "/stats", timeout=3)
                key = "get_stats"
            if s == 200:
                metrics.ok(key, lat)
            else:
                metrics.err(key, s)
            time.sleep(random.uniform(0.003, 0.02))

    def burst_worker():
        while not stop.is_set():
            time.sleep(random.uniform(2.0, 5.0))
            for _ in range(random.randint(40, 120)):
                sess = random.choice(sessions)
                m = random.choice(markets)
                mid = int(m["id"])
                payload = {
                    "market_id": mid,
                    "buy": random.random() < 0.5,
                    "quantity": random.randint(5000, 50000),
                    "price": f"{choose_price_cents(mid) / 100.0:.2f}",
                    "expires_in_seconds": random.choice([2, 5, 15]),
                }
                s, _, lat = http_json(
                    args.base, "POST", "/orders/async", token=sess["token"], payload=payload, timeout=4
                )
                if s in (200, 202):
                    metrics.ok("orders_async_burst", lat)
                else:
                    metrics.err("orders_async_burst", s)
                if stop.is_set():
                    break

    print(
        f"[run] start duration={args.duration}s order_workers={args.order_workers} "
        f"read_workers={args.read_workers} burst_workers={args.burst_workers}",
        flush=True,
    )
    threads = []
    for _ in range(args.order_workers):
        t = threading.Thread(target=order_worker, daemon=True)
        t.start()
        threads.append(t)
    for _ in range(args.read_workers):
        t = threading.Thread(target=read_worker, daemon=True)
        t.start()
        threads.append(t)
    for _ in range(args.burst_workers):
        t = threading.Thread(target=burst_worker, daemon=True)
        t.start()
        threads.append(t)

    start = time.time()
    while time.time() - start < args.duration:
        time.sleep(5)
        counts, errors, _, _ = metrics.snapshot()
        total_ok = sum(counts.values())
        total_err = sum(errors.values())
        print(
            f"t+{int(time.time()-start):03d}s ok={total_ok} err={total_err} "
            f"orders_async={counts.get('orders_async',0)} "
            f"orderbook={counts.get('get_orderbook',0)}"
        )

    stop.set()
    for t in threads:
        t.join(timeout=0.3)

    counts, errors, lats, err_status = metrics.snapshot()
    print("\n=== SUMMARY ===")
    print("ok_counts", json.dumps(counts, sort_keys=True))
    print("err_counts", json.dumps(errors, sort_keys=True))
    print("err_status", json.dumps(err_status, sort_keys=True))
    for key, vals in sorted(lats.items()):
        if not vals:
            continue
        p50 = percentile(vals, 0.50)
        p95 = percentile(vals, 0.95)
        p99 = percentile(vals, 0.99)
        mean = statistics.fmean(vals)
        print(
            f"{key}: n={len(vals)} mean_ms={mean:.2f} p50_ms={p50:.2f} p95_ms={p95:.2f} p99_ms={p99:.2f} max_ms={max(vals):.2f}"
        )


if __name__ == "__main__":
    main()
