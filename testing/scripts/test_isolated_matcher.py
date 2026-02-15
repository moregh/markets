#!/usr/bin/env python3
import json
import time
import urllib.request

BASE = "http://127.0.0.1:18080"


def req(method: str, path: str, payload=None, timeout=5):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    r = urllib.request.Request(f"{BASE}{path}", data=data, method=method, headers=headers)
    t0 = time.perf_counter()
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        dt = (time.perf_counter() - t0) * 1000.0
        return body, dt


def main():
    health, dt = req("GET", "/health")
    print("health", health, f"{dt:.2f}ms")

    # Seed 200 sellers with 100 BTC each and asks at 300 EUR.
    seeded = 0
    for uid in range(2, 202):
        req("POST", "/wallets/fund-product", {"user_id": uid, "product_id": 1, "amount_units": 100})
        req(
            "POST",
            "/orders",
            {
                "user_id": uid,
                "market_id": 2,
                "buy": False,
                "quantity": 100,
                "price_cents": 30000,
            },
        )
        seeded += 1
    print("seeded_sellers", seeded)

    # Fund one large buyer and place a single huge order.
    req("POST", "/wallets/fund-currency", {"user_id": 9001, "currency_id": 2, "amount_cents": 500_000_000_00})
    o, dt = req(
        "POST",
        "/orders",
        {
            "user_id": 9001,
            "market_id": 2,
            "buy": True,
            "quantity": 20000,
            "price_cents": 200000,
        },
    )
    oid = o["id"]
    print("big_order_submit", o, f"{dt:.2f}ms")

    # Probe API responsiveness while matching is active.
    samples = []
    for _ in range(40):
        _, qdt = req("GET", "/markets/2/orderbook?limit=100")
        samples.append(qdt)
        time.sleep(0.05)
    samples.sort()
    p50 = samples[len(samples) // 2]
    p95 = samples[int(len(samples) * 0.95) - 1]
    print(f"orderbook_latency_ms p50={p50:.2f} p95={p95:.2f} max={samples[-1]:.2f}")

    fin, _ = req("GET", f"/orders/{oid}")
    dbg, _ = req("GET", "/debug/matcher")
    tr, _ = req("GET", "/markets/2/trades?limit=10")
    print("big_order_final", fin)
    print("debug", dbg)
    print("recent_trades_count", tr.get("count"))


if __name__ == "__main__":
    main()

