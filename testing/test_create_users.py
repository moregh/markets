#!/usr/bin/env python3
"""Create test users and persist credentials to users.csv.

- Registers USER_COUNT users with usernames user_IDX.
- Uses random passwords.
- Writes username/password pairs to users.csv.
"""

from __future__ import annotations

import csv
import json
import secrets
import string
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "http://localhost:8000"
USER_COUNT = 250
START_INDEX = 1
OUTPUT_CSV = Path(__file__).resolve().with_name("users.csv")
REQUEST_TIMEOUT_SECONDS = 10


def random_password(length: int = 18) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def post_json(url: str, payload: dict) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else {})
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        parsed = json.loads(body) if body else {}
        return exc.code, parsed


def register_user(username: str, password: str) -> tuple[bool, str]:
    email = f"{username}@example.com"
    status, body = post_json(
        f"{BASE_URL}/auth/register",
        {"username": username, "email": email, "password": password},
    )
    if status == 200:
        return True, "created"
    detail = body.get("detail", f"HTTP {status}")
    return False, str(detail)


def write_csv(rows: list[tuple[str, str]]) -> None:
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["username", "password"])
        writer.writerows(rows)


def main() -> None:
    created_rows: list[tuple[str, str]] = []
    failures: list[tuple[str, str]] = []

    for idx in range(START_INDEX, START_INDEX + USER_COUNT):
        username = f"user_{idx}"
        password = random_password()
        ok, message = register_user(username, password)
        if ok:
            created_rows.append((username, password))
            print(f"[OK] {username}")
        else:
            failures.append((username, message))
            print(f"[FAIL] {username}: {message}")

    write_csv(created_rows)

    print("\nSummary")
    print(f"  Requested: {USER_COUNT}")
    print(f"  Created:   {len(created_rows)}")
    print(f"  Failed:    {len(failures)}")
    print(f"  Output:    {OUTPUT_CSV.resolve()}")

    if failures:
        print("\nFailed users:")
        for username, reason in failures:
            print(f"  - {username}: {reason}")


if __name__ == "__main__":
    main()
