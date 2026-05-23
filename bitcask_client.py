#!/usr/bin/env python3
"""
bitcask_client.py — Bitcask HTTP client

Usage:
    ./bitcask_client.py --view-all
        Fetches all key-value pairs from the server and writes them to a
        timestamped CSV file (e.g. "1746034451.csv") in the current directory.

    ./bitcask_client.py --view --key=SOME_KEY
        Prints the value of SOME_KEY to stdout.

    ./bitcask_client.py --perf --clients=N
        Spawns N threads, each fetching all keys and writing results to a
        separate timestamped CSV file (e.g. "1746034451_thread_3.csv").

Environment variables:
    BITCASK_HOST   Host of the Bitcask server (default: localhost)
    BITCASK_PORT   Port of the Bitcask server (default: 9092)
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


# ── Configuration ──────────────────────────────────────────────────────────────

HOST = os.environ.get("BITCASK_HOST", "localhost")
PORT = os.environ.get("BITCASK_PORT", "9092")
BASE_URL = f"http://{HOST}:{PORT}"


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _get(path: str) -> bytes:
    """Perform a GET request and return the raw response body."""
    url = BASE_URL.rstrip("/") + path
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise KeyError(f"Key not found: {path.lstrip('/')}")
        raise RuntimeError(f"HTTP {e.code} from {url}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Cannot reach server at {url}: {e.reason}") from e


def fetch_all() -> dict:
    """Fetch all keys and their values as a dict from GET /"""
    raw = _get("/")
    return json.loads(raw)


def fetch_key(key: str) -> str:
    """Fetch a single key's value as a JSON string from GET /{key}"""
    raw = _get(f"/{key}")
    return raw.decode("utf-8")


# ── CSV helpers ────────────────────────────────────────────────────────────────

def write_csv(data: dict, filename: str) -> None:
    """Write {key: value} mapping to a CSV file with columns 'key' and 'value'."""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["key", "value"])
        for key, value in data.items():
            # Serialise value to a compact JSON string for the CSV cell
            writer.writerow([key, json.dumps(value, separators=(",", ":"))])
    print(f"Written: {filename}  ({len(data)} entries)")


# ── Modes ─────────────────────────────────────────────────────────────────────

def mode_view_all() -> None:
    """--view-all: dump all keys to a timestamped CSV file."""
    data = fetch_all()
    timestamp = int(time.time())
    filename = f"{timestamp}.csv"
    write_csv(data, filename)


def mode_view(key: str) -> None:
    """--view --key=X: print the value of a single key to stdout."""
    try:
        value = fetch_key(key)
        print(value)
    except KeyError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def mode_perf(num_clients: int) -> None:
    """--perf --clients=N: N threads each dump all keys to their own CSV."""
    # Use a single shared timestamp so all thread files share the same base name
    timestamp = int(time.time())

    def worker(thread_id: int) -> str:
        data = fetch_all()
        filename = f"{timestamp}_thread_{thread_id}.csv"
        write_csv(data, filename)
        return filename

    print(f"Starting {num_clients} concurrent client threads …")
    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=num_clients) as pool:
        futures = {pool.submit(worker, i + 1): i + 1 for i in range(num_clients)}
        errors = 0
        for future in as_completed(futures):
            tid = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"  Thread {tid} FAILED: {exc}", file=sys.stderr)
                errors += 1

    elapsed = time.monotonic() - start
    print(f"\nCompleted {num_clients} threads in {elapsed:.2f}s  ({errors} errors)")


# ── Argument parsing ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="bitcask_client.py",
        description="Bitcask HTTP client — view, query, and load-test the Bitcask server.",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--view-all",
        action="store_true",
        help="Fetch all key-value pairs and write to a timestamped CSV file.",
    )
    group.add_argument(
        "--view",
        action="store_true",
        help="Print the value of a single key to stdout (requires --key).",
    )
    group.add_argument(
        "--perf",
        action="store_true",
        help="Spawn N client threads each dumping all keys to CSV (requires --clients).",
    )

    parser.add_argument(
        "--key",
        metavar="KEY",
        help="Key to look up (used with --view).",
    )
    parser.add_argument(
        "--clients",
        metavar="N",
        type=int,
        default=1,
        help="Number of concurrent client threads (used with --perf).",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="Bitcask server host (overrides BITCASK_HOST env var, default: localhost).",
    )
    parser.add_argument(
        "--port",
        default=None,
        help="Bitcask server port (overrides BITCASK_PORT env var, default: 9092).",
    )

    args = parser.parse_args()

    # Allow CLI overrides for host/port
    global BASE_URL
    host = args.host or HOST
    port = args.port or PORT
    BASE_URL = f"http://{host}:{port}"

    if args.view_all:
        mode_view_all()

    elif args.view:
        if not args.key:
            parser.error("--view requires --key=SOME_KEY")
        mode_view(args.key)

    elif args.perf:
        if args.clients < 1:
            parser.error("--clients must be >= 1")
        mode_perf(args.clients)


if __name__ == "__main__":
    main()
