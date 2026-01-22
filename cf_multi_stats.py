#!/usr/bin/env python3
"""
cf_multi_stats.py
Optimized for serverless (Vercel)
"""

from __future__ import annotations

import requests
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Optional
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

START_DATE = datetime(2025, 11, 1, tzinfo=timezone.utc)
START_TS = int(START_DATE.timestamp())

API_URL = "https://codeforces.com/api/user.status"
PAGE_SIZE = 1000
MAX_WORKERS = 5   # safe for CF + Vercel

# ---------- SHARED SESSION ----------
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "cf-stats-vercel/1.0"
})


def fetch_all_submissions(handle: str) -> List[dict]:
    submissions: List[dict] = []
    start_index = 1

    while True:
        resp = _SESSION.get(
            API_URL,
            params={
                "handle": handle,
                "from": start_index,
                "count": PAGE_SIZE
            },
            timeout=20
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "OK":
            raise RuntimeError(f"API error for handle {handle}")

        batch = data.get("result", [])
        if not batch:
            break

        for sub in batch:
            ts = sub.get("creationTimeSeconds", 0)
            if ts < START_TS:
                return submissions  # EARLY EXIT
            submissions.append(sub)

        if len(batch) < PAGE_SIZE:
            break

        start_index += len(batch)

    return submissions


def _process_handle(handle: str):
    try:
        subs = fetch_all_submissions(handle)
    except Exception as e:
        return handle, None, str(e)

    solved_local: Dict[
        Tuple[Optional[int], Optional[str]],
        Optional[int]
    ] = {}

    global_local: Dict[
        Tuple[Optional[int], Optional[str]],
        Dict[str, object]
    ] = {}

    for sub in subs:
        if sub.get("verdict") != "OK":
            continue

        prob = sub.get("problem", {})
        key = (prob.get("contestId"), prob.get("index"))

        if key in solved_local:
            continue

        rating = prob.get("rating")
        tags = prob.get("tags", [])

        solved_local[key] = rating
        global_local[key] = {
            "rating": rating,
            "tags": tags,
        }

    rated = [r for r in solved_local.values() if r is not None]

    result = {
        "problems": len(solved_local),
        "rated_problems": len(rated),
        "avg_rating": sum(rated) / len(rated) if rated else 0.0,
    }

    return handle, result, global_local


def summarize_handles(handles: List[str]):
    results = {}
    global_solved: Dict[
        Tuple[Optional[int], Optional[str]],
        Dict[str, object]
    ] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(_process_handle, h)
            for h in handles
        ]

        for f in as_completed(futures):
            h, res, data = f.result()

            if res is None:
                print(f"[ERROR] Failed fetching {h}: {data}", file=sys.stderr)
                results[h] = None
                continue

            results[h] = res

            for k, v in data.items():
                global_solved.setdefault(k, v)

    return results, global_solved


def print_report(results: dict,
                 global_solved: Dict[Tuple[Optional[int], Optional[str]], dict],
                 start_date: datetime = START_DATE) -> None:

    handles = list(results.keys())
    print(f"Codeforces statistics from {start_date.date()} (inclusive)\n")

    max_handle_len = max((len(h) for h in handles), default=6)
    header = f"{'handle'.ljust(max_handle_len)} | problems | rated_count | avg_rating"
    print(header)
    print("-" * len(header))

    for h, v in results.items():
        if v is None:
            print(f"{h.ljust(max_handle_len)} | ERROR fetching data")
            continue

        print(
            f"{h.ljust(max_handle_len)} | "
            f"{v['problems']:8d} | "
            f"{v['rated_problems']:11d} | "
            f"{v['avg_rating']:10.2f}"
        )

    global_rated = [
        v["rating"]
        for v in global_solved.values()
        if v["rating"] is not None
    ]

    print("\nAGGREGATED (deduplicated across accounts):")
    print(f"  Total unique problems solved: {len(global_solved)}")
    print(f"  Total unique rated problems: {len(global_rated)}")
    print(
        f"  Average problem rating: "
        f"{(sum(global_rated) / len(global_rated)) if global_rated else 0.0:.2f}"
    )


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Compute Codeforces solved problem statistics from Nov 2025 onward"
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--handles", nargs="+")
    group.add_argument("--file")
    args = p.parse_args()

    if args.handles:
        handles = args.handles
    else:
        with open(args.file, "r", encoding="utf-8") as fh:
            handles = [ln.strip() for ln in fh if ln.strip()]

    results, global_solved = summarize_handles(handles)
    print_report(results, global_solved)
