from __future__ import annotations

import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, List, Optional
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://codeforces.com/api/user.status"
PAGE_SIZE = 1000
MAX_WORKERS = 5

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "cf-stats-vercel/1.0"
})


def fetch_all_submissions(handle: str, start_ts: int, end_ts: int) -> List[dict]:
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

            if ts < start_ts:
                return submissions  # EARLY EXIT (older than range)

            if ts >= end_ts:
                continue  # too new

            submissions.append(sub)

        if len(batch) < PAGE_SIZE:
            break

        start_index += len(batch)

    return submissions


def _process_handle(handle: str, start_ts: int, end_ts: int):
    try:
        subs = fetch_all_submissions(handle, start_ts, end_ts)
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


def summarize_handles(
    handles: List[str],
    start_date: datetime,
    end_date: datetime
):
    # start of start_date
    start_ts = int(start_date.replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp())

    # start of NEXT day (exclusive upper bound)
    end_ts = int((end_date + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp())

    results = {}
    global_solved: Dict[
        Tuple[Optional[int], Optional[str]],
        Dict[str, object]
    ] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(_process_handle, h, start_ts, end_ts)
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
