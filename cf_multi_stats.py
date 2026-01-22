from __future__ import annotations

import os
import json
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests import Session

# =======================
# PUBLIC CONSTANTS
# =======================

START_DATE = datetime(2025, 11, 1, tzinfo=timezone.utc)
START_TS = int(START_DATE.timestamp())

API_URL = "https://codeforces.com/api/user.status"

# =======================
# PERFORMANCE CONFIG
# =======================

PAGE_SIZE = 1000
MAX_WORKERS = 6              # tune: 4–8 safe; CF rate limit tolerant
CACHE_DIR = "cache"
CACHE_VERSION = 1            # bump if cache format changes

os.makedirs(CACHE_DIR, exist_ok=True)

# =======================
# CACHE UTILITIES
# =======================

def _cache_path(handle: str) -> str:
    safe = handle.replace("/", "_")
    return os.path.join(CACHE_DIR, f"{safe}.json")


def load_handle_cache(handle: str) -> dict:
    path = _cache_path(handle)
    if not os.path.exists(path):
        return {"v": CACHE_VERSION, "last_ts": 0, "solved": {}}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if data.get("v") != CACHE_VERSION:
                return {"v": CACHE_VERSION, "last_ts": 0, "solved": {}}
            return data
    except Exception:
        return {"v": CACHE_VERSION, "last_ts": 0, "solved": {}}


def save_handle_cache(handle: str, data: dict) -> None:
    path = _cache_path(handle)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)

# =======================
# FETCHING (FAST PATH)
# =======================

def fetch_new_submissions(
    handle: str,
    session: Session,
    stop_ts: int
) -> Tuple[List[dict], int]:
    """
    Fetch submissions newer than stop_ts.
    Stops immediately once older submissions are seen.
    """
    out: List[dict] = []
    newest_ts = stop_ts
    start_index = 1

    while True:
        resp = session.get(
            API_URL,
            params={
                "handle": handle,
                "from": start_index,
                "count": PAGE_SIZE
            },
            timeout=20
        )
        resp.raise_for_status()
        payload = resp.json()

        if payload.get("status") != "OK":
            break

        batch = payload.get("result", [])
        if not batch:
            break

        stop = False
        for sub in batch:
            cts = sub.get("creationTimeSeconds", 0)
            if cts <= stop_ts:
                stop = True
                break
            out.append(sub)
            if cts > newest_ts:
                newest_ts = cts

        if stop or len(batch) < PAGE_SIZE:
            break

        start_index += len(batch)

    return out, newest_ts

# =======================
# PER-HANDLE PROCESSING
# =======================

def process_handle(handle: str) -> Tuple[str, Optional[dict]]:
    """
    Fetch + update cache + compute summary for one handle.
    """
    try:
        session = requests.Session()
        cache = load_handle_cache(handle)
        last_ts = cache["last_ts"]
        solved = cache["solved"]  # "contestId:index" -> rating or None

        new_subs, newest_seen = fetch_new_submissions(handle, session, last_ts)

        for sub in new_subs:
            if sub.get("verdict") != "OK":
                continue
            if sub.get("creationTimeSeconds", 0) < START_TS:
                continue

            prob = sub.get("problem", {})
            cid = prob.get("contestId")
            idx = prob.get("index")
            if cid is None or idx is None:
                continue

            key = f"{cid}:{idx}"
            rating = prob.get("rating")

            if key not in solved or (solved[key] is None and rating is not None):
                solved[key] = rating

        if newest_seen > last_ts:
            cache["last_ts"] = newest_seen

        cache["solved"] = solved
        save_handle_cache(handle, cache)

        rated = [r for r in solved.values() if r is not None]
        return handle, {
            "problems": len(solved),
            "rated_problems": len(rated),
            "avg_rating": (sum(rated) / len(rated)) if rated else 0.0,
        }

    except Exception as e:
        print(f"[ERROR] {handle}: {e}", file=sys.stderr)
        return handle, None

# =======================
# PUBLIC API
# =======================

def summarize_handles(
    handles: List[str],
) -> Tuple[
    dict,
    Dict[Tuple[Optional[int], Optional[str]], Optional[int]]
]:
    """
    Parallel, cached, incremental summarization.
    """
    results: dict = {}
    global_solved: Dict[str, Optional[int]] = {}

    workers = min(MAX_WORKERS, max(1, len(handles)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(process_handle, h) for h in handles]

        for fut in as_completed(futures):
            handle, res = fut.result()
            results[handle] = res

            if res is None:
                continue

            cache = load_handle_cache(handle)
            for k, rating in cache["solved"].items():
                if k not in global_solved or (
                    global_solved[k] is None and rating is not None
                ):
                    global_solved[k] = rating

    # convert "cid:index" → (cid, index) for compatibility
    parsed_global = {}
    for k, r in global_solved.items():
        cid, idx = k.split(":")
        parsed_global[(int(cid), idx)] = r

    return results, parsed_global
