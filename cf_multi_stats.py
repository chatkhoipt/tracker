#!/usr/bin/env python3
"""
cf_multi_stats.py

Usage:
    python cf_multi_stats.py --handles user1 user2
    python cf_multi_stats.py --file handles.txt --csv summary.csv

Requirements:
    pip install requests
"""
from __future__ import annotations

import argparse
import requests
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Optional
import csv
import sys

START_DATE = datetime(2025, 11, 1, tzinfo=timezone.utc)
START_TS = int(START_DATE.timestamp())

API_URL = "https://codeforces.com/api/user.status"
PAGE_SIZE = 1000


def fetch_all_submissions(handle: str, page_size: int = PAGE_SIZE) -> List[dict]:
    submissions: List[dict] = []
    start_index = 1

    while True:
        resp = requests.get(
            API_URL,
            params={"handle": handle, "from": start_index, "count": page_size},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "OK":
            raise RuntimeError(f"API error for handle {handle}")

        batch = data.get("result", [])
        if not batch:
            break

        submissions.extend(batch)
        if len(batch) < page_size:
            break

        start_index += len(batch)

    return submissions


def summarize_handles(handles: List[str]) -> Tuple[
    dict,
    Dict[Tuple[Optional[int], Optional[str]], Optional[int]]
]:
    """
    Returns:
      - per-account results
      - global_solved: deduplicated across all accounts
    """
    results = {}
    global_solved: Dict[Tuple[Optional[int], Optional[str]], Optional[int]] = {}

    for h in handles:
        try:
            subs = fetch_all_submissions(h)
        except Exception as e:
            print(f"[ERROR] Failed fetching {h}: {e}", file=sys.stderr)
            results[h] = None
            continue

        solved_local: Dict[Tuple[Optional[int], Optional[str]], Optional[int]] = {}

        for sub in subs:
            if sub.get("verdict") != "OK":
                continue
            if sub.get("creationTimeSeconds", 0) < START_TS:
                continue

            prob = sub.get("problem", {})
            key = (prob.get("contestId"), prob.get("index"))
            rating = prob.get("rating")

            if key not in solved_local:
                solved_local[key] = rating
                global_solved.setdefault(key, rating)

        rated = [r for r in solved_local.values() if r is not None]

        results[h] = {
            "problems": len(solved_local),
            "rated_problems": len(rated),
            "avg_rating": sum(rated) / len(rated) if rated else 0.0,
        }

    return results, global_solved


def print_report(results: dict,
                 global_solved: Dict[Tuple[Optional[int], Optional[str]], Optional[int]],
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

    # ---- GLOBAL (DEDUPLICATED) ----
    global_rated = [r for r in global_solved.values() if r is not None]

    print("\nAGGREGATED (deduplicated across accounts):")
    print(f"  Total unique problems solved: {len(global_solved)}")
    print(f"  Total unique rated problems: {len(global_rated)}")
    print(
        f"  Average problem rating: "
        f"{(sum(global_rated) / len(global_rated)) if global_rated else 0.0:.2f}"
    )


def write_csv(results: dict,
              global_solved: Dict[Tuple[Optional[int], Optional[str]], Optional[int]],
              out_path: str,
              start_date: datetime = START_DATE) -> None:

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["from_date", start_date.isoformat()])
        writer.writerow(["handle", "problems", "rated_problems", "avg_rating"])

        for h, v in results.items():
            if v is None:
                writer.writerow([h, "ERROR", "", ""])
            else:
                writer.writerow([
                    h,
                    v["problems"],
                    v["rated_problems"],
                    f"{v['avg_rating']:.2f}"
                ])

        writer.writerow([])
        writer.writerow(["AGGREGATED"])
        writer.writerow(["unique_problems", len(global_solved)])
        writer.writerow([
            "average_rating",
            f"{(sum(r for r in global_solved.values() if r is not None) / max(1, len([r for r in global_solved.values() if r is not None]))):.2f}"
        ])


def parse_args():
    p = argparse.ArgumentParser(
        description="Compute Codeforces solved problem statistics from Nov 2025 onward"
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--handles", nargs="+", help="Codeforces handles")
    group.add_argument("--file", help="File with one handle per line")
    p.add_argument("--csv", help="Write CSV output (optional)")
    return p.parse_args()


def main():
    args = parse_args()

    if args.handles:
        handles = args.handles
    else:
        try:
            with open(args.file, "r", encoding="utf-8") as fh:
                handles = [ln.strip() for ln in fh if ln.strip()]
        except Exception as e:
            print(f"Failed to read handles file: {e}", file=sys.stderr)
            sys.exit(1)

    print(f"Fetching statistics for {len(handles)} handle(s)...")
    results, global_solved = summarize_handles(handles)

    print()
    print_report(results, global_solved)

    if args.csv:
        try:
            write_csv(results, global_solved, args.csv)
            print(f"\nCSV written to: {args.csv}")
        except Exception as e:
            print(f"Failed to write CSV: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
