"""Download Chicago Taxi Trips from the SODA API: one CSV per day, idempotent.

Usage:
    python scripts/download_data.py --start 2023-01-01 --end 2023-01-31
Optional: export SODA_APP_TOKEN=... to avoid throttling.
"""
import argparse
import csv
import io
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests

URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
PAGE_SIZE = 50_000
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

session = requests.Session()


def day_filter(day: date) -> str:
    nxt = day + timedelta(days=1)
    return (
        f"trip_start_timestamp >= '{day}T00:00:00' "
        f"AND trip_start_timestamp < '{nxt}T00:00:00'"
    )


def get(params: dict) -> str:
    """GET with exponential backoff; returns the response body as text."""
    for attempt in range(5):
        try:
            r = session.get(URL, params=params, timeout=120)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f"  retry {attempt + 1}/5 in {wait}s ({exc})", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Request failed after 5 attempts: {params}")


def count_rows(text: str) -> int:
    """Data rows in a CSV page (header excluded), robust to quoted fields."""
    return max(sum(1 for _ in csv.reader(io.StringIO(text))) - 1, 0)


def download_day(day: date) -> int:
    target = OUT_DIR / f"trips_{day}.csv"
    if target.exists():
        print(f"{day}: already downloaded, skipping")
        return 0

    tmp = target.with_suffix(".tmp")
    total, offset = 0, 0
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        while True:
            text = get({
                "$where": day_filter(day),
                "$order": "trip_start_timestamp, trip_id",  # stable order for paging
                "$limit": PAGE_SIZE,
                "$offset": offset,
            })
            rows = count_rows(text)
            if offset == 0:
                f.write(text)                               # keep the header once
            elif "\n" in text:
                f.write(text.split("\n", 1)[1])             # drop repeated header
            total += rows
            if rows < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    tmp.rename(target)                                      # atomic: no half files
    print(f"{day}: {total} rows")
    return total


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    args = p.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    day, end, grand = date.fromisoformat(args.start), date.fromisoformat(args.end), 0
    while day <= end:
        grand += download_day(day)
        day += timedelta(days=1)
    print(f"Done. {grand} new rows.")


if __name__ == "__main__":
    main()
