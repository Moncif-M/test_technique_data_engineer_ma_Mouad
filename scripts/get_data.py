"""Download Chicago Taxi Trips from the SODA API: one CSV per day, idempotent.

Usage:
    python scripts/get_data.py --start 2023-01-01 --end 2023-01-31
"""

import argparse
import csv
import io
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

import requests

URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
PAGE_SIZE = 50_000
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
RETRIES = 5

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

session = requests.Session()


def day_filter(day: date) -> str:
    """Return the SODA ``$where`` clause covering one calendar day."""
    nxt = day + timedelta(days=1)
    return f"trip_start_timestamp >= '{day}T00:00:00' AND trip_start_timestamp < '{nxt}T00:00:00'"


def get(params: dict[str, object]) -> str:
    """GET with exponential backoff; return the response body as text.

    Raises:
        RuntimeError: if every attempt fails.
    """
    for attempt in range(RETRIES):
        try:
            response = session.get(URL, params=params, timeout=120)
            response.raise_for_status()
            response.encoding = "utf-8"
            return response.text
        except requests.RequestException as exc:
            wait = 2**attempt
            logger.warning("retry %d/%d in %ds (%s)", attempt + 1, RETRIES, wait, exc)
            time.sleep(wait)
    raise RuntimeError(f"Request failed after {RETRIES} attempts: {params}")


def count_rows(text: str) -> int:
    """Count data rows in a CSV page (header excluded), robust to quoted fields."""
    return max(sum(1 for _ in csv.reader(io.StringIO(text))) - 1, 0)


def download_day(day: date) -> int:
    """Download one day into a temporary file and atomically rename it."""
    target = OUT_DIR / f"trips_{day}.csv"
    if target.exists():
        logger.info("%s: already downloaded, skipping", day)
        return 0

    tmp = target.with_suffix(".tmp")
    total, offset = 0, 0
    with open(tmp, "w", encoding="utf-8", newline="") as handle:
        while True:
            text = get(
                {
                    "$where": day_filter(day),
                    "$order": "trip_start_timestamp, trip_id",
                    "$limit": PAGE_SIZE,
                    "$offset": offset,
                }
            )
            rows = count_rows(text)
            if offset == 0:
                handle.write(text)
            elif "\n" in text:
                handle.write(text.split("\n", 1)[1])
            total += rows
            if rows < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    tmp.rename(target)
    logger.info("%s: %d rows", day, total)
    return total


def parse_day(value: str) -> date:
    """Parse an ISO date, raising a clear ``ValueError`` when it is invalid."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid date {value!r}: expected YYYY-MM-DD") from exc


def main() -> None:
    """Download every day in the requested inclusive range.

    Raises:
        ValueError: if a date is invalid or ``--start`` is after ``--end``.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    args = parser.parse_args()

    day, end = parse_day(args.start), parse_day(args.end)
    if day > end:
        raise ValueError(f"start {args.start} is after end {args.end}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    grand = 0
    while day <= end:
        grand += download_day(day)
        day += timedelta(days=1)
    logger.info("Done. %d new rows.", grand)


if __name__ == "__main__":
    main()
