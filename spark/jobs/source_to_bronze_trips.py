"""Bronze: load raw CSVs as-is (all strings) into Delta, partitioned by trip date."""

import argparse
import os
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F

from utils.config import BRONZE_PATH, RAW_DIR
from utils.log import get_logger
from utils.session import get_spark

logger = get_logger(__name__)


def parse_date_range(start: str, end: str) -> tuple[date, date]:
    """Parse and validate an inclusive ISO date range.

    Raises:
        ValueError: if a date is invalid or ``start`` is after ``end``.
    """
    try:
        first, last = date.fromisoformat(start), date.fromisoformat(end)
    except ValueError as exc:
        raise ValueError(f"invalid date range {start!r}..{end!r}: {exc}") from exc
    if first > last:
        raise ValueError(f"start {start} is after end {end}")
    return first, last


def raw_files(start: str, end: str) -> list[str]:
    """Return local ``file://`` URIs for every daily CSV in the range."""
    day, last = parse_date_range(start, end)
    files = []
    while day <= last:
        path = f"{RAW_DIR}/trips_{day}.csv"
        if os.path.exists(path):
            files.append(f"file://{path}")
        else:
            logger.warning("%s not found, skipping", path)
        day += timedelta(days=1)
    return files


def load_bronze(spark: SparkSession, files: list[str]) -> DataFrame:
    """Read the raw CSVs and add ingestion metadata plus the trip-date partition."""
    return (
        spark.read.option("header", True)
        .csv(files)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("trip_date", F.substring("trip_start_timestamp", 1, 10))
    )


def main() -> None:
    """Load the requested date range from local CSVs into the bronze Delta table."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    args = parser.parse_args()

    files = raw_files(args.start, args.end)
    if not files:
        raise RuntimeError("No input files found: run scripts/get_data.py first")

    spark = get_spark("bronze_ingest")
    df = load_bronze(spark, files)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("trip_date")
        .save(BRONZE_PATH)
    )

    rows = spark.read.format("delta").load(BRONZE_PATH).count()
    logger.info("BRONZE: %d files loaded, %d rows in table", len(files), rows)


if __name__ == "__main__":
    main()
