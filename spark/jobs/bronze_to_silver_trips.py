"""Silver: typed, validated, deduplicated trips and a quarantine table for rejects.

Reruns replace only the trip dates being processed (dynamic partition overwrite).
"""

import argparse
from datetime import date

from pyspark.sql import DataFrame, Window, functions as F

from utils.config import BRONZE_PATH, QUARANTINE_PATH, SILVER_PATH
from utils.log import get_logger
from utils.session import get_spark

logger = get_logger(__name__)

MAX_TRIP_SECONDS = 24 * 3600
MAX_TRIP_MILES = 200


def cast_types(df: DataFrame) -> DataFrame:
    """Cast bronze strings to typed columns and keep the original row in ``_raw``.

    Casting returns null on failure, so ``_raw`` keeps the original text for
    rejected records and makes quarantined rows easy to debug.
    """
    return df.select(
        F.struct(*df.columns).alias("_raw"),
        "trip_id",
        "taxi_id",
        F.to_timestamp("trip_start_timestamp").alias("trip_start_ts"),
        F.to_timestamp("trip_end_timestamp").alias("trip_end_ts"),
        F.col("trip_seconds").cast("int").alias("trip_seconds"),
        F.col("trip_miles").cast("double").alias("trip_miles"),
        "pickup_census_tract",
        "dropoff_census_tract",
        F.col("pickup_community_area").cast("int").alias("pickup_community_area"),
        F.col("dropoff_community_area").cast("int").alias("dropoff_community_area"),
        F.col("fare").cast("double").alias("fare"),
        F.col("tips").cast("double").alias("tips"),
        F.col("tolls").cast("double").alias("tolls"),
        F.col("extras").cast("double").alias("extras"),
        F.col("trip_total").cast("double").alias("trip_total"),
        "payment_type",
        "company",
        F.col("pickup_centroid_latitude").cast("double").alias("pickup_lat"),
        F.col("pickup_centroid_longitude").cast("double").alias("pickup_lon"),
        F.col("dropoff_centroid_latitude").cast("double").alias("dropoff_lat"),
        F.col("dropoff_centroid_longitude").cast("double").alias("dropoff_lon"),
        "_source_file",
        "_ingested_at",
    )


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split typed rows into valid trips and rejected trips with a reason.

    The first matching quality rule wins and sets ``reject_reason``; valid rows
    are returned without that helper column, rejected rows keep their raw text.
    """
    reason = (
        F.when(F.col("trip_id").isNull(), "null_trip_id")
        .when(F.col("trip_start_ts").isNull(), "invalid_start_timestamp")
        .when(F.col("trip_end_ts").isNull(), "invalid_end_timestamp")
        .when(F.col("trip_end_ts") < F.col("trip_start_ts"), "end_before_start")
        .when(
            F.col("trip_seconds").isNull() | (F.col("trip_seconds") <= 0),
            "non_positive_duration",
        )
        .when(F.col("trip_seconds") > MAX_TRIP_SECONDS, "duration_outlier")
        .when(
            F.col("trip_miles").isNull() | (F.col("trip_miles") < 0),
            "invalid_distance",
        )
        .when(F.col("trip_miles") > MAX_TRIP_MILES, "distance_outlier")
        .when(F.col("fare").isNull() | (F.col("fare") < 0), "invalid_fare")
        .when(
            F.col("trip_total").isNull() | (F.col("trip_total") < 0),
            "invalid_total",
        )
    )
    flagged = df.withColumn("reject_reason", reason)

    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason", "_raw")
    rejected = (
        flagged.filter(F.col("reject_reason").isNotNull())
        .select("_raw.*", "reject_reason")
        .withColumn("_rejected_at", F.current_timestamp())
    )
    return valid, rejected


def add_derived(df: DataFrame) -> DataFrame:
    """Add trip date, hour, day of week and the total-mismatch flag."""
    parts = (
        F.coalesce("fare", F.lit(0.0))
        + F.coalesce("tips", F.lit(0.0))
        + F.coalesce("tolls", F.lit(0.0))
        + F.coalesce("extras", F.lit(0.0))
    )
    return (
        df.withColumn("trip_date", F.to_date("trip_start_ts"))
        .withColumn("trip_hour", F.hour("trip_start_ts"))
        .withColumn("trip_dow", F.dayofweek("trip_start_ts"))
        .withColumn("total_mismatch", F.abs(parts - F.col("trip_total")) > 0.01)
        .withColumn("_processed_at", F.current_timestamp())
    )


def dedup_latest(df: DataFrame) -> DataFrame:
    """Keep one row per ``trip_id``: the most recently ingested version."""
    window = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def validate_range(start: str | None, end: str | None) -> None:
    """Ensure the optional processing range is complete and correctly ordered.

    Raises:
        ValueError: if only one bound is given, a date is invalid, or start is
            after end.
    """
    if (start is None) != (end is None):
        raise ValueError("--start and --end must be provided together")
    if start and end:
        try:
            first, last = date.fromisoformat(start), date.fromisoformat(end)
        except ValueError as exc:
            raise ValueError(f"invalid date range {start!r}..{end!r}: {exc}") from exc
        if first > last:
            raise ValueError(f"start {start} is after end {end}")


def in_range(df: DataFrame, start: str | None, end: str | None) -> DataFrame:
    """Filter the table to the inclusive processing range, if one is given."""
    if start and end:
        return df.filter(F.col("trip_date").cast("string").between(start, end))
    return df


def main() -> None:
    """Clean the requested bronze range into silver and the quarantine table."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="YYYY-MM-DD (omit both for a full run)")
    parser.add_argument("--end", help="YYYY-MM-DD")
    args = parser.parse_args()
    validate_range(args.start, args.end)

    spark = get_spark("silver_clean")

    bronze = in_range(spark.read.format("delta").load(BRONZE_PATH), args.start, args.end)
    n_in = bronze.count()
    logger.info("BRONZE: %d rows read for the requested range", n_in)

    valid, rejected = split_valid_rejected(cast_types(bronze))
    silver = dedup_latest(add_derived(valid))

    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("trip_date")
        .save(SILVER_PATH)
    )

    (
        rejected.write.format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("trip_date")
        .save(QUARANTINE_PATH)
    )

    n_silver = in_range(spark.read.format("delta").load(SILVER_PATH), args.start, args.end).count()
    quarantine = in_range(spark.read.format("delta").load(QUARANTINE_PATH), args.start, args.end)
    n_rejected = quarantine.count()
    reasons = {row["reject_reason"]: row["count"] for row in quarantine.groupBy("reject_reason").count().collect()}

    logger.info(
        "SILVER: in=%d kept=%d rejected=%d duplicates_removed=%d",
        n_in,
        n_silver,
        n_rejected,
        n_in - n_silver - n_rejected,
    )
    logger.info("SILVER: reject reasons %s", reasons)


if __name__ == "__main__":
    main()
