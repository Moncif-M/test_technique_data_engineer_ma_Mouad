"""Gold star schema: fact_trips + dim_date, dim_payment_type, dim_zone (full rebuild)."""

from pyspark.sql import DataFrame, SparkSession, functions as F

from utils.config import GOLD_BASE, SILVER_PATH
from utils.log import get_logger
from utils.session import get_spark

logger = get_logger(__name__)

STAR_BASE = f"{GOLD_BASE}/star"
UNKNOWN_ZONE = -1


def dim_date(spark: SparkSession, trips: DataFrame) -> DataFrame:
    """Return one calendar row per day between the first and last trip.

    Raises:
        ValueError: if the input has no usable trip dates.
    """
    lo, hi = trips.agg(F.min("trip_date"), F.max("trip_date")).first()
    if lo is None or hi is None:
        raise ValueError("cannot build dim_date from an empty silver table")
    dates = spark.sql(f"select explode(sequence(date'{lo}', date'{hi}', interval 1 day)) as date")
    return dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def dim_payment_type(trips: DataFrame) -> DataFrame:
    """Return one row per payment type with a deterministic hash key."""
    return (
        trips.select(F.coalesce("payment_type", F.lit("Unknown")).alias("payment_type"))
        .distinct()
        .withColumn("payment_type_key", F.xxhash64("payment_type"))
        .select("payment_type_key", "payment_type")
    )


def dim_zone(spark: SparkSession, trips: DataFrame) -> DataFrame:
    """Return every pickup/dropoff community area plus the unknown-zone row."""
    known = (
        trips.select(F.col("pickup_community_area").alias("zone_key"))
        .union(trips.select(F.col("dropoff_community_area").alias("zone_key")))
        .filter(F.col("zone_key").isNotNull())
        .distinct()
        .withColumn("is_known", F.lit(True))
    )
    unknown = spark.createDataFrame([(UNKNOWN_ZONE, False)], "zone_key int, is_known boolean")
    return known.unionByName(unknown)


def fact_trips(trips: DataFrame) -> DataFrame:
    """Return one fact row per trip, with pickup and dropoff keys to ``dim_zone``."""
    return trips.select(
        "trip_id",
        F.date_format("trip_date", "yyyyMMdd").cast("int").alias("date_key"),
        "trip_hour",
        F.xxhash64(F.coalesce("payment_type", F.lit("Unknown"))).alias("payment_type_key"),
        F.coalesce("pickup_community_area", F.lit(UNKNOWN_ZONE)).alias("pickup_zone_key"),
        F.coalesce("dropoff_community_area", F.lit(UNKNOWN_ZONE)).alias("dropoff_zone_key"),
        "company",
        "trip_seconds",
        "trip_miles",
        "fare",
        "tips",
        "tolls",
        "extras",
        "trip_total",
        "total_mismatch",
        "trip_date",
    )


def write(df: DataFrame, name: str, partition: str | None = None) -> None:
    """Overwrite one star-schema table and log its row count.

    ``partition`` switches the write to dynamic partition overwrite so only the
    days present in the frame are replaced.
    """
    writer = df.write.format("delta").mode("overwrite")
    if partition:
        writer = writer.option("partitionOverwriteMode", "dynamic").partitionBy(partition)
    writer.save(f"{STAR_BASE}/{name}")
    logger.info("STAR: %s -> %d rows", name, df.count())


def main() -> None:
    """Rebuild the dimensions and the fact table from the silver layer."""
    spark = get_spark("gold_star_schema")
    trips = spark.read.format("delta").load(SILVER_PATH)
    if trips.isEmpty():
        raise ValueError(f"no trips found in silver at {SILVER_PATH}")

    write(dim_date(spark, trips), "dim_date")
    write(dim_payment_type(trips), "dim_payment_type")
    write(dim_zone(spark, trips), "dim_zone")
    write(fact_trips(trips), "fact_trips", partition="trip_date")


if __name__ == "__main__":
    main()
