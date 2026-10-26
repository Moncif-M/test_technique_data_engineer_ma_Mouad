"""Gold KPI tables: daily revenue, hourly trip stats, top pickup zones and payment mix.

Reads silver and rebuilds each table in full, so re-running the job is idempotent.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from utils.config import GOLD_BASE, SILVER_PATH
from utils.log import get_logger
from utils.session import get_spark

logger = get_logger(__name__)


def daily_revenue(trips: DataFrame) -> DataFrame:
    """Return one row per day with trip counts, revenue, fares, tips and averages."""
    return (
        trips.groupBy("trip_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue"),
            F.round(F.sum("fare"), 2).alias("fare_revenue"),
            F.round(F.sum("tips"), 2).alias("tips"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
        )
        .orderBy("trip_date")
    )


def trip_stats_by_hour(trips: DataFrame) -> DataFrame:
    """Return one row per hour of day with trip volume, duration, distance and speed."""
    return (
        trips.groupBy("trip_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("trip_seconds") / 60, 2).alias("avg_duration_min"),
            F.round(F.avg("trip_miles"), 2).alias("avg_miles"),
            F.round(F.sum("trip_miles") / (F.sum("trip_seconds") / 3600), 2).alias("avg_speed_mph"),
        )
        .orderBy("trip_hour")
    )


def top_pickup_zones(trips: DataFrame) -> DataFrame:
    """Return trips and revenue per pickup area, ranked from busiest to quietest."""
    return (
        trips.filter(F.col("pickup_community_area").isNotNull())
        .groupBy("pickup_community_area")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue"),
        )
        .withColumn("rank_by_trips", F.rank().over(Window.orderBy(F.desc("trips"))))
        .orderBy("rank_by_trips")
    )


def payment_type_mix(trips: DataFrame) -> DataFrame:
    """Return trips, revenue and percentage share for each payment type.

    Raises:
        ValueError: if the input has no rows, because the share is undefined.
    """
    total = trips.count()
    if total == 0:
        raise ValueError("cannot compute payment mix for an empty silver table")
    return (
        trips.groupBy("payment_type")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue"),
        )
        .withColumn("trip_share_pct", F.round(F.col("trips") / total * 100, 2))
        .orderBy(F.desc("trips"))
    )


def write(df: DataFrame, name: str) -> None:
    """Overwrite one gold KPI table and log its row count."""
    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/{name}")
    logger.info("GOLD: %s -> %d rows", name, df.count())


def main() -> None:
    """Rebuild every gold KPI table from the silver layer."""
    spark = get_spark("gold_aggregate")
    trips = spark.read.format("delta").load(SILVER_PATH)
    if trips.isEmpty():
        raise ValueError(f"no trips found in silver at {SILVER_PATH}")

    write(daily_revenue(trips), "daily_revenue")
    write(trip_stats_by_hour(trips), "trip_stats_by_hour")
    write(top_pickup_zones(trips), "top_pickup_zones")
    write(payment_type_mix(trips), "payment_type_mix")


if __name__ == "__main__":
    main()
