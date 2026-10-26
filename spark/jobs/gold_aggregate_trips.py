"""Gold KPI tables: daily revenue, hourly trip stats, top pickup zones and payment mix.

Reads silver and rebuilds each table in full, so re-running the job is idempotent."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.config import GOLD_BASE, SILVER_PATH
from utils.session import get_spark


def daily_revenue(trips: DataFrame) -> DataFrame:                                     # one row per day
    return (
        trips.groupBy("trip_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue"),                         # tips and extras included
            F.round(F.sum("fare"), 2).alias("fare_revenue"),                          # fare only
            F.round(F.sum("tips"), 2).alias("tips"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
        )
        .orderBy("trip_date")
    )


def trip_stats_by_hour(trips: DataFrame) -> DataFrame:                                # one row per hour of day
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


def top_pickup_zones(trips: DataFrame) -> DataFrame:                                  # trips and revenue per pickup area
    return (
        trips.filter(F.col("pickup_community_area").isNotNull())                      # drop trips with no pickup area
        .groupBy("pickup_community_area")
        .agg(F.count("*").alias("trips"), F.round(F.sum("trip_total"), 2).alias("revenue"))
        .withColumn("rank_by_trips", F.rank().over(Window.orderBy(F.desc("trips"))))  # 1 = busiest zone
        .orderBy("rank_by_trips")
    )


def payment_type_mix(trips: DataFrame) -> DataFrame:                                  # share of trips per payment type
    total = trips.count()                                                             # total trips, used for the share
    return (
        trips.groupBy("payment_type")
        .agg(F.count("*").alias("trips"), F.round(F.sum("trip_total"), 2).alias("revenue"))
        .withColumn("trip_share_pct", F.round(F.col("trips") / total * 100, 2))
        .orderBy(F.desc("trips"))
    )


def write(df: DataFrame, name: str) -> None:                                          # overwrite one gold table
    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/{name}")
    print(f"GOLD: {name} -> {df.count()} rows")


def main() -> None:
    spark = get_spark("gold_aggregate")
    trips = spark.read.format("delta").load(SILVER_PATH)                              # read the silver table

    write(daily_revenue(trips), "daily_revenue")
    write(trip_stats_by_hour(trips), "trip_stats_by_hour")
    write(top_pickup_zones(trips), "top_pickup_zones")
    write(payment_type_mix(trips), "payment_type_mix")


if __name__ == "__main__":
    main()
