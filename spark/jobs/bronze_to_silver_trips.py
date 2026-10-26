"""Silver: typed, validated, deduplicated trips and a quarantine table for rejects
Reruns replace only the trip dates being processed (dynamic partition overwrite)"""
import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from utils.config import BRONZE_PATH, QUARANTINE_PATH, SILVER_PATH
from utils.session import get_spark

MAX_TRIP_SECONDS = 24 * 3600                                                    # longer than a day is not a real trip
MAX_TRIP_MILES = 200                                                            # a taxi trip shouldn't be more than 200 miles normally



def cast_types(df: DataFrame) -> DataFrame:
    return df.select(
        F.struct(*df.columns).alias("_raw"),                                    # keeping a typed string struct with the original value for better debugging when
        "trip_id",                                                              # rejecting a record since casting returns null on failure
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


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:          # adds a reject_reason column and returns two dataframes a valid one when reject_reason
    reason = (                                                                   # is null and a rejected one when there is a reject_reason
        F.when(F.col("trip_id").isNull(), "null_trip_id")
        .when(F.col("trip_start_ts").isNull(), "invalid_start_timestamp")
        .when(F.col("trip_end_ts").isNull(), "invalid_end_timestamp")
        .when(F.col("trip_end_ts") < F.col("trip_start_ts"), "end_before_start")
        .when(F.col("trip_seconds").isNull() | (F.col("trip_seconds") <= 0), "non_positive_duration")
        .when(F.col("trip_seconds") > MAX_TRIP_SECONDS, "duration_outlier")
        .when(F.col("trip_miles").isNull() | (F.col("trip_miles") < 0), "invalid_distance")
        .when(F.col("trip_miles") > MAX_TRIP_MILES, "distance_outlier")
        .when(F.col("fare").isNull() | (F.col("fare") < 0), "invalid_fare")
        .when(F.col("trip_total").isNull() | (F.col("trip_total") < 0), "invalid_total")
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
    parts = (F.coalesce("fare", F.lit(0.0)) + F.coalesce("tips", F.lit(0.0))       # replaces null values on monetary values with 0 and sums
             + F.coalesce("tolls", F.lit(0.0)) + F.coalesce("extras", F.lit(0.0))) 
    return (
        df.withColumn("trip_date", F.to_date("trip_start_ts"))
        .withColumn("trip_hour", F.hour("trip_start_ts"))
        .withColumn("trip_dow", F.dayofweek("trip_start_ts"))        # 1 = Sunday
        .withColumn("total_mismatch", F.abs(parts - F.col("trip_total")) > 0.01)
        .withColumn("_processed_at", F.current_timestamp())
    )


def dedup_latest(df: DataFrame) -> DataFrame:                                       # One row per trip_id; the most recently ingested version
    w = Window.partitionBy("trip_id").orderBy(
        F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")



def in_range(df: DataFrame, start, end) -> DataFrame:
    if start and end:
        return df.filter(F.col("trip_date").cast("string").between(start, end))
    return df


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", help="YYYY-MM-DD (omit both for a full run)")
    p.add_argument("--end", help="YYYY-MM-DD")
    args = p.parse_args()

    spark = get_spark("silver_clean")

    bronze = in_range(spark.read.format("delta").load(BRONZE_PATH), args.start, args.end)
    n_in = bronze.count()

    casted = cast_types(bronze)
    valid, rejected = split_valid_rejected(casted)
    silver = dedup_latest(add_derived(valid))

    (silver.write.format("delta").mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("trip_date").save(SILVER_PATH))

    (rejected.write.format("delta").mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("trip_date").save(QUARANTINE_PATH))


    n_silver = in_range(spark.read.format("delta").load(SILVER_PATH), args.start, args.end).count()
    q = in_range(spark.read.format("delta").load(QUARANTINE_PATH), args.start, args.end)
    n_rejected = q.count()

    print(f"SILVER: in={n_in} kept={n_silver} rejected={n_rejected} "
          f"duplicates_removed={n_in - n_silver - n_rejected}")
    q.groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)


if __name__ == "__main__":
    main()
