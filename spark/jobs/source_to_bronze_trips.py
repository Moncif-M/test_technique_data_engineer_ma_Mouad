"""Bronze: load raw CSVs as-is (all strings) into Delta, partitioned by trip date."""
import argparse
import os
from datetime import date, timedelta

from pyspark.sql import functions as F

from utils.config import BRONZE_PATH, RAW_DIR
from utils.session import get_spark


def raw_files(start: str, end: str) -> list[str]:
    day, last, files = date.fromisoformat(start), date.fromisoformat(end), []
    while day <= last:
        path = f"{RAW_DIR}/trips_{day}.csv"
        if os.path.exists(path):
            files.append(f"file://{path}")
        else:
            print(f"WARNING: {path} not found, skipping")
        day += timedelta(days=1)
    return files


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    args = p.parse_args()

    files = raw_files(args.start, args.end)
    if not files:
        raise SystemExit("No input files found: run scripts/get_data.py first")

    spark = get_spark("bronze_ingest")

    df = (
        spark.read.option("header", True)   
        .csv(files)
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("trip_date", F.substring("trip_start_timestamp", 1, 10))             # Partitioning by trip_date is perfect, it avoids skewing because the number
                                                                                         # of trips each day are close
    )

    (
        df.write.format("delta")
        .mode("overwrite")                                                              
        .option("partitionOverwriteMode", "dynamic")                                      # dynamic: only the days loaded are replaced
        .partitionBy("trip_date")
        .save(BRONZE_PATH)
    )

    count = spark.read.format("delta").load(BRONZE_PATH).count()
    print(f"BRONZE: {len(files)} files loaded, {count} rows in table")


if __name__ == "__main__":
    main()
