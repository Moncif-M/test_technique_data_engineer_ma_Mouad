"""Integration test: runs the real jobs on a handful of rows against the running object store (Silo)"""
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from utils.session import get_spark

pytestmark = pytest.mark.integration

SPARK_DIR = Path(__file__).resolve().parents[2] / "spark"
BUCKET = os.getenv("LAKE_BUCKET", "lake")
COLUMNS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds", "trip_miles",
    "pickup_census_tract", "dropoff_census_tract", "pickup_community_area", "dropoff_community_area",
    "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company",
    "pickup_centroid_latitude", "pickup_centroid_longitude", "pickup_centroid_location",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude", "dropoff_centroid_location",
]
VALID_ROW = {                                                      # a valid raw trip; tests override single fields
    "trip_id": "t1", "taxi_id": "x1",
    "trip_start_timestamp": "2023-01-01T08:00:00.000", "trip_end_timestamp": "2023-01-01T08:15:00.000",
    "trip_seconds": "900", "trip_miles": "3.5", "pickup_community_area": "8", "dropoff_community_area": "32",
    "fare": "12.5", "tips": "2", "tolls": "0", "extras": "1", "trip_total": "15.5",
    "payment_type": "Credit Card", "company": "Flash Cab",
    "pickup_centroid_latitude": "41.9", "pickup_centroid_longitude": "-87.6",
    "dropoff_centroid_latitude": "41.88", "dropoff_centroid_longitude": "-87.63",
}
DAY1 = [                                                           # Jan 1: 3 valid, 1 zero duration, 1 duplicate of t1
    {"trip_id": "t1"},
    {"trip_id": "t2", "payment_type": "Cash"},
    {"trip_id": "t3", "payment_type": "Mobile"},
    {"trip_id": "t4", "trip_seconds": "0"},
    {"trip_id": "t1"},
]
DAY2 = [                                                           # Jan 2: 3 valid (t8 has no area), 1 negative fare
    {"trip_id": "t5"},
    {"trip_id": "t6"},
    {"trip_id": "t7", "fare": "-5"},
    {"trip_id": "t8", "pickup_community_area": ""},
]


def write_day(raw_dir: Path, day: str, rows: list[dict]) -> None:  # one CSV per day, with the source's header
    lines = [",".join(COLUMNS)]
    for overrides in rows:
        row = {**VALID_ROW, **overrides}
        row["trip_start_timestamp"] = row["trip_start_timestamp"].replace("2023-01-01", day)
        row["trip_end_timestamp"] = row["trip_end_timestamp"].replace("2023-01-01", day)
        lines.append(",".join(row.get(c, "") for c in COLUMNS))
    (raw_dir / f"trips_{day}.csv").write_text("\n".join(lines) + "\n")


def run_pipeline(env: dict) -> None:                               # the same jobs, in the same order as the DAG
    jobs = [
        ("source_to_bronze_trips.py", ["--start", "2023-01-01", "--end", "2023-01-02"]),
        ("bronze_to_silver_trips.py", ["--start", "2023-01-01", "--end", "2023-01-02"]),
        ("gold_star_schema_trips.py", []),
        ("gold_aggregate_trips.py", []),
    ]
    for script, args in jobs:
        subprocess.run([sys.executable, f"jobs/{script}", *args], cwd=SPARK_DIR, env=env, check=True)


@pytest.fixture(scope="module")
def lake(tmp_path_factory):                                        # an isolated prefix in the bucket, removed at the end
    raw_dir = tmp_path_factory.mktemp("raw")
    write_day(raw_dir, "2023-01-01", DAY1)
    write_day(raw_dir, "2023-01-02", DAY2)
    prefix = f"it-tests/{uuid.uuid4().hex[:8]}/"
    env = {**os.environ, "LAKE_PREFIX": prefix, "RAW_DIR": str(raw_dir)}
    spark = get_spark("integration-tests")
    yield {"spark": spark, "env": env, "base": f"s3a://{BUCKET}/{prefix}"}
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/{prefix}")
    hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(hadoop_path, True)


def table(lake: dict, path: str):
    return lake["spark"].read.format("delta").load(lake["base"] + path)


def counts(lake: dict) -> dict:                                    # row counts of every layer, to compare runs
    return {
        "bronze": table(lake, "bronze/taxi_trips").count(),
        "silver": table(lake, "silver/taxi_trips").count(),
        "quarantine": table(lake, "silver/taxi_trips_rejected").count(),
        "fact": table(lake, "gold/star/fact_trips").count(),
        "daily": table(lake, "gold/daily_revenue").count(),
    }


def test_each_layer_has_the_expected_rows(lake):
    run_pipeline(lake["env"])
    assert counts(lake) == {"bronze": 9, "silver": 6, "quarantine": 2, "fact": 6, "daily": 2}


def test_rejected_rows_are_quarantined_with_their_reason(lake):
    reasons = {r.reject_reason for r in table(lake, "silver/taxi_trips_rejected").collect()}
    assert reasons == {"non_positive_duration", "invalid_fare"}


def test_gold_totals_match_silver(lake):
    daily = table(lake, "gold/daily_revenue").agg({"trips": "sum"}).collect()[0][0]
    assert daily == table(lake, "silver/taxi_trips").count()


def test_trips_without_a_pickup_area_map_to_the_unknown_zone(lake):
    keys = {r.pickup_zone_key for r in table(lake, "gold/star/fact_trips").collect()}
    zones = {r.zone_key for r in table(lake, "gold/star/dim_zone").collect()}
    assert -1 in keys
    assert keys <= zones                                           # every fact key exists in the dimension


def test_rerunning_the_pipeline_changes_nothing(lake):
    before = counts(lake)
    run_pipeline(lake["env"])
    assert counts(lake) == before                                  # idempotence: same input, same tables
