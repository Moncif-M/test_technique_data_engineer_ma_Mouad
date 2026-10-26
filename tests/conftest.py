"""Shared fixtures: a small local Spark session (no object store, no Delta) and a silver-like trip builder"""
from datetime import date

import pytest
from pyspark.sql import SparkSession

SILVER_TRIP = {                                                   # one valid silver trip; tests override single fields
    "trip_id": "t1", "trip_date": date(2023, 1, 1), "trip_hour": 10,
    "trip_seconds": 600, "trip_miles": 2.0,
    "fare": 8.0, "tips": 2.0, "tolls": 0.0, "extras": 0.0, "trip_total": 10.0,
    "payment_type": "Credit Card", "company": "Flash Cab",
    "pickup_community_area": 8, "dropoff_community_area": 32, "total_mismatch": False,
}
SILVER_SCHEMA = (
    "trip_id string, trip_date date, trip_hour int, trip_seconds int, trip_miles double, "
    "fare double, tips double, tolls double, extras double, trip_total double, "
    "payment_type string, company string, pickup_community_area int, "
    "dropoff_community_area int, total_mismatch boolean"
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("tests")  # one core is enough for a handful of rows
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")              # small data, so few shuffle partitions
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def silver_trips(spark):
    def build(*overrides):                                        # build a silver-like DataFrame, one row per override
        rows = [tuple({**SILVER_TRIP, **o}[k] for k in SILVER_TRIP) for o in overrides]
        return spark.createDataFrame(rows, SILVER_SCHEMA)

    return build
