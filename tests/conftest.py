"""Shared fixtures: a small local Spark session and a silver-like trip builder."""

from collections.abc import Callable, Iterator
from datetime import date
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

SILVER_TRIP = {
    "trip_id": "t1",
    "trip_date": date(2023, 1, 1),
    "trip_hour": 10,
    "trip_seconds": 600,
    "trip_miles": 2.0,
    "fare": 8.0,
    "tips": 2.0,
    "tolls": 0.0,
    "extras": 0.0,
    "trip_total": 10.0,
    "payment_type": "Credit Card",
    "company": "Flash Cab",
    "pickup_community_area": 8,
    "dropoff_community_area": 32,
    "total_mismatch": False,
}
SILVER_SCHEMA = (
    "trip_id string, trip_date date, trip_hour int, trip_seconds int, trip_miles double, "
    "fare double, tips double, tolls double, extras double, trip_total double, "
    "payment_type string, company string, pickup_community_area int, "
    "dropoff_community_area int, total_mismatch boolean"
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Provide one lightweight local Spark session for the whole test session."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def silver_trips(spark: SparkSession) -> Callable[..., DataFrame]:
    """Return a factory that builds a silver-like frame from field overrides."""

    def build(*overrides: dict[str, Any]) -> DataFrame:
        rows = [tuple({**SILVER_TRIP, **o}[k] for k in SILVER_TRIP) for o in overrides]
        return spark.createDataFrame(rows, SILVER_SCHEMA)

    return build
