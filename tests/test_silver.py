# Unit tests for the silver transformations (types, quality rules, dedup, derived columns)
import pytest
from jobs.bronze_to_silver_trips import add_derived, cast_types, dedup_latest, split_valid_rejected

VALID_TRIP = {                                          # one valid raw trip: every column is a string, as in bronze
    "trip_id": "t1", "taxi_id": "x1",
    "trip_start_timestamp": "2023-01-01T10:00:00.000", "trip_end_timestamp": "2023-01-01T10:15:00.000",
    "trip_seconds": "900", "trip_miles": "3.5",
    "pickup_census_tract": None, "dropoff_census_tract": None,
    "pickup_community_area": "8", "dropoff_community_area": "32",
    "fare": "12.5", "tips": "2", "tolls": "0", "extras": "1", "trip_total": "15.5",
    "payment_type": "Credit Card", "company": "Flash Cab",
    "pickup_centroid_latitude": "41.9", "pickup_centroid_longitude": "-87.6",
    "dropoff_centroid_latitude": "41.88", "dropoff_centroid_longitude": "-87.63",
    "_source_file": "trips_2023-01-01.csv", "_ingested_at": "2026-10-25 10:00:00", "trip_date": "2023-01-01",
}


def trips_df(spark, *overrides):                        # build a bronze-like DataFrame; each override changes one trip
    rows = [tuple({**VALID_TRIP, **o}[k] for k in VALID_TRIP) for o in overrides]
    return spark.createDataFrame(rows, ", ".join(f"{k} string" for k in VALID_TRIP))


def test_valid_trip_is_kept(spark):
    valid, rejected = split_valid_rejected(cast_types(trips_df(spark, {})))
    assert valid.count() == 1
    assert rejected.count() == 0


@pytest.mark.parametrize("override, reason", [
    ({"trip_id": None}, "null_trip_id"),
    ({"trip_start_timestamp": "garbage"}, "invalid_start_timestamp"),
    ({"trip_end_timestamp": "garbage"}, "invalid_end_timestamp"),
    ({"trip_end_timestamp": "2023-01-01T09:00:00.000"}, "end_before_start"),
    ({"trip_seconds": "0"}, "non_positive_duration"),
    ({"trip_seconds": "100000"}, "duration_outlier"),
    ({"trip_miles": "-1"}, "invalid_distance"),
    ({"trip_miles": "500"}, "distance_outlier"),
    ({"fare": "-5"}, "invalid_fare"),
    ({"trip_total": "-1"}, "invalid_total"),
])
def test_bad_trip_is_rejected_with_its_reason(spark, override, reason):
    valid, rejected = split_valid_rejected(cast_types(trips_df(spark, override)))
    assert valid.count() == 0
    assert [r.reject_reason for r in rejected.collect()] == [reason]


def test_rejected_row_keeps_the_original_text(spark):
    _, rejected = split_valid_rejected(cast_types(trips_df(spark, {"trip_seconds": "n/a"})))
    assert rejected.collect()[0].trip_seconds == "n/a"  # the quarantine holds the raw value, not a null


def test_dedup_keeps_the_latest_ingestion(spark):
    older = {"_ingested_at": "2026-10-25 10:00:00", "fare": "10"}
    newer = {"_ingested_at": "2026-10-26 10:00:00", "fare": "20"}
    rows = dedup_latest(cast_types(trips_df(spark, older, newer))).collect()
    assert len(rows) == 1
    assert rows[0].fare == 20.0


def test_derived_columns(spark):
    row = add_derived(cast_types(trips_df(spark, {}))).collect()[0]
    assert str(row.trip_date) == "2023-01-01"
    assert row.trip_hour == 10
    assert row.trip_dow == 1                            # 1 January 2023 is a Sunday (1 = Sunday)
    assert row.total_mismatch is False                  # 12.5 + 2 + 0 + 1 equals the 15.5 total
