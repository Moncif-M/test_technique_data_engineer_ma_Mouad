"""Unit tests for the star schema: dimensions are complete and the fact table has no orphan keys"""
from datetime import date

from jobs.gold_star_schema_trips import dim_date, dim_payment_type, dim_zone, fact_trips


def test_dim_date_covers_every_day_between_first_and_last(spark, silver_trips):
    trips = silver_trips({"trip_date": date(2023, 1, 1)}, {"trip_date": date(2023, 1, 3)})
    rows = dim_date(spark, trips).orderBy("date_key").collect()
    assert [r.date_key for r in rows] == [20230101, 20230102, 20230103]
    assert [r.is_weekend for r in rows] == [True, False, False]  # 1 January 2023 is a Sunday


def test_dim_payment_type_names_missing_values_unknown(silver_trips):
    trips = silver_trips({"payment_type": "Cash"}, {"payment_type": None})
    assert {r.payment_type for r in dim_payment_type(trips).collect()} == {"Cash", "Unknown"}


def test_dim_zone_has_pickups_dropoffs_and_the_unknown_row(spark, silver_trips):
    trips = silver_trips(
        {"pickup_community_area": 8, "dropoff_community_area": 32},
        {"pickup_community_area": None, "dropoff_community_area": 5},
    )
    assert {r.zone_key for r in dim_zone(spark, trips).collect()} == {8, 32, 5, -1}


def test_fact_trips_has_no_orphan_keys(spark, silver_trips):
    trips = silver_trips({}, {"pickup_community_area": None}, {"dropoff_community_area": None, "payment_type": None})
    fact, zones, payments = fact_trips(trips), dim_zone(spark, trips), dim_payment_type(trips)
    assert fact.count() == 3                                     # grain: one row per trip
    for key in ("pickup_zone_key", "dropoff_zone_key"):
        assert fact.join(zones, fact[key] == zones["zone_key"], "left_anti").count() == 0
    assert fact.join(payments, "payment_type_key", "left_anti").count() == 0
