"""Unit tests for the gold KPI aggregates"""
from datetime import date

from jobs.gold_aggregate_trips import daily_revenue, payment_type_mix, top_pickup_zones, trip_stats_by_hour


def test_daily_revenue_sums_per_day(silver_trips):
    trips = silver_trips(
        {"trip_total": 10.0, "fare": 8.0, "tips": 2.0},
        {"trip_total": 20.0, "fare": 16.0, "tips": 4.0},
        {"trip_date": date(2023, 1, 2), "trip_total": 5.0, "fare": 5.0, "tips": 0.0},
    )
    day1, day2 = daily_revenue(trips).collect()
    assert (day1.trips, day1.revenue, day1.tips) == (2, 30.0, 6.0)
    assert (day2.trips, day2.revenue) == (1, 5.0)


def test_hourly_speed_is_total_miles_over_total_hours(silver_trips):
    trips = silver_trips(
        {"trip_seconds": 600, "trip_miles": 5.0},   # 30 mph
        {"trip_seconds": 3000, "trip_miles": 5.0},  # 6 mph
    )
    row = trip_stats_by_hour(trips).collect()[0]
    assert row.avg_duration_min == 30.0
    assert row.avg_speed_mph == 10.0                # 10 miles in 1 hour, not the 18 mph average of the two speeds


def test_top_zones_are_ranked_by_trips_and_skip_unknown_areas(silver_trips):
    trips = silver_trips(
        {"pickup_community_area": 8}, {"pickup_community_area": 8}, {"pickup_community_area": 8},
        {"pickup_community_area": 32},
        {"pickup_community_area": None},
    )
    rows = top_pickup_zones(trips).collect()
    assert [(r.pickup_community_area, r.trips) for r in rows] == [(8, 3), (32, 1)]


def test_payment_mix_shares(silver_trips):
    trips = silver_trips(
        {"payment_type": "Credit Card"}, {"payment_type": "Credit Card"},
        {"payment_type": "Credit Card"}, {"payment_type": "Cash"},
    )
    shares = {r.payment_type: r.trip_share_pct for r in payment_type_mix(trips).collect()}
    assert shares == {"Credit Card": 75.0, "Cash": 25.0}
