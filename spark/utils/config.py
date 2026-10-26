import os

BUCKET = os.getenv("LAKE_BUCKET", "lake")
PREFIX = os.getenv("LAKE_PREFIX", "").strip("/")
RAW_DIR = os.getenv("RAW_DIR", "/opt/airflow/data/raw")

BASE_PATH = f"s3a://{BUCKET}/{PREFIX}" if PREFIX else f"s3a://{BUCKET}"

BRONZE_PATH = f"{BASE_PATH}/bronze/taxi_trips"
SILVER_PATH = f"{BASE_PATH}/silver/taxi_trips"
QUARANTINE_PATH = f"{BASE_PATH}/silver/taxi_trips_rejected"
GOLD_BASE = f"{BASE_PATH}/gold"
