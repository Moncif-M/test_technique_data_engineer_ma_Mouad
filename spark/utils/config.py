import os

BUCKET = os.getenv("LAKE_BUCKET", "lake")
RAW_DIR = "/opt/airflow/data/raw"

BRONZE_PATH = f"s3a://{BUCKET}/bronze/taxi_trips"
SILVER_PATH = f"s3a://{BUCKET}/silver/taxi_trips"
QUARANTINE_PATH = f"s3a://{BUCKET}/silver/taxi_trips_rejected"
GOLD_BASE = f"s3a://{BUCKET}/gold"
