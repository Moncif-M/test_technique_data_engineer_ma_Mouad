# Exposure: copy the gold Delta tables into the Postgres warehouse, one schema per kind of table
import os

import psycopg2
from utils.config import GOLD_BASE
from utils.session import get_spark

TABLES = {                                       # schema -> {table: path under GOLD_BASE}
    "kpi": {
        "daily_revenue": "daily_revenue",
        "trip_stats_by_hour": "trip_stats_by_hour",
        "top_pickup_zones": "top_pickup_zones",
        "payment_type_mix": "payment_type_mix",
    },
    "star": {
        "dim_date": "star/dim_date",
        "dim_payment_type": "star/dim_payment_type",
        "dim_zone": "star/dim_zone",
        "fact_trips": "star/fact_trips",
    },
}

HOST = os.getenv("WAREHOUSE_HOST", "warehouse")  # service name from docker-compose
DB = os.getenv("WAREHOUSE_DB", "analytics")
USER = os.getenv("WAREHOUSE_USER", "analytics")
PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "analytics")
URL = f"jdbc:postgresql://{HOST}:5432/{DB}"
PROPS = {"user": USER, "password": PASSWORD, "driver": "org.postgresql.Driver"}


def ensure_schemas() -> None:                    # create the schemas if missing (safe to repeat)
    conn = psycopg2.connect(host=HOST, dbname=DB, user=USER, password=PASSWORD)
    with conn, conn.cursor() as cur:
        for schema in TABLES:
            cur.execute(f"create schema if not exists {schema}")
    conn.close()


def main() -> None:
    ensure_schemas()
    spark = get_spark("gold_to_postgres")
    for schema, tables in TABLES.items():
        for name, path in tables.items():
            df = spark.read.format("delta").load(f"{GOLD_BASE}/{path}")
            (
                df.write.mode("overwrite")
                .option("truncate", "true")      # empty and refill: reruns never duplicate rows
                .option("batchsize", 10000)      # bigger inserts for the 1.4M-row fact table
                .jdbc(URL, f"{schema}.{name}", properties=PROPS)
            )
            print(f"EXPOSE: {schema}.{name} -> {df.count()} rows")


if __name__ == "__main__":
    main()
