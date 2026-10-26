"""Exposure: copy the gold Delta tables into the Postgres warehouse."""

import os

import psycopg2

from utils.config import GOLD_BASE
from utils.log import get_logger
from utils.session import get_spark

logger = get_logger(__name__)

TABLES = {
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

HOST = os.getenv("WAREHOUSE_HOST", "warehouse")
DB = os.getenv("WAREHOUSE_DB", "analytics")
USER = os.getenv("WAREHOUSE_USER", "analytics")
PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "analytics")
URL = f"jdbc:postgresql://{HOST}:5432/{DB}"
PROPS = {"user": USER, "password": PASSWORD, "driver": "org.postgresql.Driver"}


def ensure_schemas() -> None:
    """Create the target schemas when missing.

    Raises:
        RuntimeError: if the warehouse cannot be reached or the DDL fails.
    """
    try:
        conn = psycopg2.connect(host=HOST, dbname=DB, user=USER, password=PASSWORD)
    except psycopg2.Error as exc:
        raise RuntimeError(f"could not connect to warehouse {HOST}/{DB}: {exc}") from exc

    try:
        with conn, conn.cursor() as cur:
            for schema in TABLES:
                cur.execute(f"create schema if not exists {schema}")
    except psycopg2.Error as exc:
        raise RuntimeError(f"could not create warehouse schemas: {exc}") from exc
    finally:
        conn.close()


def main() -> None:
    """Copy every gold table into its matching warehouse schema."""
    ensure_schemas()
    spark = get_spark("gold_to_postgres")
    for schema, tables in TABLES.items():
        for name, path in tables.items():
            df = spark.read.format("delta").load(f"{GOLD_BASE}/{path}")
            (
                df.write.mode("overwrite")
                .option("truncate", "true")
                .option("batchsize", 10000)
                .jdbc(URL, f"{schema}.{name}", properties=PROPS)
            )
            logger.info("EXPOSE: %s.%s -> %d rows", schema, name, df.count())


if __name__ == "__main__":
    main()
